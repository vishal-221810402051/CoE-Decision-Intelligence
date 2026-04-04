from __future__ import annotations

import json
import re
import time
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Any

from openai import OpenAI

import app.config as app_config
from app.intelligence.actor_resolver import actor_present_in_transcript
from app.intelligence.evidence_engine import build_evidence_binding
from app.models.intelligence import IntelligenceExtractionResult
from app.services.intelligence.contract import get_canonical_intelligence_path

PASS_A_MAX_RETRIES = getattr(
    app_config,
    "PASS_A_MAX_RETRIES",
    getattr(app_config, "INTELLIGENCE_PASS_A_MAX_RETRIES", 2),
)
PASS_B_MAX_RETRIES = getattr(
    app_config,
    "PASS_B_MAX_RETRIES",
    getattr(app_config, "INTELLIGENCE_PASS_B_MAX_RETRIES", 2),
)
API_BACKOFF_SECONDS = getattr(
    app_config,
    "API_BACKOFF_SECONDS",
    getattr(app_config, "INTELLIGENCE_BACKOFF_SECONDS", [2, 5]),
)

PASS_A_KEYS = [
    "decisions",
    "risks",
    "action_plan",
    "deadlines",
    "stakeholders",
    "roadmap",
    "timeline_mentions",
]
FORENSIC_TYPES = {"decision", "suggestion", "risk", "timeline", "question"}
FORENSIC_CLASSIFICATIONS = {"FACT", "SUGGESTION", "UNCERTAIN", "QUESTION"}
DECISION_STATE_ENUM = {"proposed", "discussed", "agreed", "confirmed"}
DECISION_COMMITMENT_MARKERS = (
    "we will",
    "i will",
    "decided",
    "confirmed",
    "agreed",
    "let's",
)
DECISION_HYPOTHETICAL_MARKERS = (
    "maybe",
    "we can",
    "should",
    "might",
    "could",
    "possibly",
)
CERTAINTY_ENUM = {"UNCERTAIN", "CONDITIONAL", "DIRECT"}
CERTAINTY_CONDITIONAL_MARKERS = (
    "if you say yes",
    "not a yes",
    "not final",
    "depends",
    "conditional",
)
CERTAINTY_UNCERTAIN_MARKERS = (
    "maybe",
    "perhaps",
    "can",
    "could",
    "might",
    "explore",
    "likely",
    "potentially",
    "i think",
    "i was thinking",
    "sounds reasonable",
    "may not",
    "can define",
    "need to define",
    "need to check",
)


class ConfigError(RuntimeError):
    pass


class InputArtifactError(RuntimeError):
    pass


class ApiExecutionError(RuntimeError):
    pass


class JsonValidationError(RuntimeError):
    pass


class SchemaValidationError(RuntimeError):
    pass


class EvidenceValidationError(RuntimeError):
    pass


class ConsolidationError(RuntimeError):
    pass


class WriteProtectionError(RuntimeError):
    pass


PASS_A_PROMPT = """
You are a STRICT forensic transcript intelligence extractor.

STRICT INPUT BOUNDARY:
You are ONLY allowed to use the provided RAW TRANSCRIPT chunk.
- Do NOT use prior knowledge
- Do NOT use previous conversations
- Do NOT assume missing information
- If something is not explicitly present -> mark as UNKNOWN

RULES:
1. You MUST ONLY use information present in the raw transcript chunk
2. Every output item MUST include exact supporting quote
3. If no evidence -> DO NOT include it
4. Do NOT infer missing details
5. Preserve uncertainty (do not convert "maybe" into facts)
6. You are NOT allowed to:
   - Add business interpretation
   - Add strategic meaning
   - Add financial assumptions
7. ONLY extract what is explicitly present

Return valid JSON only using this exact shape:
{{
  "items": [
    {{
      "type": "decision | suggestion | risk | timeline | question",
      "content": "...",
      "classification": "FACT | SUGGESTION | UNCERTAIN | QUESTION",
      "evidence": "exact quote from transcript"
    }}
  ]
}}

RAW TRANSCRIPT CHUNK:
=====
{chunk_text}
=====
"""


PASS_B_SUMMARY_PROMPT = """
You are given structured extracted meeting intelligence.
Generate a concise executive summary using ONLY the provided structured items.
Do not add any fact that is not present in the input JSON.
Return JSON only using this shape:
{{"summary": ""}}

Summary constraints:
- 5 to 8 sentences
- concise
- executive-level
- preserve uncertainty terms ("maybe", "depends", "question") when present
- do not convert suggestions/questions into confirmed decisions

Structured input JSON:
=====
{items_json}
=====
"""


class DecisionIntelligenceService:
    @staticmethod
    def extract_intelligence(meeting_id: str) -> IntelligenceExtractionResult:
        start_time = time.time()
        meeting_dir = Path("data") / "processed" / meeting_id
        transcript_path = meeting_dir / "transcript" / "transcript_raw.txt"
        raw_source_path = Path("data") / "raw" / meeting_id / "transcript_raw.txt"
        intelligence_dir = meeting_dir / "intelligence"
        metadata_dir = meeting_dir / "metadata"
        intelligence_path = get_canonical_intelligence_path(meeting_dir)
        metadata_path = metadata_dir / app_config.INTELLIGENCE_METADATA_FILE_NAME

        if not meeting_dir.exists():
            raise InputArtifactError(f"Meeting folder not found: {meeting_dir}")
        if not transcript_path.exists():
            raise InputArtifactError(f"Raw transcript not found: {transcript_path}")
        if not raw_source_path.exists():
            raise InputArtifactError(
                f"Input integrity lock failed: canonical raw source missing for meeting_id '{meeting_id}' at {raw_source_path}"
            )

        api_key = _get_api_key()
        client = OpenAI(api_key=api_key)

        transcript = transcript_path.read_text(encoding="utf-8")
        raw_source_text = raw_source_path.read_text(encoding="utf-8")
        if transcript.strip() == "":
            raise InputArtifactError(f"Raw transcript is empty: {transcript_path}")
        if raw_source_text.strip() == "":
            raise InputArtifactError(f"Raw source transcript is empty: {raw_source_path}")
        if transcript.strip() != raw_source_text.strip():
            raise InputArtifactError(
                "Input integrity lock failed: processed raw transcript does not match canonical source transcript"
            )
        transcript_hash = hashlib.sha256(transcript.encode("utf-8")).hexdigest()

        cached = _try_load_cached_result(
            meeting_id=meeting_id,
            transcript_hash=transcript_hash,
            intelligence_path=intelligence_path,
            metadata_path=metadata_path,
        )
        if cached is not None:
            return cached

        chunks = _chunk_paragraph_aware(
            transcript,
            app_config.INTELLIGENCE_CHUNK_SIZE,
            app_config.INTELLIGENCE_CHUNK_OVERLAP,
        )

        pass_a_retries_used = 0
        raw_items: dict[str, list[dict[str, Any]]] = {k: [] for k in PASS_A_KEYS}
        chunk_start_hints = _compute_chunk_start_hints(
            transcript, chunks, app_config.INTELLIGENCE_CHUNK_OVERLAP
        )

        for index, chunk in enumerate(chunks):
            extracted, used = _run_pass_a_with_retry(client, chunk)
            pass_a_retries_used += used
            chunk_start_hint = chunk_start_hints[index]
            for key in PASS_A_KEYS:
                for item in extracted.get(key, []):
                    if isinstance(item, dict):
                        item["_chunk_start_hint"] = chunk_start_hint
                        raw_items[key].append(item)

        consolidated, items_rejected_due_to_evidence = _consolidate_items(raw_items, transcript)
        consolidated = _apply_truth_annotations(consolidated, transcript)

        summary, pass_b_retries_used = _run_pass_b_with_retry(client, consolidated)
        if summary.strip() == "":
            raise SchemaValidationError("Summary is empty")

        meeting_context = _load_meeting_context(meeting_dir, meeting_id)

        intelligence_payload = {
            "meeting_context": meeting_context,
            "summary": summary.strip(),
            "decisions": consolidated["decisions"],
            "risks": consolidated["risks"],
            "action_plan": consolidated["action_plan"],
            "roadmap": consolidated["roadmap"],
            "deadlines": consolidated["deadlines"],
            "stakeholders": consolidated["stakeholders"],
            "timeline_mentions": consolidated["timeline_mentions"],
        }

        _validate_intelligence_payload(intelligence_payload, transcript)

        processing_time_seconds = round(time.time() - start_time, 3)
        metadata_payload = {
            "meeting_id": meeting_id,
            "model": app_config.INTELLIGENCE_MODEL,
            "prompt_version": app_config.INTELLIGENCE_PROMPT_VERSION,
            "transcript_hash": transcript_hash,
            "input_char_length": len(transcript),
            "chunked": len(chunks) > 1,
            "chunk_count": len(chunks),
            "pass_a_retries_used": pass_a_retries_used,
            "pass_b_retries_used": pass_b_retries_used,
            "items_rejected_due_to_evidence": items_rejected_due_to_evidence,
            "processing_time_seconds": processing_time_seconds,
            "validation_passed": True,
            "status": "intelligence_completed",
        }

        try:
            _safe_write_json(intelligence_dir, intelligence_path, intelligence_payload)
            _safe_write_json(metadata_dir, metadata_path, metadata_payload)
        except Exception as exc:
            if intelligence_path.exists():
                intelligence_path.unlink()
            if metadata_path.exists():
                metadata_path.unlink()
            raise WriteProtectionError(
                f"Fail-closed write triggered for intelligence artifacts: {exc}"
            ) from exc

        return IntelligenceExtractionResult(
            meeting_id=meeting_id,
            intelligence_path=intelligence_path,
            metadata_path=metadata_path,
            model=app_config.INTELLIGENCE_MODEL,
            prompt_version=app_config.INTELLIGENCE_PROMPT_VERSION,
            chunked=len(chunks) > 1,
            chunk_count=len(chunks),
            pass_a_retries_used=pass_a_retries_used,
            pass_b_retries_used=pass_b_retries_used,
            processing_time_seconds=processing_time_seconds,
            validation_passed=True,
            status="intelligence_completed",
        )


def _try_load_cached_result(
    meeting_id: str,
    transcript_hash: str,
    intelligence_path: Path,
    metadata_path: Path,
) -> IntelligenceExtractionResult | None:
    if not intelligence_path.exists() or not metadata_path.exists():
        return None
    try:
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    except Exception:
        return None

    if metadata.get("status") != "intelligence_completed":
        return None
    if metadata.get("validation_passed") is not True:
        return None
    if metadata.get("meeting_id") != meeting_id:
        return None
    if metadata.get("model") != app_config.INTELLIGENCE_MODEL:
        return None
    if metadata.get("prompt_version") != app_config.INTELLIGENCE_PROMPT_VERSION:
        return None
    if metadata.get("transcript_hash") != transcript_hash:
        return None
    if "input_char_length" not in metadata:
        return None
    if "items_rejected_due_to_evidence" not in metadata:
        return None

    return IntelligenceExtractionResult(
        meeting_id=meeting_id,
        intelligence_path=intelligence_path,
        metadata_path=metadata_path,
        model=str(metadata.get("model", app_config.INTELLIGENCE_MODEL)),
        prompt_version=str(
            metadata.get("prompt_version", app_config.INTELLIGENCE_PROMPT_VERSION)
        ),
        chunked=bool(metadata.get("chunked", False)),
        chunk_count=int(metadata.get("chunk_count", 0)),
        pass_a_retries_used=int(metadata.get("pass_a_retries_used", 0)),
        pass_b_retries_used=int(metadata.get("pass_b_retries_used", 0)),
        processing_time_seconds=float(metadata.get("processing_time_seconds", 0.0)),
        validation_passed=bool(metadata.get("validation_passed", False)),
        status=str(metadata.get("status", "intelligence_completed")),
    )


def _get_api_key() -> str:
    if not app_config.config.OPENAI_API_KEY:
        raise ConfigError("OPENAI_API_KEY is missing in environment")
    return app_config.config.OPENAI_API_KEY


def _chunk_paragraph_aware(text: str, chunk_size: int, overlap: int) -> list[str]:
    paragraphs = text.split("\n\n")
    base_chunks: list[str] = []
    current = ""

    for para in paragraphs:
        candidate = para if not current else current + "\n\n" + para
        if len(candidate) <= chunk_size:
            current = candidate
            continue

        if current:
            base_chunks.append(current)
            current = ""

        if len(para) <= chunk_size:
            current = para
            continue

        sentences = re.split(r"(?<=[.!?])\s+", para)
        sentence_chunk = ""
        for sentence in sentences:
            trial = sentence if not sentence_chunk else sentence_chunk + " " + sentence
            if len(trial) <= chunk_size:
                sentence_chunk = trial
            else:
                if sentence_chunk:
                    base_chunks.append(sentence_chunk)
                sentence_chunk = sentence
        if sentence_chunk:
            current = sentence_chunk

    if current:
        base_chunks.append(current)

    if not base_chunks:
        return [text]

    chunks: list[str] = []
    for index, chunk in enumerate(base_chunks):
        if index == 0 or overlap <= 0:
            chunks.append(chunk)
            continue
        prefix = base_chunks[index - 1][-overlap:]
        chunks.append(prefix + "\n" + chunk)
    return chunks


def _compute_chunk_start_hints(
    transcript: str, chunks: list[str], overlap: int
) -> list[int]:
    hints: list[int] = []
    search_start = 0

    for idx, chunk in enumerate(chunks):
        position = transcript.find(chunk, search_start)
        if position == -1 and idx > 0 and overlap > 0:
            split_parts = chunk.split("\n", 1)
            body = split_parts[1] if len(split_parts) == 2 else chunk
            position = transcript.find(body, search_start)
        if position == -1:
            position = max(0, search_start)

        hints.append(position)
        search_start = max(search_start, position + 1)

    return hints


def _run_pass_a_with_retry(
    client: OpenAI, chunk: str
) -> tuple[dict[str, list[dict[str, Any]]], int]:
    retries_used = 0

    for attempt in range(PASS_A_MAX_RETRIES + 1):
        try:
            response = client.chat.completions.create(
                model=app_config.INTELLIGENCE_MODEL,
                temperature=0,
                seed=42,
                response_format={"type": "json_object"},
                messages=[
                    {
                        "role": "user",
                        "content": PASS_A_PROMPT.format(chunk_text=chunk),
                    }
                ],
            )
            content = response.choices[0].message.content or ""
            parsed = json.loads(content)
            extracted = _map_forensic_items_to_pass_a(parsed)
            for key in PASS_A_KEYS:
                if not isinstance(extracted[key], list):
                    raise JsonValidationError(f"Pass A key '{key}' must be an array")
            return extracted, retries_used
        except (json.JSONDecodeError, JsonValidationError) as exc:
            if attempt == PASS_A_MAX_RETRIES:
                raise JsonValidationError(f"Pass A JSON parse failed: {exc}") from exc
        except Exception as exc:
            if attempt == PASS_A_MAX_RETRIES:
                raise ApiExecutionError(f"Pass A API execution failed: {exc}") from exc

        retries_used += 1
        _sleep_retry(retries_used)

    raise ApiExecutionError("Pass A retries exhausted")


def _map_forensic_items_to_pass_a(parsed: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    if not isinstance(parsed, dict):
        raise JsonValidationError("Pass A output must be a JSON object")
    items = parsed.get("items", [])
    if not isinstance(items, list):
        raise JsonValidationError("Pass A output must include items[]")

    extracted: dict[str, list[dict[str, Any]]] = {key: [] for key in PASS_A_KEYS}

    for item in items:
        if not isinstance(item, dict):
            continue
        item_type = str(item.get("type", "")).strip().lower()
        content = str(item.get("content", "")).strip()
        classification = str(item.get("classification", "")).strip().upper()
        evidence = str(item.get("evidence", "")).strip()

        if item_type not in FORENSIC_TYPES:
            continue
        if classification not in FORENSIC_CLASSIFICATIONS:
            continue
        if not content or not evidence:
            continue

        decision_state = _derive_decision_state(content, evidence)
        is_valid_decision = _is_valid_decision_statement(content, evidence)
        certainty_class = _infer_certainty_from_span(f"{content} {evidence}")
        if classification in {"UNCERTAIN", "QUESTION"}:
            certainty_class = "UNCERTAIN"
        elif classification == "SUGGESTION" and certainty_class == "DIRECT":
            certainty_class = "CONDITIONAL"

        if item_type == "decision":
            if not is_valid_decision:
                # Downgrade non-committed or hypothetical "decisions" to suggestions.
                extracted["action_plan"].append(
                    {
                        "task": content,
                        "owner": "",
                        "priority": "medium",
                        "status": "open",
                        "classification": "SUGGESTION" if classification != "QUESTION" else "QUESTION",
                        "source_type": "suggestion",
                        "certainty_class": certainty_class,
                        "evidence": evidence,
                    }
                )
                continue
            confidence = "high" if classification == "FACT" else "medium" if classification == "SUGGESTION" else "low"
            extracted["decisions"].append(
                {
                    "text": content,
                    "confidence": confidence,
                    "state": decision_state,
                    "classification": classification,
                    "source_type": item_type,
                    "certainty_class": certainty_class,
                    "evidence": evidence,
                }
            )
        elif item_type == "risk":
            severity = "high" if classification == "FACT" else "medium"
            extracted["risks"].append(
                {
                    "text": content,
                    "severity": severity,
                    "classification": classification,
                    "source_type": item_type,
                    "certainty_class": certainty_class,
                    "evidence": evidence,
                }
            )
        elif item_type == "suggestion":
            priority = "medium" if classification in {"SUGGESTION", "UNCERTAIN"} else "high"
            extracted["action_plan"].append(
                {
                    "task": content,
                    "owner": "",
                    "priority": priority,
                    "status": "open",
                    "classification": classification,
                    "source_type": item_type,
                    "certainty_class": certainty_class,
                    "evidence": evidence,
                }
            )
        elif item_type == "timeline":
            extracted["timeline_mentions"].append(
                {
                    "text": content,
                    "raw_time_reference": content,
                    "classification": classification,
                    "source_type": item_type,
                    "certainty_class": certainty_class,
                    "evidence": evidence,
                }
            )
            if re.search(r"\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec|year|month|week|monday|tuesday|wednesday|thursday|friday|saturday|sunday)\b", content, flags=re.IGNORECASE):
                extracted["deadlines"].append(
                    {
                        "event": content,
                        "date": content,
                        "classification": classification,
                        "source_type": item_type,
                        "certainty_class": certainty_class,
                        "evidence": evidence,
                    }
                )
        elif item_type == "question":
            extracted["action_plan"].append(
                {
                    "task": f"Clarify: {content}",
                    "owner": "",
                    "priority": "high",
                    "status": "open",
                    "classification": classification,
                    "source_type": item_type,
                    "certainty_class": certainty_class,
                    "evidence": evidence,
                }
            )

    return extracted


def _is_valid_decision_statement(content: str, evidence: str) -> bool:
    text = f"{content} {evidence}".lower()
    has_commitment = any(marker in text for marker in DECISION_COMMITMENT_MARKERS)
    has_hypothetical = any(marker in text for marker in DECISION_HYPOTHETICAL_MARKERS)
    return has_commitment and not has_hypothetical


def _derive_decision_state(content: str, evidence: str) -> str:
    text = f"{content} {evidence}".lower()
    if any(token in text for token in ("confirmed", "decided", "finalized")):
        return "confirmed"
    if any(token in text for token in ("agreed", "yes exactly", "yes, exactly")):
        return "agreed"
    if any(token in text for token in DECISION_COMMITMENT_MARKERS):
        return "discussed"
    return "proposed"


def _infer_certainty_from_span(span: str) -> str:
    lowered = str(span or "").lower()
    if not lowered.strip():
        return "UNCERTAIN"
    if any(token in lowered for token in CERTAINTY_CONDITIONAL_MARKERS):
        return "CONDITIONAL"
    if any(token in lowered for token in CERTAINTY_UNCERTAIN_MARKERS):
        return "UNCERTAIN"
    if "yes" in lowered and any(token in lowered for token in ("not", "depends", "maybe")):
        return "CONDITIONAL"
    return "DIRECT"


def _coerce_certainty(value: Any, fallback_span: str = "") -> str:
    if isinstance(value, str):
        normalized = value.strip().upper()
        if normalized in CERTAINTY_ENUM:
            return normalized
    return _infer_certainty_from_span(fallback_span)


def _min_certainty(left: str, right: str) -> str:
    rank = {"UNCERTAIN": 0, "CONDITIONAL": 1, "DIRECT": 2}
    l = left if left in rank else "UNCERTAIN"
    r = right if right in rank else "UNCERTAIN"
    return l if rank[l] <= rank[r] else r


def _run_pass_b_with_retry(
    client: OpenAI, consolidated: dict[str, list[dict[str, Any]]]
) -> tuple[str, int]:
    retries_used = 0
    pass_b_input = {
        "decisions": consolidated["decisions"],
        "risks": consolidated["risks"],
        "action_plan": consolidated["action_plan"],
        "roadmap": consolidated["roadmap"],
        "deadlines": consolidated["deadlines"],
        "stakeholders": consolidated["stakeholders"],
        "timeline_mentions": consolidated["timeline_mentions"],
    }
    items_json = json.dumps(pass_b_input, ensure_ascii=False)

    for attempt in range(PASS_B_MAX_RETRIES + 1):
        try:
            response = client.chat.completions.create(
                model=app_config.INTELLIGENCE_MODEL,
                temperature=0,
                seed=42,
                response_format={"type": "json_object"},
                messages=[
                    {
                        "role": "user",
                        "content": PASS_B_SUMMARY_PROMPT.format(items_json=items_json),
                    }
                ],
            )
            content = response.choices[0].message.content or ""
            parsed = json.loads(content)
            summary = str(parsed.get("summary", "")).strip()
            if not summary:
                raise ConsolidationError("Pass B returned empty summary")
            return summary, retries_used
        except Exception as exc:
            if attempt == PASS_B_MAX_RETRIES:
                raise ConsolidationError(f"Pass B failed: {exc}") from exc
            retries_used += 1
            _sleep_retry(retries_used)

    raise ConsolidationError("Pass B retries exhausted")


def _sleep_retry(retry_number: int) -> None:
    if retry_number <= 0:
        return
    index = min(retry_number - 1, len(API_BACKOFF_SECONDS) - 1)
    seconds = API_BACKOFF_SECONDS[index]
    time.sleep(seconds)


def normalize_key(text: str) -> str:
    text = text.lower().strip()
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"[^\w\s]", "", text)
    return text


def count_non_empty_fields(item: dict[str, Any]) -> int:
    weights = {
        "owner": 3,
        "date": 3,
        "priority": 2,
        "role": 2,
        "text": 1,
        "task": 1,
        "step": 1,
        "event": 1,
        "name": 1,
        "raw_time_reference": 1,
        "time_horizon": 1,
        "confidence": 1,
        "severity": 1,
        "status": 1,
    }
    score = 0
    for key, value in item.items():
        if key in {"evidence", "_evidence_position", "_chunk_start_hint"}:
            continue
        if isinstance(value, str) and value.strip():
            score += weights.get(key, 1)
        elif isinstance(value, int):
            score += weights.get(key, 1)
    return score


def find_evidence_position(evidence: str, transcript: str, start: int = 0) -> int:
    safe_start = max(0, int(start))
    pos = transcript.find(evidence, safe_start)
    if pos != -1:
        return pos
    return transcript.find(evidence)


def validate_evidence_exists(item: dict, transcript_text: str) -> int:
    evidence = str(item.get("evidence", "")).strip()
    if not evidence:
        raise EvidenceValidationError("Empty evidence")
    start_hint = item.get("_chunk_start_hint", 0)
    if not isinstance(start_hint, int):
        start_hint = 0
    position = find_evidence_position(evidence, transcript_text, start_hint)
    if position == -1:
        raise EvidenceValidationError(f"Non-verbatim evidence: {evidence!r}")
    return position


def _coerce_enum(value: Any, allowed: set[str], default: str) -> str:
    if not isinstance(value, str):
        return default
    normalized = value.strip().lower().replace(" ", "_")
    if normalized in allowed:
        return normalized
    return default


def _to_str(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    return ""


def _extract_explicit_role_or_unknown(name: str, role: str, evidence: str) -> str:
    if not name or not evidence:
        return "unknown"
    pattern = rf"\b{re.escape(name)}\b.*\bresponsible\s+for\b"
    if re.search(pattern, evidence, flags=re.IGNORECASE):
        return role or "unknown"
    return "unknown"


def deduplicate_items(
    items: list[dict[str, Any]], transcript_text: str, primary_field: str
) -> tuple[list[dict[str, Any]], int]:
    keyed: dict[str, dict[str, Any]] = {}
    items_rejected_due_to_evidence = 0

    for item in items:
        if primary_field not in item:
            continue
        key = normalize_key(str(item.get(primary_field, "")))
        if not key:
            continue

        try:
            position = validate_evidence_exists(item, transcript_text)
        except EvidenceValidationError:
            items_rejected_due_to_evidence += 1
            continue
        candidate = dict(item)
        candidate["_evidence_position"] = position

        existing = keyed.get(key)
        if existing is None:
            keyed[key] = candidate
            continue

        existing_pos = int(existing["_evidence_position"])
        if position < existing_pos:
            keyed[key] = candidate
            continue

        if position == existing_pos:
            candidate_score = count_non_empty_fields(candidate)
            existing_score = count_non_empty_fields(existing)
            if candidate_score > existing_score:
                keyed[key] = candidate

    ordered = sorted(keyed.values(), key=lambda x: int(x["_evidence_position"]))
    cleaned: list[dict[str, Any]] = []
    for item in ordered:
        final_item = dict(item)
        final_item.pop("_evidence_position", None)
        final_item.pop("_chunk_start_hint", None)
        cleaned.append(final_item)
    return cleaned, items_rejected_due_to_evidence


def _consolidate_items(
    raw_items: dict[str, list[dict[str, Any]]], transcript: str
) -> tuple[dict[str, list[dict[str, Any]]], int]:
    rejected_total = 0

    decisions: list[dict[str, Any]] = []
    for item in raw_items["decisions"]:
        cleaned = {
            "text": _to_str(item.get("text", "")),
            "confidence": _coerce_enum(
                item.get("confidence"), {"high", "medium", "low"}, "low"
            ),
            "state": _coerce_enum(item.get("state"), DECISION_STATE_ENUM, "discussed"),
            "classification": _to_str(item.get("classification", "")),
            "source_type": _to_str(item.get("source_type", "")),
            "certainty_class": _coerce_certainty(item.get("certainty_class"), _to_str(item.get("evidence", ""))),
            "evidence": _to_str(item.get("evidence", "")),
        }
        if isinstance(item.get("_chunk_start_hint"), int):
            cleaned["_chunk_start_hint"] = item["_chunk_start_hint"]
        if cleaned["text"] and cleaned["evidence"]:
            decisions.append(cleaned)
    decisions, rejected = deduplicate_items(decisions, transcript, "text")
    rejected_total += rejected

    risks: list[dict[str, Any]] = []
    for item in raw_items["risks"]:
        cleaned = {
            "text": _to_str(item.get("text", "")),
            "severity": _coerce_enum(
                item.get("severity"), {"high", "medium", "low"}, "low"
            ),
            "classification": _to_str(item.get("classification", "")),
            "source_type": _to_str(item.get("source_type", "")),
            "certainty_class": _coerce_certainty(item.get("certainty_class"), _to_str(item.get("evidence", ""))),
            "evidence": _to_str(item.get("evidence", "")),
        }
        if isinstance(item.get("_chunk_start_hint"), int):
            cleaned["_chunk_start_hint"] = item["_chunk_start_hint"]
        if cleaned["text"] and cleaned["evidence"]:
            risks.append(cleaned)
    risks, rejected = deduplicate_items(risks, transcript, "text")
    rejected_total += rejected

    action_plan: list[dict[str, Any]] = []
    for item in raw_items["action_plan"]:
        cleaned = {
            "task": _to_str(item.get("task", "")),
            "owner": _to_str(item.get("owner", "")),
            "priority": _coerce_enum(
                item.get("priority"), {"high", "medium", "low"}, "medium"
            ),
            "status": "open",
            "classification": _to_str(item.get("classification", "")),
            "source_type": _to_str(item.get("source_type", "")),
            "certainty_class": _coerce_certainty(item.get("certainty_class"), _to_str(item.get("evidence", ""))),
            "evidence": _to_str(item.get("evidence", "")),
        }
        if isinstance(item.get("_chunk_start_hint"), int):
            cleaned["_chunk_start_hint"] = item["_chunk_start_hint"]
        if cleaned["task"] and cleaned["evidence"]:
            action_plan.append(cleaned)
    action_plan, rejected = deduplicate_items(action_plan, transcript, "task")
    rejected_total += rejected

    deadlines: list[dict[str, Any]] = []
    for item in raw_items["deadlines"]:
        cleaned = {
            "event": _to_str(item.get("event", "")),
            "date": _to_str(item.get("date", "")),
            "classification": _to_str(item.get("classification", "")),
            "source_type": _to_str(item.get("source_type", "")),
            "certainty_class": _coerce_certainty(item.get("certainty_class"), _to_str(item.get("evidence", ""))),
            "evidence": _to_str(item.get("evidence", "")),
        }
        if isinstance(item.get("_chunk_start_hint"), int):
            cleaned["_chunk_start_hint"] = item["_chunk_start_hint"]
        if cleaned["event"] and cleaned["evidence"]:
            deadlines.append(cleaned)
    deadlines, rejected = deduplicate_items(deadlines, transcript, "event")
    rejected_total += rejected

    stakeholders: list[dict[str, Any]] = []
    for item in raw_items["stakeholders"]:
        evidence_text = _to_str(item.get("evidence", ""))
        name_text = _to_str(item.get("name", ""))
        role_text = _to_str(item.get("role", ""))
        cleaned = {
            "name": name_text,
            "role": _extract_explicit_role_or_unknown(name_text, role_text, evidence_text),
            "classification": _to_str(item.get("classification", "")),
            "source_type": _to_str(item.get("source_type", "")),
            "certainty_class": _coerce_certainty(item.get("certainty_class"), evidence_text),
            "evidence": evidence_text,
        }
        if isinstance(item.get("_chunk_start_hint"), int):
            cleaned["_chunk_start_hint"] = item["_chunk_start_hint"]
        if cleaned["name"] and cleaned["evidence"]:
            stakeholders.append(cleaned)
    stakeholders, rejected = deduplicate_items(stakeholders, transcript, "name")
    rejected_total += rejected

    roadmap: list[dict[str, Any]] = []
    for item in raw_items["roadmap"]:
        cleaned = {
            "step": _to_str(item.get("step", "")),
            "time_horizon": _coerce_enum(
                item.get("time_horizon"),
                {"immediate", "short_term", "mid_term", "long_term"},
                "short_term",
            ),
            "classification": _to_str(item.get("classification", "")),
            "source_type": _to_str(item.get("source_type", "")),
            "certainty_class": _coerce_certainty(item.get("certainty_class"), _to_str(item.get("evidence", ""))),
            "evidence": _to_str(item.get("evidence", "")),
        }
        if isinstance(item.get("_chunk_start_hint"), int):
            cleaned["_chunk_start_hint"] = item["_chunk_start_hint"]
        if cleaned["step"] and cleaned["evidence"]:
            roadmap.append(cleaned)
    roadmap, rejected = deduplicate_items(roadmap, transcript, "step")
    rejected_total += rejected
    roadmap = [
        {
            "step_order": idx + 1,
            "step": item["step"],
            "time_horizon": item["time_horizon"],
            "classification": item.get("classification", ""),
            "source_type": item.get("source_type", ""),
            "certainty_class": item.get("certainty_class", "UNCERTAIN"),
            "evidence": item["evidence"],
        }
        for idx, item in enumerate(roadmap)
    ]

    timeline_mentions: list[dict[str, Any]] = []
    for item in raw_items["timeline_mentions"]:
        cleaned = {
            "text": _to_str(item.get("text", "")),
            "raw_time_reference": _to_str(item.get("raw_time_reference", "")),
            "classification": _to_str(item.get("classification", "")),
            "source_type": _to_str(item.get("source_type", "")),
            "certainty_class": _coerce_certainty(item.get("certainty_class"), _to_str(item.get("evidence", ""))),
            "evidence": _to_str(item.get("evidence", "")),
        }
        if isinstance(item.get("_chunk_start_hint"), int):
            cleaned["_chunk_start_hint"] = item["_chunk_start_hint"]
        if cleaned["text"] and cleaned["evidence"]:
            timeline_mentions.append(cleaned)
    timeline_mentions, rejected = deduplicate_items(
        timeline_mentions, transcript, "text"
    )
    rejected_total += rejected

    return (
        {
            "decisions": decisions,
            "risks": risks,
            "action_plan": action_plan,
            "roadmap": roadmap,
            "deadlines": deadlines,
            "stakeholders": stakeholders,
            "timeline_mentions": timeline_mentions,
        },
        rejected_total,
    )


def _load_meeting_context(meeting_dir: Path, meeting_id: str) -> dict[str, str]:
    intake_metadata = meeting_dir / "metadata" / "intake.json"
    meeting_datetime = ""
    source_audio_file = ""

    if intake_metadata.exists():
        try:
            payload = json.loads(intake_metadata.read_text(encoding="utf-8"))
            meeting_datetime = str(payload.get("created_at", "") or "")
            source_audio_file = str(payload.get("stored_file_name", "") or "")
        except Exception:
            meeting_datetime = ""
            source_audio_file = ""

    return {
        "meeting_id": meeting_id,
        "meeting_datetime": meeting_datetime,
        "source_audio_file": source_audio_file,
    }


def _validate_intelligence_payload(payload: dict[str, Any], transcript: str) -> None:
    expected_keys = {
        "meeting_context",
        "summary",
        "decisions",
        "risks",
        "action_plan",
        "roadmap",
        "deadlines",
        "stakeholders",
        "timeline_mentions",
    }
    if set(payload.keys()) != expected_keys:
        raise SchemaValidationError("Top-level keys do not match required schema")

    meeting_context = payload["meeting_context"]
    if not isinstance(meeting_context, dict):
        raise SchemaValidationError("meeting_context must be an object")
    for key in ["meeting_id", "meeting_datetime", "source_audio_file"]:
        if key not in meeting_context or not isinstance(meeting_context[key], str):
            raise SchemaValidationError(f"meeting_context.{key} must be a string")

    summary = payload["summary"]
    if not isinstance(summary, str) or summary.strip() == "":
        raise SchemaValidationError("summary must be a non-empty string")

    _validate_decisions(payload["decisions"], transcript)
    _validate_risks(payload["risks"], transcript)
    _validate_action_plan(payload["action_plan"], transcript)
    _validate_roadmap(payload["roadmap"], transcript)
    _validate_deadlines(payload["deadlines"], transcript)
    _validate_stakeholders(payload["stakeholders"], transcript)
    _validate_timeline_mentions(payload["timeline_mentions"], transcript)


def _validate_decisions(items: Any, transcript: str) -> None:
    if not isinstance(items, list):
        raise SchemaValidationError("decisions must be an array")
    for item in items:
        if not isinstance(item, dict):
            raise SchemaValidationError("decision item must be an object")
        for key in ["text", "confidence", "state", "evidence"]:
            if key not in item or not isinstance(item[key], str):
                raise SchemaValidationError(f"decision.{key} must be a string")
        if item["confidence"] not in {"high", "medium", "low"}:
            raise SchemaValidationError("decision confidence has invalid enum")
        if item["state"] not in DECISION_STATE_ENUM:
            raise SchemaValidationError("decision state has invalid enum")
        _validate_classification(item)
        validate_evidence_exists(item, transcript)
        _validate_truth_metadata(item, transcript)


def _validate_risks(items: Any, transcript: str) -> None:
    if not isinstance(items, list):
        raise SchemaValidationError("risks must be an array")
    for item in items:
        if not isinstance(item, dict):
            raise SchemaValidationError("risk item must be an object")
        for key in ["text", "severity", "evidence"]:
            if key not in item or not isinstance(item[key], str):
                raise SchemaValidationError(f"risk.{key} must be a string")
        if item["severity"] not in {"high", "medium", "low"}:
            raise SchemaValidationError("risk severity has invalid enum")
        _validate_classification(item)
        validate_evidence_exists(item, transcript)
        _validate_truth_metadata(item, transcript)


def _validate_action_plan(items: Any, transcript: str) -> None:
    if not isinstance(items, list):
        raise SchemaValidationError("action_plan must be an array")
    for item in items:
        if not isinstance(item, dict):
            raise SchemaValidationError("action_plan item must be an object")
        for key in ["task", "owner", "priority", "status", "evidence"]:
            if key not in item or not isinstance(item[key], str):
                raise SchemaValidationError(f"action_plan.{key} must be a string")
        if item["priority"] not in {"high", "medium", "low"}:
            raise SchemaValidationError("action_plan priority has invalid enum")
        if item["status"] != "open":
            raise SchemaValidationError("action_plan status must be 'open'")
        _validate_classification(item)
        validate_evidence_exists(item, transcript)
        _validate_truth_metadata(item, transcript)


def _validate_roadmap(items: Any, transcript: str) -> None:
    if not isinstance(items, list):
        raise SchemaValidationError("roadmap must be an array")
    for item in items:
        if not isinstance(item, dict):
            raise SchemaValidationError("roadmap item must be an object")
        if "step_order" not in item or not isinstance(item["step_order"], int):
            raise SchemaValidationError("roadmap.step_order must be an integer")
        if item["step_order"] < 1:
            raise SchemaValidationError("roadmap.step_order must be >= 1")
        for key in ["step", "time_horizon", "evidence"]:
            if key not in item or not isinstance(item[key], str):
                raise SchemaValidationError(f"roadmap.{key} must be a string")
        if item["time_horizon"] not in {
            "immediate",
            "short_term",
            "mid_term",
            "long_term",
        }:
            raise SchemaValidationError("roadmap time_horizon has invalid enum")
        _validate_classification(item)
        validate_evidence_exists(item, transcript)
        _validate_truth_metadata(item, transcript)


def _validate_deadlines(items: Any, transcript: str) -> None:
    if not isinstance(items, list):
        raise SchemaValidationError("deadlines must be an array")
    for item in items:
        if not isinstance(item, dict):
            raise SchemaValidationError("deadline item must be an object")
        for key in ["event", "date", "evidence"]:
            if key not in item or not isinstance(item[key], str):
                raise SchemaValidationError(f"deadlines.{key} must be a string")
        _validate_classification(item)
        validate_evidence_exists(item, transcript)
        _validate_truth_metadata(item, transcript)


def _validate_stakeholders(items: Any, transcript: str) -> None:
    if not isinstance(items, list):
        raise SchemaValidationError("stakeholders must be an array")
    for item in items:
        if not isinstance(item, dict):
            raise SchemaValidationError("stakeholder item must be an object")
        for key in ["name", "role", "evidence"]:
            if key not in item or not isinstance(item[key], str):
                raise SchemaValidationError(f"stakeholders.{key} must be a string")
        _validate_classification(item)
        validate_evidence_exists(item, transcript)
        _validate_truth_metadata(item, transcript)


def _validate_timeline_mentions(items: Any, transcript: str) -> None:
    if not isinstance(items, list):
        raise SchemaValidationError("timeline_mentions must be an array")
    for item in items:
        if not isinstance(item, dict):
            raise SchemaValidationError("timeline mention item must be an object")
        for key in ["text", "raw_time_reference", "evidence"]:
            if key not in item or not isinstance(item[key], str):
                raise SchemaValidationError(
                    f"timeline_mentions.{key} must be a string"
                )
        _validate_classification(item)
        validate_evidence_exists(item, transcript)
        _validate_truth_metadata(item, transcript)


def _validate_classification(item: dict[str, Any]) -> None:
    classification = str(item.get("classification", "")).strip().upper()
    if classification not in FORENSIC_CLASSIFICATIONS:
        raise SchemaValidationError("classification must be FACT|SUGGESTION|UNCERTAIN|QUESTION")


def _validate_truth_metadata(item: dict[str, Any], transcript: str) -> None:
    support = str(item.get("support_level", "")).strip()
    if support not in {"DIRECTLY_SUPPORTED", "ACCEPTABLE_INFERENCE", "WEAK_INFERENCE"}:
        raise SchemaValidationError("support_level is missing or invalid")
    if str(item.get("claim_strength", "")).strip() not in {"direct", "inferred", "weak"}:
        raise SchemaValidationError("claim_strength is missing or invalid")
    span = str(item.get("evidence_span", "")).strip()
    if not span or span not in transcript:
        raise SchemaValidationError("evidence_span must be an exact transcript substring")
    if item.get("evidence_start_index") is None or item.get("evidence_end_index") is None:
        raise SchemaValidationError("evidence indexes are required")
    try:
        conf = float(item.get("evidence_confidence", 0.0))
    except Exception as exc:  # pragma: no cover - defensive guard
        raise SchemaValidationError("evidence_confidence must be numeric") from exc
    if conf < 0.5:
        raise SchemaValidationError("critical field evidence_confidence below threshold")
    certainty = str(item.get("certainty_class", "")).strip().upper()
    if certainty not in CERTAINTY_ENUM:
        raise SchemaValidationError("certainty_class is missing or invalid")


def _annotate_item_truth(
    item: dict[str, Any],
    claim_text: str,
    transcript: str,
    claim_type: str,
) -> dict[str, Any]:
    evidence = str(item.get("evidence", "")).strip()
    preferred = [evidence] if evidence else []
    binding = build_evidence_binding(
        claim=claim_text,
        transcript=transcript,
        preferred_spans=preferred,
        claim_type=claim_type,
    )
    if evidence and float(binding.get("evidence_confidence", 0.0)) < 0.5:
        binding = build_evidence_binding(
            claim=evidence,
            transcript=transcript,
            preferred_spans=[evidence],
            claim_type=claim_type,
        )
    if binding.get("evidence_span"):
        item["evidence"] = binding["evidence_span"]
    item["support_level"] = binding["support_level"]
    item["claim_strength"] = binding["claim_strength"]
    item["evidence_span"] = binding["evidence_span"]
    item["evidence_start_index"] = binding["evidence_start_index"]
    item["evidence_end_index"] = binding["evidence_end_index"]
    item["evidence_confidence"] = binding["evidence_confidence"]
    existing = _coerce_certainty(item.get("certainty_class"), evidence)
    derived = _infer_certainty_from_span(binding.get("evidence_span", "") or evidence)
    item["certainty_class"] = _min_certainty(existing, derived)
    return item


def _apply_truth_annotations(
    consolidated: dict[str, list[dict[str, Any]]], transcript: str
) -> dict[str, list[dict[str, Any]]]:
    for item in consolidated.get("decisions", []):
        if not isinstance(item, dict):
            continue
        claim = str(item.get("text", "")).strip()
        _annotate_item_truth(item, claim, transcript, claim_type="decision")

    for item in consolidated.get("risks", []):
        if not isinstance(item, dict):
            continue
        claim = str(item.get("text", "")).strip()
        _annotate_item_truth(item, claim, transcript, claim_type="warning")

    for item in consolidated.get("action_plan", []):
        if not isinstance(item, dict):
            continue
        claim = str(item.get("task", "")).strip()
        _annotate_item_truth(item, claim, transcript, claim_type="decision")

    for item in consolidated.get("roadmap", []):
        if not isinstance(item, dict):
            continue
        claim = str(item.get("step", "")).strip()
        _annotate_item_truth(item, claim, transcript, claim_type="decision")

    for item in consolidated.get("deadlines", []):
        if not isinstance(item, dict):
            continue
        claim = str(item.get("event", "")).strip() or str(item.get("date", "")).strip()
        _annotate_item_truth(item, claim, transcript, claim_type="timeline")

    filtered_stakeholders: list[dict[str, Any]] = []
    for item in consolidated.get("stakeholders", []):
        if not isinstance(item, dict):
            continue
        name = str(item.get("name", "")).strip()
        if name and not actor_present_in_transcript(name, transcript, alias_map=None):
            continue
        claim = f"{name} {str(item.get('role', '')).strip()}".strip() or name
        _annotate_item_truth(item, claim, transcript, claim_type="owner")
        filtered_stakeholders.append(item)
    consolidated["stakeholders"] = filtered_stakeholders

    for item in consolidated.get("timeline_mentions", []):
        if not isinstance(item, dict):
            continue
        claim = str(item.get("text", "")).strip() or str(item.get("raw_time_reference", "")).strip()
        _annotate_item_truth(item, claim, transcript, claim_type="timeline")

    return consolidated


def _safe_write_json(directory: Path, target: Path, payload: dict[str, Any]) -> None:
    try:
        directory.mkdir(parents=True, exist_ok=True)
        tmp_path = target.with_suffix(target.suffix + ".tmp")
        with tmp_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, ensure_ascii=False)
        tmp_path.replace(target)
    except Exception as exc:
        raise WriteProtectionError(f"Failed writing JSON to {target}: {exc}") from exc
