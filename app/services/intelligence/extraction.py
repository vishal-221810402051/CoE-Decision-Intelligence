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
from app.models.intelligence import IntelligenceExtractionResult

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
You are an extraction engine.
Extract ONLY transcript-grounded facts from the chunk below.

Rules:
- Return valid JSON only.
- Do not invent names, dates, owners, risks, decisions, or tasks.
- If a field is unknown, use an empty string.
- Evidence must be an exact verbatim snippet copied from the chunk.
- Keep evidence short.
- Do not include keys outside the required schema.

Return this JSON object shape:
{{
  "decisions": [{{"text":"", "confidence":"high|medium|low", "evidence":""}}],
  "risks": [{{"text":"", "severity":"high|medium|low", "evidence":""}}],
  "action_plan": [{{"task":"", "owner":"", "priority":"high|medium|low", "status":"open", "evidence":""}}],
  "roadmap": [{{"step_order": 1, "step":"", "time_horizon":"immediate|short_term|mid_term|long_term", "evidence":""}}],
  "deadlines": [{{"event":"", "date":"", "evidence":""}}],
  "stakeholders": [{{"name":"", "role":"", "evidence":""}}],
  "timeline_mentions": [{{"text":"", "raw_time_reference":"", "evidence":""}}]
}}

Chunk:
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
        transcript_path = meeting_dir / "transcript" / "transcript_clean.txt"
        intelligence_dir = meeting_dir / "intelligence"
        metadata_dir = meeting_dir / "metadata"
        intelligence_path = intelligence_dir / app_config.INTELLIGENCE_OUTPUT_FILE_NAME
        metadata_path = metadata_dir / app_config.INTELLIGENCE_METADATA_FILE_NAME

        if not meeting_dir.exists():
            raise InputArtifactError(f"Meeting folder not found: {meeting_dir}")
        if not transcript_path.exists():
            raise InputArtifactError(f"Clean transcript not found: {transcript_path}")

        api_key = _get_api_key()
        client = OpenAI(api_key=api_key)

        transcript = transcript_path.read_text(encoding="utf-8")
        if transcript.strip() == "":
            raise InputArtifactError(f"Clean transcript is empty: {transcript_path}")
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

        consolidated, items_rejected_due_to_evidence = _consolidate_items(
            raw_items, transcript
        )

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
            extracted = {key: parsed.get(key, []) for key in PASS_A_KEYS}
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
        cleaned = {
            "name": _to_str(item.get("name", "")),
            "role": _to_str(item.get("role", "")),
            "evidence": _to_str(item.get("evidence", "")),
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
            "evidence": item["evidence"],
        }
        for idx, item in enumerate(roadmap)
    ]

    timeline_mentions: list[dict[str, Any]] = []
    for item in raw_items["timeline_mentions"]:
        cleaned = {
            "text": _to_str(item.get("text", "")),
            "raw_time_reference": _to_str(item.get("raw_time_reference", "")),
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
        for key in ["text", "confidence", "evidence"]:
            if key not in item or not isinstance(item[key], str):
                raise SchemaValidationError(f"decision.{key} must be a string")
        if item["confidence"] not in {"high", "medium", "low"}:
            raise SchemaValidationError("decision confidence has invalid enum")
        validate_evidence_exists(item, transcript)


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
        validate_evidence_exists(item, transcript)


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
        validate_evidence_exists(item, transcript)


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
        validate_evidence_exists(item, transcript)


def _validate_deadlines(items: Any, transcript: str) -> None:
    if not isinstance(items, list):
        raise SchemaValidationError("deadlines must be an array")
    for item in items:
        if not isinstance(item, dict):
            raise SchemaValidationError("deadline item must be an object")
        for key in ["event", "date", "evidence"]:
            if key not in item or not isinstance(item[key], str):
                raise SchemaValidationError(f"deadlines.{key} must be a string")
        validate_evidence_exists(item, transcript)


def _validate_stakeholders(items: Any, transcript: str) -> None:
    if not isinstance(items, list):
        raise SchemaValidationError("stakeholders must be an array")
    for item in items:
        if not isinstance(item, dict):
            raise SchemaValidationError("stakeholder item must be an object")
        for key in ["name", "role", "evidence"]:
            if key not in item or not isinstance(item[key], str):
                raise SchemaValidationError(f"stakeholders.{key} must be a string")
        validate_evidence_exists(item, transcript)


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
        validate_evidence_exists(item, transcript)


def _safe_write_json(directory: Path, target: Path, payload: dict[str, Any]) -> None:
    try:
        directory.mkdir(parents=True, exist_ok=True)
        tmp_path = target.with_suffix(target.suffix + ".tmp")
        with tmp_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, ensure_ascii=False)
        tmp_path.replace(target)
    except Exception as exc:
        raise WriteProtectionError(f"Failed writing JSON to {target}: {exc}") from exc
