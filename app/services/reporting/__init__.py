from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from html import escape
from pathlib import Path
from typing import Any

from app.config import (
    REPORT_DIR_NAME,
    REPORT_HTML_FILE,
    REPORT_METADATA_FILE,
    REPORT_PAYLOAD_FILE,
    REPORT_PDF_FILE,
    config,
)
from app.services.processing_mode import get_processing_mode


class ReportGenerationError(Exception):
    def __init__(self, message: str, *, status: str = "failed") -> None:
        super().__init__(message)
        self.status = status


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _meeting_dir(meeting_id: str) -> Path:
    return config.PROCESSED_PATH / str(meeting_id).strip()


def _report_paths(meeting_id: str) -> dict[str, Path]:
    report_dir = _meeting_dir(meeting_id) / REPORT_DIR_NAME
    return {
        "report_dir": report_dir,
        "report_payload": report_dir / REPORT_PAYLOAD_FILE,
        "report_html": report_dir / REPORT_HTML_FILE,
        "report_pdf": report_dir / REPORT_PDF_FILE,
        "report_metadata": report_dir / REPORT_METADATA_FILE,
    }


def _read_required_text(path: Path, label: str) -> str:
    if not path.exists() or not path.is_file():
        raise ReportGenerationError(f"Missing required artifact: {label}", status="failed")
    content = path.read_text(encoding="utf-8")
    if not content.strip():
        raise ReportGenerationError(f"Required artifact is empty: {label}", status="failed")
    return content


def _read_required_json(path: Path, label: str) -> dict[str, Any]:
    if not path.exists() or not path.is_file():
        raise ReportGenerationError(f"Missing required artifact: {label}", status="failed")
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception as exc:
        raise ReportGenerationError(f"Invalid JSON in {label}: {exc}", status="failed") from exc
    if not isinstance(payload, dict) or not payload:
        raise ReportGenerationError(f"Required JSON artifact is empty: {label}", status="failed")
    return payload


def _is_valid_pdf(path: Path) -> bool:
    try:
        if not path.exists() or not path.is_file():
            return False
        with path.open("rb") as handle:
            return handle.read(5) == b"%PDF-"
    except Exception:
        return False


def _to_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    return []


def _clean_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _split_to_bullets(text: str, *, minimum: int = 3, maximum: int = 5) -> list[str]:
    cleaned = _clean_text(text)
    if not cleaned:
        return ["No executive summary available."]

    chunks = [
        _clean_text(part)
        for part in re.split(r"(?<=[.!?])\s+", cleaned)
        if _clean_text(part)
    ]
    if not chunks:
        chunks = [cleaned]

    bullets: list[str] = []
    for item in chunks:
        if item not in bullets:
            bullets.append(item)
        if len(bullets) >= maximum:
            break

    if len(bullets) < minimum and len(cleaned.split()) > 8:
        midpoint = max(8, len(cleaned.split()) // 2)
        words = cleaned.split()
        candidate_parts = [" ".join(words[:midpoint]), " ".join(words[midpoint:])]
        for candidate in candidate_parts:
            candidate_clean = _clean_text(candidate)
            if candidate_clean and candidate_clean not in bullets:
                bullets.append(candidate_clean)
            if len(bullets) >= minimum:
                break

    return bullets[:maximum]


def _ensure_sentence(text: Any, *, max_chars: int = 220) -> str:
    cleaned = _clean_text(text)
    if not cleaned:
        return ""

    cleaned = re.sub(r"(?i)^discussed\b", "The meeting focused on", cleaned)
    cleaned = re.sub(r"(?i)\bit was mentioned that\b", "", cleaned)
    cleaned = re.sub(r"(?i)\bthey talked about\b", "The discussion addressed", cleaned)
    cleaned = re.sub(r"(?i)\bthere is a need to\b", "The team needs to", cleaned)
    cleaned = _clean_text(cleaned)
    if not cleaned:
        return ""

    if len(cleaned) > max_chars:
        cleaned = cleaned[: max_chars - 3].rstrip(" ,;:") + "..."

    if cleaned.endswith((".", "!", "?")):
        return cleaned
    return f"{cleaned}."


def _build_executive_summary_lines(
    intelligence: dict[str, Any],
    executive: dict[str, Any],
    decisions: list[dict[str, Any]],
    risks: list[dict[str, Any]],
    timeline: list[dict[str, Any]],
    follow_ups: list[dict[str, Any]],
    governance: dict[str, Any],
) -> tuple[list[str], bool, bool]:
    summary_payload = executive.get("executive_summary", {}) if isinstance(executive, dict) else {}
    strategic_objective = executive.get("strategic_objective", {}) if isinstance(executive, dict) else {}
    execution_structure = executive.get("execution_structure", {}) if isinstance(executive, dict) else {}

    objective_text = _ensure_sentence(strategic_objective.get("objective", "")) if isinstance(strategic_objective, dict) else ""
    meaning_text = _ensure_sentence(summary_payload.get("meaning_of_meeting", "")) if isinstance(summary_payload, dict) else ""
    intelligence_summary = _ensure_sentence(intelligence.get("summary", ""))
    context_text = objective_text or meaning_text or intelligence_summary or "The meeting focused on setting direction for the initiative."
    context_line = f"Context: {context_text}"

    has_decision_flag = bool(decisions)
    core_lines: list[str] = []

    if has_decision_flag:
        top_decisions = decisions[:2]
        for idx, row in enumerate(top_decisions):
            decision_text = _ensure_sentence(row.get("decision", ""))
            if not decision_text:
                continue
            owner = _clean_text(row.get("owner", ""))
            if idx == 0:
                if owner:
                    core_lines.append(f"Core outcome: {decision_text[:-1]} Ownership is assigned to {owner}.")
                else:
                    core_lines.append(f"Core outcome: {decision_text[:-1]} Ownership remains unassigned.")
            else:
                core_lines.append(f"Core outcome: {decision_text}")
    else:
        core_lines.append("Core outcome: No concrete decision was finalized; discussion remains exploratory.")

    commitment_text = _ensure_sentence(summary_payload.get("commitment", "")) if isinstance(summary_payload, dict) else ""
    intent_text = _ensure_sentence(summary_payload.get("intent", "")) if isinstance(summary_payload, dict) else ""
    direction_line = ""
    if commitment_text:
        direction_line = f"Core outcome: {commitment_text}"
    elif intent_text:
        direction_line = f"Core outcome: {intent_text}"
    elif objective_text:
        direction_line = f"Core outcome: Direction remains focused on {objective_text}"
    if direction_line and direction_line not in core_lines:
        core_lines.append(direction_line)

    if len(core_lines) < 2:
        core_lines.append("Core outcome: Responsibility and scope still require explicit confirmation.")
    core_lines = core_lines[:3]

    risk_lines: list[str] = []
    has_risk_flag = False
    execution_risk = _clean_text(execution_structure.get("execution_risk_score", "")).lower() if isinstance(execution_structure, dict) else ""

    governance_gap = ""
    if isinstance(governance, dict):
        for gap in _to_list(governance.get("key_gaps")):
            gap_text = _ensure_sentence(gap)
            if gap_text:
                governance_gap = gap_text
                break

    high_risk_text = ""
    for row in risks:
        if not isinstance(row, dict):
            continue
        if _normalize_severity(row.get("severity"), "low") != "high":
            continue
        candidate = _ensure_sentence(row.get("risk", ""))
        if candidate and "no explicit risks identified" not in candidate.lower():
            high_risk_text = candidate
            break

    if execution_risk == "high":
        has_risk_flag = True
        risk_lines.append(
            "Key risk/concern: Execution risk is high because authority, governance, or ownership remains partially defined."
        )
        if governance_gap:
            risk_lines.append(f"Key risk/concern: {governance_gap}")
    elif high_risk_text:
        has_risk_flag = True
        risk_lines.append(f"Key risk/concern: {high_risk_text}")
    elif governance_gap:
        has_risk_flag = True
        risk_lines.append(f"Key risk/concern: {governance_gap}")
    else:
        risk_lines.append("Key risk/concern: No high-severity structural risk signal is currently confirmed.")

    risk_lines = risk_lines[:2]

    next_signal = ""
    for row in timeline:
        if not isinstance(row, dict):
            continue
        signal = _ensure_sentence(row.get("signal", ""))
        if not signal or "no explicit timeline signals identified" in signal.lower():
            continue
        next_signal = signal
        break
    if not next_signal:
        for row in follow_ups:
            if not isinstance(row, dict):
                continue
            question = _ensure_sentence(row.get("question", ""))
            if question and "no explicit follow-up questions identified" not in question.lower():
                next_signal = question
                break
    if not next_signal:
        next_signal = "No immediate timeline signal is confirmed."
    next_line = f"Immediate next signal: {next_signal}"

    lines = [context_line] + core_lines + risk_lines + [next_line]
    final_lines: list[str] = []
    seen: set[str] = set()
    for raw_line in lines:
        line = _ensure_sentence(raw_line, max_chars=260)
        key = line.lower()
        if not line or key in seen:
            continue
        seen.add(key)
        final_lines.append(line)

    if len(final_lines) < 5:
        filler = "Core outcome: Ownership, governance, and timeline require explicit confirmation before execution."
        if filler.lower() not in seen:
            final_lines.insert(min(3, len(final_lines)), filler)

    return final_lines[:7], has_decision_flag, has_risk_flag


def _confidence_to_float(value: Any) -> float:
    normalized = _clean_text(value).lower()
    if normalized == "high":
        return 0.9
    if normalized == "medium":
        return 0.65
    if normalized == "low":
        return 0.4
    return 0.5


def _timeline_type(signal: str) -> str:
    lower = signal.lower()
    if any(token in lower for token in ["deadline", "due", "by ", "end of"]):
        return "deadline_hint"
    if any(token in lower for token in ["next", "follow", "meeting", "tuesday"]):
        return "followup_marker"
    return "start_window"


def _extract_entities(stakeholders: list[Any], decisions: list[dict[str, Any]]) -> list[str]:
    entities: list[str] = []

    for row in stakeholders:
        if isinstance(row, dict):
            for key in ["name", "actor", "stakeholder"]:
                name = _clean_text(row.get(key, ""))
                if name and name not in entities:
                    entities.append(name)
        else:
            name = _clean_text(row)
            if name and name not in entities:
                entities.append(name)

    for row in decisions:
        owner = _clean_text(row.get("owner", ""))
        if owner and owner not in entities:
            entities.append(owner)

    return entities


def _extract_key_phrases(decisions: list[dict[str, Any]], actions: list[dict[str, Any]]) -> list[str]:
    phrases: list[str] = []
    for row in decisions:
        phrase = _clean_text(row.get("decision", ""))
        if len(phrase.split()) >= 4 and phrase not in phrases:
            phrases.append(phrase)
    for row in actions:
        phrase = _clean_text(row.get("action", ""))
        if len(phrase.split()) >= 4 and phrase not in phrases:
            phrases.append(phrase)
    return phrases


def _extract_doc_text(pdf_path: Path) -> str:
    try:
        raw = pdf_path.read_bytes()
    except Exception:
        return ""

    decoded = raw.decode("latin-1", errors="ignore")
    # Conservative text salvage for deterministic V1.
    cleaned = re.sub(r"[^\x20-\x7E\n\r\t]", " ", decoded)
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.lower().strip()


def _tokenize(text: str) -> list[str]:
    tokens = re.findall(r"[a-z][a-z0-9_\-]{2,}", text.lower())
    stop_words = {
        "the", "and", "for", "with", "from", "that", "this", "will", "your", "into",
        "have", "has", "are", "was", "were", "you", "our", "their", "about", "there",
        "they", "them", "would", "could", "should", "than", "then", "just", "also",
    }
    return [token for token in tokens if token not in stop_words]


def _resolve_selected_docs(meeting_id: str, selected_doc_ids: list[str]) -> list[dict[str, str]]:
    meeting_docs_root = _meeting_dir(meeting_id) / "docs"
    if not meeting_docs_root.exists() or not meeting_docs_root.is_dir():
        raise ReportGenerationError(
            "Source-document validation is enabled, but no meeting docs folder exists.",
            status="blocked",
        )

    unique_doc_ids: list[str] = []
    for doc_id in selected_doc_ids:
        normalized = str(doc_id).strip()
        if normalized and normalized not in unique_doc_ids:
            unique_doc_ids.append(normalized)

    if not unique_doc_ids:
        raise ReportGenerationError(
            "Source-document validation is enabled, but no source docs were selected.",
            status="blocked",
        )

    resolved: list[dict[str, str]] = []
    for doc_id in unique_doc_ids:
        doc_root = meeting_docs_root / doc_id
        source_dir = doc_root / "source"
        if not source_dir.exists() or not source_dir.is_dir():
            raise ReportGenerationError(
                f"Selected source doc is missing source folder: {doc_id}",
                status="blocked",
            )
        pdf_paths = sorted([path for path in source_dir.glob("*.pdf") if path.is_file()])
        if not pdf_paths:
            raise ReportGenerationError(
                f"Selected source doc does not contain a PDF: {doc_id}",
                status="blocked",
            )
        pdf_path = pdf_paths[0]
        if not _is_valid_pdf(pdf_path):
            raise ReportGenerationError(
                f"Selected source doc is not a valid PDF binary: {doc_id}",
                status="blocked",
            )
        resolved.append(
            {
                "doc_id": doc_id,
                "pdf_path": str(pdf_path),
                "relative_pdf_path": str(pdf_path.relative_to(_meeting_dir(meeting_id))),
            }
        )
    return resolved


def validate_against_docs(meeting_id: str, selected_doc_ids: list[str], decisions: list[dict[str, Any]], actions: list[dict[str, Any]], entities: list[str]) -> dict[str, Any]:
    resolved_docs = _resolve_selected_docs(meeting_id, selected_doc_ids)
    key_phrases = _extract_key_phrases(decisions, actions)
    phrase_checks = [phrase.lower() for phrase in key_phrases]

    keyword_space = _tokenize(" ".join(key_phrases + entities))
    keyword_set = set(keyword_space)

    details: list[dict[str, Any]] = []
    for row in resolved_docs:
        pdf_path = Path(row["pdf_path"])
        text = _extract_doc_text(pdf_path)

        matched_entities = [entity for entity in entities if entity.lower() in text]
        matched_phrases = [phrase for phrase in key_phrases if phrase.lower() in text]

        if keyword_set:
            doc_tokens = set(_tokenize(text))
            overlap_score = round(len(keyword_set.intersection(doc_tokens)) / max(len(keyword_set), 1), 3)
        else:
            overlap_score = 0.0

        if overlap_score >= 0.35 and matched_entities and matched_phrases:
            status = "supported"
        elif overlap_score < 0.08 and not matched_entities and not matched_phrases and phrase_checks:
            status = "not_found"
        else:
            status = "unclear"

        details.append(
            {
                "doc_id": row["doc_id"],
                "status": status,
                "overlap_score": overlap_score,
                "matched_entities": matched_entities,
                "matched_phrases": matched_phrases,
            }
        )

    summary = {
        "supported": sum(1 for row in details if row["status"] == "supported"),
        "not_found": sum(1 for row in details if row["status"] == "not_found"),
        "unclear": sum(1 for row in details if row["status"] == "unclear"),
    }

    return {
        "summary": summary,
        "details": details,
        "source_docs_used": [row["doc_id"] for row in details],
    }


def _normalize_decisions(decision_data: dict[str, Any]) -> list[dict[str, Any]]:
    total_candidates = 0
    dropped_vague = 0
    dropped_duplicate = 0
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for record in _to_list(decision_data.get("decision_records")):
        if not isinstance(record, dict):
            continue
        total_candidates += 1
        statement = _clean_text(record.get("statement", ""))
        if not statement:
            continue

        if _is_non_actionable_decision_text(statement):
            dropped_vague += 1
            continue

        dedupe_key = _dedupe_key(statement)
        if dedupe_key in seen:
            dropped_duplicate += 1
            continue
        seen.add(dedupe_key)

        owner = _clean_text(record.get("primary_owner", ""))
        confidence = _clean_text(record.get("confidence", "low")).lower() or "low"
        evidence_count = len(_to_list(record.get("evidence")))
        timeline = _extract_timeline_hint(record)
        status = _derive_decision_presentation_status(record, owner)
        group = _derive_decision_group(record, status)

        rows.append(
            {
                "decision": statement,
                "owner": owner,
                "confidence": confidence,
                "evidence_count": evidence_count,
                "timeline": timeline,
                "status": status,
                "group": group,
            }
        )
    print(
        f"[DECISION_PRESENTATION_FILTER] total_candidates={total_candidates} "
        f"dropped_vague={dropped_vague} dropped_duplicate={dropped_duplicate} final={len(rows)}"
    )
    return rows


def _is_non_actionable_decision_text(text: Any) -> bool:
    normalized = _clean_text(text).lower()
    if not normalized:
        return True
    if "?" in normalized:
        return True

    vague_prefixes = (
        "we discussed",
        "it was mentioned",
        "they talked about",
        "there is a need to",
        "discussion happened",
        "it was discussed",
        "we talked about",
    )
    if any(normalized.startswith(prefix) for prefix in vague_prefixes):
        return True

    informational_patterns = (
        "for information",
        "fyi",
        "as an update",
        "this was shared",
        "status update",
    )
    if any(pattern in normalized for pattern in informational_patterns):
        return True

    return False


def _extract_timeline_hint(record: dict[str, Any]) -> str:
    for signal in _to_list(record.get("timeline_signals")):
        if not isinstance(signal, dict):
            continue
        hint = _clean_text(signal.get("raw_reference") or signal.get("signal"))
        if hint:
            return hint
    return ""


def _derive_decision_presentation_status(record: dict[str, Any], owner: str) -> str:
    if not owner:
        return "Requires Clarification"

    state = _clean_text(record.get("state", "")).lower()
    decision_status = _clean_text(record.get("decision_status", "")).lower()
    if state in {"tentative", "pending", "blocked"} or decision_status in {"tentative", "conditional", "blocked"}:
        return "Pending"

    commitments = _to_list(record.get("commitments"))
    has_conditional_commitment = False
    has_explicit_commitment = False
    for commitment in commitments:
        if not isinstance(commitment, dict):
            continue
        ctype = _clean_text(commitment.get("type", "")).lower()
        cstatus = _clean_text(commitment.get("status", "")).lower()
        if ctype in {"requested_commitment", "unresolved_commitment"} or cstatus == "unresolved":
            has_conditional_commitment = True
        if ctype == "explicit_commitment" and cstatus in {"accepted", "open"}:
            has_explicit_commitment = True

    if has_conditional_commitment:
        return "Pending"
    if state == "confirmed" or decision_status == "confirmed" or has_explicit_commitment:
        return "Confirmed"
    return "Pending"


def _derive_decision_group(record: dict[str, Any], status: str) -> str:
    if status != "Confirmed":
        return "Open Decisions / Pending Clarifications"

    statement = _clean_text(record.get("statement", "")).lower()
    strategic_tokens = (
        "strategy",
        "governance",
        "ownership",
        "funding",
        "revenue",
        "business",
        "initiative",
        "partnership",
        "program",
        "center of excellence",
        "coe",
    )
    if any(token in statement for token in strategic_tokens):
        return "Strategic Decisions"
    return "Operational Actions"


def _decision_presentation_groups(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, str]]]:
    groups: dict[str, list[dict[str, str]]] = {
        "Strategic Decisions": [],
        "Operational Actions": [],
        "Open Decisions / Pending Clarifications": [],
    }
    for row in rows:
        group = _clean_text(row.get("group", ""))
        if group not in groups:
            group = "Operational Actions"
        groups[group].append(
            {
                "Decision Card": _format_decision_card(row),
            }
        )
    return groups


def _format_decision_card(row: dict[str, Any]) -> str:
    decision_text = _clean_text(row.get("decision", ""))
    owner = _clean_text(row.get("owner", "")) or "Unassigned"
    timeline = _clean_text(row.get("timeline", "")) or "Not specified"
    status = _clean_text(row.get("status", "")) or "Pending"
    return (
        "[DECISION]\n"
        f"-> {decision_text}\n\n"
        "[OWNER]\n"
        f"-> {owner}\n\n"
        "[TIMELINE]\n"
        f"-> {timeline}\n\n"
        "[STATUS]\n"
        f"-> {status}"
    )


def _normalize_risks(intelligence_data: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for risk in _to_list(intelligence_data.get("risks")):
        if isinstance(risk, dict):
            risk_text = _clean_text(
                risk.get("risk")
                or risk.get("description")
                or risk.get("title")
                or risk.get("text")
                or ""
            )
            severity = _clean_text(risk.get("severity", "medium")).lower() or "medium"
            confidence = _clean_text(risk.get("confidence", "low")).lower() or "low"
            owner = _clean_text(risk.get("owner") or risk.get("actor") or "")
        else:
            risk_text = _clean_text(risk)
            severity = "medium"
            confidence = "low"
            owner = ""

        if not risk_text:
            continue

        rows.append(
            {
                "risk": risk_text,
                "severity": severity,
                "confidence": confidence,
                "owner": owner,
                "mitigation": None,
                "source": "intelligence",
            }
        )
    return rows


def _normalize_actions(intelligence_data: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for action in _to_list(intelligence_data.get("action_plan")):
        if isinstance(action, dict):
            action_text = _clean_text(action.get("action") or action.get("task") or action.get("description") or "")
            owner = _clean_text(action.get("owner") or action.get("actor") or "")
            due_hint = _clean_text(action.get("due_hint") or action.get("deadline") or "") or None
        else:
            action_text = _clean_text(action)
            owner = ""
            due_hint = None

        if not action_text:
            continue

        rows.append(
            {
                "action": action_text,
                "owner": owner,
                "status": "open",
                "due_hint": due_hint,
                "source": "intelligence",
            }
        )
    return rows


def _normalize_timeline(intelligence_data: dict[str, Any]) -> list[dict[str, Any]]:
    timeline_rows: list[dict[str, Any]] = []
    for item in _to_list(intelligence_data.get("timeline_mentions")):
        if isinstance(item, dict):
            signal = _clean_text(item.get("signal") or item.get("mention") or item.get("text") or "")
            signal_type = _clean_text(item.get("type") or "") or _timeline_type(signal)
            confidence = item.get("confidence")
            if isinstance(confidence, (int, float)):
                confidence_value = round(float(confidence), 3)
            else:
                confidence_value = _confidence_to_float(confidence)
        else:
            signal = _clean_text(item)
            signal_type = _timeline_type(signal)
            confidence_value = 0.6

        if not signal:
            continue

        timeline_rows.append(
            {
                "signal": signal,
                "type": signal_type,
                "confidence": confidence_value,
                "sources": ["intelligence"],
            }
        )
    return timeline_rows


def _dedupe_key(*values: Any) -> str:
    return "|".join(_clean_text(value).lower() for value in values if _clean_text(value))


def _normalize_severity(value: Any, default: str = "medium") -> str:
    normalized = _clean_text(value).lower()
    if normalized in {"high", "medium", "low"}:
        return normalized
    return default


def _normalize_confidence(value: Any, default: str = "medium") -> str:
    normalized = _clean_text(value).lower()
    if normalized in {"high", "medium", "low"}:
        return normalized
    return default


def aggregate_risks(
    intelligence: dict[str, Any],
    executive: dict[str, Any],
    decision: dict[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()

    def add_risk(row: dict[str, Any]) -> None:
        risk_text = _clean_text(row.get("risk", ""))
        if not risk_text:
            return
        key = _dedupe_key(risk_text)
        if not key or key in seen:
            return
        seen.add(key)
        rows.append(
            {
                "risk": risk_text,
                "severity": _normalize_severity(row.get("severity"), "medium"),
                "confidence": _normalize_confidence(row.get("confidence"), "medium"),
                "owner": _clean_text(row.get("owner", "")),
                "mitigation": row.get("mitigation"),
                "source": _clean_text(row.get("source", "")) or "unknown",
                "notes": _clean_text(row.get("notes", "")),
            }
        )

    for row in _normalize_risks(intelligence):
        add_risk(row)

    risk_posture = executive.get("risk_posture", {})
    if isinstance(risk_posture, dict):
        for driver in _to_list(risk_posture.get("drivers")):
            add_risk(
                {
                    "risk": driver,
                    "severity": risk_posture.get("overall", "medium"),
                    "confidence": risk_posture.get("confidence", "medium"),
                    "source": "executive.risk_posture",
                }
            )

    for warning in _to_list(executive.get("executive_warnings")):
        if not isinstance(warning, dict):
            continue
        add_risk(
            {
                "risk": warning.get("warning", ""),
                "severity": warning.get("severity", "medium"),
                "confidence": warning.get("confidence", "medium"),
                "source": "executive.executive_warnings",
                "notes": warning.get("reason", ""),
            }
        )

    for flag in _to_list(executive.get("negotiation_flags")):
        if not isinstance(flag, dict):
            continue
        if _normalize_severity(flag.get("severity", "low"), "low") != "high":
            continue
        add_risk(
            {
                "risk": flag.get("topic", ""),
                "severity": flag.get("severity", "high"),
                "confidence": flag.get("confidence", "medium"),
                "source": "executive.negotiation_flags",
                "notes": f"status={_clean_text(flag.get('status', 'open')) or 'open'}",
            }
        )

    for record in _to_list(decision.get("decision_records")):
        if not isinstance(record, dict):
            continue
        statement = _clean_text(record.get("statement", ""))
        state = _clean_text(record.get("state", ""))
        if state == "blocked" and statement:
            add_risk(
                {
                    "risk": statement,
                    "severity": "high",
                    "confidence": _normalize_confidence(record.get("confidence", "medium"), "medium"),
                    "source": "decision.blocked_decision",
                    "notes": "Decision state is blocked.",
                }
            )

        for dep in _to_list(record.get("dependencies")):
            if not isinstance(dep, dict):
                continue
            if _clean_text(dep.get("status", "")).lower() != "open":
                continue
            if _clean_text(dep.get("blocking_level", "")).lower() != "high":
                continue
            add_risk(
                {
                    "risk": dep.get("reason", "") or f"Open high dependency: {_clean_text(dep.get('type', 'dependency'))}",
                    "severity": "high",
                    "confidence": "medium",
                    "source": "decision.dependencies",
                    "notes": _clean_text(dep.get("type", "")),
                }
            )

    operational_summary = decision.get("operational_summary", {})
    if isinstance(operational_summary, dict):
        for blocker in _to_list(operational_summary.get("high_blockers")):
            add_risk(
                {
                    "risk": blocker,
                    "severity": "high",
                    "confidence": "medium",
                    "source": "decision.operational_summary",
                    "notes": "Listed as high blocker.",
                }
            )

    if not rows:
        rows.append(
            {
                "risk": "No explicit risks identified from available signals",
                "severity": "low",
                "confidence": "low",
                "owner": "",
                "mitigation": None,
                "source": "system",
                "notes": "",
            }
        )
    return rows


def aggregate_actions(
    intelligence: dict[str, Any],
    executive: dict[str, Any],
    decision: dict[str, Any],
) -> dict[str, list[dict[str, Any]]]:
    actions: list[dict[str, Any]] = []
    follow_ups: list[dict[str, Any]] = []
    seen_actions: set[str] = set()
    seen_followups: set[str] = set()

    def add_action(row: dict[str, Any]) -> None:
        action_text = _clean_text(row.get("action", ""))
        if not action_text:
            return
        key = _dedupe_key(action_text, row.get("owner", ""))
        if not key or key in seen_actions:
            return
        seen_actions.add(key)
        actions.append(
            {
                "action": action_text,
                "owner": _clean_text(row.get("owner", "")),
                "status": _clean_text(row.get("status", "open")).lower() or "open",
                "due_hint": _clean_text(row.get("due_hint", "")) or None,
                "source": _clean_text(row.get("source", "")) or "unknown",
            }
        )

    def add_follow_up(question: str, source: str, priority: str = "medium") -> None:
        text = _clean_text(question)
        if not text:
            return
        key = _dedupe_key(text)
        if not key or key in seen_followups:
            return
        seen_followups.add(key)
        follow_ups.append(
            {
                "question": text,
                "priority": _normalize_severity(priority, "medium"),
                "source": source,
            }
        )

    for row in _normalize_actions(intelligence):
        add_action(row)

    for record in _to_list(decision.get("decision_records")):
        if not isinstance(record, dict):
            continue
        primary_owner = _clean_text(record.get("primary_owner", ""))

        for commitment in _to_list(record.get("commitments")):
            if not isinstance(commitment, dict):
                continue
            add_action(
                {
                    "action": commitment.get("commitment", ""),
                    "owner": commitment.get("actor", "") or primary_owner,
                    "status": commitment.get("status", "open"),
                    "due_hint": None,
                    "source": "decision.commitments",
                }
            )

        for gap in _to_list(record.get("decision_gaps")):
            if not isinstance(gap, dict):
                continue
            add_action(
                {
                    "action": gap.get("question", "") or gap.get("gap_type", ""),
                    "owner": primary_owner,
                    "status": "open",
                    "due_hint": None,
                    "source": "decision.gaps",
                }
            )

        for dep in _to_list(record.get("dependencies")):
            if not isinstance(dep, dict):
                continue
            if _clean_text(dep.get("status", "")).lower() != "open":
                continue
            add_action(
                {
                    "action": dep.get("reason", "") or f"Resolve {_clean_text(dep.get('type', 'dependency'))}",
                    "owner": primary_owner,
                    "status": "open",
                    "due_hint": None,
                    "source": "decision.dependencies",
                }
            )

    for row in _to_list(executive.get("recommended_next_questions")):
        if not isinstance(row, dict):
            continue
        add_follow_up(
            question=str(row.get("question", "")),
            source="executive.recommended_next_questions",
            priority=str(row.get("priority", "medium")),
        )

    if not actions:
        actions.append(
            {
                "action": "No explicit actions identified from available signals",
                "owner": "",
                "status": "open",
                "due_hint": None,
                "source": "system",
            }
        )

    if not follow_ups:
        follow_ups.append(
            {
                "question": "No explicit follow-up questions identified from available signals",
                "priority": "low",
                "source": "system",
            }
        )

    return {"actions": actions, "follow_ups": follow_ups}


def aggregate_timeline(intelligence: dict[str, Any], decision: dict[str, Any]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}

    def add_timeline(signal: Any, signal_type: Any, confidence: Any, source: str) -> None:
        signal_text = _clean_text(signal)
        type_text = _clean_text(signal_type) or "timeline_signal"
        if not signal_text:
            return
        key = _dedupe_key(signal_text, type_text)
        if not key:
            return

        confidence_value: Any = confidence
        if isinstance(confidence, (int, float)):
            confidence_value = round(float(confidence), 3)
        else:
            normalized_conf = _normalize_confidence(confidence, "")
            confidence_value = normalized_conf if normalized_conf else _clean_text(confidence) or "medium"

        row = merged.get(key)
        if row is None:
            merged[key] = {
                "signal": signal_text,
                "type": type_text,
                "confidence": confidence_value,
                "sources": [source],
            }
            return

        if source not in row["sources"]:
            row["sources"].append(source)

        rank = {"low": 1, "medium": 2, "high": 3}
        current = str(row.get("confidence", "")).lower()
        incoming = str(confidence_value).lower()
        if incoming in rank and (current not in rank or rank[incoming] > rank[current]):
            row["confidence"] = incoming

    for row in _to_list(intelligence.get("timeline_mentions")):
        if isinstance(row, dict):
            add_timeline(
                row.get("text") or row.get("signal") or row.get("raw_time_reference"),
                row.get("type") or _timeline_type(_clean_text(row.get("text") or row.get("signal") or "")),
                row.get("confidence") or row.get("support_level"),
                "intelligence.timeline_mentions",
            )
        else:
            add_timeline(row, _timeline_type(_clean_text(row)), "medium", "intelligence.timeline_mentions")

    for row in _to_list(intelligence.get("deadlines")):
        if not isinstance(row, dict):
            continue
        signal = _clean_text(row.get("date") or row.get("event"))
        add_timeline(
            signal,
            "deadline_hint",
            row.get("confidence") or row.get("support_level") or "medium",
            "intelligence.deadlines",
        )

    for record in _to_list(decision.get("decision_records")):
        if not isinstance(record, dict):
            continue
        for signal in _to_list(record.get("timeline_signals")):
            if not isinstance(signal, dict):
                continue
            add_timeline(
                signal.get("raw_reference") or signal.get("signal"),
                signal.get("signal_type") or signal.get("type") or "timeline_signal",
                signal.get("confidence", "medium"),
                "decision.timeline_signals",
            )

    output = list(merged.values())
    output.sort(key=lambda item: (_clean_text(item.get("signal", "")).lower(), _clean_text(item.get("type", "")).lower()))
    if not output:
        output.append(
            {
                "signal": "No explicit timeline signals identified from available signals",
                "type": "timeline_signal",
                "confidence": "low",
                "sources": ["system"],
            }
        )
    return output


def build_governance_section(executive: dict[str, Any], decision: dict[str, Any]) -> dict[str, Any]:
    power = executive.get("power_structure", {})
    execution = executive.get("execution_structure", {})
    roles = _to_list(executive.get("role_clarity_assessment"))
    operational = decision.get("operational_summary", {})

    key_gaps: list[str] = []
    seen: set[str] = set()

    def add_gap(text: Any) -> None:
        gap = _clean_text(text)
        key = gap.lower()
        if not gap or key in seen:
            return
        seen.add(key)
        key_gaps.append(gap)

    if isinstance(power, dict):
        for item in _to_list(power.get("unknown_authority_gaps")):
            add_gap(item)

    if isinstance(execution, dict):
        for field in ["authority_clarity", "compensation_clarity", "governance_clarity"]:
            value = _clean_text(execution.get(field, ""))
            if value in {"partial", "undefined"}:
                add_gap(f"{field} is {value}")

    for row in roles:
        if not isinstance(row, dict):
            continue
        clarity = _clean_text(row.get("clarity", "")).lower()
        if clarity in {"partial", "undefined"}:
            add_gap(f"Role clarity for {_clean_text(row.get('actor', 'unknown'))} is {clarity}")

    for record in _to_list(decision.get("decision_records")):
        if not isinstance(record, dict):
            continue
        for dep in _to_list(record.get("dependencies")):
            if not isinstance(dep, dict):
                continue
            dep_type = _clean_text(dep.get("type", "")).lower()
            dep_status = _clean_text(dep.get("status", "")).lower()
            if dep_status == "open" and dep_type in {"authority_dependency", "governance_dependency"}:
                add_gap(dep.get("reason", "") or f"Open {dep_type}")

    missing_owner_count = 0
    if isinstance(operational, dict):
        try:
            missing_owner_count = int(operational.get("missing_owners_count", 0))
        except Exception:
            missing_owner_count = 0

    return {
        "authority_clarity": _clean_text(execution.get("authority_clarity", "unknown")) if isinstance(execution, dict) else "unknown",
        "execution_clarity": _clean_text(execution.get("execution_risk_score", "unknown")) if isinstance(execution, dict) else "unknown",
        "primary_executor": _clean_text(execution.get("primary_executor", "")) if isinstance(execution, dict) else "",
        "missing_owners_count": missing_owner_count,
        "key_gaps": key_gaps,
    }


def build_operational_summary(decision: dict[str, Any]) -> dict[str, Any]:
    operational = decision.get("operational_summary", {})
    if not isinstance(operational, dict):
        operational = {}
    try:
        blocked_count = int(operational.get("blocked_count", 0))
    except Exception:
        blocked_count = 0
    try:
        open_dependencies_count = int(operational.get("open_dependencies_count", 0))
    except Exception:
        open_dependencies_count = 0

    return {
        "blocked_count": blocked_count,
        "open_dependencies_count": open_dependencies_count,
        "high_blockers": _to_list(operational.get("high_blockers")),
    }


def build_report_payload(meeting_id: str, mode: str) -> dict[str, Any]:
    meeting_dir = _meeting_dir(meeting_id)

    transcript_clean = _read_required_text(
        meeting_dir / "transcript" / "transcript_clean.txt",
        "transcript/transcript_clean.txt",
    )
    intelligence = _read_required_json(
        meeting_dir / "intelligence" / "intelligence.json",
        "intelligence/intelligence.json",
    )
    executive = _read_required_json(
        meeting_dir / "executive" / "executive_intelligence.json",
        "executive/executive_intelligence.json",
    )
    decision = _read_required_json(
        meeting_dir / "decision" / "decision_intelligence_v2.json",
        "decision/decision_intelligence_v2.json",
    )

    decisions = _normalize_decisions(decision)
    risks = aggregate_risks(intelligence, executive, decision)
    action_bundle = aggregate_actions(intelligence, executive, decision)
    actions = action_bundle["actions"]
    follow_ups = action_bundle["follow_ups"]
    timeline = aggregate_timeline(intelligence, decision)
    governance = build_governance_section(executive, decision)
    operational_summary = build_operational_summary(decision)
    stakeholders = _to_list(intelligence.get("stakeholders"))
    entities = _extract_entities(stakeholders, decisions)
    executive_summary_bullets, has_decision_flag, has_risk_flag = _build_executive_summary_lines(
        intelligence=intelligence,
        executive=executive,
        decisions=decisions,
        risks=risks,
        timeline=timeline,
        follow_ups=follow_ups,
        governance=governance,
    )
    print(
        f"[EXEC_SUMMARY] summary_length={len(executive_summary_bullets)} "
        f"has_decision_flag={has_decision_flag} has_risk_flag={has_risk_flag}"
    )
    grouped_decisions = _decision_presentation_groups(decisions)
    grouped_counts = {name: len(items) for name, items in grouped_decisions.items()}
    pending_count = sum(1 for row in decisions if _clean_text(row.get("status", "")).lower() == "pending")
    unassigned_count = sum(1 for row in decisions if not _clean_text(row.get("owner", "")))
    print(
        f"[DECISION_PRESENTATION] grouped_counts={grouped_counts} "
        f"pending_count={pending_count} unassigned_count={unassigned_count}"
    )

    notes = [
        f"Transcript length: {len(transcript_clean)} characters.",
        "Report generated deterministically from canonical meeting artifacts.",
    ]

    source_docs_used: list[str] = []
    doc_validation: dict[str, Any] | None = None
    if mode == "transcript_plus_docs":
        mode_payload = get_processing_mode(meeting_id)
        selected_ids = mode_payload.get("selected_source_doc_ids", [])
        validation = validate_against_docs(
            meeting_id=meeting_id,
            selected_doc_ids=selected_ids if isinstance(selected_ids, list) else [],
            decisions=decisions,
            actions=actions,
            entities=entities,
        )
        source_docs_used = _to_list(validation.get("source_docs_used"))
        doc_validation = {
            "summary": validation.get("summary", {"supported": 0, "not_found": 0, "unclear": 0}),
            "details": validation.get("details", []),
        }

    sections = {
        "executive_summary": executive_summary_bullets,
        "decisions": decisions,
        "key_decisions": decisions,
        "governance": governance,
        "risks": risks,
        "actions": actions,
        "follow_ups": follow_ups,
        "timeline": timeline,
        "operational_summary": operational_summary,
        "notes": notes,
        "doc_validation": doc_validation,
    }

    return {
        "header": {
            "meeting_id": meeting_id,
            "processing_mode": mode,
            "generated_at": _now_iso(),
            "report_version": "v1",
        },
        "meeting_id": meeting_id,
        "processing_mode": mode,
        "generated_at": _now_iso(),
        "sections": sections,
        "source_docs_used": source_docs_used,
    }


def _display_mode_label(mode: Any) -> str:
    normalized = _clean_text(mode).lower()
    if normalized == "transcript_only":
        return "Transcript Only"
    if normalized in {"transcript_plus_docs", "transcript_and_docs"}:
        return "Transcript + Source Documents"
    return _clean_text(mode) or "Unknown"


def _display_empty_message(kind: str) -> str:
    mapping = {
        "risks": "No material risks were identified from available signals.",
        "actions": "No concrete actions were identified from available signals.",
        "timeline": "No timeline signals were identified from available signals.",
        "follow_ups": "No open questions were identified for the next meeting.",
        "doc_validation": "No source-document alignment was run (Transcript-Only basis).",
        "report": "No report has been generated for this meeting yet.",
    }
    return mapping.get(kind, "No data available.")


def _truncate_presentation_text(text: Any, *, limit: int = 160) -> str:
    cleaned = _clean_text(text)
    if len(cleaned) <= limit:
        return cleaned
    return f"{cleaned[:limit].rstrip()}..."


def _to_dict_rows(value: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in _to_list(value):
        if isinstance(row, dict):
            rows.append(row)
    return rows


def _build_executive_brief(sections: dict[str, Any]) -> list[str]:
    executive_summary = [item for item in _to_list(sections.get("executive_summary")) if _clean_text(item)]
    decisions = _to_dict_rows(sections.get("decisions"))
    risks = _to_dict_rows(sections.get("risks"))
    actions = _to_dict_rows(sections.get("actions"))
    follow_ups = _to_dict_rows(sections.get("follow_ups"))

    decided_text = _clean_text(decisions[0].get("decision", "")) if decisions else ""
    risk_text = _clean_text(risks[0].get("risk", "")) if risks else ""
    next_step = _clean_text(actions[0].get("action", "")) if actions else ""
    if not next_step and follow_ups:
        next_step = _clean_text(follow_ups[0].get("question", ""))

    if not decided_text and executive_summary:
        decided_text = executive_summary[0]
    if not risk_text and len(executive_summary) > 1:
        risk_text = executive_summary[1]
    if not next_step and len(executive_summary) > 2:
        next_step = executive_summary[2]

    if not decided_text:
        decided_text = "No explicit decision signal was captured from the available artifacts."
    if not risk_text:
        risk_text = "No material risk signal was captured from the available artifacts."
    if not next_step:
        next_step = "No explicit next-step signal was captured from the available artifacts."

    return [
        f"What was decided: {_truncate_presentation_text(decided_text, limit=180)}",
        f"What is at risk: {_truncate_presentation_text(risk_text, limit=180)}",
        f"What must happen next: {_truncate_presentation_text(next_step, limit=180)}",
    ]


def _presentation_decisions(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "Decision": _truncate_presentation_text(row.get("decision", ""), limit=180),
            "Owner": _clean_text(row.get("owner", "")),
            "Confidence": _clean_text(row.get("confidence", "")),
            "Evidence Count": _clean_text(row.get("evidence_count", "")),
        }
        for row in rows
    ]


def _presentation_risks(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "Risk": _truncate_presentation_text(row.get("risk", ""), limit=180),
            "Impact": _clean_text(row.get("severity", "")),
            "Confidence": _clean_text(row.get("confidence", "")),
        }
        for row in rows
    ]


def _presentation_actions(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "Action": _truncate_presentation_text(row.get("action", ""), limit=180),
            "Owner": _clean_text(row.get("owner", "")),
            "Deadline": _clean_text(row.get("due_hint", "")),
            "Status": _clean_text(row.get("status", "")),
        }
        for row in rows
    ]


def _timeline_date_display(signal: str, signal_type: str) -> str:
    normalized_signal = _clean_text(signal)
    lower_signal = normalized_signal.lower()
    patterns = [
        r"\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*\s+\d{1,2}(?:,\s*\d{4})?\b",
        r"\b(?:monday|tuesday|wednesday|thursday|friday|saturday|sunday)\b(?:\s+after\s+\d{1,2}(?::\d{2})?)?",
        r"\b(?:first|second|third|fourth)\s+(?:week|month)\s+of\s+[a-z]+\b",
        r"\bend of [a-z]+\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, lower_signal, flags=re.IGNORECASE)
        if match:
            return _clean_text(match.group(0)).title()
    return _clean_text(signal_type).replace("_", " ").title()


def _presentation_timeline(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[str, dict[str, Any]] = {}
    for row in rows:
        event = _truncate_presentation_text(row.get("signal", ""), limit=170)
        date_value = _timeline_date_display(_clean_text(row.get("signal", "")), _clean_text(row.get("type", "")))
        confidence = _clean_text(row.get("confidence", ""))
        key = _dedupe_key(event, date_value, confidence)
        if key and key not in deduped:
            deduped[key] = {
                "Event": event,
                "Date": date_value,
                "Confidence": confidence,
            }
    return list(deduped.values())


def _presentation_followups(rows: list[dict[str, Any]]) -> list[str]:
    items: list[str] = []
    seen: set[str] = set()
    for row in rows:
        question = _truncate_presentation_text(row.get("question", ""), limit=180)
        if not question:
            continue
        key = question.lower()
        if key in seen:
            continue
        seen.add(key)
        items.append(question)
    return items


def _render_dict_table(rows: list[dict[str, Any]], empty_message: str) -> str:
    if not rows:
        return f"<p>{escape(empty_message)}</p>"
    headers = list(rows[0].keys())
    header_html = "".join(f"<th>{escape(str(item))}</th>" for item in headers)
    body_rows: list[str] = []
    for row in rows:
        cells = "".join(f"<td>{escape(str(row.get(item, '')))}</td>" for item in headers)
        body_rows.append(f"<tr>{cells}</tr>")
    return (
        "<table style='width:100%;border-collapse:collapse;margin-top:8px;' border='1' cellpadding='6'>"
        f"<thead><tr>{header_html}</tr></thead>"
        f"<tbody>{''.join(body_rows)}</tbody>"
        "</table>"
    )


def _render_bullets(items: list[str], empty_message: str) -> str:
    valid_items = [item for item in items if _clean_text(item)]
    if not valid_items:
        return f"<li>{escape(empty_message)}</li>"
    return "".join(f"<li>{escape(_clean_text(item))}</li>" for item in valid_items)


def generate_html_report(payload: dict[str, Any]) -> str:
    sections = payload.get("sections", {}) if isinstance(payload, dict) else {}
    if not isinstance(sections, dict):
        sections = {}

    decisions = _to_dict_rows(sections.get("decisions"))
    risks = _to_dict_rows(sections.get("risks"))
    actions = _to_dict_rows(sections.get("actions"))
    follow_ups = _to_dict_rows(sections.get("follow_ups"))
    timeline = _to_dict_rows(sections.get("timeline"))
    notes = [item for item in _to_list(sections.get("notes")) if _clean_text(item)]
    governance = sections.get("governance", {})
    if not isinstance(governance, dict):
        governance = {}
    operational_summary = sections.get("operational_summary", {})
    if not isinstance(operational_summary, dict):
        operational_summary = {}
    doc_validation = sections.get("doc_validation")
    executive_brief = _build_executive_brief(sections)

    decision_groups = _decision_presentation_groups(decisions)
    risk_rows = _presentation_risks(risks)
    action_rows = _presentation_actions(actions)
    timeline_rows = _presentation_timeline(timeline)
    follow_up_items = _presentation_followups(follow_ups)
    governance_rows = [
        {"Field": "Authority Clarity", "Value": _clean_text(governance.get("authority_clarity", "unknown"))},
        {"Field": "Execution Clarity", "Value": _clean_text(governance.get("execution_clarity", "unknown"))},
        {"Field": "Primary Executor", "Value": _clean_text(governance.get("primary_executor", "")) or "Not specified"},
        {"Field": "Missing Owners", "Value": _clean_text(governance.get("missing_owners_count", 0))},
    ]
    governance_gap_rows = [{"Gap": _clean_text(item)} for item in _to_list(governance.get("key_gaps")) if _clean_text(item)]
    operational_rows = [
        {"Metric": "Blocked Decisions", "Value": _clean_text(operational_summary.get("blocked_count", 0))},
        {"Metric": "Open Dependencies", "Value": _clean_text(operational_summary.get("open_dependencies_count", 0))},
    ]
    operational_blockers = [_clean_text(item) for item in _to_list(operational_summary.get("high_blockers")) if _clean_text(item)]

    title = f"Meeting Report - {payload.get('meeting_id', '')}"
    mode = _display_mode_label(payload.get("processing_mode", ""))
    report_version = _clean_text(payload.get("header", {}).get("report_version") if isinstance(payload.get("header"), dict) else "")
    source_docs_used = [item for item in _to_list(payload.get("source_docs_used")) if _clean_text(item)]

    doc_validation_html = f"<p>{escape(_display_empty_message('doc_validation'))}</p>"
    if isinstance(doc_validation, dict):
        summary = doc_validation.get("summary", {}) if isinstance(doc_validation.get("summary"), dict) else {}
        details = doc_validation.get("details", []) if isinstance(doc_validation.get("details"), list) else []
        doc_validation_html = (
            "<h3>Summary</h3>"
            f"<p>Supported: {escape(str(summary.get('supported', 0)))} | "
            f"Not Found: {escape(str(summary.get('not_found', 0)))} | "
            f"Unclear: {escape(str(summary.get('unclear', 0)))}</p>"
            "<h3>Details</h3>"
            f"{_render_dict_table(details if details and isinstance(details[0], dict) else [], _display_empty_message('doc_validation'))}"
        )

    decision_sections_html = (
        "<h2>Decision Presentation</h2>"
        "<h3>Strategic Decisions</h3>"
        f"{_render_dict_table(decision_groups.get('Strategic Decisions', []), 'No strategic decisions available.')}"
        "<h3>Operational Actions</h3>"
        f"{_render_dict_table(decision_groups.get('Operational Actions', []), 'No operational actions available.')}"
        "<h3>Open Decisions / Pending Clarifications</h3>"
        f"{_render_dict_table(decision_groups.get('Open Decisions / Pending Clarifications', []), 'No open decisions or clarifications pending.')}"
    )

    return (
        "<!doctype html>"
        "<html><head><meta charset='utf-8'><title>"
        f"{escape(title)}"
        "</title>"
        "<style>"
        "body{font-family:Arial,sans-serif;margin:24px;color:#111827;line-height:1.45;}"
        "h1{margin-bottom:6px;}"
        "h2{font-size:22px;border-bottom:1px solid #d1d5db;padding-bottom:6px;margin-top:28px;}"
        "h3{font-size:17px;margin-top:16px;margin-bottom:6px;}"
        ".meta{color:#374151;margin:2px 0;}"
        ".brief{background:#f8fafc;border:1px solid #e5e7eb;border-left:4px solid #2563eb;border-radius:8px;padding:12px 14px;margin-top:10px;}"
        ".section{margin-top:14px;}"
        "table th{background:#f3f4f6;text-align:left;}"
        "</style></head>"
        "<body>"
        f"<h1>{escape(title)}</h1>"
        f"<p class='meta'><strong>Generated At:</strong> {escape(str(payload.get('generated_at', '')))}</p>"
        f"<p class='meta'><strong>Report Basis:</strong> {escape(mode)}</p>"
        f"<p class='meta'><strong>Report Version:</strong> {escape(report_version or 'v1')}</p>"
        "<hr/>"
        "<h2>Executive Brief</h2>"
        "<div class='brief'>"
        f"<ul>{_render_bullets(executive_brief, 'No executive brief available.')}</ul>"
        "</div>"
        f"{decision_sections_html}"
        "<h2>Governance &amp; Ownership</h2>"
        f"{_render_dict_table(governance_rows, 'No governance signals were identified.')}"
        "<h3>Key Gaps</h3>"
        f"{_render_dict_table(governance_gap_rows, 'No governance gaps were identified from available signals.')}"
        "<h2>Top Risks</h2>"
        f"{_render_dict_table(risk_rows, _display_empty_message('risks'))}"
        "<h2>Action Register</h2>"
        f"{_render_dict_table(action_rows, _display_empty_message('actions'))}"
        "<h2>Open Questions for Next Meeting</h2>"
        f"<ul>{_render_bullets(follow_up_items, _display_empty_message('follow_ups'))}</ul>"
        "<h2>Timeline &amp; Deadlines</h2>"
        f"{_render_dict_table(timeline_rows, _display_empty_message('timeline'))}"
        "<h2>Execution Status Snapshot</h2>"
        f"{_render_dict_table(operational_rows, 'No execution status metrics were identified from available signals.')}"
        "<h3>High Blockers</h3>"
        f"<ul>{_render_bullets(operational_blockers, 'No high blockers were identified from available signals.')}</ul>"
        "<h2>Appendix</h2>"
        "<h3>Source-Document Alignment</h3>"
        f"{doc_validation_html}"
        "<h3>Notes</h3>"
        f"<ul>{_render_bullets(notes, 'No additional notes were recorded.')}</ul>"
        "<h3>Provenance</h3>"
        f"<p>{escape('Source docs used: ' + (', '.join(source_docs_used) if source_docs_used else 'None'))}</p>"
        "</body></html>"
    )


def _truncate_pdf_cell_text(text: str, max_length: int = 300) -> str:
    cleaned = _clean_text(text)
    if len(cleaned) <= max_length:
        return cleaned
    return f"{cleaned[:max_length]}..."


def _pdf_column_widths(headers: list[str], usable_width: float) -> list[float]:
    key = tuple(_clean_text(item).lower() for item in headers)
    ratio_map: dict[tuple[str, ...], list[float]] = {
        ("decision", "owner", "confidence", "evidence_count"): [0.50, 0.20, 0.15, 0.15],
        ("decision", "owner", "confidence", "evidence count"): [0.50, 0.20, 0.15, 0.15],
        ("risk", "impact", "confidence"): [0.60, 0.20, 0.20],
        ("action", "owner", "deadline", "status"): [0.50, 0.20, 0.15, 0.15],
        ("event", "date", "confidence"): [0.50, 0.25, 0.25],
        ("doc_id", "status", "overlap_score", "matched_entities", "matched_phrases"): [0.20, 0.10, 0.10, 0.30, 0.30],
        ("field", "value"): [0.34, 0.66],
        ("metric", "value"): [0.45, 0.55],
        ("gap",): [1.0],
    }

    ratios = ratio_map.get(key)
    if ratios and len(ratios) == len(headers):
        return [usable_width * ratio for ratio in ratios]

    equal_width = usable_width / max(len(headers), 1)
    return [equal_width for _ in headers]


def _pdf_table_data(
    rows: list[dict[str, Any]],
    headers: list[str],
    paragraph_builder: Any,
) -> list[list[Any]]:
    table: list[list[Any]] = [headers]

    for row in rows:
        record: list[Any] = []
        for header in headers:
            value = row.get(header, "")

            if isinstance(value, list):
                text = ", ".join(_clean_text(item) for item in value if _clean_text(item))
            else:
                text = _clean_text(value)

            text = _truncate_pdf_cell_text(text)

            if isinstance(value, (int, float)) and not isinstance(value, bool):
                record.append(text)
            else:
                record.append(paragraph_builder(text))
        table.append(record)

    return table


def generate_pdf_report(payload: dict[str, Any], output_path: Path) -> dict[str, Any]:
    try:
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import LETTER
        from reportlab.lib.styles import getSampleStyleSheet
        from reportlab.platypus import (
            ListFlowable,
            ListItem,
            PageBreak,
            Paragraph,
            SimpleDocTemplate,
            Spacer,
            Table,
            TableStyle,
        )
    except Exception:
        return {
            "ok": False,
            "reason": "unavailable",
            "error": "PDF generation unavailable: reportlab is not installed.",
        }

    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        doc = SimpleDocTemplate(str(output_path), pagesize=LETTER)
        usable_width = doc.width
        styles = getSampleStyleSheet()
        story: list[Any] = []

        sections = payload.get("sections", {}) if isinstance(payload.get("sections"), dict) else {}
        title = f"Meeting Report - {_clean_text(payload.get('meeting_id', ''))}"
        report_basis = _display_mode_label(payload.get("processing_mode", ""))
        report_version = _clean_text(
            payload.get("header", {}).get("report_version")
            if isinstance(payload.get("header"), dict)
            else ""
        ) or "v1"

        decisions = _to_dict_rows(sections.get("decisions"))
        risks = _to_dict_rows(sections.get("risks"))
        actions = _to_dict_rows(sections.get("actions"))
        follow_ups = _to_dict_rows(sections.get("follow_ups"))
        timeline = _to_dict_rows(sections.get("timeline"))
        governance = sections.get("governance", {})
        if not isinstance(governance, dict):
            governance = {}
        operational_summary = sections.get("operational_summary", {})
        if not isinstance(operational_summary, dict):
            operational_summary = {}
        notes = [item for item in _to_list(sections.get("notes")) if _clean_text(item)]
        source_docs_used = [item for item in _to_list(payload.get("source_docs_used")) if _clean_text(item)]
        doc_validation = sections.get("doc_validation")

        risk_rows = _presentation_risks(risks)
        action_rows = _presentation_actions(actions)
        timeline_rows = _presentation_timeline(timeline)
        follow_up_items = _presentation_followups(follow_ups)
        executive_brief = _build_executive_brief(sections)
        executive_summary_lines = [item for item in _to_list(sections.get("executive_summary")) if _clean_text(item)]
        governance_rows = [
            {"Field": "Authority Clarity", "Value": _clean_text(governance.get("authority_clarity", "unknown"))},
            {"Field": "Execution Clarity", "Value": _clean_text(governance.get("execution_clarity", "unknown"))},
            {"Field": "Primary Executor", "Value": _clean_text(governance.get("primary_executor", "")) or "Not specified"},
            {"Field": "Missing Owners", "Value": _clean_text(governance.get("missing_owners_count", 0))},
        ]
        governance_gap_rows = [{"Gap": _clean_text(item)} for item in _to_list(governance.get("key_gaps")) if _clean_text(item)]
        operational_rows = [
            {"Metric": "Blocked Decisions", "Value": _clean_text(operational_summary.get("blocked_count", 0))},
            {"Metric": "Open Dependencies", "Value": _clean_text(operational_summary.get("open_dependencies_count", 0))},
        ]
        operational_blockers = [_clean_text(item) for item in _to_list(operational_summary.get("high_blockers")) if _clean_text(item)]

        decision_group_rows: dict[str, list[dict[str, Any]]] = {
            "Strategic Decisions": [],
            "Operational Actions": [],
            "Open Decisions / Pending Clarifications": [],
        }
        for row in decisions:
            if not isinstance(row, dict):
                continue
            group_name = _clean_text(row.get("group", ""))
            if group_name not in decision_group_rows:
                group_name = "Operational Actions"
            decision_group_rows[group_name].append(row)

        def make_cell_paragraph(text: str) -> Any:
            return Paragraph(_truncate_pdf_cell_text(text).replace("\n", "<br/>"), styles["Normal"])

        def add_table_section(title_text: str, rows: list[dict[str, Any]], headers: list[str], empty_message: str) -> None:
            story.append(Paragraph(title_text, styles["Heading3"]))
            if not rows:
                story.append(Paragraph(empty_message, styles["Normal"]))
                story.append(Spacer(1, 10))
                return
            data = _pdf_table_data(rows, headers, make_cell_paragraph)
            col_widths = _pdf_column_widths(headers, usable_width)
            table = Table(
                data,
                colWidths=col_widths,
                repeatRows=1,
                hAlign="LEFT",
            )
            table.setStyle(
                TableStyle(
                    [
                        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#e5e7eb")),
                        ("TEXTCOLOR", (0, 0), (-1, 0), colors.black),
                        ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#9ca3af")),
                        ("VALIGN", (0, 0), (-1, -1), "TOP"),
                        ("WORDWRAP", (0, 0), (-1, -1), "CJK"),
                        ("LEFTPADDING", (0, 0), (-1, -1), 6),
                        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                        ("TOPPADDING", (0, 0), (-1, -1), 4),
                        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
                        ("FONTSIZE", (0, 0), (-1, -1), 8),
                    ]
                )
            )
            story.append(table)
            story.append(Spacer(1, 10))

        def add_card(title_text: str, body_html: str, *, background: str, border: str) -> None:
            title_html = escape(_clean_text(title_text))
            card = Table(
                [[Paragraph(f"<b>{title_html}</b><br/><br/>{body_html}", styles["Normal"])]],
                colWidths=[usable_width],
                hAlign="LEFT",
            )
            card.setStyle(
                TableStyle(
                    [
                        ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor(background)),
                        ("BOX", (0, 0), (-1, -1), 0.8, colors.HexColor(border)),
                        ("LINEBEFORE", (0, 0), (0, -1), 3.0, colors.HexColor(border)),
                        ("LEFTPADDING", (0, 0), (-1, -1), 10),
                        ("RIGHTPADDING", (0, 0), (-1, -1), 10),
                        ("TOPPADDING", (0, 0), (-1, -1), 8),
                        ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
                    ]
                )
            )
            story.append(card)
            story.append(Spacer(1, 8))

        # 1. Title Page
        story.append(Paragraph("CoE Decision Intelligence", styles["Title"]))
        story.append(Spacer(1, 10))
        story.append(Paragraph(f"Meeting Report: {_clean_text(payload.get('meeting_id', ''))}", styles["Heading2"]))
        story.append(Paragraph(f"Generated At: {_clean_text(payload.get('generated_at', ''))}", styles["Normal"]))
        story.append(Paragraph(f"Report Basis: {report_basis}", styles["Normal"]))
        story.append(Paragraph(f"Report Version: {report_version}", styles["Normal"]))
        story.append(Spacer(1, 14))

        quick_scan_items = executive_brief[:3]
        quick_scan_html = "<br/>".join(f"- {escape(_clean_text(item))}" for item in quick_scan_items if _clean_text(item))
        if not quick_scan_html:
            quick_scan_html = "No executive brief available."
        add_card("30-Second Scan", quick_scan_html, background="#F8FAFC", border="#2563EB")
        story.append(PageBreak())

        # 2. Executive Summary
        story.append(Paragraph("Executive Summary", styles["Heading2"]))
        summary_items = executive_summary_lines[:7]
        if summary_items:
            bullet_items = [ListItem(Paragraph(escape(_clean_text(item)), styles["Normal"])) for item in summary_items]
            story.append(ListFlowable(bullet_items, bulletType="bullet"))
        else:
            story.append(Paragraph("No executive summary available.", styles["Normal"]))
        story.append(Spacer(1, 12))

        # 3. Key Decisions
        story.append(Paragraph("Key Decisions", styles["Heading2"]))
        has_any_decisions = any(decision_group_rows.values())
        if not has_any_decisions:
            story.append(Paragraph("No decision records available from current artifacts.", styles["Normal"]))
            story.append(Spacer(1, 10))
        else:
            for group_name in ["Strategic Decisions", "Operational Actions", "Open Decisions / Pending Clarifications"]:
                story.append(Paragraph(group_name, styles["Heading3"]))
                group_rows = decision_group_rows.get(group_name, [])
                if not group_rows:
                    story.append(Paragraph("No items in this group.", styles["Normal"]))
                    story.append(Spacer(1, 6))
                    continue
                for row in group_rows:
                    decision_text = escape(_truncate_pdf_cell_text(_clean_text(row.get("decision", "")), 260))
                    owner = escape(_clean_text(row.get("owner", "")) or "Unassigned")
                    timeline_hint = escape(_clean_text(row.get("timeline", "")) or "Not specified")
                    status_text = escape(_clean_text(row.get("status", "")) or "Pending")
                    status_key = _clean_text(row.get("status", "")).lower()
                    confidence = _clean_text(row.get("confidence", "")).lower()
                    is_missing_owner = owner == "Unassigned"
                    is_critical = status_key == "confirmed" and (
                        _clean_text(group_name).lower() == "strategic decisions" or confidence == "high"
                    )

                    if is_missing_owner:
                        bg, border = "#FFFBEB", "#D97706"
                    elif is_critical:
                        bg, border = "#ECFDF5", "#059669"
                    else:
                        bg, border = "#F8FAFC", "#64748B"

                    body_html = (
                        "<b>[DECISION]</b><br/>"
                        f"-> {decision_text}<br/><br/>"
                        "<b>[OWNER]</b><br/>"
                        f"-> {owner}<br/><br/>"
                        "<b>[TIMELINE]</b><br/>"
                        f"-> {timeline_hint}<br/><br/>"
                        "<b>[STATUS]</b><br/>"
                        f"-> {status_text}"
                    )
                    add_card(group_name, body_html, background=bg, border=border)

        # 4. Risks & Warnings
        story.append(Paragraph("Risks & Warnings", styles["Heading2"]))
        if not risks:
            story.append(Paragraph(_display_empty_message("risks"), styles["Normal"]))
            story.append(Spacer(1, 10))
        else:
            severity_rank = {"high": 3, "medium": 2, "low": 1}
            sorted_risks = sorted(
                [row for row in risks if isinstance(row, dict)],
                key=lambda row: severity_rank.get(_clean_text(row.get("severity", "low")).lower(), 1),
                reverse=True,
            )
            for row in sorted_risks[:10]:
                risk_text = escape(_truncate_pdf_cell_text(_clean_text(row.get("risk", "")), 260))
                severity = _clean_text(row.get("severity", "medium")).lower() or "medium"
                confidence = escape(_clean_text(row.get("confidence", "medium")) or "medium")
                source = escape(_clean_text(row.get("source", "")) or "unknown")
                owner = escape(_clean_text(row.get("owner", "")) or "Unassigned")

                if severity == "high":
                    bg, border, title_text = "#FEF2F2", "#DC2626", "High-Risk Warning"
                elif severity == "medium":
                    bg, border, title_text = "#FFFBEB", "#D97706", "Risk Warning"
                else:
                    bg, border, title_text = "#F8FAFC", "#64748B", "Risk Signal"

                body_html = (
                    f"<b>Risk:</b> {risk_text}<br/>"
                    f"<b>Severity:</b> {escape(severity.title())} | <b>Confidence:</b> {confidence}<br/>"
                    f"<b>Owner:</b> {owner} | <b>Source:</b> {source}"
                )
                add_card(title_text, body_html, background=bg, border=border)

        # 5. Action Plan
        story.append(Paragraph("Action Plan", styles["Heading2"]))
        add_table_section(
            "Action Register",
            action_rows[:12],
            ["Action", "Owner", "Deadline", "Status"],
            _display_empty_message("actions"),
        )
        if follow_up_items:
            story.append(Paragraph("Pending Clarifications", styles["Heading3"]))
            follow_items = [ListItem(Paragraph(escape(_clean_text(item)), styles["Normal"])) for item in follow_up_items[:8] if _clean_text(item)]
            story.append(ListFlowable(follow_items, bulletType="bullet"))
            story.append(Spacer(1, 8))

        # 6. Metadata
        story.append(Paragraph("Metadata", styles["Heading2"]))
        metadata_rows = [
            {"Field": "Meeting ID", "Value": _clean_text(payload.get("meeting_id", ""))},
            {"Field": "Generated At", "Value": _clean_text(payload.get("generated_at", ""))},
            {"Field": "Report Basis", "Value": report_basis},
            {"Field": "Report Version", "Value": report_version},
            {"Field": "Decisions Count", "Value": str(len(decisions))},
            {"Field": "Risks Count", "Value": str(len(risks))},
            {"Field": "Actions Count", "Value": str(len(actions))},
            {"Field": "Timeline Signals Count", "Value": str(len(timeline_rows))},
            {"Field": "Missing Owners", "Value": _clean_text(governance.get("missing_owners_count", 0))},
            {"Field": "Source Docs Used", "Value": ", ".join(_clean_text(item) for item in source_docs_used) if source_docs_used else "None"},
        ]
        add_table_section("Report Metadata", metadata_rows, ["Field", "Value"], "No metadata available.")
        add_table_section(
            "Execution Status Snapshot",
            operational_rows,
            ["Metric", "Value"],
            "No execution status metrics were identified from available signals.",
        )
        add_table_section(
            "Governance & Ownership",
            governance_rows,
            ["Field", "Value"],
            "No governance signals were identified.",
        )
        add_table_section(
            "Governance Gaps",
            governance_gap_rows,
            ["Gap"],
            "No governance gaps were identified from available signals.",
        )
        if operational_blockers:
            story.append(Paragraph("High Blockers", styles["Heading3"]))
            blocker_items = [ListItem(Paragraph(escape(_clean_text(item)), styles["Normal"])) for item in operational_blockers[:8]]
            story.append(ListFlowable(blocker_items, bulletType="bullet"))
            story.append(Spacer(1, 8))

        if isinstance(doc_validation, dict):
            summary = doc_validation.get("summary", {}) if isinstance(doc_validation.get("summary"), dict) else {}
            story.append(
                Paragraph(
                    "Source-Doc Validation: "
                    f"Supported={_clean_text(summary.get('supported', 0))}, "
                    f"Not Found={_clean_text(summary.get('not_found', 0))}, "
                    f"Unclear={_clean_text(summary.get('unclear', 0))}",
                    styles["Normal"],
                )
            )
            story.append(Spacer(1, 6))

        if notes:
            story.append(Paragraph("Notes", styles["Heading3"]))
            notes_items = [ListItem(Paragraph(escape(_clean_text(item)), styles["Normal"])) for item in notes if _clean_text(item)]
            story.append(ListFlowable(notes_items, bulletType="bullet"))

        doc.build(story)
        return {"ok": True, "reason": "generated", "error": None}
    except Exception as exc:
        return {
            "ok": False,
            "reason": "failed",
            "error": f"PDF generation failed: {exc}",
        }


def write_report_files(meeting_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    paths = _report_paths(meeting_id)
    paths["report_dir"].mkdir(parents=True, exist_ok=True)

    paths["report_payload"].write_text(
        json.dumps(payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    html = generate_html_report(payload)
    paths["report_html"].write_text(html, encoding="utf-8")

    pdf_result = generate_pdf_report(payload, paths["report_pdf"])
    if not pdf_result.get("ok") and paths["report_pdf"].exists():
        try:
            paths["report_pdf"].unlink()
        except Exception:
            pass

    return {
        "paths": paths,
        "pdf_result": pdf_result,
    }


def _write_metadata(meeting_id: str, metadata: dict[str, Any]) -> Path:
    paths = _report_paths(meeting_id)
    paths["report_dir"].mkdir(parents=True, exist_ok=True)
    paths["report_metadata"].write_text(
        json.dumps(metadata, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return paths["report_metadata"]


def generate_report(meeting_id: str) -> dict[str, Any]:
    mode_payload = get_processing_mode(meeting_id)
    mode = str(mode_payload.get("processing_mode", "transcript_only")).strip().lower()
    if mode not in {"transcript_only", "transcript_plus_docs"}:
        mode = "transcript_only"

    metadata: dict[str, Any] = {
        "status": "failed",
        "processing_mode": mode,
        "report_version": "v1",
        "generated_at": _now_iso(),
        "source_docs_used": [],
        "doc_validation_summary": None,
        "pdf_status": "not_attempted",
        "error": None,
    }

    try:
        payload = build_report_payload(meeting_id, mode)
        write_result = write_report_files(meeting_id, payload)
        pdf_result = write_result.get("pdf_result", {}) if isinstance(write_result, dict) else {}

        metadata["source_docs_used"] = _to_list(payload.get("source_docs_used"))
        sections = payload.get("sections", {}) if isinstance(payload.get("sections"), dict) else {}
        doc_validation = sections.get("doc_validation")
        if isinstance(doc_validation, dict):
            summary = doc_validation.get("summary", {})
            if isinstance(summary, dict):
                metadata["doc_validation_summary"] = {
                    "supported": int(summary.get("supported", 0)),
                    "not_found": int(summary.get("not_found", 0)),
                    "unclear": int(summary.get("unclear", 0)),
                }

        if isinstance(pdf_result, dict):
            reason = _clean_text(pdf_result.get("reason", ""))
            if pdf_result.get("ok"):
                metadata["status"] = "completed"
                metadata["pdf_status"] = "generated"
                metadata["error"] = None
            elif reason == "unavailable":
                metadata["status"] = "completed"
                metadata["pdf_status"] = "unavailable"
                metadata["error"] = "PDF generation unavailable, HTML available."
            else:
                metadata["status"] = "completed"
                metadata["pdf_status"] = "failed"
                metadata["error"] = "PDF generation failed, HTML available."
        else:
            metadata["status"] = "completed"
            metadata["pdf_status"] = "not_attempted"
            metadata["error"] = None

    except ReportGenerationError as exc:
        metadata["status"] = exc.status
        metadata["error"] = str(exc)
    except Exception as exc:
        metadata["status"] = "failed"
        metadata["error"] = f"Unexpected report generation error: {exc}"

    metadata_path = _write_metadata(meeting_id, metadata)
    return {
        "meeting_id": meeting_id,
        "status": metadata["status"],
        "metadata_path": str(metadata_path),
        "error": metadata["error"],
        "pdf_status": metadata.get("pdf_status", "not_attempted"),
    }
