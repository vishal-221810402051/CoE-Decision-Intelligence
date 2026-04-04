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
    rows: list[dict[str, Any]] = []
    for record in _to_list(decision_data.get("decision_records")):
        if not isinstance(record, dict):
            continue
        statement = _clean_text(record.get("statement", ""))
        if not statement:
            continue

        owner = _clean_text(record.get("primary_owner", ""))
        confidence = _clean_text(record.get("confidence", "low")).lower() or "low"
        evidence_count = len(_to_list(record.get("evidence")))

        rows.append(
            {
                "decision": statement,
                "owner": owner,
                "confidence": confidence,
                "evidence_count": evidence_count,
            }
        )
    return rows


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

    executive_summary_seed = ""
    if isinstance(executive.get("executive_summary"), dict):
        executive_summary_seed = _clean_text(
            executive.get("executive_summary", {}).get("meaning_of_meeting", "")
        )
        intent = _clean_text(executive.get("executive_summary", {}).get("intent", ""))
        commitment = _clean_text(executive.get("executive_summary", {}).get("commitment", ""))
        if intent:
            executive_summary_seed = f"{executive_summary_seed}. {intent}" if executive_summary_seed else intent
        if commitment:
            executive_summary_seed = f"{executive_summary_seed}. {commitment}" if executive_summary_seed else commitment
    if not executive_summary_seed:
        executive_summary_seed = _clean_text(intelligence.get("summary", ""))

    executive_summary_bullets = _split_to_bullets(executive_summary_seed, minimum=3, maximum=5)
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


def _render_dict_table(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "<p>None</p>"
    headers = list(rows[0].keys())
    header_html = "".join(f"<th>{escape(str(item))}</th>" for item in headers)
    body_rows: list[str] = []
    for row in rows:
        cells = "".join(f"<td>{escape(str(row.get(item, '')))}</td>" for item in headers)
        body_rows.append(f"<tr>{cells}</tr>")
    return (
        "<table style='width:100%;border-collapse:collapse;' border='1' cellpadding='6'>"
        f"<thead><tr>{header_html}</tr></thead>"
        f"<tbody>{''.join(body_rows)}</tbody>"
        "</table>"
    )


def _render_bullets(items: list[str]) -> str:
    if not items:
        return "<li>None</li>"
    return "".join(f"<li>{escape(_clean_text(item))}</li>" for item in items if _clean_text(item))


def generate_html_report(payload: dict[str, Any]) -> str:
    sections = payload.get("sections", {}) if isinstance(payload, dict) else {}
    if not isinstance(sections, dict):
        sections = {}

    decisions = sections.get("decisions", [])
    risks = sections.get("risks", [])
    actions = sections.get("actions", [])
    timeline = sections.get("timeline", [])
    notes = sections.get("notes", [])
    executive_summary = sections.get("executive_summary", [])
    doc_validation = sections.get("doc_validation")

    title = f"Meeting Report - {payload.get('meeting_id', '')}"
    mode = _clean_text(payload.get("processing_mode", ""))

    doc_validation_html = "<p>Not applicable for transcript-only mode.</p>"
    if isinstance(doc_validation, dict):
        summary = doc_validation.get("summary", {}) if isinstance(doc_validation.get("summary"), dict) else {}
        details = doc_validation.get("details", []) if isinstance(doc_validation.get("details"), list) else []
        doc_validation_html = (
            "<h3>Summary</h3>"
            f"<p>Supported: {escape(str(summary.get('supported', 0)))} | "
            f"Not Found: {escape(str(summary.get('not_found', 0)))} | "
            f"Unclear: {escape(str(summary.get('unclear', 0)))}</p>"
            "<h3>Details</h3>"
            f"{_render_dict_table(details if details and isinstance(details[0], dict) else [])}"
        )

    return (
        "<!doctype html>"
        "<html><head><meta charset='utf-8'><title>"
        f"{escape(title)}"
        "</title></head><body style='font-family:Arial,sans-serif;margin:24px;color:#1f2937;'>"
        f"<h1>{escape(title)}</h1>"
        f"<p><strong>Generated At:</strong> {escape(str(payload.get('generated_at', '')))}</p>"
        f"<p><strong>Processing Mode:</strong> {escape(mode)}</p>"
        "<hr/>"
        "<h2>Executive Summary</h2>"
        f"<ul>{_render_bullets(executive_summary if isinstance(executive_summary, list) else [])}</ul>"
        "<h2>Decisions</h2>"
        f"{_render_dict_table(decisions if decisions and isinstance(decisions[0], dict) else [])}"
        "<h2>Risks</h2>"
        f"{_render_dict_table(risks if risks and isinstance(risks[0], dict) else [])}"
        "<h2>Actions</h2>"
        f"{_render_dict_table(actions if actions and isinstance(actions[0], dict) else [])}"
        "<h2>Timeline</h2>"
        f"{_render_dict_table(timeline if timeline and isinstance(timeline[0], dict) else [])}"
        "<h2>Notes</h2>"
        f"<ul>{_render_bullets(notes if isinstance(notes, list) else [])}</ul>"
        "<h2>Source Document Validation</h2>"
        f"{doc_validation_html}"
        "</body></html>"
    )


def _truncate_pdf_cell_text(text: str, max_length: int = 300) -> str:
    cleaned = _clean_text(text)
    if len(cleaned) <= max_length:
        return cleaned
    return f"{cleaned[:max_length]}..."


def _pdf_column_widths(headers: list[str], usable_width: float) -> list[float]:
    key = tuple(headers)
    ratio_map: dict[tuple[str, ...], list[float]] = {
        ("decision", "owner", "confidence", "evidence_count"): [0.50, 0.20, 0.15, 0.15],
        ("risk", "severity", "confidence", "owner", "mitigation"): [0.45, 0.15, 0.10, 0.15, 0.15],
        ("action", "owner", "status", "due_hint"): [0.50, 0.20, 0.15, 0.15],
        ("signal", "type", "confidence"): [0.50, 0.25, 0.25],
        ("doc_id", "status", "overlap_score", "matched_entities", "matched_phrases"): [0.20, 0.10, 0.10, 0.30, 0.30],
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

        story.append(Paragraph(title, styles["Title"]))
        story.append(Spacer(1, 8))
        story.append(Paragraph(f"Generated At: {_clean_text(payload.get('generated_at', ''))}", styles["Normal"]))
        story.append(Paragraph(f"Processing Mode: {_clean_text(payload.get('processing_mode', ''))}", styles["Normal"]))
        story.append(Spacer(1, 12))

        story.append(Paragraph("Executive Summary", styles["Heading2"]))
        executive_bullets = sections.get("executive_summary", []) if isinstance(sections.get("executive_summary"), list) else []
        if executive_bullets:
            bullet_items = [ListItem(Paragraph(_clean_text(item), styles["Normal"])) for item in executive_bullets if _clean_text(item)]
            story.append(ListFlowable(bullet_items, bulletType="bullet"))
        else:
            story.append(Paragraph("No executive summary available.", styles["Normal"]))
        story.append(Spacer(1, 12))

        def make_cell_paragraph(text: str) -> Any:
            return Paragraph(_truncate_pdf_cell_text(text), styles["Normal"])

        def add_table_section(title_text: str, rows: list[dict[str, Any]], headers: list[str]) -> None:
            story.append(Paragraph(title_text, styles["Heading2"]))
            if not rows:
                story.append(Paragraph("None", styles["Normal"]))
                story.append(Spacer(1, 8))
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

        decisions = sections.get("decisions", []) if isinstance(sections.get("decisions"), list) else []
        add_table_section("Decisions", decisions, ["decision", "owner", "confidence", "evidence_count"])

        risks = sections.get("risks", []) if isinstance(sections.get("risks"), list) else []
        add_table_section("Risks", risks, ["risk", "severity", "confidence", "owner", "mitigation"])

        actions = sections.get("actions", []) if isinstance(sections.get("actions"), list) else []
        add_table_section("Actions", actions, ["action", "owner", "status", "due_hint"])

        timeline = sections.get("timeline", []) if isinstance(sections.get("timeline"), list) else []
        add_table_section("Timeline", timeline, ["signal", "type", "confidence"])

        story.append(Paragraph("Notes", styles["Heading2"]))
        notes = sections.get("notes", []) if isinstance(sections.get("notes"), list) else []
        for note in notes:
            note_text = _clean_text(note)
            if note_text:
                story.append(Paragraph(f"- {note_text}", styles["Normal"]))
        story.append(Spacer(1, 10))

        story.append(Paragraph("Source Document Validation", styles["Heading2"]))
        doc_validation = sections.get("doc_validation")
        if isinstance(doc_validation, dict):
            summary = doc_validation.get("summary", {}) if isinstance(doc_validation.get("summary"), dict) else {}
            story.append(
                Paragraph(
                    "Supported: "
                    f"{_clean_text(summary.get('supported', 0))} | "
                    "Not Found: "
                    f"{_clean_text(summary.get('not_found', 0))} | "
                    "Unclear: "
                    f"{_clean_text(summary.get('unclear', 0))}",
                    styles["Normal"],
                )
            )
            details = doc_validation.get("details", []) if isinstance(doc_validation.get("details"), list) else []
            add_table_section(
                "Validation Details",
                details,
                ["doc_id", "status", "overlap_score", "matched_entities", "matched_phrases"],
            )
        else:
            story.append(Paragraph("Not applicable for transcript-only mode.", styles["Normal"]))

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
