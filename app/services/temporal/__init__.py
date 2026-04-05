from __future__ import annotations

import json
import re
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from app.config import DATA_PATH


TEMPORAL_DIR_NAME = "temporal"
TEMPORAL_OUTPUT_FILE = "temporal_intelligence.json"
TEMPORAL_METADATA_FILE = "temporal_metadata.json"

TEMPORAL_ITEM_TYPES = {
    "meeting",
    "follow_up",
    "deadline",
    "reminder",
    "milestone",
    "time_window",
    "tentative_date",
    "unresolved_temporal_question",
}
TEMPORAL_CERTAINTY = {"direct", "conditional", "uncertain"}
TEMPORAL_CONFIDENCE = {"high", "medium", "low"}
NORMALIZED_TYPES = {"exact_date", "exact_datetime", "date_range", "relative_time", "unresolved_text"}
PARSE_STATUS = {"parsed", "partial", "unresolved"}

UNCERTAIN_MARKERS = (
    "maybe",
    "perhaps",
    "might",
    "could",
    "potentially",
    "possibly",
    "i think",
    "not sure",
    "tentative",
    "we need to check",
    "need to define",
)
CONDITIONAL_MARKERS = (
    "if",
    "depends",
    "not final",
    "not a yes",
    "yes, no, maybe",
    "subject to",
)
FOLLOWUP_MARKERS = (
    "follow-up",
    "follow up",
    "next meeting",
    "meet again",
    "we'll meet",
    "let's meet",
    "schedule",
)
MEETING_MARKERS = (
    "meeting",
    "meet",
    "call",
)
DEADLINE_MARKERS = (
    "deadline",
    "before",
    "by",
    "due",
    "must be done by",
)
REMINDER_MARKERS = (
    "remind",
    "remember",
    "check in",
)
MILESTONE_MARKERS = (
    "phase",
    "long-term",
    "long term",
    "years",
    "months",
    "quarter",
)
WINDOW_MARKERS = (
    "first week",
    "second week",
    "end of",
    "start of",
    "between",
)
QUESTION_MARKERS = ("?", "what", "when", "how soon", "which date")
RELATIVE_MARKERS = (
    "a week",
    "in a week",
    "next week",
    "five to one week",
    "soon",
    "later",
)

MONTH_TO_NUM = {
    "january": 1,
    "jan": 1,
    "february": 2,
    "feb": 2,
    "march": 3,
    "mar": 3,
    "april": 4,
    "apr": 4,
    "may": 5,
    "june": 6,
    "jun": 6,
    "july": 7,
    "jul": 7,
    "august": 8,
    "aug": 8,
    "september": 9,
    "sep": 9,
    "sept": 9,
    "october": 10,
    "oct": 10,
    "november": 11,
    "nov": 11,
    "december": 12,
    "dec": 12,
}
WEEKDAY_TO_NUM = {
    "monday": 0,
    "tuesday": 1,
    "wednesday": 2,
    "thursday": 3,
    "friday": 4,
    "saturday": 5,
    "sunday": 6,
}

MONTH_NAME_PATTERN = r"(?:january|jan|february|feb|march|mar|april|apr|may|june|jun|july|jul|august|aug|september|sep|sept|october|oct|november|nov|december|dec)"
WEEKDAY_NAME_PATTERN = r"(?:monday|tuesday|wednesday|thursday|friday|saturday|sunday)"


def _safe_read_json(path: Path) -> dict[str, Any]:
    try:
        if not path.exists() or not path.is_file():
            return {}
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
        if isinstance(payload, dict):
            return payload
    except Exception:
        return {}
    return {}


def _safe_read_text(path: Path) -> str:
    try:
        if not path.exists() or not path.is_file():
            return ""
        return path.read_text(encoding="utf-8")
    except Exception:
        return ""


def _now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


def _norm_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _slug(value: str) -> str:
    lowered = _norm_text(value).lower()
    lowered = re.sub(r"[^a-z0-9]+", "-", lowered).strip("-")
    return lowered or "na"


def _contains_any(text: str, words: tuple[str, ...]) -> bool:
    lowered = text.lower()
    for word in words:
        token = _norm_text(word).lower()
        if not token:
            continue
        if re.fullmatch(r"[^\w\s]+", token):
            if token in lowered:
                return True
            continue
        pattern = rf"(?<!\w){re.escape(token)}(?!\w)"
        if re.search(pattern, lowered):
            return True
    return False


def _parse_date_like(value: str) -> date | None:
    text = _norm_text(value)
    if not text:
        return None
    for parser in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            return datetime.strptime(text, parser).date()
        except Exception:
            continue
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
    except Exception:
        return None


def _month_number(token: str) -> int | None:
    text = _norm_text(token).lower().replace(".", "")
    if not text:
        return None
    if text in MONTH_TO_NUM:
        return MONTH_TO_NUM[text]
    if len(text) >= 3 and text[:3] in MONTH_TO_NUM:
        return MONTH_TO_NUM[text[:3]]
    return None


def _derive_anchor_date(meeting_dir: Path, intelligence: dict[str, Any]) -> date | None:
    candidates: list[str] = []
    if isinstance(intelligence.get("meeting_context"), dict):
        candidates.append(str(intelligence.get("meeting_context", {}).get("meeting_datetime", "")))
    intake_path = meeting_dir / "metadata" / "intake.json"
    intake = _safe_read_json(intake_path)
    if intake:
        candidates.append(str(intake.get("created_at", "")))

    for candidate in candidates:
        parsed = _parse_date_like(candidate)
        if parsed is not None:
            return parsed
    return None


def _pick_evidence_span(*values: Any) -> str:
    for value in values:
        text = _norm_text(value)
        if text:
            return text
    return ""


def _is_meeting_intent(intent: str) -> bool:
    lowered = _norm_text(intent).lower()
    return _contains_any(lowered, MEETING_MARKERS) or _contains_any(lowered, FOLLOWUP_MARKERS)


def _is_relative_phrase(text: str) -> bool:
    lowered = _norm_text(text).lower()
    return _contains_any(lowered, RELATIVE_MARKERS)


def _is_boundary_deadline_phrase(text: str) -> bool:
    lowered = _norm_text(text).lower()
    if any(token in lowered for token in DEADLINE_MARKERS):
        return True
    if re.search(r"\bby\s+\d{4}-\d{2}-\d{2}\b", lowered):
        return True
    if re.search(r"\bbefore\s+[a-z0-9]+\b", lowered):
        return True
    return False


def _has_explicit_calendar_marker(text: str) -> bool:
    lowered = _norm_text(text).lower()
    if re.search(r"\b\d{4}-\d{2}-\d{2}\b", lowered):
        return True
    if re.search(r"\b\d{1,2}:\d{2}\b", lowered):
        return True
    if re.search(r"\b(?:monday|tuesday|wednesday|thursday|friday|saturday|sunday)\b", lowered):
        return True
    if re.search(r"\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*\b", lowered):
        return True
    return False


def _extract_from_decision(decision: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    records = decision.get("decision_records", [])
    if not isinstance(records, list):
        return out
    for rec in records:
        if not isinstance(rec, dict):
            continue
        intent = _norm_text(rec.get("statement", ""))
        rid = _norm_text(rec.get("decision_id", ""))
        meeting_intent = _is_meeting_intent(intent)
        for signal in rec.get("timeline_signals", []):
            if not isinstance(signal, dict):
                continue
            source_signal_type = _norm_text(signal.get("signal_type", "")).lower()
            raw_reference = _norm_text(signal.get("raw_reference") or signal.get("signal") or "")
            evidence_span = _pick_evidence_span(
                signal.get("evidence_span"),
                signal.get("raw_reference"),
            )
            if not raw_reference or not evidence_span:
                continue
            if meeting_intent and source_signal_type == "start_window":
                continue
            if not meeting_intent and source_signal_type == "followup_marker":
                continue
            out.append(
                {
                    "raw_reference": raw_reference,
                    "evidence_span": evidence_span,
                    "source_artifact": "decision.decision_records.timeline_signals",
                    "source_priority": 1,
                    "intent": intent or raw_reference,
                    "source_signal_type": source_signal_type,
                    "source_confidence": _norm_text(signal.get("confidence", "")).lower(),
                    "source_support_level": _norm_text(signal.get("support_level", "")),
                    "source_evidence_confidence": signal.get("evidence_confidence", 0.0),
                    "decision_id": rid,
                }
            )
        for dep in rec.get("dependencies", []):
            if not isinstance(dep, dict):
                continue
            if _norm_text(dep.get("type", "")).lower() != "timeline_dependency":
                continue
            reason = _norm_text(dep.get("reason", ""))
            evidence_span = _pick_evidence_span(dep.get("evidence_span"), reason)
            if not reason or not evidence_span:
                continue
            out.append(
                {
                    "raw_reference": reason,
                    "evidence_span": evidence_span,
                    "source_artifact": "decision.decision_records.dependencies.timeline_dependency",
                    "source_priority": 1,
                    "intent": intent or reason,
                    "source_signal_type": "timeline_dependency",
                    "source_confidence": "medium",
                    "source_support_level": _norm_text(dep.get("support_level", "")),
                    "source_evidence_confidence": dep.get("evidence_confidence", 0.0),
                    "decision_id": rid,
                }
            )
    return out


def _extract_from_intelligence_deadlines(intelligence: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in intelligence.get("deadlines", []) if isinstance(intelligence.get("deadlines"), list) else []:
        if not isinstance(row, dict):
            continue
        event = _norm_text(row.get("event", ""))
        date_text = _norm_text(row.get("date", ""))
        evidence_span = _pick_evidence_span(row.get("evidence_span"), row.get("evidence"), date_text)
        raw_reference = date_text or event
        if not raw_reference or not evidence_span:
            continue
        out.append(
            {
                "raw_reference": raw_reference,
                "evidence_span": evidence_span,
                "source_artifact": "intelligence.deadlines",
                "source_priority": 2,
                "intent": event or raw_reference,
                "source_signal_type": "deadline_hint",
                "source_confidence": _norm_text(row.get("support_level", "")).lower(),
                "source_support_level": _norm_text(row.get("support_level", "")),
                "source_evidence_confidence": row.get("evidence_confidence", 0.0),
            }
        )
    return out


def _extract_from_intelligence_timeline(intelligence: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    rows = intelligence.get("timeline_mentions", [])
    if not isinstance(rows, list):
        return out
    for row in rows:
        if not isinstance(row, dict):
            continue
        raw_reference = _norm_text(row.get("raw_time_reference") or row.get("text") or "")
        evidence_span = _pick_evidence_span(row.get("evidence_span"), row.get("evidence"), raw_reference)
        if not raw_reference or not evidence_span:
            continue
        out.append(
            {
                "raw_reference": raw_reference,
                "evidence_span": evidence_span,
                "source_artifact": "intelligence.timeline_mentions",
                "source_priority": 3,
                "intent": _norm_text(row.get("text", "")) or raw_reference,
                "source_signal_type": "timeline_mention",
                "source_confidence": _norm_text(row.get("support_level", "")).lower(),
                "source_support_level": _norm_text(row.get("support_level", "")),
                "source_evidence_confidence": row.get("evidence_confidence", 0.0),
            }
        )
    return out


def _looks_timing_related(text: str) -> bool:
    lowered = text.lower()
    return any(
        token in lowered
        for token in (
            "timeline",
            "time",
            "date",
            "schedule",
            "start",
            "deadline",
            "follow-up",
            "follow up",
            "meeting",
            "week",
            "month",
            "april",
            "may",
            "june",
            "tuesday",
        )
    )


def _extract_from_executive(executive: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []

    for row in executive.get("negotiation_flags", []) if isinstance(executive.get("negotiation_flags"), list) else []:
        if not isinstance(row, dict):
            continue
        topic = _norm_text(row.get("topic", ""))
        reason = _norm_text(row.get("reason", ""))
        context = f"{topic} {reason}".strip()
        if not _looks_timing_related(context):
            continue
        evidence_span = _pick_evidence_span(row.get("evidence_span"), row.get("evidence"), context)
        raw_reference = topic or reason
        if not raw_reference or not evidence_span:
            continue
        out.append(
            {
                "raw_reference": raw_reference,
                "evidence_span": evidence_span,
                "source_artifact": "executive.negotiation_flags",
                "source_priority": 4,
                "intent": topic or raw_reference,
                "source_signal_type": "executive_flag",
                "source_confidence": _norm_text(row.get("confidence", "")).lower(),
                "source_support_level": _norm_text(row.get("support_level", "")),
                "source_evidence_confidence": row.get("evidence_confidence", 0.0),
                "source_status": _norm_text(row.get("status", "")).lower(),
            }
        )

    for row in executive.get("executive_warnings", []) if isinstance(executive.get("executive_warnings"), list) else []:
        if not isinstance(row, dict):
            continue
        warning = _norm_text(row.get("warning", ""))
        reason = _norm_text(row.get("reason", ""))
        context = f"{warning} {reason}".strip()
        if not _looks_timing_related(context):
            continue
        evidence_span = _pick_evidence_span(row.get("evidence_span"), row.get("evidence"), context)
        raw_reference = warning or reason
        if not raw_reference or not evidence_span:
            continue
        out.append(
            {
                "raw_reference": raw_reference,
                "evidence_span": evidence_span,
                "source_artifact": "executive.executive_warnings",
                "source_priority": 4,
                "intent": warning or raw_reference,
                "source_signal_type": "executive_warning",
                "source_confidence": _norm_text(row.get("confidence", "")).lower(),
                "source_support_level": _norm_text(row.get("support_level", "")),
                "source_evidence_confidence": row.get("evidence_confidence", 0.0),
            }
        )

    for row in executive.get("recommended_next_questions", []) if isinstance(executive.get("recommended_next_questions"), list) else []:
        if not isinstance(row, dict):
            continue
        question = _norm_text(row.get("question", ""))
        if not _looks_timing_related(question):
            continue
        evidence_span = _pick_evidence_span(row.get("evidence_span"), row.get("evidence"), question)
        if not question or not evidence_span:
            continue
        out.append(
            {
                "raw_reference": question,
                "evidence_span": evidence_span,
                "source_artifact": "executive.recommended_next_questions",
                "source_priority": 4,
                "intent": question,
                "source_signal_type": "recommended_question",
                "source_confidence": _norm_text(row.get("priority", "medium")).lower(),
                "source_support_level": _norm_text(row.get("support_level", "")),
                "source_evidence_confidence": row.get("evidence_confidence", 0.0),
            }
        )

    return out


BACKFILL_PATTERNS: tuple[tuple[str, str], ...] = (
    (r"\bfirst\s+of\s+may\b", "start_window"),
    (r"\bfirst\s+week\s+of\s+may\b", "start_window"),
    (r"\bfirst\s+june\b", "start_window"),
    (r"\bmaybe\s+june\b", "tentative"),
    (r"\bend\s+of\s+april\b", "deadline_hint"),
    (r"\btuesday\s+after\s+5:30\b", "followup_marker"),
    (r"\bafter\s+5:30\b", "followup_marker"),
    (r"\btuesday\b", "followup_marker"),
    (r"\bnext\s+meeting\b", "followup_marker"),
    (r"\bfive\s+to\s+one\s+week\b", "relative"),
    (r"\ba\s+week\b", "relative"),
    (r"\bfour\s+years\b", "milestone"),
)

EXPLICIT_DATE_PATTERNS: tuple[str, ...] = (
    rf"\b\d{{1,2}}(?:st|nd|rd|th)?(?:\s+of)?\s+{MONTH_NAME_PATTERN}\b",
    rf"\b{MONTH_NAME_PATTERN}\s+\d{{1,2}}(?:st|nd|rd|th)?\b",
    rf"\b(?:next\s+)?{WEEKDAY_NAME_PATTERN}\b",
    r"\b\d{4}-\d{2}-\d{2}\b",
)

EXPLICIT_TIME_PATTERNS: tuple[str, ...] = (
    r"\b\d{1,2}:\d{2}\s*(?:a\.?m\.?|p\.?m\.?)\b",
    r"\b\d{1,2}\s*(?:a\.?m\.?|p\.?m\.?)\b",
)

MEETING_INTENT_PATTERNS: tuple[str, ...] = (
    r"\bnext\s+meeting\b",
    r"\bmeet(?:-up)?\s+on\b",
    r"\bschedule(?:d)?\s+for\b",
    r"\bfollow[\s-]?up\s+on\b",
    r"\blet'?s\s+meet\b",
    r"\bwe\s+are\s+having\s+a\s+meet(?:-up)?\b",
    r"\bwe\s+have\s+a\s+meeting\b",
)


def _split_sentences(text: str) -> list[str]:
    chunks = [x.strip() for x in re.split(r"(?<=[.!?])\s+|\n+", text) if x.strip()]
    return chunks


def _collect_pattern_matches(text: str, patterns: tuple[str, ...]) -> list[str]:
    matches: list[str] = []
    for pattern in patterns:
        for match in re.finditer(pattern, text, flags=re.IGNORECASE):
            raw = _norm_text(match.group(0)).strip(" ,;")
            if raw and raw.lower() not in [x.lower() for x in matches]:
                matches.append(raw)
    return matches


def _has_explicit_meeting_intent(text: str) -> bool:
    lowered = _norm_text(text).lower()
    if _is_meeting_intent(lowered):
        return True
    return any(re.search(pattern, lowered, flags=re.IGNORECASE) for pattern in MEETING_INTENT_PATTERNS)


def _extract_explicit_transcript_temporal(transcript_text: str, covered_raw_refs: set[str]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    if not transcript_text.strip():
        return out

    for sentence in _split_sentences(transcript_text):
        sentence_text = _norm_text(sentence)
        if not sentence_text:
            continue

        has_meeting_intent = _has_explicit_meeting_intent(sentence_text)
        date_refs = _collect_pattern_matches(sentence_text, EXPLICIT_DATE_PATTERNS)
        time_refs = _collect_pattern_matches(sentence_text, EXPLICIT_TIME_PATTERNS)

        if not date_refs:
            continue

        for date_ref in date_refs:
            raw_candidates = [date_ref]
            if has_meeting_intent and time_refs:
                raw_candidates.insert(0, f"{date_ref} {time_refs[0]}")

            raw_reference = ""
            for candidate_raw in raw_candidates:
                lowered = _norm_text(candidate_raw).lower()
                if lowered and lowered not in covered_raw_refs:
                    raw_reference = _norm_text(candidate_raw)
                    break

            if not raw_reference:
                continue

            out.append(
                {
                    "raw_reference": raw_reference,
                    "evidence_span": sentence_text,
                    "source_artifact": "transcript.explicit_fallback",
                    "source_priority": 5,
                    "intent": sentence_text[:200],
                    "source_signal_type": "followup_marker" if has_meeting_intent else "timeline_mention",
                    "source_confidence": "high" if has_meeting_intent else "medium",
                    "source_support_level": "DIRECTLY_SUPPORTED",
                    "source_evidence_confidence": 1.0,
                }
            )
            covered_raw_refs.add(raw_reference.lower())
            covered_raw_refs.add(date_ref.lower())

    return out


def _extract_from_transcript_backfill(transcript_clean: str, covered_raw_refs: set[str]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    if not transcript_clean.strip():
        return out
    sentences = _split_sentences(transcript_clean)
    for sentence in sentences:
        lowered_sentence = sentence.lower()
        for pattern, signal_type in BACKFILL_PATTERNS:
            for match in re.finditer(pattern, sentence, flags=re.IGNORECASE):
                raw = _norm_text(match.group(0))
                if not raw:
                    continue
                if _norm_text(raw).lower() in covered_raw_refs:
                    continue
                out.append(
                    {
                        "raw_reference": raw,
                        "evidence_span": _norm_text(sentence),
                        "source_artifact": "transcript.clean.backfill",
                        "source_priority": 5,
                        "intent": _norm_text(sentence)[:200],
                        "source_signal_type": signal_type,
                        "source_confidence": "medium" if "maybe" not in lowered_sentence else "low",
                        "source_support_level": "DIRECTLY_SUPPORTED",
                        "source_evidence_confidence": 1.0,
                    }
                )
                covered_raw_refs.add(raw.lower())
    return out


def _classify_temporal_type(candidate: dict[str, Any]) -> str:
    raw = _norm_text(candidate.get("raw_reference", ""))
    evidence = _norm_text(candidate.get("evidence_span", ""))
    intent = _norm_text(candidate.get("intent", ""))
    source_signal_type = _norm_text(candidate.get("source_signal_type", "")).lower()
    merged = f"{raw} {evidence} {intent}".lower()

    if source_signal_type == "recommended_question" and _contains_any(merged, QUESTION_MARKERS):
        return "unresolved_temporal_question"
    if source_signal_type in {"executive_warning", "timeline_dependency"}:
        return "reminder"
    if _contains_any(merged, UNCERTAIN_MARKERS) and _has_explicit_calendar_marker(merged):
        return "tentative_date"
    if source_signal_type in {"followup_marker"} or (_is_meeting_intent(intent) and _has_explicit_calendar_marker(merged)):
        if re.search(r"\b(?:monday|tuesday|wednesday|thursday|friday|saturday|sunday)\b", merged) or re.search(r"\bafter\s+\d{1,2}:\d{2}\b", merged):
            return "meeting"
        return "follow_up"
    if source_signal_type in {"deadline_hint"} and _is_boundary_deadline_phrase(merged):
        return "deadline"
    if source_signal_type in {"start_window"}:
        return "time_window"
    if source_signal_type in {"milestone", "timeline_mention"} and _contains_any(merged, MILESTONE_MARKERS):
        return "milestone"

    if _contains_any(merged, QUESTION_MARKERS) and _looks_timing_related(merged):
        return "unresolved_temporal_question"
    if _contains_any(merged, UNCERTAIN_MARKERS) and _looks_timing_related(merged):
        return "tentative_date"
    if _contains_any(merged, FOLLOWUP_MARKERS):
        return "follow_up"
    if _is_boundary_deadline_phrase(merged):
        return "deadline"
    if _contains_any(merged, WINDOW_MARKERS):
        return "time_window"
    if _is_relative_phrase(merged):
        if _is_meeting_intent(intent):
            return "follow_up"
        return "reminder"
    if _contains_any(merged, REMINDER_MARKERS):
        return "reminder"
    if _contains_any(merged, MILESTONE_MARKERS):
        return "milestone"
    return "reminder"


def _extract_time_of_day(text: str) -> str:
    ampm_match = re.search(r"\b(\d{1,2})(?::(\d{2}))?\s*(a\.?m\.?|p\.?m\.?)\b", text, flags=re.IGNORECASE)
    if ampm_match:
        hh = int(ampm_match.group(1))
        mm = int(ampm_match.group(2) or "00")
        meridiem = ampm_match.group(3).lower().replace(".", "")
        if 1 <= hh <= 12 and 0 <= mm <= 59:
            if meridiem.startswith("p") and hh != 12:
                hh += 12
            if meridiem.startswith("a") and hh == 12:
                hh = 0
            return f"{hh:02d}:{mm:02d}"

    match = re.search(r"\b(\d{1,2}):(\d{2})\b", text)
    if not match:
        return ""
    hh = int(match.group(1))
    mm = int(match.group(2))
    if 0 <= hh <= 23 and 0 <= mm <= 59:
        return f"{hh:02d}:{mm:02d}"
    return ""


def _normalize_time(raw_reference: str, anchor_date: date | None) -> dict[str, Any]:
    raw = _norm_text(raw_reference)
    lowered = raw.lower()
    anchor = anchor_date.isoformat() if anchor_date else ""
    year = anchor_date.year if anchor_date else datetime.utcnow().year

    iso_dt = re.search(r"\b(\d{4})-(\d{2})-(\d{2})[t\s](\d{2}):(\d{2})(?::(\d{2}))?\b", lowered)
    if iso_dt:
        value = f"{iso_dt.group(1)}-{iso_dt.group(2)}-{iso_dt.group(3)}T{iso_dt.group(4)}:{iso_dt.group(5)}:{iso_dt.group(6) or '00'}"
        return {
            "type": "exact_datetime",
            "value": value,
            "anchor": anchor,
            "time_of_day": f"{iso_dt.group(4)}:{iso_dt.group(5)}",
            "timezone": "unknown",
            "parse_status": "parsed",
            "temporal_granularity": "exact",
        }

    iso = re.search(r"\b(\d{4})-(\d{2})-(\d{2})\b", lowered)
    if iso:
        value = f"{iso.group(1)}-{iso.group(2)}-{iso.group(3)}"
        tod = _extract_time_of_day(lowered)
        return {
            "type": "exact_datetime" if tod else "exact_date",
            "value": f"{value}T{tod}:00" if tod else value,
            "anchor": anchor,
            "time_of_day": tod,
            "timezone": "unknown",
            "parse_status": "parsed",
            "temporal_granularity": "exact",
        }

    day_month = re.search(rf"\b(\d{{1,2}})(?:st|nd|rd|th)?(?:\s+of)?\s+({MONTH_NAME_PATTERN})\b", lowered)
    if day_month:
        d = int(day_month.group(1))
        m = _month_number(day_month.group(2))
        if m is not None:
            try:
                resolved_date = date(year, m, d).isoformat()
                tod = _extract_time_of_day(lowered)
                return {
                    "type": "exact_datetime" if tod else "exact_date",
                    "value": f"{resolved_date}T{tod}:00" if tod else resolved_date,
                    "anchor": anchor,
                    "time_of_day": tod,
                    "timezone": "unknown",
                    "parse_status": "parsed",
                    "temporal_granularity": "exact",
                }
            except ValueError:
                pass

    month_day = re.search(rf"\b({MONTH_NAME_PATTERN})\s+(\d{{1,2}})(?:st|nd|rd|th)?\b", lowered)
    if month_day:
        m = _month_number(month_day.group(1))
        d = int(month_day.group(2))
        if m is not None:
            try:
                resolved_date = date(year, m, d).isoformat()
                tod = _extract_time_of_day(lowered)
                return {
                    "type": "exact_datetime" if tod else "exact_date",
                    "value": f"{resolved_date}T{tod}:00" if tod else resolved_date,
                    "anchor": anchor,
                    "time_of_day": tod,
                    "timezone": "unknown",
                    "parse_status": "parsed",
                    "temporal_granularity": "exact",
                }
            except ValueError:
                pass

    maybe_month = re.search(r"\bmaybe\s+([a-z]+)\b", lowered)
    if maybe_month and _month_number(maybe_month.group(1)) is not None:
        return {
            "type": "unresolved_text",
            "value": raw,
            "anchor": anchor,
            "time_of_day": _extract_time_of_day(lowered),
            "timezone": "unknown",
            "parse_status": "partial",
            "temporal_granularity": "unknown",
        }

    first_month = re.search(r"\bfirst(?:\s+of)?\s+([a-z]+)\b", lowered)
    if first_month and _month_number(first_month.group(1)) is not None:
        m = int(_month_number(first_month.group(1)))
        value = date(year, m, 1).isoformat()
        parse_status = "partial" if "maybe" in lowered else "parsed"
        return {
            "type": "exact_date" if parse_status == "parsed" else "unresolved_text",
            "value": value if parse_status == "parsed" else raw,
            "anchor": anchor,
            "time_of_day": _extract_time_of_day(lowered),
            "timezone": "unknown",
            "parse_status": parse_status,
            "temporal_granularity": "exact" if parse_status == "parsed" else "unknown",
        }

    first_week = re.search(r"\bfirst\s+week\s+of\s+([a-z]+)\b", lowered)
    if first_week and _month_number(first_week.group(1)) is not None:
        m = int(_month_number(first_week.group(1)))
        start = date(year, m, 1)
        end = date(year, m, 7)
        return {
            "type": "date_range",
            "value": f"{start.isoformat()}..{end.isoformat()}",
            "anchor": anchor,
            "time_of_day": _extract_time_of_day(lowered),
            "timezone": "unknown",
            "parse_status": "parsed",
            "temporal_granularity": "window",
        }

    end_of = re.search(r"\bend\s+of\s+([a-z]+)\b", lowered)
    if end_of and _month_number(end_of.group(1)) is not None:
        m = int(_month_number(end_of.group(1)))
        next_month = date(year + (1 if m == 12 else 0), 1 if m == 12 else m + 1, 1)
        month_end = next_month - timedelta(days=1)
        window_start = month_end - timedelta(days=6)
        return {
            "type": "date_range",
            "value": f"{window_start.isoformat()}..{month_end.isoformat()}",
            "anchor": anchor,
            "time_of_day": _extract_time_of_day(lowered),
            "timezone": "unknown",
            "parse_status": "parsed",
            "temporal_granularity": "window",
        }

    weekday_match = re.search(r"\b(monday|tuesday|wednesday|thursday|friday|saturday|sunday)\b", lowered)
    if weekday_match:
        day = weekday_match.group(1)
        day_value = day
        if anchor_date:
            offset = (WEEKDAY_TO_NUM[day] - anchor_date.weekday()) % 7
            offset = 7 if offset == 0 else offset
            day_value = (anchor_date + timedelta(days=offset)).isoformat()
        tod = _extract_time_of_day(lowered)
        if anchor_date:
            return {
                "type": "exact_datetime" if tod else "exact_date",
                "value": f"{day_value}T{tod}:00" if tod else day_value,
                "anchor": anchor,
                "time_of_day": tod,
                "timezone": "unknown",
                "parse_status": "parsed",
                "temporal_granularity": "exact",
            }
        suffix = f" {tod}" if tod else ""
        return {
            "type": "relative_time",
            "value": f"{day_value}{suffix}",
            "anchor": anchor,
            "time_of_day": tod,
            "timezone": "unknown",
            "parse_status": "partial",
            "temporal_granularity": "relative",
        }

    if re.search(r"\bin\s+a?\s*week\b|\ba\s+week\b", lowered):
        return {
            "type": "relative_time",
            "value": "P1W",
            "anchor": anchor,
            "time_of_day": _extract_time_of_day(lowered),
            "timezone": "unknown",
            "parse_status": "partial",
            "temporal_granularity": "relative",
        }
    if re.search(r"\bfive\s+to\s+one\s+week\b", lowered):
        return {
            "type": "relative_time",
            "value": "P1W..P5W",
            "anchor": anchor,
            "time_of_day": _extract_time_of_day(lowered),
            "timezone": "unknown",
            "parse_status": "partial",
            "temporal_granularity": "relative",
        }

    return {
        "type": "unresolved_text",
        "value": raw,
        "anchor": anchor,
        "time_of_day": _extract_time_of_day(lowered),
        "timezone": "unknown",
        "parse_status": "unresolved",
        "temporal_granularity": "unknown",
    }


def _infer_certainty(candidate: dict[str, Any], temporal_type: str) -> str:
    text = f"{_norm_text(candidate.get('raw_reference'))} {_norm_text(candidate.get('evidence_span'))}".lower()
    if _contains_any(text, CONDITIONAL_MARKERS):
        return "conditional"
    if _contains_any(text, UNCERTAIN_MARKERS):
        return "uncertain"
    support = _norm_text(candidate.get("source_support_level", "")).upper()
    if support == "WEAK_INFERENCE":
        return "uncertain"
    if temporal_type in {"tentative_date", "unresolved_temporal_question"}:
        return "uncertain"
    if temporal_type == "time_window":
        return "conditional"
    return "direct"


def _source_confidence_rank(candidate: dict[str, Any]) -> int:
    value = candidate.get("source_evidence_confidence", 0.0)
    try:
        numeric = float(value)
    except Exception:
        numeric = 0.0
    if numeric >= 0.85:
        return 3
    if numeric >= 0.6:
        return 2
    if numeric > 0:
        return 1
    text = _norm_text(candidate.get("source_confidence", "")).lower()
    if text == "high":
        return 3
    if text in {"medium", "acceptable_inference"}:
        return 2
    if text in {"low", "weak_inference"}:
        return 1
    return 1


def _infer_confidence(candidate: dict[str, Any], normalized_time: dict[str, Any], certainty: str) -> str:
    rank = _source_confidence_rank(candidate)
    parse_status = _norm_text(normalized_time.get("parse_status", "")).lower()

    if parse_status == "unresolved":
        rank = min(rank, 1)
    elif parse_status == "partial":
        rank = min(rank, 2)

    if certainty == "uncertain":
        rank = min(rank, 1)
    elif certainty == "conditional":
        rank = min(rank, 2)

    if rank >= 3:
        return "high"
    if rank == 2:
        return "medium"
    return "low"


def _compute_calendar_readiness(item: dict[str, Any]) -> tuple[bool, list[str]]:
    blockers: list[str] = []
    ttype = _norm_text(item.get("type", "")).lower()
    certainty = _norm_text(item.get("certainty_class", "")).lower()
    confidence = _norm_text(item.get("confidence", "")).lower()
    ntime = item.get("normalized_time", {})
    parse_status = _norm_text(ntime.get("parse_status", "")).lower() if isinstance(ntime, dict) else ""
    ntype = _norm_text(ntime.get("type", "")).lower() if isinstance(ntime, dict) else ""
    evidence = _norm_text(item.get("evidence_span", ""))

    if not evidence:
        blockers.append("missing_evidence")
    if ttype not in {"meeting", "follow_up", "deadline", "reminder"}:
        blockers.append("no_actionable_type")
    if certainty != "direct":
        blockers.append("uncertain_or_conditional")
    if confidence == "low":
        blockers.append("low_confidence")
    if parse_status != "parsed":
        blockers.append("temporal_not_fully_normalized")
    if ntype == "unresolved_text":
        blockers.append("unresolved_time_expression")
    if ttype in {"meeting", "follow_up"}:
        merged = f"{_norm_text(item.get('raw_reference', ''))} {evidence}".lower()
        if not _has_explicit_calendar_marker(merged):
            blockers.append("missing_explicit_meeting_time")

    return len(blockers) == 0, sorted(set(blockers))


def _build_title(temporal_type: str, intent: str, raw_reference: str) -> str:
    prefix = {
        "meeting": "Meeting",
        "follow_up": "Follow-Up",
        "deadline": "Deadline",
        "reminder": "Reminder",
        "milestone": "Milestone",
        "time_window": "Time Window",
        "tentative_date": "Tentative Date",
        "unresolved_temporal_question": "Open Timing Question",
    }.get(temporal_type, "Temporal Item")
    return f"{prefix}: {intent or raw_reference}"


def _recommended_action(temporal_type: str, certainty: str, normalized_time: dict[str, Any]) -> str:
    parse_status = _norm_text(normalized_time.get("parse_status", "")).lower()
    if temporal_type in {"unresolved_temporal_question", "tentative_date"}:
        return "Clarify exact date/time in next discussion."
    if certainty != "direct":
        return "Validate timing before scheduling."
    if parse_status != "parsed":
        return "Resolve temporal ambiguity and then schedule."
    if temporal_type in {"meeting", "follow_up"}:
        return "Prepare a calendar candidate slot."
    if temporal_type == "deadline":
        return "Track as a deadline reminder."
    return "Track as temporal reference."


def _build_temporal_item(idx: int, candidate: dict[str, Any], anchor_date: date | None) -> dict[str, Any] | None:
    raw_reference = _norm_text(candidate.get("raw_reference", ""))
    evidence_span = _norm_text(candidate.get("evidence_span", ""))
    source_artifact = _norm_text(candidate.get("source_artifact", ""))
    if not raw_reference or not evidence_span or not source_artifact:
        return None

    temporal_type = _classify_temporal_type(candidate)
    normalized_time = _normalize_time(raw_reference, anchor_date)
    certainty = _infer_certainty(candidate, temporal_type)
    confidence = _infer_confidence(candidate, normalized_time, certainty)
    intent = _norm_text(candidate.get("intent", "")) or raw_reference
    calendar_ready, blockers = _compute_calendar_readiness(
        {
            "type": temporal_type,
            "certainty_class": certainty,
            "confidence": confidence,
            "normalized_time": normalized_time,
            "evidence_span": evidence_span,
        }
    )

    item_id = f"T{idx:03d}-{_slug(intent)[:18]}"
    return {
        "item_id": item_id,
        "type": temporal_type,
        "title": _build_title(temporal_type, intent, raw_reference),
        "intent": intent,
        "raw_reference": raw_reference,
        "normalized_time": normalized_time,
        "certainty_class": certainty,
        "confidence": confidence,
        "support_level": _norm_text(candidate.get("source_support_level", "")) or "DIRECTLY_SUPPORTED",
        "evidence_span": evidence_span,
        "source_artifacts": [source_artifact],
        "recommended_action": _recommended_action(temporal_type, certainty, normalized_time),
        "calendar_ready": calendar_ready,
        "calendar_blockers": blockers,
        "source_priority": int(candidate.get("source_priority", 5)),
    }


def _dedupe_key(item: dict[str, Any]) -> str:
    ntime = item.get("normalized_time", {})
    if not isinstance(ntime, dict):
        ntime = {}
    ntype = _norm_text(ntime.get("type", "")).lower()
    nvalue = _norm_text(ntime.get("value", "")).lower()
    intent = _norm_text(item.get("intent", "")).lower()
    ttype = _norm_text(item.get("type", "")).lower()
    return f"{ttype}|{ntype}|{nvalue}|{intent}"


def _confidence_rank(value: str) -> int:
    if value == "high":
        return 3
    if value == "medium":
        return 2
    return 1


def _certainty_rank(value: str) -> int:
    if value == "direct":
        return 3
    if value == "conditional":
        return 2
    return 1


def _pick_best(primary: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
    p_conf = _confidence_rank(_norm_text(primary.get("confidence", "")).lower())
    i_conf = _confidence_rank(_norm_text(incoming.get("confidence", "")).lower())
    if i_conf > p_conf:
        return incoming
    if i_conf < p_conf:
        return primary
    p_cert = _certainty_rank(_norm_text(primary.get("certainty_class", "")).lower())
    i_cert = _certainty_rank(_norm_text(incoming.get("certainty_class", "")).lower())
    if i_cert > p_cert:
        return incoming
    if i_cert < p_cert:
        return primary
    p_pri = int(primary.get("source_priority", 5))
    i_pri = int(incoming.get("source_priority", 5))
    if i_pri < p_pri:
        return incoming
    return primary


def _signal_strength(item: dict[str, Any]) -> int:
    ntime = item.get("normalized_time", {})
    if not isinstance(ntime, dict):
        ntime = {}
    ntype = _norm_text(ntime.get("type", "")).lower()
    parse_status = _norm_text(ntime.get("parse_status", "")).lower()
    merged = f"{_norm_text(item.get('raw_reference', ''))} {_norm_text(item.get('evidence_span', ''))}".lower()

    if ntype in {"exact_date", "exact_datetime"} and parse_status == "parsed":
        strength = 5
    elif ntype == "date_range" and parse_status in {"parsed", "partial"}:
        strength = 3
    elif ntype == "relative_time" and parse_status == "parsed":
        strength = 2
    elif ntype == "relative_time" and parse_status == "partial":
        strength = 1
    else:
        strength = 0

    if _norm_text(item.get("type", "")).lower() in {"meeting", "follow_up"} and _has_explicit_calendar_marker(merged):
        strength = min(6, strength + 1)
    return strength


def _merge_lists(a: list[Any], b: list[Any]) -> list[Any]:
    merged: list[Any] = []
    for value in list(a) + list(b):
        text = _norm_text(value)
        if text and text not in merged:
            merged.append(text)
    return merged


def _consolidate_meeting_followups(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    passthrough: list[dict[str, Any]] = []
    for item in items:
        ttype = _norm_text(item.get("type", "")).lower()
        if ttype not in {"meeting", "follow_up"}:
            passthrough.append(item)
            continue
        intent_key = _norm_text(item.get("intent", "")).lower()
        grouped.setdefault(intent_key, []).append(item)

    collapsed: list[dict[str, Any]] = []
    for _, rows in grouped.items():
        if len(rows) == 1:
            collapsed.append(rows[0])
            continue
        ordered = sorted(
            rows,
            key=lambda x: (
                -_signal_strength(x),
                -_confidence_rank(_norm_text(x.get("confidence", "")).lower()),
                int(x.get("source_priority", 5)),
            ),
        )
        primary = ordered[0]
        supporting: list[str] = []
        for candidate in ordered[1:]:
            top_strength = _signal_strength(primary)
            cand_strength = _signal_strength(candidate)
            top_val = _normalized_value(primary)
            cand_val = _normalized_value(candidate)
            cand_ntype = _norm_text(candidate.get("normalized_time", {}).get("type", "") if isinstance(candidate.get("normalized_time"), dict) else "").lower()
            if (
                cand_val == top_val
                or cand_strength <= max(1, top_strength - 2)
                or cand_ntype in {"unresolved_text", "relative_time"}
            ):
                supporting.append(_norm_text(candidate.get("raw_reference", "")))
                primary["source_artifacts"] = _merge_lists(
                    primary.get("source_artifacts", []),
                    candidate.get("source_artifacts", []),
                )
                continue
            collapsed.append(candidate)
        if supporting:
            primary["supporting_references"] = _merge_lists(
                primary.get("supporting_references", []),
                supporting,
            )
        collapsed.append(primary)

    return passthrough + collapsed


def _dedupe_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_key: dict[str, dict[str, Any]] = {}
    for item in items:
        key = _dedupe_key(item)
        if key not in by_key:
            by_key[key] = item
            continue

        current = by_key[key]
        winner = _pick_best(current, item)
        loser = item if winner is current else current
        winner["source_artifacts"] = _merge_lists(
            winner.get("source_artifacts", []),
            loser.get("source_artifacts", []),
        )
        winner["calendar_blockers"] = sorted(
            set(
                _merge_lists(
                    winner.get("calendar_blockers", []),
                    loser.get("calendar_blockers", []),
                )
            )
        )
        by_key[key] = winner

    result = list(by_key.values())
    result.sort(
        key=lambda x: (
            _norm_text(x.get("type", "")).lower(),
            _norm_text(x.get("intent", "")).lower(),
            _norm_text(x.get("normalized_time", {}).get("value", "") if isinstance(x.get("normalized_time"), dict) else "").lower(),
        )
    )
    for idx, item in enumerate(result, start=1):
        item["item_id"] = f"T{idx:03d}-{_slug(_norm_text(item.get('intent', '')) or _norm_text(item.get('raw_reference', '')))[:18]}"
        item.pop("source_priority", None)
    return result


def _normalized_value(item: dict[str, Any]) -> str:
    ntime = item.get("normalized_time", {})
    if not isinstance(ntime, dict):
        return ""
    return f"{_norm_text(ntime.get('type', '')).lower()}::{_norm_text(ntime.get('value', '')).lower()}"


def _detect_conflicts(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_intent: dict[str, list[dict[str, Any]]] = {}
    for item in items:
        intent_key = _norm_text(item.get("intent", "")).lower()
        if not intent_key:
            continue
        by_intent.setdefault(intent_key, []).append(item)

    conflicts: list[dict[str, Any]] = []
    for intent_key, rows in by_intent.items():
        actionable = [r for r in rows if _norm_text(r.get("type", "")).lower() in {"meeting", "follow_up", "deadline"}]
        if len(actionable) < 2:
            continue
        ordered = sorted(actionable, key=lambda r: _signal_strength(r), reverse=True)
        strongest = ordered[0]
        strong_value = _normalized_value(strongest)
        strong_strength = _signal_strength(strongest)

        contenders: list[dict[str, Any]] = [strongest]
        for candidate in ordered[1:]:
            cand_value = _normalized_value(candidate)
            cand_strength = _signal_strength(candidate)
            if not cand_value or cand_value == strong_value:
                continue
            if cand_strength >= max(3, strong_strength - 1):
                contenders.append(candidate)

        values = sorted({_normalized_value(row) for row in contenders if _normalized_value(row)})
        if len(values) <= 1:
            continue

        conflict_id = f"C-{_slug(intent_key)[:16]}-{len(conflicts)+1:02d}"
        conflicts.append(
            {
                "conflict_id": conflict_id,
                "intent": strongest.get("intent", ""),
                "item_ids": sorted([_norm_text(x.get("item_id", "")) for x in contenders if _norm_text(x.get("item_id", ""))]),
                "values": values,
                "reason": "Conflicting normalized times detected for the same intent.",
            }
        )
    conflicts.sort(key=lambda x: (_norm_text(x.get("intent", "")).lower(), _norm_text(x.get("conflict_id", "")).lower()))
    return conflicts


def _apply_conflicts_to_items(items: list[dict[str, Any]], conflicts: list[dict[str, Any]]) -> None:
    blocked_ids: set[str] = set()
    for conflict in conflicts:
        for item_id in conflict.get("item_ids", []):
            if isinstance(item_id, str) and item_id.strip():
                blocked_ids.add(item_id.strip())
    for item in items:
        iid = _norm_text(item.get("item_id", ""))
        if iid in blocked_ids:
            item["calendar_ready"] = False
            blockers = item.get("calendar_blockers", [])
            if not isinstance(blockers, list):
                blockers = []
            blockers.append("conflict_detected")
            item["calendar_blockers"] = sorted(set(_merge_lists(blockers, [])))


def _is_transcript_backed(evidence_span: str, transcript_clean: str, transcript_raw: str) -> bool:
    ev = _norm_text(evidence_span)
    if not ev:
        return False
    return ev in transcript_clean or ev in transcript_raw


def _validate_temporal_payload(payload: dict[str, Any]) -> tuple[bool, list[str]]:
    errors: list[str] = []
    if not isinstance(payload, dict):
        return False, ["payload must be object"]

    required_top = {"meeting_id", "generated_at", "schema_version", "anchor_meeting_date", "items", "conflicts", "summary"}
    missing = sorted(required_top - set(payload.keys()))
    if missing:
        errors.append(f"missing top-level keys: {', '.join(missing)}")

    items = payload.get("items", [])
    if not isinstance(items, list):
        errors.append("items must be an array")
        items = []
    for idx, item in enumerate(items):
        prefix = f"items[{idx}]"
        if not isinstance(item, dict):
            errors.append(f"{prefix} must be object")
            continue
        if _norm_text(item.get("type", "")) not in TEMPORAL_ITEM_TYPES:
            errors.append(f"{prefix}.type invalid")
        if _norm_text(item.get("certainty_class", "")) not in TEMPORAL_CERTAINTY:
            errors.append(f"{prefix}.certainty_class invalid")
        if _norm_text(item.get("confidence", "")) not in TEMPORAL_CONFIDENCE:
            errors.append(f"{prefix}.confidence invalid")
        if not _norm_text(item.get("raw_reference", "")):
            errors.append(f"{prefix}.raw_reference missing")
        if not _norm_text(item.get("evidence_span", "")):
            errors.append(f"{prefix}.evidence_span missing")
        ntime = item.get("normalized_time")
        if not isinstance(ntime, dict):
            errors.append(f"{prefix}.normalized_time missing")
            continue
        if _norm_text(ntime.get("type", "")) not in NORMALIZED_TYPES:
            errors.append(f"{prefix}.normalized_time.type invalid")
        if _norm_text(ntime.get("parse_status", "")) not in PARSE_STATUS:
            errors.append(f"{prefix}.normalized_time.parse_status invalid")
        if _norm_text(ntime.get("temporal_granularity", "")) not in {"exact", "window", "relative", "unknown"}:
            errors.append(f"{prefix}.normalized_time.temporal_granularity invalid")

    conflicts = payload.get("conflicts", [])
    if not isinstance(conflicts, list):
        errors.append("conflicts must be an array")

    summary = payload.get("summary", {})
    if not isinstance(summary, dict):
        errors.append("summary must be an object")
    else:
        for key in ("item_count", "calendar_ready_count", "conflict_count"):
            if not isinstance(summary.get(key), int):
                errors.append(f"summary.{key} must be int")

    return len(errors) == 0, errors


def generate_temporal_intelligence(meeting_id: str) -> dict[str, Any]:
    started = time.time()
    meeting_dir = Path(DATA_PATH) / "processed" / str(meeting_id).strip()
    temporal_dir = meeting_dir / TEMPORAL_DIR_NAME
    temporal_path = temporal_dir / TEMPORAL_OUTPUT_FILE
    metadata_path = temporal_dir / TEMPORAL_METADATA_FILE
    temporal_dir.mkdir(parents=True, exist_ok=True)

    intelligence = _safe_read_json(meeting_dir / "intelligence" / "intelligence.json")
    decision = _safe_read_json(meeting_dir / "decision" / "decision_intelligence_v2.json")
    executive = _safe_read_json(meeting_dir / "executive" / "executive_intelligence.json")
    transcript_clean = _safe_read_text(meeting_dir / "transcript" / "transcript_clean.txt")
    transcript_raw = _safe_read_text(meeting_dir / "transcript" / "transcript_raw.txt")

    anchor_date = _derive_anchor_date(meeting_dir, intelligence)

    candidates: list[dict[str, Any]] = []
    candidates.extend(_extract_from_decision(decision))
    candidates.extend(_extract_from_intelligence_deadlines(intelligence))
    candidates.extend(_extract_from_intelligence_timeline(intelligence))
    candidates.extend(_extract_from_executive(executive))

    covered = {_norm_text(c.get("raw_reference", "")).lower() for c in candidates if _norm_text(c.get("raw_reference", ""))}
    candidates.extend(_extract_explicit_transcript_temporal(transcript_clean, covered))
    candidates.extend(_extract_explicit_transcript_temporal(transcript_raw, covered))
    candidates.extend(_extract_from_transcript_backfill(transcript_clean, covered))

    built_items: list[dict[str, Any]] = []
    for idx, candidate in enumerate(candidates, start=1):
        item = _build_temporal_item(idx=idx, candidate=candidate, anchor_date=anchor_date)
        if item is not None:
            built_items.append(item)

    transcript_backed_items = [
        item
        for item in built_items
        if _is_transcript_backed(_norm_text(item.get("evidence_span", "")), transcript_clean, transcript_raw)
    ]

    deduped = _dedupe_items(transcript_backed_items)
    deduped = _consolidate_meeting_followups(deduped)
    deduped = _dedupe_items(deduped)
    conflicts = _detect_conflicts(deduped)
    _apply_conflicts_to_items(deduped, conflicts)

    payload = {
        "meeting_id": str(meeting_id).strip(),
        "generated_at": _now_iso(),
        "schema_version": "phase11_v1",
        "anchor_meeting_date": anchor_date.isoformat() if anchor_date else "",
        "items": deduped,
        "conflicts": conflicts,
        "summary": {
            "item_count": len(deduped),
            "calendar_ready_count": sum(1 for x in deduped if bool(x.get("calendar_ready"))),
            "conflict_count": len(conflicts),
        },
    }

    validation_passed, validation_errors = _validate_temporal_payload(payload)
    metadata = {
        "meeting_id": str(meeting_id).strip(),
        "status": "completed" if validation_passed else "failed",
        "generated_at": payload["generated_at"],
        "processing_time_seconds": round(time.time() - started, 3),
        "item_count": payload["summary"]["item_count"],
        "calendar_ready_count": payload["summary"]["calendar_ready_count"],
        "conflict_count": payload["summary"]["conflict_count"],
        "validation_passed": validation_passed,
        "validation_errors": validation_errors,
        "source_artifacts": {
            "intelligence": str(meeting_dir / "intelligence" / "intelligence.json"),
            "decision": str(meeting_dir / "decision" / "decision_intelligence_v2.json"),
            "executive": str(meeting_dir / "executive" / "executive_intelligence.json"),
            "transcript_clean": str(meeting_dir / "transcript" / "transcript_clean.txt"),
            "transcript_raw": str(meeting_dir / "transcript" / "transcript_raw.txt"),
        },
    }

    temporal_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    metadata_path.write_text(json.dumps(metadata, indent=2, ensure_ascii=False), encoding="utf-8")

    return {
        "meeting_id": str(meeting_id).strip(),
        "status": metadata["status"],
        "temporal_path": str(temporal_path),
        "metadata_path": str(metadata_path),
        "item_count": metadata["item_count"],
        "calendar_ready_count": metadata["calendar_ready_count"],
        "conflict_count": metadata["conflict_count"],
        "validation_passed": metadata["validation_passed"],
    }
