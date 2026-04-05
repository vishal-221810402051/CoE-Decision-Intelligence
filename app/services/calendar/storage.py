from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable

from app.config import config
from app.services.calendar.schemas import (
    ApprovalLogEntry,
    ApprovalState,
    CalendarCandidate,
    CalendarCandidateMetadata,
    EligibilityStatus,
    SyncStatus,
)
from app.services.calendar.utils import now_iso


CALENDAR_DIR_NAME = "calendar"
CANDIDATES_FILE_NAME = "calendar_candidates.json"
METADATA_FILE_NAME = "calendar_candidate_metadata.json"
APPROVAL_LOG_FILE_NAME = "calendar_approval_log.jsonl"


def _calendar_dir(meeting_id: str) -> Path:
    return config.PROCESSED_PATH / str(meeting_id).strip() / CALENDAR_DIR_NAME


def _candidates_path(meeting_id: str) -> Path:
    return _calendar_dir(meeting_id) / CANDIDATES_FILE_NAME


def _metadata_path(meeting_id: str) -> Path:
    return _calendar_dir(meeting_id) / METADATA_FILE_NAME


def _approval_log_path(meeting_id: str) -> Path:
    return _calendar_dir(meeting_id) / APPROVAL_LOG_FILE_NAME


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


def load_candidates(meeting_id: str) -> list[CalendarCandidate]:
    payload = _safe_read_json(_candidates_path(meeting_id))
    rows = payload.get("candidates", [])
    if not isinstance(rows, list):
        return []
    out: list[CalendarCandidate] = []
    for row in rows:
        if isinstance(row, dict):
            out.append(CalendarCandidate.from_dict(row))
    return out


def save_candidates(meeting_id: str, candidates: Iterable[CalendarCandidate]) -> Path:
    cdir = _calendar_dir(meeting_id)
    cdir.mkdir(parents=True, exist_ok=True)
    rows = [candidate.to_dict() for candidate in candidates]
    rows.sort(
        key=lambda row: (
            str(row.get("display_date", "")).strip(),
            str(row.get("type", "")).strip(),
            str(row.get("candidate_id", "")).strip(),
        )
    )
    payload = {
        "meeting_id": str(meeting_id).strip(),
        "candidates": rows,
    }
    path = _candidates_path(meeting_id)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return path


def load_metadata(meeting_id: str) -> dict[str, Any]:
    return _safe_read_json(_metadata_path(meeting_id))


def build_metadata(
    meeting_id: str,
    candidates: list[CalendarCandidate],
    source_temporal_hash: str,
    generated_at: str | None = None,
) -> dict[str, Any]:
    generated_ts = generated_at or now_iso()
    eligible_count = sum(1 for row in candidates if row.eligibility_status == EligibilityStatus.ELIGIBLE.value)
    blocked_count = sum(1 for row in candidates if row.eligibility_status == EligibilityStatus.BLOCKED.value)
    pending_count = sum(1 for row in candidates if row.approval_state == ApprovalState.PENDING.value)
    approved_count = sum(1 for row in candidates if row.approval_state == ApprovalState.APPROVED.value)
    rejected_count = sum(1 for row in candidates if row.approval_state == ApprovalState.REJECTED.value)
    validation_passed = all(
        bool(row.candidate_id)
        and row.sync_status == SyncStatus.NOT_QUEUED.value
        and bool(row.temporal_item_id)
        for row in candidates
    )
    metadata = CalendarCandidateMetadata(
        meeting_id=str(meeting_id).strip(),
        generated_at=generated_ts,
        source_temporal_hash=str(source_temporal_hash or "").strip(),
        candidate_count=len(candidates),
        eligible_count=eligible_count,
        pending_count=pending_count,
        approved_count=approved_count,
        rejected_count=rejected_count,
        blocked_count=blocked_count,
        validation_passed=validation_passed,
    )
    return metadata.to_dict()


def save_metadata(meeting_id: str, metadata: dict[str, Any]) -> Path:
    cdir = _calendar_dir(meeting_id)
    cdir.mkdir(parents=True, exist_ok=True)
    path = _metadata_path(meeting_id)
    payload = dict(metadata) if isinstance(metadata, dict) else {}
    payload["meeting_id"] = str(meeting_id).strip()
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return path


def append_approval_log(meeting_id: str, log_entry: dict[str, Any]) -> Path:
    cdir = _calendar_dir(meeting_id)
    cdir.mkdir(parents=True, exist_ok=True)
    path = _approval_log_path(meeting_id)
    raw = dict(log_entry) if isinstance(log_entry, dict) else {}
    entry = ApprovalLogEntry(
        timestamp=str(raw.get("timestamp", now_iso())).strip() or now_iso(),
        actor=str(raw.get("actor", "unknown")).strip() or "unknown",
        source=str(raw.get("source", "dashboard_ui")).strip() or "dashboard_ui",
        candidate_id=str(raw.get("candidate_id", "")).strip(),
        old_approval_state=str(raw.get("old_approval_state", "")).strip(),
        new_approval_state=str(raw.get("new_approval_state", "")).strip(),
        reason=str(raw.get("reason", "")).strip(),
        meeting_id=str(raw.get("meeting_id", str(meeting_id).strip())).strip(),
    )
    payload = entry.to_dict()
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False) + "\n")
    return path


def calendar_paths(meeting_id: str) -> dict[str, str]:
    return {
        "calendar_dir": str(_calendar_dir(meeting_id)),
        "candidates_path": str(_candidates_path(meeting_id)),
        "metadata_path": str(_metadata_path(meeting_id)),
        "approval_log_path": str(_approval_log_path(meeting_id)),
    }
