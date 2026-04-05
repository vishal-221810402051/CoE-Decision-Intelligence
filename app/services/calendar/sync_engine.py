from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from app.integrations.google_calendar import create_google_calendar_event
from app.services.calendar.dedup import compute_dedup_key
from app.services.calendar.schemas import SyncStatus
from app.services.calendar.storage import (
    load_candidates_payload,
    load_sync_log,
    save_sync_log,
    update_candidate,
)
from app.services.calendar.utils import resolve_timezone


DEFAULT_CALENDAR_ID = "primary"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _queued_approved_candidates(meeting_id: str) -> list[dict[str, Any]]:
    payload = load_candidates_payload(meeting_id)
    rows = payload.get("candidates", [])
    if not isinstance(rows, list):
        return []
    selected: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        if str(row.get("approval_state", "")).strip() != "approved":
            continue
        if str(row.get("sync_status", "")).strip() != SyncStatus.QUEUED.value:
            continue
        selected.append(dict(row))
    return selected


def _add_1_hour(value: str) -> str:
    start = datetime.fromisoformat(value)
    end = start + timedelta(hours=1)
    return end.isoformat(timespec="seconds")


def _build_event(candidate: dict[str, Any]) -> dict[str, Any]:
    normalized = candidate.get("normalized_time", {})
    if not isinstance(normalized, dict):
        normalized = {}
    ntype = str(normalized.get("type", "")).strip().lower()
    nvalue = str(normalized.get("value", "")).strip()
    timezone_name = resolve_timezone(candidate)

    summary = str(candidate.get("title", "")).strip() or "Meeting"
    description = str(candidate.get("summary", "")).strip()

    if ntype == "exact_datetime":
        return {
            "summary": summary,
            "description": description,
            "start": {"dateTime": nvalue, "timeZone": timezone_name},
            "end": {"dateTime": _add_1_hour(nvalue), "timeZone": timezone_name},
        }

    if ntype == "exact_date":
        start_date = nvalue
        end_date = (datetime.fromisoformat(f"{start_date}T00:00:00") + timedelta(days=1)).date().isoformat()
        return {
            "summary": summary,
            "description": description,
            "start": {"date": start_date},
            "end": {"date": end_date},
        }

    raise ValueError(f"Unsupported normalized time type for sync: {ntype or 'unknown'}")


def process_calendar_sync(meeting_id: str, calendar_id: str = DEFAULT_CALENDAR_ID) -> dict[str, Any]:
    payload = load_candidates_payload(meeting_id)
    all_rows = payload.get("candidates", [])
    if not isinstance(all_rows, list):
        all_rows = []

    synced_dedup_keys: set[str] = set()
    for row in all_rows:
        if not isinstance(row, dict):
            continue
        if str(row.get("sync_status", "")).strip() != SyncStatus.SYNCED.value:
            continue
        if not str(row.get("external_event_id", "")).strip():
            continue
        synced_dedup_keys.add(compute_dedup_key(row))

    candidates = _queued_approved_candidates(meeting_id)
    sync_log = load_sync_log(meeting_id)
    result: dict[str, Any] = {
        "meeting_id": str(meeting_id).strip(),
        "attempted": 0,
        "synced": 0,
        "failed": 0,
        "skipped": 0,
        "items": [],
    }

    for candidate in candidates:
        candidate_id = str(candidate.get("candidate_id", "")).strip()
        if not candidate_id:
            continue
        dedup_key = compute_dedup_key(candidate)
        if str(candidate.get("eligibility_status", "")).strip() != "eligible":
            result["skipped"] += 1
            result["items"].append({"candidate_id": candidate_id, "status": "skipped", "reason": "not_eligible"})
            continue
        if dedup_key in synced_dedup_keys:
            print(f"[SYNC_GUARD] skipped duplicate: {candidate_id} key={dedup_key}")
            update_candidate(
                meeting_id=meeting_id,
                candidate_id=candidate_id,
                updates={
                    "sync_status": SyncStatus.SKIPPED.value,
                    "last_sync_at": _utc_now_iso(),
                    "last_sync_error": "duplicate_of_synced_candidate",
                },
            )
            sync_log.append(
                {
                    "candidate_id": candidate_id,
                    "status": SyncStatus.SKIPPED.value,
                    "timestamp": _utc_now_iso(),
                    "reason": "duplicate_of_synced_candidate",
                    "calendar_id": calendar_id,
                    "dedup_key": dedup_key,
                }
            )
            result["skipped"] += 1
            result["items"].append(
                {
                    "candidate_id": candidate_id,
                    "status": SyncStatus.SKIPPED.value,
                    "reason": "duplicate_of_synced_candidate",
                }
            )
            continue
        if str(candidate.get("external_event_id", "")).strip():
            result["skipped"] += 1
            result["items"].append({"candidate_id": candidate_id, "status": "skipped", "reason": "already_synced"})
            continue

        result["attempted"] += 1
        try:
            normalized = candidate.get("normalized_time", {})
            normalized_value = ""
            if isinstance(normalized, dict):
                normalized_value = str(normalized.get("value", "")).strip()
            timezone_name = resolve_timezone(candidate)
            print("[CALENDAR_SYNC]")
            print("datetime:", normalized_value)
            print("timezone:", timezone_name)
            event = _build_event(candidate)
            response = create_google_calendar_event(event=event, calendar_id=calendar_id)
            if bool(response.get("success")):
                event_id = str(response.get("event_id", "")).strip()
                update_candidate(
                    meeting_id=meeting_id,
                    candidate_id=candidate_id,
                    updates={
                        "sync_status": SyncStatus.SYNCED.value,
                        "external_event_id": event_id,
                        "external_calendar_id": calendar_id,
                        "last_sync_at": _utc_now_iso(),
                        "last_sync_error": "",
                    },
                )
                sync_log.append(
                    {
                        "candidate_id": candidate_id,
                        "status": SyncStatus.SYNCED.value,
                        "timestamp": _utc_now_iso(),
                        "external_event_id": event_id,
                        "calendar_id": calendar_id,
                        "dedup_key": dedup_key,
                    }
                )
                synced_dedup_keys.add(dedup_key)
                result["synced"] += 1
                result["items"].append(
                    {"candidate_id": candidate_id, "status": SyncStatus.SYNCED.value, "external_event_id": event_id}
                )
            else:
                error_text = str(response.get("error", "google_calendar_create_failed")).strip()
                update_candidate(
                    meeting_id=meeting_id,
                    candidate_id=candidate_id,
                    updates={
                        "sync_status": SyncStatus.FAILED.value,
                        "last_sync_at": _utc_now_iso(),
                        "last_sync_error": error_text,
                    },
                )
                sync_log.append(
                    {
                        "candidate_id": candidate_id,
                        "status": SyncStatus.FAILED.value,
                        "timestamp": _utc_now_iso(),
                        "error": error_text,
                        "calendar_id": calendar_id,
                    }
                )
                result["failed"] += 1
                result["items"].append(
                    {"candidate_id": candidate_id, "status": SyncStatus.FAILED.value, "error": error_text}
                )
        except Exception as exc:
            error_text = str(exc)
            update_candidate(
                meeting_id=meeting_id,
                candidate_id=candidate_id,
                updates={
                    "sync_status": SyncStatus.FAILED.value,
                    "last_sync_at": _utc_now_iso(),
                    "last_sync_error": error_text,
                },
            )
            sync_log.append(
                {
                    "candidate_id": candidate_id,
                    "status": SyncStatus.FAILED.value,
                    "timestamp": _utc_now_iso(),
                    "error": error_text,
                    "calendar_id": calendar_id,
                }
            )
            result["failed"] += 1
            result["items"].append({"candidate_id": candidate_id, "status": SyncStatus.FAILED.value, "error": error_text})

    save_sync_log(meeting_id, sync_log)
    return result
