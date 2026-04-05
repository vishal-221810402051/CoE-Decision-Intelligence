from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from app.services.calendar.google_auth import get_google_calendar_service
from app.services.calendar.schemas import SyncStatus
from app.services.calendar.storage import (
    load_candidates_payload,
    load_sync_log,
    save_sync_log,
    update_candidate,
)
from app.services.calendar.utils import resolve_timezone


DEFAULT_CALENDAR_ID = "primary"
DEFAULT_EVENT_DURATION_MINUTES = 60


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _candidate_is_syncable(candidate: dict[str, Any]) -> bool:
    return (
        str(candidate.get("approval_state", "")).strip() == "approved"
        and str(candidate.get("eligibility_status", "")).strip() == "eligible"
        and str(candidate.get("sync_status", "")).strip()
        in {SyncStatus.NOT_QUEUED.value, SyncStatus.FAILED.value}
        and not str(candidate.get("external_event_id", "")).strip()
    )


def _parse_event_start(value: str, time_of_day: str) -> datetime:
    if "T" in value:
        return datetime.fromisoformat(value)
    if time_of_day:
        return datetime.fromisoformat(f"{value}T{time_of_day}")
    return datetime.fromisoformat(f"{value}T09:00:00")


def _build_event_body(candidate: dict[str, Any]) -> dict[str, Any]:
    normalized = candidate.get("normalized_time", {})
    if not isinstance(normalized, dict):
        normalized = {}
    value = str(normalized.get("value", "")).strip()
    time_of_day = str(normalized.get("time_of_day", "")).strip()
    tz = resolve_timezone(candidate)
    all_day = bool(candidate.get("all_day", True))

    description_parts = [
        f"Meeting ID: {candidate.get('meeting_id', '')}",
        f"Temporal Item ID: {candidate.get('temporal_item_id', '')}",
        f"Confidence: {candidate.get('confidence', '')}",
        f"Certainty: {candidate.get('certainty_class', '')}",
        f"Support Level: {candidate.get('support_level', '')}",
        f"Evidence: {candidate.get('evidence_span', '')}",
    ]
    blockers = candidate.get("blockers", [])
    if isinstance(blockers, list) and blockers:
        description_parts.append(f"Blockers at generation: {', '.join(str(x) for x in blockers)}")

    body: dict[str, Any] = {
        "summary": str(candidate.get("title", "")).strip() or "Meeting Event",
        "description": "\n".join(description_parts),
    }

    if all_day:
        start_date = value
        start_dt = datetime.fromisoformat(f"{start_date}T00:00:00")
        end_date = (start_dt + timedelta(days=1)).date().isoformat()
        body["start"] = {"date": start_date}
        body["end"] = {"date": end_date}
    else:
        start_dt = _parse_event_start(value, time_of_day)
        end_dt = start_dt + timedelta(minutes=DEFAULT_EVENT_DURATION_MINUTES)
        body["start"] = {"dateTime": start_dt.isoformat(), "timeZone": tz}
        body["end"] = {"dateTime": end_dt.isoformat(), "timeZone": tz}

    return body


def sync_approved_candidates(meeting_id: str, calendar_id: str = DEFAULT_CALENDAR_ID) -> dict[str, Any]:
    payload = load_candidates_payload(meeting_id)
    candidates = payload.get("candidates", [])
    if not isinstance(candidates, list):
        candidates = []

    service = get_google_calendar_service()
    sync_log = load_sync_log(meeting_id)

    result: dict[str, Any] = {
        "meeting_id": str(meeting_id).strip(),
        "attempted": 0,
        "synced": 0,
        "failed": 0,
        "skipped": 0,
        "items": [],
    }

    for row in candidates:
        candidate = dict(row) if isinstance(row, dict) else {}
        candidate_id = str(candidate.get("candidate_id", "")).strip()
        if not candidate_id:
            continue

        if not _candidate_is_syncable(candidate):
            result["skipped"] += 1
            result["items"].append(
                {
                    "candidate_id": candidate_id,
                    "status": SyncStatus.SKIPPED.value,
                    "reason": "not_syncable",
                }
            )
            continue

        result["attempted"] += 1
        update_candidate(
            meeting_id,
            candidate_id,
            {
                "sync_status": SyncStatus.QUEUED.value,
                "last_sync_at": _utc_now_iso(),
                "last_sync_error": "",
            },
        )

        try:
            normalized = candidate.get("normalized_time", {})
            normalized_value = ""
            if isinstance(normalized, dict):
                normalized_value = str(normalized.get("value", "")).strip()
            timezone_name = resolve_timezone(candidate)
            print("[CALENDAR_SYNC]")
            print("datetime:", normalized_value)
            print("timezone:", timezone_name)
            body = _build_event_body(candidate)
            created = service.events().insert(calendarId=calendar_id, body=body).execute()
            external_event_id = str(created.get("id", "")).strip()

            update_candidate(
                meeting_id,
                candidate_id,
                {
                    "sync_status": SyncStatus.SYNCED.value,
                    "external_event_id": external_event_id,
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
                    "external_event_id": external_event_id,
                    "calendar_id": calendar_id,
                }
            )
            result["synced"] += 1
            result["items"].append(
                {
                    "candidate_id": candidate_id,
                    "status": SyncStatus.SYNCED.value,
                    "external_event_id": external_event_id,
                }
            )
        except Exception as exc:
            update_candidate(
                meeting_id,
                candidate_id,
                {
                    "sync_status": SyncStatus.FAILED.value,
                    "last_sync_at": _utc_now_iso(),
                    "last_sync_error": str(exc),
                },
            )
            sync_log.append(
                {
                    "candidate_id": candidate_id,
                    "status": SyncStatus.FAILED.value,
                    "timestamp": _utc_now_iso(),
                    "error": str(exc),
                    "calendar_id": calendar_id,
                }
            )
            result["failed"] += 1
            result["items"].append(
                {
                    "candidate_id": candidate_id,
                    "status": SyncStatus.FAILED.value,
                    "error": str(exc),
                }
            )

    save_sync_log(meeting_id, sync_log)
    return result
