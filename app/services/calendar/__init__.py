from __future__ import annotations

from typing import Any

from app.services.calendar.approval_manager import (
    approve_candidate,
    reject_candidate,
    reset_candidate_to_pending,
)
from app.services.calendar.candidate_builder import build_calendar_candidates, load_temporal_payload
from app.services.calendar.schemas import CalendarCandidate
from app.services.calendar.storage import (
    append_approval_log,
    build_metadata,
    calendar_paths,
    load_candidates,
    load_metadata,
    save_candidates,
    save_metadata,
)
from app.services.calendar.utils import compute_temporal_hash, now_iso, reconcile_candidate_with_previous


def _to_map(candidates: list[CalendarCandidate]) -> dict[str, CalendarCandidate]:
    return {candidate.candidate_id: candidate for candidate in candidates}


def generate_candidates(meeting_id: str) -> dict[str, Any]:
    meeting_key = str(meeting_id).strip()
    now_ts = now_iso()

    incoming_candidates = build_calendar_candidates(meeting_key)
    existing_candidates = load_candidates(meeting_key)
    existing_map = _to_map(existing_candidates)

    merged_candidates: list[CalendarCandidate] = []
    for incoming in incoming_candidates:
        existing = existing_map.get(incoming.candidate_id)
        reconciled = reconcile_candidate_with_previous(
            incoming=incoming,
            existing=existing,
            now_ts=now_ts,
        )
        if existing is not None and existing.candidate_hash != reconciled.candidate_hash:
            append_approval_log(
                meeting_key,
                {
                    "timestamp": now_ts,
                    "actor": "system",
                    "source": "candidate_regeneration",
                    "candidate_id": reconciled.candidate_id,
                    "old_approval_state": existing.approval_state,
                    "new_approval_state": reconciled.approval_state,
                    "reason": "candidate_content_changed_regeneration",
                    "meeting_id": meeting_key,
                },
            )
        merged_candidates.append(reconciled)

    merged_candidates.sort(
        key=lambda row: (
            row.display_date,
            row.type,
            row.title,
            row.candidate_id,
        )
    )

    candidates_path = save_candidates(meeting_key, merged_candidates)
    temporal_payload = load_temporal_payload(meeting_key)
    temporal_hash = compute_temporal_hash(temporal_payload)
    metadata = build_metadata(
        meeting_id=meeting_key,
        candidates=merged_candidates,
        source_temporal_hash=temporal_hash,
        generated_at=now_ts,
    )
    metadata_path = save_metadata(meeting_key, metadata)
    paths = calendar_paths(meeting_key)

    return {
        "status": "completed",
        "meeting_id": meeting_key,
        "candidate_count": metadata.get("candidate_count", 0),
        "eligible_count": metadata.get("eligible_count", 0),
        "pending_count": metadata.get("pending_count", 0),
        "approved_count": metadata.get("approved_count", 0),
        "rejected_count": metadata.get("rejected_count", 0),
        "blocked_count": metadata.get("blocked_count", 0),
        "source_temporal_hash": temporal_hash,
        "generated_at": metadata.get("generated_at", now_ts),
        "candidates_path": str(candidates_path),
        "metadata_path": str(metadata_path),
        "approval_log_path": str(paths["approval_log_path"]),
    }


def approve(
    meeting_id: str,
    candidate_id: str,
    actor: str = "dashboard_ui",
    source: str = "dashboard_ui",
    note: str = "",
) -> dict[str, Any]:
    return approve_candidate(
        meeting_id=meeting_id,
        candidate_id=candidate_id,
        actor=actor,
        source=source,
        note=note,
    )


def reject(
    meeting_id: str,
    candidate_id: str,
    actor: str = "dashboard_ui",
    source: str = "dashboard_ui",
    note: str = "",
) -> dict[str, Any]:
    return reject_candidate(
        meeting_id=meeting_id,
        candidate_id=candidate_id,
        actor=actor,
        source=source,
        note=note,
    )


def reset_to_pending(
    meeting_id: str,
    candidate_id: str,
    actor: str = "dashboard_ui",
    source: str = "dashboard_ui",
    note: str = "",
) -> dict[str, Any]:
    return reset_candidate_to_pending(
        meeting_id=meeting_id,
        candidate_id=candidate_id,
        actor=actor,
        source=source,
        note=note,
    )


def load_candidate_set(meeting_id: str) -> dict[str, Any]:
    meeting_key = str(meeting_id).strip()
    candidates = [row.to_dict() for row in load_candidates(meeting_key)]
    metadata = load_metadata(meeting_key)
    paths = calendar_paths(meeting_key)
    return {
        "meeting_id": meeting_key,
        "candidates": candidates,
        "metadata": metadata,
        "paths": {key: str(value) for key, value in paths.items()},
    }


def sync_approved_candidates(meeting_id: str, calendar_id: str = "primary") -> dict[str, Any]:
    from app.services.calendar.google_sync import sync_approved_candidates as _sync

    return _sync(meeting_id=meeting_id, calendar_id=calendar_id)


__all__ = [
    "generate_candidates",
    "approve",
    "reject",
    "reset_to_pending",
    "load_candidate_set",
    "sync_approved_candidates",
]
