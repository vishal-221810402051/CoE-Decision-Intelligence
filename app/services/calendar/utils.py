from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any

from app.config import DEFAULT_TIMEZONE
from app.services.calendar.schemas import ApprovalState, CalendarCandidate, SyncStatus


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _is_unknown_timezone(value: Any) -> bool:
    token = str(value or "").strip().lower()
    return token in {"", "unknown", "none", "null"}


def resolve_timezone(candidate: dict[str, Any]) -> str:
    if not isinstance(candidate, dict):
        return DEFAULT_TIMEZONE

    candidate_tz = candidate.get("timezone")
    if not _is_unknown_timezone(candidate_tz):
        return str(candidate_tz).strip()

    normalized = candidate.get("normalized_time", {})
    if isinstance(normalized, dict):
        normalized_tz = normalized.get("timezone")
        if not _is_unknown_timezone(normalized_tz):
            return str(normalized_tz).strip()

    return DEFAULT_TIMEZONE


def _canonicalize(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _canonicalize(value[k]) for k in sorted(value.keys())}
    if isinstance(value, list):
        return [_canonicalize(item) for item in value]
    return value


def _stable_json(value: Any) -> str:
    return json.dumps(_canonicalize(value), ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def sha256_hex(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def compute_candidate_hash(content_fields: dict[str, Any]) -> str:
    return sha256_hex(_stable_json(content_fields))


def compute_temporal_hash(payload: dict[str, Any]) -> str:
    if not isinstance(payload, dict):
        return ""
    normalized = deepcopy(payload)
    if isinstance(normalized, dict):
        normalized.pop("generated_at", None)
    return sha256_hex(_stable_json(normalized))


def reconcile_candidate_with_previous(
    incoming: CalendarCandidate,
    existing: CalendarCandidate | None,
    now_ts: str,
) -> CalendarCandidate:
    if existing is None:
        incoming.created_at = incoming.created_at or now_ts
        incoming.updated_at = incoming.updated_at or now_ts
        incoming.candidate_version = max(1, int(incoming.candidate_version or 1))
        incoming.approval_state = ApprovalState.PENDING.value
        incoming.sync_status = SyncStatus.NOT_QUEUED.value
        incoming.approved_by = ""
        incoming.approved_at = ""
        incoming.rejected_by = ""
        incoming.rejected_at = ""
        if incoming.approval_source in {"dashboard_ui", "android_future"}:
            incoming.approval_source = ""
        return incoming

    incoming.created_at = existing.created_at or incoming.created_at or now_ts

    if existing.candidate_hash == incoming.candidate_hash:
        incoming.candidate_version = max(1, int(existing.candidate_version or 1))
        incoming.approval_state = existing.approval_state or ApprovalState.PENDING.value
        incoming.approval_source = existing.approval_source
        incoming.approved_by = existing.approved_by
        incoming.approved_at = existing.approved_at
        incoming.rejected_by = existing.rejected_by
        incoming.rejected_at = existing.rejected_at
        incoming.approval_note = existing.approval_note
        incoming.updated_at = existing.updated_at or now_ts
    else:
        incoming.candidate_version = max(1, int(existing.candidate_version or 1) + 1)
        incoming.approval_state = ApprovalState.PENDING.value
        incoming.approval_source = ""
        incoming.approved_by = ""
        incoming.approved_at = ""
        incoming.rejected_by = ""
        incoming.rejected_at = ""
        incoming.approval_note = ""
        incoming.updated_at = now_ts

    incoming.sync_status = SyncStatus.NOT_QUEUED.value
    return incoming
