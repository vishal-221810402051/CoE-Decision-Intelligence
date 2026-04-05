from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass
class ApprovalRequestPayload:
    decision: str
    actor: str
    source: str
    note: str
    client_timestamp: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def approval_request_payload(
    decision: str,
    actor: str,
    source: str,
    note: str = "",
    client_timestamp: str = "",
) -> dict[str, Any]:
    payload = ApprovalRequestPayload(
        decision=str(decision or "").strip().lower(),
        actor=str(actor or "").strip(),
        source=str(source or "").strip(),
        note=str(note or "").strip(),
        client_timestamp=str(client_timestamp or "").strip(),
    )
    return payload.to_dict()


def list_candidates_response(
    meeting_id: str,
    candidates: list[dict[str, Any]],
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    rows = []
    for row in candidates if isinstance(candidates, list) else []:
        if isinstance(row, dict):
            rows.append(dict(row))
    rows.sort(
        key=lambda row: (
            str(row.get("display_date", "")).strip(),
            str(row.get("type", "")).strip(),
            str(row.get("candidate_id", "")).strip(),
        )
    )
    return {
        "meeting_id": str(meeting_id).strip(),
        "candidate_count": len(rows),
        "candidates": rows,
        "metadata": dict(metadata) if isinstance(metadata, dict) else {},
    }


def candidate_detail_response(
    meeting_id: str,
    candidate_id: str,
    candidate: dict[str, Any] | None,
) -> dict[str, Any]:
    return {
        "meeting_id": str(meeting_id).strip(),
        "candidate_id": str(candidate_id).strip(),
        "found": isinstance(candidate, dict),
        "candidate": dict(candidate) if isinstance(candidate, dict) else {},
    }
