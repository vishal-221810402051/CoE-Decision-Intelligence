from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.config import PROCESSING_MODE_FILE, config

ALLOWED_PROCESSING_MODES = {"transcript_only", "transcript_plus_docs"}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _processing_mode_path(meeting_id: str) -> Path:
    return config.PROCESSED_PATH / str(meeting_id).strip() / "metadata" / PROCESSING_MODE_FILE


def _default_processing_mode(meeting_id: str) -> dict[str, Any]:
    return {
        "meeting_id": str(meeting_id).strip(),
        "processing_mode": "transcript_only",
        "source_doc_validation_enabled": False,
        "selected_source_doc_ids": [],
        "report_mode": "transcript_only",
        "updated_at": _now_iso(),
        "updated_by": "ui",
    }


def _sanitize_doc_ids(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    normalized: list[str] = []
    for item in value:
        doc_id = str(item).strip()
        if doc_id and doc_id not in normalized:
            normalized.append(doc_id)
    return normalized


def _normalize_processing_mode(
    meeting_id: str,
    payload: dict[str, Any] | None,
    *,
    fallback_updated_by: str = "ui",
) -> dict[str, Any]:
    default_payload = _default_processing_mode(meeting_id)
    if not isinstance(payload, dict):
        return default_payload

    mode = str(payload.get("processing_mode", "")).strip().lower()
    if mode not in ALLOWED_PROCESSING_MODES:
        mode = "transcript_only"

    selected_doc_ids = _sanitize_doc_ids(payload.get("selected_source_doc_ids", []))
    if mode != "transcript_plus_docs":
        selected_doc_ids = []

    source_doc_validation_enabled = mode == "transcript_plus_docs"
    updated_by = str(payload.get("updated_by", "")).strip() or fallback_updated_by
    updated_at = str(payload.get("updated_at", "")).strip() or _now_iso()

    return {
        "meeting_id": str(meeting_id).strip(),
        "processing_mode": mode,
        "source_doc_validation_enabled": source_doc_validation_enabled,
        "selected_source_doc_ids": selected_doc_ids,
        "report_mode": mode,
        "updated_at": updated_at,
        "updated_by": updated_by,
    }


def load_processing_mode_file(meeting_id: str) -> dict[str, Any]:
    path = _processing_mode_path(meeting_id)
    if not path.exists() or not path.is_file():
        return _default_processing_mode(meeting_id)

    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return _default_processing_mode(meeting_id)

    return _normalize_processing_mode(meeting_id, payload)


def get_processing_mode(meeting_id: str) -> dict[str, Any]:
    return load_processing_mode_file(meeting_id)


def set_processing_mode(
    meeting_id: str,
    mode: str,
    selected_doc_ids: list[str] | None,
) -> dict[str, Any]:
    normalized_mode = str(mode).strip().lower()
    payload = {
        "meeting_id": str(meeting_id).strip(),
        "processing_mode": normalized_mode,
        "selected_source_doc_ids": selected_doc_ids or [],
        "updated_at": _now_iso(),
        "updated_by": "ui",
    }
    normalized = _normalize_processing_mode(
        meeting_id,
        payload,
        fallback_updated_by="ui",
    )

    path = _processing_mode_path(meeting_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(normalized, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return normalized
