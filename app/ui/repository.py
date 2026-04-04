from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from app.config import config


def get_meeting_dir(meeting_id: str) -> Path:
    return config.PROCESSED_PATH / str(meeting_id).strip()


def safe_read_text(path: Path) -> str | None:
    try:
        if not path.exists() or not path.is_file():
            return None
        return path.read_text(encoding="utf-8")
    except Exception:
        return None


def safe_read_json(path: Path) -> dict[str, Any] | None:
    try:
        if not path.exists() or not path.is_file():
            return None
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
        if isinstance(payload, dict):
            return payload
        return None
    except Exception:
        return None


def get_artifact_paths(meeting_id: str) -> dict[str, Path]:
    meeting_dir = get_meeting_dir(meeting_id)
    return {
        "meeting_dir": meeting_dir,
        "source_dir": meeting_dir / "source",
        "source_audio": meeting_dir / "source" / "original.m4a",
        "normalized_audio": meeting_dir / "normalized" / "audio.wav",
        "raw_transcript": meeting_dir / "transcript" / "transcript_raw.txt",
        "clean_transcript": meeting_dir / "transcript" / "transcript_clean.txt",
        "intelligence": meeting_dir / "intelligence" / "intelligence.json",
        "executive": meeting_dir / "executive" / "executive_intelligence.json",
        "decision": meeting_dir / "decision" / "decision_intelligence_v2.json",
        "metadata_dir": meeting_dir / "metadata",
    }


def list_meeting_metadata(meeting_id: str) -> dict[str, dict[str, Any] | None]:
    metadata_dir = get_meeting_dir(meeting_id) / "metadata"
    if not metadata_dir.exists() or not metadata_dir.is_dir():
        return {}

    output: dict[str, dict[str, Any] | None] = {}
    for path in sorted(metadata_dir.glob("*.json")):
        output[path.name] = safe_read_json(path)
    return output


def list_meeting_source_pdfs(meeting_id: str) -> list[Path]:
    docs_root = get_meeting_dir(meeting_id) / "docs"
    if not docs_root.exists() or not docs_root.is_dir():
        return []
    return sorted(
        [path for path in docs_root.glob("*/source/*.pdf") if path.is_file()],
        key=lambda path: str(path).lower(),
    )


def find_report_pdf(meeting_id: str) -> Path | None:
    # Phase 10.1 truth: report PDF generation contract is not implemented.
    # Keep explicit None to avoid implying report artifacts exist.
    _ = meeting_id
    return None


def list_meetings() -> list[dict[str, Any]]:
    if not config.PROCESSED_PATH.exists() or not config.PROCESSED_PATH.is_dir():
        return []

    rows: list[dict[str, Any]] = []
    for path in config.PROCESSED_PATH.iterdir():
        if not path.is_dir():
            continue
        meeting_id = path.name
        intake = safe_read_json(path / "metadata" / "intake.json")
        created_at = ""
        if isinstance(intake, dict):
            created_at = str(intake.get("created_at", "")).strip()
        rows.append(
            {
                "meeting_id": meeting_id,
                "created_at": created_at,
                "meeting_dir": path,
            }
        )

    rows.sort(
        key=lambda row: (
            str(row.get("created_at", "")),
            str(row.get("meeting_id", "")),
        ),
        reverse=True,
    )
    return rows

