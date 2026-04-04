from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from app.config import PROCESSING_MODE_FILE, config


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
    report_dir = meeting_dir / "report"
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
        "processing_mode": meeting_dir / "metadata" / PROCESSING_MODE_FILE,
        "report_dir": report_dir,
        "report_payload": report_dir / "report_payload.json",
        "report_html": report_dir / "report.html",
        "report_pdf": report_dir / "report.pdf",
        "report_metadata": report_dir / "report_metadata.json",
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
    docs = list_meeting_source_docs(meeting_id)
    return sorted(
        [Path(str(item.get("pdf_path", ""))) for item in docs if str(item.get("pdf_path", "")).strip()],
        key=lambda path: str(path).lower(),
    )


def list_meeting_source_docs(meeting_id: str) -> list[dict[str, Any]]:
    docs_root = get_meeting_dir(meeting_id) / "docs"
    if not docs_root.exists() or not docs_root.is_dir():
        return []

    rows: list[dict[str, Any]] = []
    for doc_dir in sorted(docs_root.iterdir(), key=lambda p: p.name):
        if not doc_dir.is_dir():
            continue
        metadata = safe_read_json(doc_dir / "metadata" / "document_intake.json") or {}
        source_dir = doc_dir / "source"
        source_files = sorted([path for path in source_dir.glob("*") if path.is_file()]) if source_dir.exists() else []
        source_file = source_files[0] if source_files else None
        pdf_path = None
        for candidate in source_files:
            if candidate.suffix.lower() == ".pdf":
                pdf_path = candidate
                break

        rows.append(
            {
                "doc_id": str(metadata.get("doc_id", doc_dir.name)).strip(),
                "document_role": str(metadata.get("document_role", "")).strip(),
                "source_file_name": str(metadata.get("source_file_name", "")).strip(),
                "source_extension": str(metadata.get("source_extension", "")).strip(),
                "source_path": str(source_file) if source_file else "",
                "pdf_path": str(pdf_path) if pdf_path else "",
                "metadata": metadata if isinstance(metadata, dict) else {},
            }
        )
    return rows


def find_report_pdf(meeting_id: str) -> Path | None:
    path = get_meeting_dir(meeting_id) / "report" / "report.pdf"
    if path.exists() and path.is_file():
        return path
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
