from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from app.config import (
    ALLOWED_DOCUMENT_EXTENSIONS,
    ALLOWED_DOCUMENT_ROLES,
    ALLOWED_DOCUMENT_SCOPES,
    config,
)
from app.services.audio import AudioIntakeService
from app.services.context.document_intake import DocumentIntakeService


def _write_temp_upload(uploaded_file: Any, target_dir: Path) -> Path:
    target_dir.mkdir(parents=True, exist_ok=True)
    extension = Path(uploaded_file.name).suffix.lower()
    timestamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S-%f")
    temp_path = target_dir / f"temp_{timestamp}{extension}"
    temp_path.write_bytes(uploaded_file.getbuffer())
    return temp_path


def _is_valid_pdf_bytes(data: bytes) -> bool:
    return bool(data) and data.startswith(b"%PDF-")


def intake_audio_upload(uploaded_file: Any) -> dict[str, Any]:
    if uploaded_file is None:
        return {"ok": False, "message": "No audio file selected."}

    extension = Path(uploaded_file.name).suffix.lower()
    allowed = set(config.ALLOWED_AUDIO_EXTENSIONS)
    if extension not in allowed:
        return {
            "ok": False,
            "message": f"Unsupported audio format '{extension}'. Allowed: {', '.join(sorted(allowed))}",
        }

    temp_path: Path | None = None
    try:
        temp_path = _write_temp_upload(uploaded_file, config.DATA_PATH / "inbox_audio")
        result = AudioIntakeService().intake_audio(temp_path)
        return {
            "ok": True,
            "meeting_id": result.meeting_id,
            "meeting_dir": str(result.meeting_dir),
            "stored_audio_path": str(result.original_audio_path),
            "status": result.status,
            "message": "Meeting intake completed.",
        }
    except Exception as exc:
        return {"ok": False, "message": f"Audio intake failed: {exc}"}
    finally:
        if temp_path and temp_path.exists():
            try:
                temp_path.unlink()
            except Exception:
                pass


def source_doc_upload(
    uploaded_file: Any,
    scope: str,
    linked_meeting_id: str | None,
    document_role: str,
) -> dict[str, Any]:
    if uploaded_file is None:
        return {"ok": False, "message": "No source document selected."}

    scope_value = str(scope).strip()
    role_value = str(document_role).strip()
    extension = Path(uploaded_file.name).suffix.lower()

    if scope_value not in ALLOWED_DOCUMENT_SCOPES:
        return {"ok": False, "message": f"Invalid scope: {scope_value}"}
    if role_value not in ALLOWED_DOCUMENT_ROLES:
        return {"ok": False, "message": f"Invalid document role: {role_value}"}
    if extension not in ALLOWED_DOCUMENT_EXTENSIONS:
        return {"ok": False, "message": f"Unsupported document extension '{extension}'."}

    temp_path: Path | None = None
    try:
        temp_path = _write_temp_upload(uploaded_file, config.DATA_PATH / "inbox_audio")
        if extension == ".pdf":
            data = temp_path.read_bytes()
            if not _is_valid_pdf_bytes(data):
                return {
                    "ok": False,
                    "message": "Invalid PDF file (not a real PDF binary).",
                }

        result = DocumentIntakeService().intake_document(
            source_path=str(temp_path),
            scope=scope_value,
            document_role=role_value,
            linked_meeting_id=linked_meeting_id,
        )
        return {
            "ok": True,
            "doc_id": result.doc_id,
            "stored_document_path": str(result.stored_document_path),
            "metadata_path": str(result.metadata_path),
            "status": result.status,
            "message": "Source document uploaded.",
        }
    except Exception as exc:
        return {"ok": False, "message": f"Source document upload failed: {exc}"}
    finally:
        if temp_path and temp_path.exists():
            try:
                temp_path.unlink()
            except Exception:
                pass

