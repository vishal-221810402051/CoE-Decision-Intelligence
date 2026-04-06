from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import BackgroundTasks, FastAPI, File, Form, Query, Request, UploadFile
from fastapi.responses import JSONResponse

from app.config import config
from app.services.audio import AudioIntakeService
from app.services.calendar import approve, load_candidate_set, reject
from app.services.calendar.sync_engine import process_calendar_sync
from app.services.email import send_pdf_email
from app.services.meetings import (
    archive_meeting,
    get_meeting_detail,
    get_processing_status,
    is_valid_meeting_id,
    list_recent_meetings,
    resolve_report_pdf,
)
from app.services.pipeline.orchestrator import run_full_pipeline

app = FastAPI(title="CoE Decision Intelligence Upload API")

MAX_UPLOAD_BYTES = 100 * 1024 * 1024
READ_CHUNK_BYTES = 1024 * 1024


def _error(status_code: int, error_code: str, message: str) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={"error_code": error_code, "message": message},
    )


def _meeting_exists(meeting_id: str) -> bool:
    meeting_key = str(meeting_id).strip()
    if not meeting_key:
        return False
    path = config.PROCESSED_PATH / meeting_key
    return path.exists() and path.is_dir()


def _serialize_inbox_item(candidate: dict[str, Any], meeting_id: str) -> dict[str, Any]:
    return {
        "meeting_id": str(meeting_id).strip(),
        "candidate_id": str(candidate.get("candidate_id", "")).strip(),
        "title": str(candidate.get("title", "")).strip(),
        "summary": str(candidate.get("summary", "")).strip(),
        "type": str(candidate.get("type", "")).strip(),
        "display_date": str(candidate.get("display_date", "")).strip(),
        "display_time": str(candidate.get("display_time", "")).strip(),
        "confidence": str(candidate.get("confidence", "")).strip(),
        "eligibility_status": str(candidate.get("eligibility_status", "")).strip(),
        "blockers": [str(item).strip() for item in candidate.get("blockers", []) if str(item).strip()],
    }


def _is_pending_inbox_candidate(candidate: dict[str, Any]) -> bool:
    approval_state = str(candidate.get("approval_state", "")).strip().lower()
    eligibility_status = str(candidate.get("eligibility_status", "")).strip().lower()
    sync_status = str(candidate.get("sync_status", "")).strip().lower()
    external_event_id = str(candidate.get("external_event_id", "")).strip()

    return (
        approval_state == "pending"
        and eligibility_status in {"eligible", "review_required"}
        and sync_status != "synced"
        and not external_event_id
    )


def _pending_items_for_meeting(meeting_id: str) -> list[dict[str, Any]]:
    payload = load_candidate_set(meeting_id)
    candidates = payload.get("candidates", [])
    if not isinstance(candidates, list):
        return []

    items: list[dict[str, Any]] = []
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        if not _is_pending_inbox_candidate(candidate):
            continue
        items.append(_serialize_inbox_item(candidate, meeting_id))
    return items


@app.post("/api/meetings/upload-audio")
async def upload_audio(
    background_tasks: BackgroundTasks,
    audio_file: UploadFile | None = File(default=None),
    file: UploadFile | None = File(default=None),
    recorded_at: str | None = Form(default=None),
) -> object:
    _ = recorded_at
    uploaded_file = audio_file or file
    if uploaded_file is None:
        return _error(400, "MISSING_FILE", "Missing required multipart field: audio_file (or file).")

    original_name = str(uploaded_file.filename or "").strip()
    if not original_name:
        return _error(400, "INVALID_FILENAME", "Uploaded file name is missing.")

    extension = Path(original_name).suffix.lower()
    allowed = set(config.ALLOWED_AUDIO_EXTENSIONS)
    if extension not in allowed:
        allowed_list = ", ".join(sorted(allowed))
        return _error(
            422,
            "UNSUPPORTED_EXTENSION",
            f"Unsupported audio format '{extension}'. Allowed formats: {allowed_list}",
        )

    temp_dir = config.DATA_PATH / "inbox_audio"
    temp_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S-%f")
    temp_path = temp_dir / f"api_temp_{timestamp}{extension}"

    written_bytes = 0
    try:
        with temp_path.open("wb") as handle:
            while True:
                chunk = await uploaded_file.read(READ_CHUNK_BYTES)
                if not chunk:
                    break
                written_bytes += len(chunk)
                if written_bytes > MAX_UPLOAD_BYTES:
                    return _error(
                        413,
                        "FILE_TOO_LARGE",
                        f"Uploaded file exceeds size limit of {MAX_UPLOAD_BYTES} bytes.",
                    )
                handle.write(chunk)

        if written_bytes == 0:
            return _error(400, "EMPTY_FILE", "Uploaded file is empty.")

        result = AudioIntakeService().intake_audio(temp_path)
        meeting_id = str(result.meeting_id).strip()
        print(f"[UPLOAD_DONE] {meeting_id}")
        background_tasks.add_task(run_full_pipeline, meeting_id)
        return {"meeting_id": meeting_id, "status": "accepted"}
    except ValueError as exc:
        return _error(422, "INTAKE_VALIDATION_ERROR", str(exc))
    except FileNotFoundError as exc:
        return _error(500, "INTAKE_FILE_ERROR", str(exc))
    except Exception as exc:  # pragma: no cover
        return _error(500, "INTERNAL_ERROR", f"Upload intake failed: {exc}")
    finally:
        if uploaded_file is not None:
            await uploaded_file.close()
        if temp_path.exists():
            try:
                temp_path.unlink()
            except Exception:
                pass


@app.get("/api/meetings/recent")
async def meetings_recent(limit: int = Query(default=5, ge=1, le=50)) -> object:
    try:
        items = list_recent_meetings(limit=limit)
    except Exception as exc:
        return _error(500, "RECENT_MEETINGS_LOAD_FAILED", f"Failed to load recent meetings: {exc}")
    return {"items": items, "limit": int(limit)}


@app.get("/api/meetings/{meeting_id}")
async def meeting_detail(meeting_id: str) -> object:
    meeting_key = str(meeting_id).strip()
    if not is_valid_meeting_id(meeting_key):
        return _error(400, "INVALID_MEETING_ID", "meeting_id must match pattern MTG-* and contain no path separators.")
    if not _meeting_exists(meeting_key):
        return _error(404, "MEETING_NOT_FOUND", f"Meeting not found: {meeting_key}")
    try:
        payload = get_meeting_detail(meeting_key)
    except FileNotFoundError:
        return _error(404, "MEETING_NOT_FOUND", f"Meeting not found: {meeting_key}")
    except Exception as exc:
        return _error(500, "MEETING_DETAIL_LOAD_FAILED", f"Failed to load meeting detail: {exc}")
    return payload


@app.get("/api/meetings/{meeting_id}/processing-status")
async def meeting_processing_status(meeting_id: str) -> object:
    meeting_key = str(meeting_id).strip()
    if not is_valid_meeting_id(meeting_key):
        return _error(400, "INVALID_MEETING_ID", "meeting_id must match pattern MTG-* and contain no path separators.")
    if not _meeting_exists(meeting_key):
        return _error(404, "MEETING_NOT_FOUND", f"Meeting not found: {meeting_key}")
    try:
        payload = get_processing_status(meeting_key)
    except Exception as exc:
        return _error(500, "PROCESSING_STATUS_LOAD_FAILED", f"Failed to load processing status: {exc}")
    return payload


@app.delete("/api/meetings/{meeting_id}")
async def meeting_delete(meeting_id: str) -> object:
    meeting_key = str(meeting_id).strip()
    if not is_valid_meeting_id(meeting_key):
        return _error(400, "INVALID_MEETING_ID", "meeting_id must match pattern MTG-* and contain no path separators.")
    try:
        result = archive_meeting(meeting_key)
    except ValueError:
        return _error(400, "INVALID_MEETING_ID", "meeting_id is invalid.")
    except FileNotFoundError:
        return _error(404, "MEETING_NOT_FOUND", f"Meeting not found: {meeting_key}")
    except Exception as exc:
        return _error(500, "MEETING_DELETE_FAILED", f"Failed to archive meeting: {exc}")
    return result


@app.post("/api/meetings/{meeting_id}/forward-pdf")
async def forward_meeting_pdf(meeting_id: str) -> object:
    meeting_key = str(meeting_id).strip()
    if not is_valid_meeting_id(meeting_key):
        return _error(400, "INVALID_MEETING_ID", "meeting_id must match pattern MTG-* and contain no path separators.")
    if not _meeting_exists(meeting_key):
        return _error(404, "MEETING_NOT_FOUND", f"Meeting not found: {meeting_key}")

    pdf_path = resolve_report_pdf(meeting_key)
    if pdf_path is None:
        return _error(404, "PDF_NOT_FOUND", f"Report PDF not found for meeting: {meeting_key}")

    try:
        result = send_pdf_email(meeting_key, pdf_path)
    except FileNotFoundError:
        return _error(404, "PDF_NOT_FOUND", f"Report PDF not found for meeting: {meeting_key}")
    except Exception as exc:
        return _error(500, "EMAIL_SEND_FAILED", f"Failed to forward report PDF: {exc}")
    return result


@app.get("/api/inbox/pending")
async def inbox_pending() -> object:
    items: list[dict[str, Any]] = []
    if config.PROCESSED_PATH.exists() and config.PROCESSED_PATH.is_dir():
        for meeting_dir in sorted([path for path in config.PROCESSED_PATH.iterdir() if path.is_dir()], key=lambda p: p.name):
            meeting_id = meeting_dir.name
            try:
                items.extend(_pending_items_for_meeting(meeting_id))
            except Exception:
                continue

    items.sort(
        key=lambda row: (
            str(row.get("display_date", "")).strip(),
            str(row.get("meeting_id", "")).strip(),
            str(row.get("candidate_id", "")).strip(),
        )
    )
    return {"items": items}


@app.get("/api/inbox/pending/{meeting_id}")
async def inbox_pending_by_meeting(meeting_id: str) -> object:
    meeting_key = str(meeting_id).strip()
    if not _meeting_exists(meeting_key):
        return _error(404, "MEETING_NOT_FOUND", f"Meeting not found: {meeting_key}")

    try:
        items = _pending_items_for_meeting(meeting_key)
    except Exception as exc:
        return _error(500, "INBOX_LOAD_FAILED", f"Failed to load inbox items: {exc}")

    items.sort(
        key=lambda row: (
            str(row.get("display_date", "")).strip(),
            str(row.get("candidate_id", "")).strip(),
        )
    )
    return {"items": items}


@app.post("/api/inbox/decision")
async def inbox_decision(request: Request) -> object:
    try:
        payload = await request.json()
    except Exception:
        return _error(400, "INVALID_REQUEST_BODY", "Request body must be valid JSON.")

    if not isinstance(payload, dict):
        return _error(400, "INVALID_REQUEST_BODY", "Request body must be a JSON object.")

    meeting_id = str(payload.get("meeting_id", "")).strip()
    candidate_id = str(payload.get("candidate_id", "")).strip()
    decision = str(payload.get("decision", "")).strip().lower()
    actor = str(payload.get("actor", "")).strip()

    if not meeting_id:
        return _error(400, "MISSING_MEETING_ID", "meeting_id is required.")
    if not candidate_id:
        return _error(400, "MISSING_CANDIDATE_ID", "candidate_id is required.")
    if decision not in {"approved", "rejected"}:
        return _error(400, "INVALID_DECISION", "decision must be 'approved' or 'rejected'.")
    if not actor:
        return _error(400, "MISSING_ACTOR", "actor is required.")
    if not _meeting_exists(meeting_id):
        return _error(404, "MEETING_NOT_FOUND", f"Meeting not found: {meeting_id}")

    try:
        existing = load_candidate_set(meeting_id)
        rows = existing.get("candidates", [])
        if not isinstance(rows, list) or not any(
            isinstance(row, dict) and str(row.get("candidate_id", "")).strip() == candidate_id
            for row in rows
        ):
            return _error(404, "CANDIDATE_NOT_FOUND", f"Candidate not found: {candidate_id}")

        if decision == "approved":
            result = approve(
                meeting_id=meeting_id,
                candidate_id=candidate_id,
                actor=actor,
                source="android_app",
            )
            if str(result.get("status", "")).strip().lower() == "ok":
                try:
                    process_calendar_sync(meeting_id)
                except Exception as sync_exc:
                    print(f"[CALENDAR_SYNC_ERROR] {meeting_id} {candidate_id}: {sync_exc}")
        else:
            result = reject(
                meeting_id=meeting_id,
                candidate_id=candidate_id,
                actor=actor,
                source="android_app",
            )
    except Exception as exc:
        return _error(500, "DECISION_RECORD_FAILED", f"Failed to record decision: {exc}")

    if str(result.get("status", "")).strip().lower() != "ok":
        message = str(result.get("message", "decision_failed")).strip()
        if message == "candidate_not_found":
            return _error(404, "CANDIDATE_NOT_FOUND", f"Candidate not found: {candidate_id}")
        if message == "invalid_transition":
            return _error(400, "INVALID_TRANSITION", "Candidate is not in a pending state.")
        return _error(500, "DECISION_RECORD_FAILED", f"Decision failed: {message}")

    return {
        "meeting_id": str(result.get("meeting_id", meeting_id)).strip(),
        "candidate_id": str(result.get("candidate_id", candidate_id)).strip(),
        "approval_state": str(result.get("approval_state", "")).strip(),
        "status": "recorded",
    }
