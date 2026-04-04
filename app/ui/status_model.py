from __future__ import annotations

from pathlib import Path

from app.ui.repository import get_artifact_paths, safe_read_json


STATUS_COMPLETED = "completed"
STATUS_MISSING = "missing"
STATUS_UNKNOWN = "unknown"


def is_valid_json_artifact(path: Path) -> bool:
    payload = safe_read_json(path)
    return isinstance(payload, dict) and bool(payload)


def _meta_status_ok(path: Path, expected: str) -> bool:
    payload = safe_read_json(path)
    if not isinstance(payload, dict):
        return False
    return str(payload.get("status", "")).strip() == expected


def _json_stage_status(artifact_path: Path, metadata_path: Path, expected_meta: str) -> str:
    artifact_exists = artifact_path.exists() and artifact_path.is_file()
    meta_ok = _meta_status_ok(metadata_path, expected_meta)
    if artifact_exists and meta_ok and is_valid_json_artifact(artifact_path):
        return STATUS_COMPLETED
    if not artifact_exists and not meta_ok:
        return STATUS_MISSING
    return STATUS_UNKNOWN


def compute_stage_status(meeting_id: str) -> dict[str, str]:
    paths = get_artifact_paths(meeting_id)
    metadata_dir = paths["metadata_dir"]

    intake_meta = metadata_dir / "intake.json"
    normalization_meta = metadata_dir / "normalization.json"
    transcription_meta = metadata_dir / "transcription.json"
    cleanup_meta = metadata_dir / "cleanup.json"
    intelligence_meta = metadata_dir / "intelligence_metadata.json"
    executive_meta = metadata_dir / "executive_metadata.json"
    decision_meta = metadata_dir / "decision_v2_metadata.json"

    intake_done = _meta_status_ok(intake_meta, "intake_completed")
    normalization_done = _meta_status_ok(
        normalization_meta, "normalization_completed"
    ) and paths["normalized_audio"].exists()
    transcription_done = _meta_status_ok(
        transcription_meta, "transcription_completed"
    ) and paths["raw_transcript"].exists()
    cleanup_done = _meta_status_ok(cleanup_meta, "cleanup_completed") and paths[
        "clean_transcript"
    ].exists()

    if intake_done:
        intake_status = STATUS_COMPLETED
    elif intake_meta.exists():
        intake_status = STATUS_UNKNOWN
    else:
        intake_status = STATUS_MISSING

    if normalization_done:
        normalization_status = STATUS_COMPLETED
    elif normalization_meta.exists() or paths["normalized_audio"].exists():
        normalization_status = STATUS_UNKNOWN
    else:
        normalization_status = STATUS_MISSING

    if transcription_done:
        transcription_status = STATUS_COMPLETED
    elif transcription_meta.exists() or paths["raw_transcript"].exists():
        transcription_status = STATUS_UNKNOWN
    else:
        transcription_status = STATUS_MISSING

    if cleanup_done:
        cleanup_status = STATUS_COMPLETED
    elif cleanup_meta.exists() or paths["clean_transcript"].exists():
        cleanup_status = STATUS_UNKNOWN
    else:
        cleanup_status = STATUS_MISSING

    intelligence_status = _json_stage_status(
        artifact_path=paths["intelligence"],
        metadata_path=intelligence_meta,
        expected_meta="intelligence_completed",
    )
    executive_status = _json_stage_status(
        artifact_path=paths["executive"],
        metadata_path=executive_meta,
        expected_meta="executive_intelligence_completed",
    )
    decision_status = _json_stage_status(
        artifact_path=paths["decision"],
        metadata_path=decision_meta,
        expected_meta="decision_intelligence_v2_completed",
    )

    return {
        "intake": intake_status,
        "normalization": normalization_status,
        "transcription": transcription_status,
        "cleanup": cleanup_status,
        "intelligence": intelligence_status,
        "executive": executive_status,
        "decision": decision_status,
    }

