from __future__ import annotations

import shutil
from pathlib import Path

from app.config import config
from app.services.audio import AudioNormalizationService
from app.services.calendar import generate_candidates
from app.services.cleanup import TranscriptCleanupService
from app.services.decision import DecisionIntelligenceV2Service
from app.services.executive import ExecutiveIntelligenceService
from app.services.intelligence import DecisionIntelligenceService
from app.services.reporting import generate_report
from app.services.temporal import generate_temporal_intelligence
from app.services.transcription import TranscriptionService


def _mirror_raw_transcript(meeting_id: str) -> None:
    meeting_key = str(meeting_id).strip()
    source = config.PROCESSED_PATH / meeting_key / "transcript" / "transcript_raw.txt"
    target = config.DATA_PATH / "raw" / meeting_key / "transcript_raw.txt"
    if not source.exists() or not source.is_file():
        raise FileNotFoundError(f"Transcript raw artifact not found for mirror: {source}")
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, target)


def run_full_pipeline(meeting_id: str) -> None:
    meeting_key = str(meeting_id).strip()
    print(f"[PIPELINE_TRIGGERED] {meeting_key}")
    print(f"[PROCESS_START] {meeting_key}")

    try:
        print("[NORMALIZATION]")
        AudioNormalizationService().normalize_meeting(meeting_key)

        print("[TRANSCRIPTION]")
        TranscriptionService().transcribe_meeting(meeting_key)

        _mirror_raw_transcript(meeting_key)

        print("[CLEANUP]")
        TranscriptCleanupService.cleanup_meeting(meeting_key)

        print("[INTELLIGENCE]")
        DecisionIntelligenceService.extract_intelligence(meeting_key)

        print("[EXECUTIVE]")
        ExecutiveIntelligenceService().run(meeting_key)

        print("[DECISION]")
        DecisionIntelligenceV2Service().run(meeting_key)

        print("[TEMPORAL]")
        generate_temporal_intelligence(meeting_key)

        print("[CALENDAR]")
        generate_candidates(meeting_key)

        print("[REPORT]")
        generate_report(meeting_key)

        print(f"[READY_FOR_APPROVAL] {meeting_key}")
    except Exception as exc:
        print(f"[PIPELINE_ERROR] {meeting_key} -> {exc}")
