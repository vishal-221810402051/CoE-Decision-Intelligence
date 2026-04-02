from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


class Config:
    APP_ENV: str = os.getenv("APP_ENV", "development")
    OPENAI_API_KEY: str | None = os.getenv("OPENAI_API_KEY")
    DATA_PATH: Path = Path(os.getenv("DATA_PATH", "./data")).resolve()

    PROCESSED_PATH: Path = DATA_PATH / "processed"
    REPORTS_PATH: Path = DATA_PATH / "reports"
    ARCHIVE_PATH: Path = DATA_PATH / "archive"
    DB_PATH: Path = DATA_PATH / "db"

    ALLOWED_AUDIO_EXTENSIONS: set[str] = {".m4a", ".mp3", ".wav"}

    FFMPEG_BINARY: str = os.getenv("FFMPEG_BINARY", "ffmpeg")
    NORMALIZED_DIR_NAME: str = "normalized"
    NORMALIZED_AUDIO_FILE_NAME: str = "audio.wav"
    NORMALIZATION_METADATA_FILE_NAME: str = "normalization.json"
    NORMALIZATION_CHANNELS: int = 1
    NORMALIZATION_SAMPLE_RATE_HZ: int = 16000
    NORMALIZATION_CODEC: str = "pcm_s16le"
    NORMALIZATION_OVERWRITE_POLICY: str = "replace_existing_output"

    TRANSCRIPT_DIR_NAME: str = "transcript"
    TRANSCRIPT_RAW_FILE_NAME: str = "transcript_raw.txt"
    TRANSCRIPTION_METADATA_FILE_NAME: str = "transcription.json"
    TRANSCRIPTION_MODEL: str = "gpt-4o-mini-transcribe"


config = Config()

TRANSCRIPT_DIR_NAME = config.TRANSCRIPT_DIR_NAME
TRANSCRIPT_RAW_FILE_NAME = config.TRANSCRIPT_RAW_FILE_NAME
TRANSCRIPTION_METADATA_FILE_NAME = config.TRANSCRIPTION_METADATA_FILE_NAME
TRANSCRIPTION_MODEL = config.TRANSCRIPTION_MODEL

DATA_PATH = Path("data")

CONTEXT_DOCS_PATH = DATA_PATH / "context_docs"

DOCS_DIR_NAME = "docs"
DOC_SOURCE_DIR_NAME = "source"
DOC_METADATA_DIR_NAME = "metadata"

DOC_STORED_BASENAME = "source"
DOCUMENT_INTAKE_METADATA_FILE_NAME = "document_intake.json"

ALLOWED_DOCUMENT_EXTENSIONS = {".pdf", ".docx", ".txt", ".md"}
ALLOWED_DOCUMENT_SCOPES = {"meeting", "mission"}
ALLOWED_DOCUMENT_ROLES = {
    "mou",
    "partnership_note",
    "governance_doc",
    "curriculum_doc",
    "infrastructure_doc",
    "strategy_note",
    "general_reference"
}

DOCUMENT_INTAKE_STATUS = "document_intake_completed"

CLEANUP_MODEL = "gpt-4o-mini"  # or your chosen model
TRANSFORMATION_MODE = "faithful_cleanup"

TRANSCRIPT_RAW = "transcript_raw.txt"
TRANSCRIPT_CLEAN = "transcript_clean.txt"
CLEANUP_METADATA = "cleanup.json"

CLEANUP_CHUNK_SIZE = 4000  # chars
CLEANUP_MIN_RATIO = 0.6
