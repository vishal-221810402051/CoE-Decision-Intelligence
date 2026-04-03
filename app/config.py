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

    EXECUTIVE_MODEL: str = "gpt-4.1-mini"
    EXECUTIVE_OUTPUT_DIR: str = "executive"
    EXECUTIVE_OUTPUT_FILE: str = "executive_intelligence.json"
    EXECUTIVE_METADATA_FILE: str = "executive_metadata.json"
    EXECUTIVE_PROMPT_VERSION: str = "phase03_v1"
    MISSION_REGISTRY_PATH: str = "data/context/mission_registry.json"


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

INTELLIGENCE_MODEL = "gpt-4o-mini"
INTELLIGENCE_PROMPT_VERSION = "phase06_v1"
INTELLIGENCE_CHUNK_SIZE = 6000
INTELLIGENCE_CHUNK_OVERLAP = 300
INTELLIGENCE_PASS_A_MAX_RETRIES = 2
INTELLIGENCE_PASS_B_MAX_RETRIES = 2
INTELLIGENCE_BACKOFF_SECONDS = [2, 5]
INTELLIGENCE_OUTPUT_FILE_NAME = "intelligence.json"
INTELLIGENCE_METADATA_FILE_NAME = "intelligence_metadata.json"

PASS_A_MAX_RETRIES = 2
PASS_B_MAX_RETRIES = 2
API_BACKOFF_SECONDS = [2, 5]

EXECUTIVE_MODEL = config.EXECUTIVE_MODEL
EXECUTIVE_OUTPUT_DIR = config.EXECUTIVE_OUTPUT_DIR
EXECUTIVE_OUTPUT_FILE = config.EXECUTIVE_OUTPUT_FILE
EXECUTIVE_METADATA_FILE = config.EXECUTIVE_METADATA_FILE
EXECUTIVE_PROMPT_VERSION = config.EXECUTIVE_PROMPT_VERSION
MISSION_REGISTRY_PATH = config.MISSION_REGISTRY_PATH
