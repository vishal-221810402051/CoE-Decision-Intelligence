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


config = Config()
