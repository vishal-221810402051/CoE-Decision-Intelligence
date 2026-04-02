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


config = Config()
