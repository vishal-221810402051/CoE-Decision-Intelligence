from dataclasses import dataclass
from pathlib import Path


@dataclass
class IntelligenceExtractionResult:
    meeting_id: str
    intelligence_path: Path
    metadata_path: Path
    model: str
    prompt_version: str
    chunked: bool
    chunk_count: int
    pass_a_retries_used: int
    pass_b_retries_used: int
    processing_time_seconds: float
    validation_passed: bool
    status: str
