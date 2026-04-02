from dataclasses import dataclass


@dataclass
class CleanupResult:
    meeting_id: str
    input_path: str
    output_path: str
    metadata_path: str
    model: str
    chunked: bool
    chunk_count: int
    transformation_mode: str
    status: str
