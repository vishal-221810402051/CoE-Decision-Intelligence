from dataclasses import dataclass
from pathlib import Path


@dataclass
class DocumentIntakeResult:
    doc_id: str
    scope: str
    linked_meeting_id: str | None
    document_role: str
    stored_document_path: Path
    metadata_path: Path
    status: str
