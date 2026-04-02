import shutil
import uuid
from datetime import datetime
from pathlib import Path
import json

from app.config import (
    DATA_PATH,
    CONTEXT_DOCS_PATH,
    DOCS_DIR_NAME,
    DOC_SOURCE_DIR_NAME,
    DOC_METADATA_DIR_NAME,
    DOC_STORED_BASENAME,
    DOCUMENT_INTAKE_METADATA_FILE_NAME,
    ALLOWED_DOCUMENT_EXTENSIONS,
    ALLOWED_DOCUMENT_SCOPES,
    ALLOWED_DOCUMENT_ROLES,
    DOCUMENT_INTAKE_STATUS
)

from app.models.document import DocumentIntakeResult


class DocumentIntakeService:

    def generate_doc_id(self):
        now = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
        rand = uuid.uuid4().hex[:6]
        return f"DOC-{now}-{rand}"

    def intake_document(self, source_path, scope, document_role, linked_meeting_id=None):
        source = Path(source_path)

        if not source.exists():
            raise FileNotFoundError(f"Source file not found: {source.resolve()}")

        if source.is_dir():
            raise ValueError("Source path must be a file, not a directory")

        ext = source.suffix.lower()
        if ext not in ALLOWED_DOCUMENT_EXTENSIONS:
            raise ValueError(f"Unsupported extension: {ext}")

        if scope not in ALLOWED_DOCUMENT_SCOPES:
            raise ValueError("Invalid scope")

        if document_role not in ALLOWED_DOCUMENT_ROLES:
            raise ValueError("Invalid document role")

        if scope == "meeting":
            if not linked_meeting_id:
                raise ValueError("Meeting scope requires linked_meeting_id")

            meeting_path = DATA_PATH / "processed" / linked_meeting_id
            if not meeting_path.exists():
                raise FileNotFoundError("Meeting folder does not exist")

            base_path = meeting_path / DOCS_DIR_NAME

        else:
            if linked_meeting_id is not None:
                raise ValueError("Mission scope should not have meeting_id")

            base_path = CONTEXT_DOCS_PATH

        doc_id = self.generate_doc_id()

        doc_path = base_path / doc_id
        source_dir = doc_path / DOC_SOURCE_DIR_NAME
        metadata_dir = doc_path / DOC_METADATA_DIR_NAME

        source_dir.mkdir(parents=True, exist_ok=True)
        metadata_dir.mkdir(parents=True, exist_ok=True)

        stored_file_name = DOC_STORED_BASENAME + ext
        stored_path = source_dir / stored_file_name

        shutil.copy2(source, stored_path)

        metadata = {
            "doc_id": doc_id,
            "linked_meeting_id": linked_meeting_id,
            "scope": scope,
            "source_file_name": source.name,
            "stored_file_name": stored_file_name,
            "source_extension": ext,
            "stored_path": str(stored_path),
            "document_role": document_role,
            "created_at": datetime.utcnow().isoformat(),
            "status": DOCUMENT_INTAKE_STATUS
        }

        metadata_path = metadata_dir / DOCUMENT_INTAKE_METADATA_FILE_NAME

        with open(metadata_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=4)

        return DocumentIntakeResult(
            doc_id=doc_id,
            scope=scope,
            linked_meeting_id=linked_meeting_id,
            document_role=document_role,
            stored_document_path=stored_path,
            metadata_path=metadata_path,
            status=DOCUMENT_INTAKE_STATUS
        )
