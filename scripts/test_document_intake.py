import sys
from app.services.context import DocumentIntakeService

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage:")
        print("python scripts/test_document_intake.py <file_path> <scope> <role> [meeting_id]")
        sys.exit(1)

    file_path = sys.argv[1]
    scope = sys.argv[2]
    role = sys.argv[3]
    meeting_id = sys.argv[4] if len(sys.argv) > 4 else None

    service = DocumentIntakeService()

    result = service.intake_document(
        source_path=file_path,
        scope=scope,
        document_role=role,
        linked_meeting_id=meeting_id
    )

    print("Document intake successful:")
    print(f"Doc ID: {result.doc_id}")
    print(f"Stored Path: {result.stored_document_path}")
    print(f"Metadata Path: {result.metadata_path}")
