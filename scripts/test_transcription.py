import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.services.transcription import TranscriptionService

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/test_transcription.py <meeting_id>")
        sys.exit(1)

    meeting_id = sys.argv[1]

    service = TranscriptionService()

    print(f"Starting transcription for {meeting_id}...")
    output = service.transcribe_meeting(meeting_id)

    print("Transcription completed.")
    print(f"Output file: {output}")
