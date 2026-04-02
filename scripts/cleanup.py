import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.cleanup.cleaner import process_transcript


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/cleanup.py <session_path>")
        sys.exit(1)

    process_transcript(sys.argv[1])
