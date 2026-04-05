import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.services.temporal import generate_temporal_intelligence


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/test_temporal_intelligence.py <meeting_id>")
        sys.exit(1)

    meeting_id = sys.argv[1]
    result = generate_temporal_intelligence(meeting_id)

    print("Temporal intelligence generation completed.")
    print(f"meeting_id={result.get('meeting_id')}")
    print(f"status={result.get('status')}")
    print(f"temporal_path={result.get('temporal_path')}")
    print(f"metadata_path={result.get('metadata_path')}")
    print(f"item_count={result.get('item_count')}")
    print(f"calendar_ready_count={result.get('calendar_ready_count')}")
    print(f"conflict_count={result.get('conflict_count')}")
    print(f"validation_passed={result.get('validation_passed')}")
