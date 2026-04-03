import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.services.decision import DecisionIntelligenceV2Service


def main() -> None:
    if len(sys.argv) != 2:
        print("Usage: python scripts/test_decision_intelligence.py <meeting_id>")
        sys.exit(1)

    meeting_id = sys.argv[1]
    result = DecisionIntelligenceV2Service().run(meeting_id)

    print("Decision intelligence v2 completed.")
    print(f"meeting_id={result.meeting_id}")
    print(f"output={result.output_path}")
    print(f"metadata={result.metadata_path}")
    print(f"status={result.status}")


if __name__ == "__main__":
    main()
