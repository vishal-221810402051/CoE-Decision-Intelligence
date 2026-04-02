import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.services.intelligence import DecisionIntelligenceService


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/test_intelligence_extraction.py <meeting_id>")
        sys.exit(1)

    meeting_id = sys.argv[1]
    result = DecisionIntelligenceService.extract_intelligence(meeting_id)

    print("Intelligence extraction completed.")
    print(f"meeting_id={result.meeting_id}")
    print(f"intelligence_path={result.intelligence_path}")
    print(f"metadata_path={result.metadata_path}")
    print(f"model={result.model}")
    print(f"prompt_version={result.prompt_version}")
    print(f"chunked={result.chunked}")
    print(f"chunk_count={result.chunk_count}")
    print(f"pass_a_retries_used={result.pass_a_retries_used}")
    print(f"pass_b_retries_used={result.pass_b_retries_used}")
    print(f"status={result.status}")
