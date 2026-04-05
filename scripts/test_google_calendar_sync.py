from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.services.calendar import approve, load_candidate_set, sync_approved_candidates


def main(meeting_id: str) -> None:
    payload = load_candidate_set(meeting_id)
    candidates = payload.get("candidates", [])

    eligible_pending = [
        c
        for c in candidates
        if c.get("eligibility_status") == "eligible"
        and c.get("approval_state") == "pending"
    ]

    if not eligible_pending:
        raise RuntimeError("No eligible pending candidates found for sync test.")

    first = eligible_pending[0]
    approve(
        meeting_id,
        first["candidate_id"],
        actor="phase12_2_test",
        source="dashboard_ui",
        note="sync test approval",
    )

    result = sync_approved_candidates(meeting_id)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("Usage: python scripts/test_google_calendar_sync.py <meeting_id>")
    main(sys.argv[1])
