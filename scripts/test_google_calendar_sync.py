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

    # --- Step 1: Try eligible + pending ---
    eligible_pending = [
        c for c in candidates
        if c.get("eligibility_status") == "eligible"
        and c.get("approval_state") == "pending"
    ]

    selected_candidate = None

    if eligible_pending:
        print("Found eligible pending candidate -> approving for sync test")

        selected_candidate = eligible_pending[0]

        approve(
            meeting_id,
            selected_candidate["candidate_id"],
            actor="phase12_2_test",
            source="dashboard_ui",
            note="sync test approval",
        )

    else:
        print("No eligible pending candidates -> falling back to approved unsynced")

        eligible_approved_unsynced = [
            c for c in candidates
            if c.get("eligibility_status") == "eligible"
            and c.get("approval_state") == "approved"
            and c.get("sync_status") in {"not_queued", "failed"}
            and not c.get("external_event_id")
        ]

        if not eligible_approved_unsynced:
            raise RuntimeError(
                "No eligible candidates available for sync (neither pending nor approved-unsynced)."
            )

        selected_candidate = eligible_approved_unsynced[0]

    print(f"Selected candidate: {selected_candidate['candidate_id']}")

    # --- Step 2: Run sync ---
    result = sync_approved_candidates(meeting_id)

    print("\n=== SYNC RESULT ===")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("Usage: python scripts/test_google_calendar_sync.py <meeting_id>")
    main(sys.argv[1])
