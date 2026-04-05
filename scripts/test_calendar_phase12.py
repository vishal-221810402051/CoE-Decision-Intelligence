from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.config import config
from app.services.calendar import approve, generate_candidates, reject
from app.services.calendar.storage import calendar_paths, load_candidates, load_metadata


def _safe_read_json(path: Path) -> dict[str, Any]:
    try:
        if not path.exists() or not path.is_file():
            return {}
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
        if isinstance(payload, dict):
            return payload
    except Exception:
        return {}
    return {}


def _canonical_without_generated_at(payload: dict[str, Any]) -> dict[str, Any]:
    cleaned = dict(payload) if isinstance(payload, dict) else {}
    cleaned.pop("generated_at", None)
    return cleaned


def _candidate_map(meeting_id: str) -> dict[str, Any]:
    return {candidate.candidate_id: candidate for candidate in load_candidates(meeting_id)}


def _find_temporal_item_id(candidate_id: str, candidates_json: dict[str, Any]) -> str:
    rows = candidates_json.get("candidates", [])
    if not isinstance(rows, list):
        return ""
    for row in rows:
        if isinstance(row, dict) and str(row.get("candidate_id", "")).strip() == candidate_id:
            return str(row.get("temporal_item_id", "")).strip()
    return ""


def _load_paths(meeting_id: str) -> dict[str, Path]:
    paths = calendar_paths(meeting_id)
    return {
        "candidates": Path(paths["candidates_path"]),
        "metadata": Path(paths["metadata_path"]),
        "approval_log": Path(paths["approval_log_path"]),
        "temporal": config.PROCESSED_PATH / meeting_id / "temporal" / "temporal_intelligence.json",
    }


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists() or not path.is_file():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if not text:
            continue
        try:
            payload = json.loads(text)
        except Exception:
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: python scripts/test_calendar_phase12.py <meeting_id>")
        return 1

    meeting_id = str(sys.argv[1]).strip()
    paths = _load_paths(meeting_id)

    # 1) Generate twice and verify determinism (ignoring generated_at metadata)
    result_first = generate_candidates(meeting_id)
    candidates_payload_first = _safe_read_json(paths["candidates"])
    metadata_first = _safe_read_json(paths["metadata"])

    result_second = generate_candidates(meeting_id)
    candidates_payload_second = _safe_read_json(paths["candidates"])
    metadata_second = _safe_read_json(paths["metadata"])

    deterministic_candidates = candidates_payload_first == candidates_payload_second
    deterministic_metadata = (
        _canonical_without_generated_at(metadata_first)
        == _canonical_without_generated_at(metadata_second)
    )
    determinism_check = deterministic_candidates and deterministic_metadata

    # 2) Approve one candidate and reject one candidate
    candidates = load_candidates(meeting_id)
    if not candidates:
        print("No candidates generated; cannot validate approval flow.")
        return 1

    pending_candidates = [c for c in candidates if c.approval_state == "pending"]
    if len(pending_candidates) < 2:
        print("Not enough pending candidates to validate approve/reject transitions.")
        return 1

    approve_target = pending_candidates[0].candidate_id
    reject_target = pending_candidates[1].candidate_id

    approve_result = approve(meeting_id, approve_target, actor="phase12_test", source="dashboard_ui", note="approval test")
    reject_result = reject(meeting_id, reject_target, actor="phase12_test", source="dashboard_ui", note="rejection test")

    log_rows = _read_jsonl(paths["approval_log"])
    approve_logged = any(
        str(row.get("candidate_id", "")).strip() == approve_target
        and str(row.get("old_approval_state", "")).strip() == "pending"
        and str(row.get("new_approval_state", "")).strip() == "approved"
        for row in log_rows
    )
    reject_logged = any(
        str(row.get("candidate_id", "")).strip() == reject_target
        and str(row.get("old_approval_state", "")).strip() == "pending"
        and str(row.get("new_approval_state", "")).strip() == "rejected"
        for row in log_rows
    )

    # 3) Regenerate unchanged and ensure approval/rejection persist
    state_after_actions = _candidate_map(meeting_id)
    approved_before_regen = state_after_actions.get(approve_target)
    rejected_before_regen = state_after_actions.get(reject_target)

    generate_candidates(meeting_id)
    state_after_regen = _candidate_map(meeting_id)
    approved_persisted = (
        approve_target in state_after_regen
        and approved_before_regen is not None
        and state_after_regen[approve_target].approval_state == approved_before_regen.approval_state
    )
    rejected_persisted = (
        reject_target in state_after_regen
        and rejected_before_regen is not None
        and state_after_regen[reject_target].approval_state == rejected_before_regen.approval_state
    )

    # 4) Simulate temporal change and verify approval reset
    temporal_backup = paths["temporal"].read_text(encoding="utf-8") if paths["temporal"].exists() else ""
    approval_reset_on_change = False
    try:
        candidates_json = _safe_read_json(paths["candidates"])
        target_temporal_item_id = _find_temporal_item_id(approve_target, candidates_json)

        temporal_payload = _safe_read_json(paths["temporal"])
        items = temporal_payload.get("items", [])
        if target_temporal_item_id and isinstance(items, list):
            mutated = False
            for item in items:
                if not isinstance(item, dict):
                    continue
                if str(item.get("item_id", "")).strip() != target_temporal_item_id:
                    continue
                intent = str(item.get("intent", "")).strip()
                if "[phase12-change]" not in intent:
                    item["intent"] = f"{intent} [phase12-change]".strip()
                else:
                    item["intent"] = intent + "x"
                mutated = True
                break

            if mutated:
                paths["temporal"].write_text(json.dumps(temporal_payload, indent=2, ensure_ascii=False), encoding="utf-8")
                pre_change_candidate = state_after_regen.get(approve_target)
                generate_candidates(meeting_id)
                state_after_change = _candidate_map(meeting_id)
                post_change_candidate = state_after_change.get(approve_target)
                if pre_change_candidate is not None and post_change_candidate is not None:
                    approval_reset_on_change = (
                        post_change_candidate.approval_state == "pending"
                        and post_change_candidate.candidate_version > pre_change_candidate.candidate_version
                    )
    finally:
        if temporal_backup:
            paths["temporal"].write_text(temporal_backup, encoding="utf-8")
            generate_candidates(meeting_id)

    final_metadata = load_metadata(meeting_id)
    final_candidates = load_candidates(meeting_id)
    sync_status_valid = all(candidate.sync_status == "not_queued" for candidate in final_candidates)
    blocked_non_eligible = all(
        (candidate.eligibility_status != "blocked") or (candidate.eligibility_status == "blocked" and candidate.candidate_state == "blocked")
        for candidate in final_candidates
    )

    print("Phase 12 calendar candidate test completed.")
    print(f"meeting_id={meeting_id}")
    print(f"determinism_check={determinism_check}")
    print(f"approve_result={approve_result}")
    print(f"reject_result={reject_result}")
    print(f"approve_logged={approve_logged}")
    print(f"reject_logged={reject_logged}")
    print(f"approval_persisted_after_regen={approved_persisted}")
    print(f"rejection_persisted_after_regen={rejected_persisted}")
    print(f"approval_reset_on_change={approval_reset_on_change}")
    print(f"sync_status_not_queued_only={sync_status_valid}")
    print(f"blocked_candidates_non_eligible={blocked_non_eligible}")
    print(f"candidate_count={final_metadata.get('candidate_count')}")
    print(f"approved_count={final_metadata.get('approved_count')}")
    print(f"rejected_count={final_metadata.get('rejected_count')}")
    print(f"pending_count={final_metadata.get('pending_count')}")
    print(f"blocked_count={final_metadata.get('blocked_count')}")
    print(f"eligible_count={final_metadata.get('eligible_count')}")
    print(f"candidates_path={paths['candidates']}")
    print(f"metadata_path={paths['metadata']}")
    print(f"approval_log_path={paths['approval_log']}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
