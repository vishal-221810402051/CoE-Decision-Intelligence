from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class DecisionIntelligenceResult:
    meeting_id: str
    output_path: str
    metadata_path: str
    model: str
    prompt_version: str
    status: str


def decision_v2_schema_defaults() -> dict[str, Any]:
    return {
        "decision_records": [],
        "operational_summary": {
            "confirmed_count": 0,
            "tentative_count": 0,
            "pending_count": 0,
            "blocked_count": 0,
            "high_blockers": [],
            "missing_owners_count": 0,
            "open_dependencies_count": 0,
        },
    }
