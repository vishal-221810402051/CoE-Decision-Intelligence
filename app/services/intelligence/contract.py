from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import app.config as app_config

REQUIRED_CANONICAL_KEYS: tuple[str, ...] = (
    "meeting_context",
    "summary",
    "decisions",
    "risks",
    "action_plan",
    "roadmap",
    "deadlines",
    "stakeholders",
    "timeline_mentions",
)


def get_canonical_intelligence_path(meeting_dir: Path) -> Path:
    return (
        meeting_dir
        / app_config.INTELLIGENCE_DIR_NAME
        / app_config.INTELLIGENCE_OUTPUT_FILE_NAME
    )


def validate_canonical_intelligence_contract(data: dict[str, Any]) -> None:
    if not isinstance(data, dict):
        raise ValueError("Canonical intelligence contract invalid: payload must be a JSON object.")

    missing_keys = [key for key in REQUIRED_CANONICAL_KEYS if key not in data]
    if missing_keys:
        raise ValueError(
            "Canonical intelligence contract invalid. Missing required keys: "
            + ", ".join(missing_keys)
        )

    if not isinstance(data.get("meeting_context"), dict):
        raise ValueError("Canonical intelligence contract invalid: 'meeting_context' must be an object.")
    if not isinstance(data.get("summary"), str):
        raise ValueError("Canonical intelligence contract invalid: 'summary' must be a string.")

    for key in (
        "decisions",
        "risks",
        "action_plan",
        "roadmap",
        "deadlines",
        "stakeholders",
        "timeline_mentions",
    ):
        if not isinstance(data.get(key), list):
            raise ValueError(f"Canonical intelligence contract invalid: '{key}' must be a list.")


def load_canonical_intelligence(meeting_dir: Path) -> dict[str, Any]:
    canonical_path = get_canonical_intelligence_path(meeting_dir)
    if not canonical_path.exists():
        raise FileNotFoundError(
            "Canonical intelligence artifact missing. Phase 06 must complete successfully before downstream phases. "
            f"Expected path: {canonical_path}"
        )

    try:
        payload = json.loads(canonical_path.read_text(encoding="utf-8-sig"))
    except Exception as exc:
        raise ValueError(
            f"Canonical intelligence contract invalid JSON at {canonical_path}: {exc}"
        ) from exc

    validate_canonical_intelligence_contract(payload)
    return payload


def adapt_canonical_intelligence_for_downstream(
    data: dict[str, Any],
) -> dict[str, Any]:
    validate_canonical_intelligence_contract(data)

    action_plan = list(data.get("action_plan", []))

    adapted: dict[str, Any] = {
        "meeting_context": dict(data.get("meeting_context", {})),
        "summary": data.get("summary", ""),
        "decisions": list(data.get("decisions", [])),
        "risks": list(data.get("risks", [])),
        "action_plan": action_plan,
        "action_items": action_plan,
        "roadmap": list(data.get("roadmap", [])),
        "deadlines": list(data.get("deadlines", [])),
        "stakeholders": list(data.get("stakeholders", [])),
        "timeline_mentions": list(data.get("timeline_mentions", [])),
        "entities": [],
        "missing_details": [],
        "inferred_risks": [],
        "critical_gaps": [],
        "suggestions": [],
        "key_points": [],
    }
    return adapted

