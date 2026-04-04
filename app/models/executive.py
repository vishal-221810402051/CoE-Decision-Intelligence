from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class ExecutiveIntelligenceResult:
    meeting_id: str
    output_path: str
    metadata_path: str
    model: str
    prompt_version: str
    status: str


def executive_schema_defaults() -> dict[str, Any]:
    return {
        "executive_summary": {
            "meaning_of_meeting": "",
            "intent": "",
            "commitment": "",
            "execution_readiness": "low",
            "confidence": "low",
            "evidence": [],
            "support_level": "WEAK_INFERENCE",
            "claim_strength": "weak",
            "evidence_span": "",
            "evidence_start_index": -1,
            "evidence_end_index": -1,
            "evidence_confidence": 0.0,
        },
        "strategic_objective": {
            "objective": "",
            "business_direction": "",
            "success_condition": "",
            "confidence": "low",
            "evidence": [],
            "support_level": "WEAK_INFERENCE",
            "claim_strength": "weak",
            "evidence_span": "",
            "evidence_start_index": -1,
            "evidence_end_index": -1,
            "evidence_confidence": 0.0,
        },
        "power_structure": {
            "sponsor": [],
            "strategic_authority": [],
            "decision_makers": [],
            "advisors": [],
            "executors": [],
            "implementation_owner": [],
            "unknown_authority_gaps": [],
            "confidence": "low",
        },
        "execution_structure": {
            "primary_executor": "",
            "responsibility_load": "low",
            "authority_clarity": "undefined",
            "compensation_clarity": "undefined",
            "governance_clarity": "undefined",
            "execution_risk_score": "low",
            "confidence": "low",
            "evidence": [],
            "support_level": "WEAK_INFERENCE",
            "claim_strength": "weak",
            "evidence_span": "",
            "evidence_start_index": -1,
            "evidence_end_index": -1,
            "evidence_confidence": 0.0,
        },
        "role_clarity_assessment": [],
        "business_model_clarity": {
            "revenue_logic": "undefined",
            "funding_logic": "undefined",
            "ownership_model": "undefined",
            "legal_governance": "undefined",
            "confidence": "low",
            "evidence": [],
            "support_level": "WEAK_INFERENCE",
            "claim_strength": "weak",
            "evidence_span": "",
            "evidence_start_index": -1,
            "evidence_end_index": -1,
            "evidence_confidence": 0.0,
        },
        "risk_posture": {
            "overall": "low",
            "drivers": [],
            "confidence": "low",
            "evidence": [],
            "support_level": "WEAK_INFERENCE",
            "claim_strength": "weak",
            "evidence_span": "",
            "evidence_start_index": -1,
            "evidence_end_index": -1,
            "evidence_confidence": 0.0,
        },
        "negotiation_flags": [],
        "recommended_next_questions": [],
        "executive_warnings": [],
    }
