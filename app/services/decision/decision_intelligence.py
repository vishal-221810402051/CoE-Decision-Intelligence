from __future__ import annotations

import hashlib
import json
import os
import re
import time
from pathlib import Path
from typing import Any

from openai import OpenAI

from app.intelligence.actor_resolver import actor_present_in_transcript, resolve_actor_from_text
from app.intelligence.evidence_engine import (
    SUPPORT_DIRECT,
    build_evidence_binding,
    is_semantically_supportive,
)
from app.config import (
    DECISION_V2_SEED,
    DECISION_V2_METADATA_FILE,
    DECISION_V2_MODEL,
    DECISION_V2_OUTPUT_DIR,
    DECISION_V2_OUTPUT_FILE,
    DECISION_V2_PROMPT_VERSION,
    MISSION_REGISTRY_PATH,
)
from app.models.decision import DecisionIntelligenceResult, decision_v2_schema_defaults
from app.services.intelligence.contract import (
    adapt_canonical_intelligence_for_downstream,
    get_canonical_intelligence_path,
    load_canonical_intelligence,
)
from app.validation.consistency_guard import validate_cross_artifact_consistency


STATE_ENUM = {"confirmed", "tentative", "pending", "blocked"}
HML = {"high", "medium", "low"}
OWNERSHIP_ENUM = {"assigned_owner", "shared_owner", "missing_owner"}
COMMITMENT_TYPE_ENUM = {
    "explicit_commitment",
    "implied_commitment",
    "requested_commitment",
    "unresolved_commitment",
}
COMMITMENT_STATUS_ENUM = {"open", "accepted", "unresolved"}
DEPENDENCY_TYPE_ENUM = {
    "governance_dependency",
    "authority_dependency",
    "funding_dependency",
    "timeline_dependency",
    "partner_dependency",
}
DEPENDENCY_STATUS_ENUM = {"open", "partially_resolved", "resolved"}
TIMELINE_TYPE_ENUM = {"start_window", "deadline_hint", "followup_marker"}
DECISION_STATUS_ENUM = {"confirmed", "tentative", "conditional", "blocked"}

UNCERTAINTY_MARKERS = (
    "idea",
    "possibility",
    "potentially",
    "maybe",
    "would consider",
    "it can be",
    "more or less defined",
)
PENDING_MARKERS = (
    "need to define",
    "need to go deeper",
    "next meeting",
    "to be decided",
    "not clear",
    "we need to check",
    "we need to finalize",
)
CONDITIONAL_MARKERS = (
    "if you say yes",
    "it's not a yes",
    "it is not a yes",
    "yes, no, maybe",
)
RESPONSIBILITY_PATTERNS = (
    "you will be responsible",
    "you will go there",
    "you will be the representative",
    "you will be the one",
    "you will work with us",
    "be our representative",
)
EXPLICIT_PATTERNS = (
    "i will",
    "i'll",
    "we will",
    "we'll",
    "yes, i want to move forward",
    "i will mark",
    "i will ask",
    "i will list",
    "we will meet",
)
REQUESTED_PATTERNS = (
    "would you consider",
    "can you",
    "think about it",
    "tell me yes, no, maybe",
    "you can tell me",
)
IMPLIED_PATTERNS = (
    "you will be responsible",
    "you will be the representative",
    "you will go there",
    "you will work with us",
)
CERTAINTY_UNCERTAIN_MARKERS = (
    "maybe",
    "perhaps",
    "depends",
    "can",
    "could",
    "might",
    "explore",
    "likely",
    "potentially",
    "i think",
    "i was thinking",
    "sounds reasonable",
    "need to define",
    "need to check",
)
CERTAINTY_CONDITIONAL_MARKERS = (
    "if you say yes",
    "not a yes",
    "not final",
    "depends",
    "conditional",
)
TIME_PATTERNS: tuple[tuple[str, str, str], ...] = (
    (r"\bfirst\s+of\s+may\b", "start_window", "medium"),
    (r"\bfirst\s+week\s+of\s+may\b", "start_window", "medium"),
    (r"\bfirst\s+june\b", "start_window", "medium"),
    (r"\bmaybe\s+june\b", "start_window", "medium"),
    (r"\bend\s+of\s+april\b", "deadline_hint", "medium"),
    (r"\btuesday\s+after\s+5:30\b", "followup_marker", "high"),
    (r"\bafter\s+5:30\b", "followup_marker", "high"),
    (r"\btuesday\b", "followup_marker", "high"),
    (r"\bnext\s+meeting\b", "followup_marker", "medium"),
    (r"\bfive\s+to\s+one\s+week\b", "deadline_hint", "low"),
    (r"\ba\s+week\b", "deadline_hint", "low"),
)
DEPENDENCY_REASON_TEMPLATES = {
    "authority_dependency": "Execution responsibility is assigned, but final operational authority is not clearly defined.",
    "governance_dependency": "Governance and reporting structure remain unresolved for operational execution.",
    "funding_dependency": "Funding or revenue-model terms remain unresolved.",
    "timeline_dependency": "Relevant timing is discussed, but execution timing is not fully finalized.",
    "partner_dependency": "Execution depends on coordination or approval with external institutional partners.",
}
DECISION_DEFAULT_MIN_CONFIDENCE_SCORE = 0.5
DECISION_LABEL_HIGH = "HIGH"
DECISION_LABEL_MEDIUM = "MEDIUM"
DECISION_LABEL_LOW = "LOW"
DECISION_EXPLICIT_VERBS = (
    "decide",
    "decided",
    "approved",
    "agreed",
    "finalized",
    "confirmed",
    "commit",
    "committed",
    "will",
    "we will",
    "i will",
)
DECISION_ACTIONABLE_VERBS = (
    "implement",
    "execute",
    "deliver",
    "start",
    "launch",
    "assign",
    "schedule",
    "submit",
    "complete",
    "move forward",
)
DECISION_GENERIC_MARKERS = (
    "we discussed",
    "we talked",
    "let's discuss",
    "think about",
    "consider",
    "maybe",
    "possibly",
    "perhaps",
)
DECISION_NON_ACTIONABLE_MARKERS = (
    "we discussed",
    "we talked",
    "it was mentioned",
    "there is a need to",
    "great thing",
    "great initiative",
    "this is an idea",
    "what do i do",
    "what are",
    "would you consider",
    "should we",
)
DECISION_OWNER_GENERIC = {
    "team",
    "we",
    "someone",
    "somebody",
    "they",
    "everyone",
    "group",
    "people",
}
DECISION_TOPIC_FAMILIES: dict[str, tuple[str, ...]] = {
    "ownership": ("owner", "ownership", "responsible", "authority", "governance"),
    "funding": ("funding", "finance", "revenue", "budget", "payment", "compensation"),
    "timeline": ("timeline", "date", "deadline", "schedule", "week", "month", "meeting"),
    "delivery": ("implement", "execution", "deliver", "launch", "deploy", "build"),
    "partnership": ("partner", "gtu", "aivancity", "laurent", "michelle"),
}


class DecisionIntelligenceV2Error(RuntimeError):
    pass


def load_mission_registry() -> dict[str, Any]:
    path = Path(MISSION_REGISTRY_PATH)
    if not path.exists():
        raise FileNotFoundError(f"Mission registry not found: {path}")
    data = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(data, dict):
        raise DecisionIntelligenceV2Error("Mission registry must be a JSON object")
    return data


def build_registry_grounding(registry_data: dict[str, Any]) -> str:
    lines: list[str] = []
    primary = registry_data.get("primary_actor", {})
    if isinstance(primary, dict) and primary:
        lines.extend(
            [
                "PRIMARY ACTOR",
                f"- Name: {primary.get('name', '')}",
                f"- Role: {primary.get('role', '')}",
            ]
        )
        for rule in primary.get("interpretation_rules", []):
            lines.append(f"- Rule: {rule}")
    institutions = registry_data.get("institutions", [])
    if institutions:
        lines.append("\nINSTITUTIONS")
        for inst in institutions:
            if isinstance(inst, dict):
                lines.append(
                    f"- {inst.get('normalized_name', inst.get('name', ''))}: {inst.get('mission_role', '')}"
                )
    stakeholders = registry_data.get("stakeholders", [])
    if stakeholders:
        lines.append("\nSTAKEHOLDERS")
        for s in stakeholders:
            if isinstance(s, dict):
                lines.append(
                    f"- {s.get('name', '')}: {s.get('role', '')} [importance={s.get('importance', '')}, signal_weight={s.get('signal_weight', '')}]"
                )
    terms = registry_data.get("mission_terms", [])
    if terms:
        lines.append("\nMISSION TERMS")
        for t in terms:
            if isinstance(t, dict):
                lines.append(
                    f"- {t.get('normalized_name', t.get('term', ''))}: {t.get('meaning', '')}"
                )
    return "\n".join(lines).strip()


def build_alias_map(registry_data: dict[str, Any]) -> dict[str, str]:
    amap: dict[str, str] = {}
    primary = registry_data.get("primary_actor", {})
    if isinstance(primary, dict):
        name = primary.get("name", "")
        if isinstance(name, str) and name:
            amap[name] = name
            amap[name.lower()] = name
    for inst in registry_data.get("institutions", []):
        if not isinstance(inst, dict):
            continue
        name = inst.get("name", "")
        norm = inst.get("normalized_name", name)
        if isinstance(name, str) and name:
            amap[name] = norm
            amap[name.lower()] = norm
        if isinstance(norm, str) and norm:
            amap[norm] = norm
            amap[norm.lower()] = norm
    for s in registry_data.get("stakeholders", []):
        if isinstance(s, dict):
            name = s.get("name", "")
            if isinstance(name, str) and name:
                amap[name] = name
                amap[name.lower()] = name
    for t in registry_data.get("mission_terms", []):
        if not isinstance(t, dict):
            continue
        term = t.get("term", "")
        norm = t.get("normalized_name", term)
        if isinstance(term, str) and term:
            amap[term] = norm
            amap[term.lower()] = norm
        if isinstance(norm, str) and norm:
            amap[norm] = norm
            amap[norm.lower()] = norm
    return amap


class DecisionIntelligenceV2Service:
    def __init__(self) -> None:
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise DecisionIntelligenceV2Error("OPENAI_API_KEY is missing")
        self.client = OpenAI(api_key=api_key)

    def run(self, meeting_id: str) -> DecisionIntelligenceResult:
        start_time = time.time()
        meeting_dir = Path("data") / "processed" / meeting_id
        transcript_dir = meeting_dir / "transcript"
        executive_dir = meeting_dir / "executive"
        metadata_dir = meeting_dir / "metadata"
        decision_dir = meeting_dir / DECISION_V2_OUTPUT_DIR

        raw_path = transcript_dir / "transcript_raw.txt"
        clean_path = transcript_dir / "transcript_clean.txt"
        canonical_intelligence_path = get_canonical_intelligence_path(meeting_dir)
        executive_path = executive_dir / "executive_intelligence.json"
        # Legacy optional input only. Active execution must not depend on this file.
        legacy_path = Path("reports") / "decision_intelligence.json"
        for req in [raw_path, clean_path, executive_path]:
            if not req.exists():
                raise FileNotFoundError(f"Required artifact not found: {req}")
        if not canonical_intelligence_path.exists():
            raise FileNotFoundError(
                "Canonical intelligence artifact missing. Phase 06 must complete successfully before downstream phases. "
                f"Expected path: {canonical_intelligence_path}"
            )

        registry = load_mission_registry()
        registry_grounding = build_registry_grounding(registry)
        alias_map = build_alias_map(registry)
        primary_actor_name = self._normalize_actor(
            str(registry.get("primary_actor", {}).get("name", "")).strip(), alias_map
        )
        raw_text = raw_path.read_text(encoding="utf-8")
        clean_text = clean_path.read_text(encoding="utf-8")
        try:
            canonical_intelligence = load_canonical_intelligence(meeting_dir)
            intelligence = adapt_canonical_intelligence_for_downstream(canonical_intelligence)
        except (FileNotFoundError, ValueError) as exc:
            raise DecisionIntelligenceV2Error(str(exc)) from exc
        executive = json.loads(executive_path.read_text(encoding="utf-8"))
        executive_primary = self._normalize_actor(
            str(executive.get("execution_structure", {}).get("primary_executor", "")).strip(),
            alias_map,
        )
        legacy = json.loads(legacy_path.read_text(encoding="utf-8")) if legacy_path.exists() else {}

        messages = [
            {"role": "system", "content": self._system_prompt()},
            {
                "role": "user",
                "content": self._build_prompt(
                    clean_text,
                    intelligence,
                    executive,
                    registry_grounding,
                    legacy,
                ),
            },
        ]
        response = self._create_completion(messages)
        parsed = self._safe_parse_json(response.choices[0].message.content or "")
        if parsed is None:
            raise DecisionIntelligenceV2Error("Model did not return valid JSON")

        payload = self._enforce_schema(parsed)
        payload = self._normalize_with_registry(payload, alias_map)
        payload = self._harden_records(
            payload,
            clean_text,
            executive,
            intelligence,
            alias_map,
            executive_primary,
            primary_actor_name,
        )
        payload = self._sort_records_deterministically(payload)
        self._enforce_cross_artifact_consistency(payload, executive, intelligence)
        if not payload.get("decision_records"):
            fallback = self._build_fallback_record_from_intelligence(
                intelligence=intelligence,
                transcript_clean=clean_text,
            )
            if fallback:
                payload["decision_records"] = [fallback]
                payload = self._sort_records_deterministically(payload)
                self._enforce_cross_artifact_consistency(payload, executive, intelligence)
        payload, grounding_stats = self._apply_decision_grounding(
            payload=payload,
            transcript_clean=clean_text,
            transcript_raw=raw_text,
        )
        payload = self._sort_records_deterministically(payload)
        self._validate_records(
            payload,
            clean_text,
            alias_map,
            executive_primary,
            primary_actor_name,
        )
        payload["operational_summary"] = self._build_operational_summary(payload["decision_records"])
        self._validate_final(payload)

        decision_dir.mkdir(parents=True, exist_ok=True)
        metadata_dir.mkdir(parents=True, exist_ok=True)
        output_path = decision_dir / DECISION_V2_OUTPUT_FILE
        metadata_path = metadata_dir / DECISION_V2_METADATA_FILE
        self._write_artifacts_fail_closed(
            output_path,
            metadata_path,
            payload,
            {
                "meeting_id": meeting_id,
                "model": DECISION_V2_MODEL,
                "prompt_version": DECISION_V2_PROMPT_VERSION,
                "processing_time_seconds": round(time.time() - start_time, 3),
                "status": "decision_intelligence_v2_completed",
                "decision_grounding": grounding_stats,
            },
        )
        print(
            "[DECISION_GROUNDING] "
            f"total_candidates={grounding_stats.get('total_candidates', 0)} "
            f"rejected_no_evidence={grounding_stats.get('rejected_no_evidence', 0)} "
            f"rejected_low_confidence={grounding_stats.get('rejected_low_confidence', 0)} "
            f"rejected_non_actionable={grounding_stats.get('rejected_non_actionable', 0)} "
            f"rejected_invalid={grounding_stats.get('rejected_invalid', 0)} "
            f"rejected_conflict={grounding_stats.get('rejected_conflict', 0)} "
            f"refined_count={grounding_stats.get('refined_count', 0)} "
            f"split_count={grounding_stats.get('split_count', 0)} "
            f"owner_unassigned_count={grounding_stats.get('owner_unassigned_count', 0)} "
            f"final_accepted={grounding_stats.get('final_accepted', 0)}"
        )
        return DecisionIntelligenceResult(
            meeting_id=meeting_id,
            output_path=str(output_path),
            metadata_path=str(metadata_path),
            model=DECISION_V2_MODEL,
            prompt_version=DECISION_V2_PROMPT_VERSION,
            status="decision_intelligence_v2_completed",
        )

    def _system_prompt(self) -> str:
        return (
            "You are a Decision Intelligence v2 Engine. "
            "Return JSON only. No markdown. No commentary. No hallucination. "
            "Use evidence-backed reasoning. Do not invent actors, money values, approvals, dates, or resolved states. "
            "Confirmed decision requires clear commitment evidence. "
            "Blocked decision means execution cannot proceed due to unresolved dependency. "
            "Use one compact operational statement per decision and concise commitment text only."
        )

    def _create_completion(self, messages: list[dict[str, str]]) -> Any:
        try:
            return self.client.chat.completions.create(
                model=DECISION_V2_MODEL,
                temperature=0,
                seed=DECISION_V2_SEED,
                response_format={"type": "json_object"},
                messages=messages,
            )
        except Exception as exc:
            text = str(exc).lower()
            if "seed" not in text and "response_format" not in text and "unsupported" not in text:
                raise
            return self.client.chat.completions.create(
                model=DECISION_V2_MODEL,
                temperature=0,
                messages=messages,
            )

    def _build_prompt(
        self,
        transcript_clean: str,
        intelligence: dict[str, Any],
        executive: dict[str, Any],
        registry_grounding: str,
        legacy_decision: dict[str, Any],
    ) -> str:
        return f"""MISSION REGISTRY GROUNDING\n{registry_grounding}\n\nTRANSCRIPT_CLEAN\n{transcript_clean}\n\nINTELLIGENCE.JSON\n{json.dumps(intelligence, ensure_ascii=False, indent=2)}\n\nEXECUTIVE_INTELLIGENCE.JSON\n{json.dumps(executive, ensure_ascii=False, indent=2)}\n\nLEGACY_DECISION_INTELLIGENCE\n{json.dumps(legacy_decision, ensure_ascii=False, indent=2)}\n\nReturn JSON with exact top-level keys: decision_records, operational_summary.\nFor each decision record use fields: decision_id, statement, state, decision_status, impact_level, confidence, primary_owner, owners, commitments, dependencies, decision_gaps, timeline_signals, evidence.\nEnums:\nstate=confirmed|tentative|pending|blocked\ndecision_status=confirmed|tentative|conditional|blocked\nownership_type=assigned_owner|shared_owner|missing_owner\ncommitment_type=explicit_commitment|implied_commitment|requested_commitment|unresolved_commitment\ncommitment status=open|accepted|unresolved\ndependency type=governance_dependency|authority_dependency|funding_dependency|timeline_dependency|partner_dependency\ndependency status=open|partially_resolved|resolved\ntimeline signal_type=start_window|deadline_hint|followup_marker\nRules: confirmed requires explicit commitment; blocked requires unresolved high blocker; no invented details; timeline signals remain raw; evidence must be exact transcript substring; keep one compact operational statement per decision.\nAdditional hard rules:\n- Preserve source uncertainty; do not upgrade maybe/depends/explore language to confirmed certainty.\n- Do not introduce concept families absent from INTELLIGENCE.JSON.\n- Drop weak dependencies and avoid generic placeholders when evidence is weak.\n- Keep ownership conservative when actor confidence is low."""

    def _safe_parse_json(self, text: str) -> dict[str, Any] | None:
        try:
            obj = json.loads(text)
            return obj if isinstance(obj, dict) else None
        except Exception:
            match = re.search(r"\{.*\}", text, re.DOTALL)
            if not match:
                return None
            try:
                obj = json.loads(match.group())
                return obj if isinstance(obj, dict) else None
            except Exception:
                return None

    def _coerce(self, value: str, allowed: set[str], fallback: str) -> str:
        return value if value in allowed else fallback

    def _str_list(self, value: Any) -> list[str]:
        if not isinstance(value, list):
            return []
        out: list[str] = []
        for item in value:
            if isinstance(item, str) and item.strip():
                out.append(item.strip())
        return out

    def _extract_actor_value(self, value: Any) -> str:
        if isinstance(value, dict):
            for key in ["actor", "name", "id"]:
                candidate = str(value.get(key, "")).strip()
                if candidate:
                    return candidate
            return ""
        if isinstance(value, str):
            return value.strip()
        return ""

    def _enforce_schema(self, data: dict[str, Any]) -> dict[str, Any]:
        defaults = decision_v2_schema_defaults()
        out = {"decision_records": [], "operational_summary": defaults["operational_summary"]}
        if not isinstance(data, dict):
            return out
        records = data.get("decision_records", [])
        if not isinstance(records, list):
            return out
        for idx, rec in enumerate(records):
            if not isinstance(rec, dict):
                continue
            out["decision_records"].append(
                {
                    "decision_id": str(rec.get("decision_id", "")).strip(),
                    "statement": str(rec.get("statement", "")).strip(),
                    "state": self._coerce(str(rec.get("state", "")).strip().lower(), STATE_ENUM, "pending"),
                    "decision_status": self._coerce(
                        str(rec.get("decision_status", "")).strip().lower(),
                        DECISION_STATUS_ENUM,
                        "tentative",
                    ),
                    "impact_level": self._coerce(str(rec.get("impact_level", "")).strip().lower(), HML, "medium"),
                    "confidence": self._coerce(str(rec.get("confidence", "")).strip().lower(), HML, "medium"),
                    "primary_owner": self._extract_actor_value(rec.get("primary_owner", "")),
                    "owners": self._normalize_owners(rec.get("owners", [])),
                    "commitments": self._normalize_commitments(rec.get("commitments", [])),
                    "dependencies": self._normalize_dependencies(rec.get("dependencies", [])),
                    "decision_gaps": self._normalize_gaps(rec.get("decision_gaps", [])),
                    "timeline_signals": self._normalize_signals(rec.get("timeline_signals", [])),
                    "evidence": self._str_list(rec.get("evidence", [])),
                    "support_level": "WEAK_INFERENCE",
                    "claim_strength": "weak",
                    "certainty_class": "UNCERTAIN",
                    "evidence_span": "",
                    "evidence_start_index": -1,
                    "evidence_end_index": -1,
                    "evidence_confidence": 0.0,
                    "owner_confidence": 0.0,
                }
            )
        return out

    def _normalize_owners(self, value: Any) -> list[dict[str, str]]:
        if not isinstance(value, list):
            return []
        out = []
        for item in value:
            if not isinstance(item, dict):
                continue
            actor = self._extract_actor_value(item.get("actor", ""))
            raw_type = str(item.get("ownership_type", "")).strip().lower()
            ownership_type = self._coerce(raw_type, OWNERSHIP_ENUM, "missing_owner")
            if not actor:
                ownership_type = "missing_owner"
            out.append(
                {
                    "actor": actor,
                    "ownership_type": ownership_type,
                }
            )
        return out

    def _normalize_commitments(self, value: Any) -> list[dict[str, str]]:
        if not isinstance(value, list):
            return []
        out = []
        for item in value:
            if not isinstance(item, dict):
                continue
            out.append(
                {
                    "actor": self._extract_actor_value(item.get("actor", "")),
                    "commitment": str(item.get("commitment", "")).strip(),
                    "commitment_type": self._coerce(
                        str(item.get("commitment_type", "")).strip().lower(),
                        COMMITMENT_TYPE_ENUM,
                        "unresolved_commitment",
                    ),
                    "status": self._coerce(
                        str(item.get("status", "")).strip().lower(),
                        COMMITMENT_STATUS_ENUM,
                        "unresolved",
                    ),
                    "confidence": self._coerce(
                        str(item.get("confidence", "")).strip().lower(), HML, "low"
                    ),
                }
            )
        return out

    def _normalize_dependencies(self, value: Any) -> list[dict[str, str]]:
        if not isinstance(value, list):
            return []
        out = []
        for item in value:
            if not isinstance(item, dict):
                continue
            out.append(
                {
                    "type": self._coerce(
                        str(item.get("type", "")).strip().lower(),
                        DEPENDENCY_TYPE_ENUM,
                        "governance_dependency",
                    ),
                    "status": self._coerce(
                        str(item.get("status", "")).strip().lower(),
                        DEPENDENCY_STATUS_ENUM,
                        "open",
                    ),
                    "blocking_level": self._coerce(
                        str(item.get("blocking_level", "")).strip().lower(), HML, "medium"
                    ),
                    "reason": str(item.get("reason", "")).strip(),
                }
            )
        return out

    def _normalize_gaps(self, value: Any) -> list[dict[str, str]]:
        if not isinstance(value, list):
            return []
        out = []
        for item in value:
            if not isinstance(item, dict):
                continue
            gap_type = str(item.get("gap_type", "")).strip()
            question = str(item.get("question", "")).strip()
            if not gap_type and not question:
                continue
            out.append(
                {
                    "gap_type": gap_type,
                    "criticality": self._coerce(
                        str(item.get("criticality", "")).strip().lower(), HML, "medium"
                    ),
                    "question": question,
                }
            )
        return out

    def _normalize_signals(self, value: Any) -> list[dict[str, str]]:
        if not isinstance(value, list):
            return []
        out = []
        for item in value:
            if not isinstance(item, dict):
                continue
            raw_reference = str(item.get("raw_reference", "")).strip()
            if not raw_reference:
                continue
            out.append(
                {
                    "signal_type": self._coerce(
                        str(item.get("signal_type", "")).strip().lower(),
                        TIMELINE_TYPE_ENUM,
                        "followup_marker",
                    ),
                    "raw_reference": raw_reference,
                    "confidence": self._coerce(
                        str(item.get("confidence", "")).strip().lower(), HML, "low"
                    ),
                }
            )
        return out

    def _normalize_with_registry(self, data: dict[str, Any], alias_map: dict[str, str]) -> dict[str, Any]:
        for rec in data["decision_records"]:
            rec["primary_owner"] = self._normalize_actor(rec.get("primary_owner", ""), alias_map)
            for owner in rec.get("owners", []):
                owner["actor"] = self._normalize_actor(owner.get("actor", ""), alias_map)
            for cmt in rec.get("commitments", []):
                cmt["actor"] = self._normalize_actor(cmt.get("actor", ""), alias_map)
        return data

    def _normalize_actor(self, actor: str, alias_map: dict[str, str]) -> str:
        if not isinstance(actor, str):
            return ""
        actor = actor.strip()
        if not actor:
            return ""
        return alias_map.get(actor, alias_map.get(actor.lower(), actor))

    def _split_sentences(self, text: str) -> list[str]:
        chunks = [s.strip() for s in re.split(r"(?<=[.!?])\s+", text) if s.strip()]
        if not chunks and text.strip():
            return [text.strip()]
        return chunks

    def _contains_time_phrase(self, text: str) -> bool:
        lowered = text.lower()
        return any(re.search(pattern, lowered) for pattern, _, _ in TIME_PATTERNS)

    def _has_responsibility_language(self, text: str) -> bool:
        lowered = text.lower()
        return any(p in lowered for p in RESPONSIBILITY_PATTERNS)

    def _collect_record_sentences(self, rec: dict[str, Any], transcript_clean: str) -> list[str]:
        transcript_sentences = self._split_sentences(transcript_clean)
        sentences: list[str] = []
        seen: set[str] = set()
        statement = str(rec.get("statement", "")).strip()
        if statement and statement not in seen:
            seen.add(statement)
            sentences.append(statement)
        for ev in rec.get("evidence", []):
            if not isinstance(ev, str):
                continue
            t = ev.strip()
            if t and t not in seen:
                seen.add(t)
                sentences.append(t)

        tokens = {
            tok
            for tok in re.findall(r"[A-Za-z0-9]+", statement.lower())
            if len(tok) >= 4 and tok not in {"that", "this", "with", "from", "will", "have", "were"}
        }
        for sentence in transcript_sentences:
            if any(ev in sentence for ev in rec.get("evidence", []) if isinstance(ev, str) and ev):
                if sentence not in seen:
                    seen.add(sentence)
                    sentences.append(sentence)
                continue
            if tokens and len(tokens & set(re.findall(r"[A-Za-z0-9]+", sentence.lower()))) >= 2:
                if sentence not in seen:
                    seen.add(sentence)
                    sentences.append(sentence)

        if not sentences and statement:
            sentences.append(statement)
        return sentences

    def _build_intelligence_anchor_corpus(self, intelligence: dict[str, Any]) -> list[str]:
        corpus: list[str] = []
        if not isinstance(intelligence, dict):
            return corpus
        for family in [
            "decisions",
            "risks",
            "action_plan",
            "roadmap",
            "deadlines",
            "stakeholders",
            "timeline_mentions",
        ]:
            rows = intelligence.get(family, [])
            if not isinstance(rows, list):
                continue
            for row in rows:
                if not isinstance(row, dict):
                    continue
                for key in ["text", "task", "step", "event", "name", "raw_time_reference"]:
                    value = str(row.get(key, "")).strip()
                    if value:
                        corpus.append(value)
                        break
        summary = str(intelligence.get("summary", "")).strip()
        if summary:
            corpus.append(summary)
        return corpus

    def _build_intelligence_certainty_anchors(self, intelligence: dict[str, Any]) -> list[tuple[str, str]]:
        anchors: list[tuple[str, str]] = []
        if not isinstance(intelligence, dict):
            return anchors
        for family in [
            "decisions",
            "risks",
            "action_plan",
            "roadmap",
            "deadlines",
            "stakeholders",
            "timeline_mentions",
        ]:
            rows = intelligence.get(family, [])
            if not isinstance(rows, list):
                continue
            for row in rows:
                if not isinstance(row, dict):
                    continue
                text = ""
                for key in ["text", "task", "step", "event", "name", "raw_time_reference"]:
                    value = str(row.get(key, "")).strip()
                    if value:
                        text = value
                        break
                if not text:
                    continue
                certainty = str(row.get("certainty_class", "UNCERTAIN")).strip().upper()
                if certainty not in {"UNCERTAIN", "CONDITIONAL", "DIRECT"}:
                    certainty = "UNCERTAIN"
                anchors.append((text, certainty))
        return anchors

    def _best_anchor_certainty(self, rec: dict[str, Any], anchors: list[tuple[str, str]]) -> str:
        if not anchors:
            return "UNCERTAIN"
        text = " ".join(
            [str(rec.get("statement", ""))]
            + [str(ev) for ev in rec.get("evidence", []) if isinstance(ev, str)]
        )
        claim_tokens = {
            tok
            for tok in re.findall(r"[A-Za-z0-9]+", text.lower())
            if len(tok) >= 3
        }
        best_overlap = 0
        best_certainty = "UNCERTAIN"
        for anchor_text, certainty in anchors:
            anchor_tokens = {
                tok
                for tok in re.findall(r"[A-Za-z0-9]+", anchor_text.lower())
                if len(tok) >= 3
            }
            overlap = len(claim_tokens & anchor_tokens)
            if overlap > best_overlap:
                best_overlap = overlap
                best_certainty = certainty
        return best_certainty if best_overlap >= 2 else "UNCERTAIN"

    def _is_record_anchored_to_intelligence(self, rec: dict[str, Any], anchor_corpus: list[str]) -> bool:
        if not anchor_corpus:
            return True
        record_text = " ".join(
            [str(rec.get("statement", ""))]
            + [str(ev) for ev in rec.get("evidence", []) if isinstance(ev, str)]
        ).strip()
        claim_tokens = {
            tok
            for tok in re.findall(r"[A-Za-z0-9]+", record_text.lower())
            if len(tok) >= 3
        }
        if not claim_tokens:
            return False
        for anchor in anchor_corpus:
            anchor_tokens = {
                tok
                for tok in re.findall(r"[A-Za-z0-9]+", anchor.lower())
                if len(tok) >= 3
            }
            if len(claim_tokens & anchor_tokens) >= 2:
                return True
        return False

    def _harden_records(
        self,
        data: dict[str, Any],
        transcript_clean: str,
        executive: dict[str, Any],
        intelligence: dict[str, Any],
        alias_map: dict[str, str],
        executive_primary: str,
        primary_actor: str,
    ) -> dict[str, Any]:
        transcript_has_yes_contradiction = ("if you say yes" in transcript_clean.lower() and "not a yes" in transcript_clean.lower())
        known_values = {v.lower() for v in alias_map.values() if isinstance(v, str)}
        anchor_corpus = self._build_intelligence_anchor_corpus(intelligence)
        certainty_anchors = self._build_intelligence_certainty_anchors(intelligence)
        records: list[dict[str, Any]] = []
        dropped_unsupported = 0
        for rec in data["decision_records"]:
            try:
                rec = self._enforce_evidence(rec, transcript_clean)
            except DecisionIntelligenceV2Error:
                dropped_unsupported += 1
                continue
            if not self._is_record_anchored_to_intelligence(rec, anchor_corpus):
                dropped_unsupported += 1
                continue
            rec = self._repair_commitments(
                rec,
                transcript_clean,
                executive_primary,
                primary_actor,
                alias_map,
            )
            rec = self._resolve_owner_from_evidence(
                rec,
                transcript_clean,
                executive_primary,
                primary_actor,
                alias_map,
            )
            rec = self._extract_timeline_signals(rec, transcript_clean)
            rec = self._augment_dependencies(rec, executive, intelligence)
            rec = self._enforce_dependency_reasons(rec)
            rec = self._coerce_state_from_evidence(rec)
            rec = self._enforce_confirmed_rule(rec)
            rec = self._enforce_blocked_rule(rec)
            rec = self._apply_decision_status(rec)
            rec = self._calibrate_impact(rec)
            rec = self._enforce_primary_owner(rec)
            rec = self._enforce_missing_owner_gap(rec)
            rec = self._apply_confidence_corrections(rec, alias_map, known_values)
            rec = self._apply_truth_binding(rec, transcript_clean)
            if float(rec.get("evidence_confidence", 0.0) or 0.0) < 0.5:
                dropped_unsupported += 1
                continue
            cap = self._best_anchor_certainty(rec, certainty_anchors)
            if cap in {"UNCERTAIN", "CONDITIONAL"}:
                rec["certainty_class"] = cap
                if rec.get("state") == "confirmed":
                    rec["state"] = "tentative"
            rec = self._filter_weak_dependencies(rec, threshold=0.6)
            rec = self._enforce_owner_conservatism(rec, min_conf=0.75)
            rec = self._coerce_state_from_evidence(rec)
            rec = self._enforce_confirmed_rule(rec)
            rec = self._enforce_blocked_rule(rec)
            rec = self._apply_decision_status(rec)
            rec = self._enforce_missing_owner_gap(rec)
            if transcript_has_yes_contradiction and rec.get("decision_status") == "confirmed":
                rec["decision_status"] = "conditional"
                if rec.get("state") == "confirmed":
                    rec["state"] = "tentative"
            if not rec.get("decision_id"):
                rec["decision_id"] = self._decision_id(rec)
            records.append(rec)
        if not records:
            fallback = self._build_fallback_record_from_intelligence(
                intelligence=intelligence,
                transcript_clean=transcript_clean,
            )
            if fallback:
                records.append(fallback)
        if dropped_unsupported and not records:
            raise DecisionIntelligenceV2Error(
                "All candidate decision records were unsupported after evidence validation"
            )
        data["decision_records"] = records
        return data

    def _build_fallback_record_from_intelligence(
        self,
        intelligence: dict[str, Any],
        transcript_clean: str,
    ) -> dict[str, Any] | None:
        decisions = intelligence.get("decisions", []) if isinstance(intelligence, dict) else []
        if not isinstance(decisions, list) or not decisions:
            return None
        first = decisions[0]
        if not isinstance(first, dict):
            return None
        statement = str(first.get("text", "")).strip()
        evidence = str(first.get("evidence", "")).strip()
        if not statement:
            return None
        if not evidence or evidence not in transcript_clean:
            evidence = self._recover_evidence({"statement": statement, "evidence": [statement]}, transcript_clean)
        if not evidence:
            return None
        rec: dict[str, Any] = {
            "decision_id": "",
            "statement": statement,
            "state": "tentative",
            "decision_status": "tentative",
            "impact_level": "medium",
            "confidence": "low",
            "primary_owner": "",
            "owners": [{"actor": "", "ownership_type": "missing_owner"}],
            "commitments": [],
            "dependencies": [],
            "decision_gaps": [
                {
                    "gap_type": "missing_owner",
                    "criticality": "high",
                    "question": "Who is the accountable owner for this decision?",
                }
            ],
            "timeline_signals": [],
            "evidence": [evidence],
            "support_level": "WEAK_INFERENCE",
            "claim_strength": "weak",
            "certainty_class": str(first.get("certainty_class", "UNCERTAIN")).strip().upper() or "UNCERTAIN",
            "evidence_span": evidence,
            "evidence_start_index": transcript_clean.find(evidence),
            "evidence_end_index": transcript_clean.find(evidence) + len(evidence),
            "evidence_confidence": 0.6,
            "owner_confidence": 0.0,
        }
        rec = self._extract_timeline_signals(rec, transcript_clean)
        rec = self._apply_truth_binding(rec, transcript_clean)
        if float(rec.get("evidence_confidence", 0.0) or 0.0) < 0.5:
            return None
        rec["decision_id"] = self._decision_id(rec)
        return rec

    def _enforce_primary_owner(self, rec: dict[str, Any]) -> dict[str, Any]:
        owners = [o for o in rec.get("owners", []) if isinstance(o, dict)]
        assigned = [
            o.get("actor", "").strip()
            for o in owners
            if o.get("ownership_type") == "assigned_owner" and o.get("actor", "").strip()
        ]
        shared = [
            o.get("actor", "").strip()
            for o in owners
            if o.get("ownership_type") == "shared_owner" and o.get("actor", "").strip()
        ]
        if assigned:
            rec["primary_owner"] = assigned[0]
        elif shared:
            rec["primary_owner"] = shared[0]
        elif str(rec.get("primary_owner", "")).strip():
            rec["primary_owner"] = str(rec.get("primary_owner", "")).strip()
        else:
            rec["primary_owner"] = ""
        return rec

    def _resolve_owner_from_evidence(
        self,
        rec: dict[str, Any],
        transcript_clean: str,
        executive_primary: str,
        primary_actor: str,
        alias_map: dict[str, str],
    ) -> dict[str, Any]:
        owners = [o for o in rec.get("owners", []) if isinstance(o, dict)]
        explicit_candidate = ""
        for owner in owners:
            actor = str(owner.get("actor", "")).strip()
            if actor and owner.get("ownership_type") in {"assigned_owner", "shared_owner"}:
                explicit_candidate = self._normalize_actor(actor, alias_map)
                break
        if not explicit_candidate:
            for cmt in rec.get("commitments", []):
                if not isinstance(cmt, dict):
                    continue
                actor = self._normalize_actor(str(cmt.get("actor", "")).strip(), alias_map)
                if actor:
                    explicit_candidate = actor
                    break

        combined = " ".join(self._collect_record_sentences(rec, transcript_clean))
        resolved = explicit_candidate
        if not resolved:
            for actor in [executive_primary, primary_actor]:
                if actor and re.search(rf"\b{re.escape(actor)}\b", combined, flags=re.IGNORECASE):
                    resolved = actor
                    break
        if not resolved and self._has_responsibility_language(combined):
            actor_guess, actor_conf = resolve_actor_from_text(
                text=combined,
                transcript=transcript_clean,
                alias_map=alias_map,
                preferred_actor=executive_primary,
                fallback_actor=primary_actor,
            )
            if actor_guess != "unknown" and actor_conf >= 0.75:
                resolved = self._normalize_actor(actor_guess, alias_map)

        if resolved:
            rec["primary_owner"] = resolved
            if not any(
                isinstance(o, dict)
                and o.get("ownership_type") == "assigned_owner"
                and str(o.get("actor", "")).strip() == resolved
                for o in owners
            ):
                owners.insert(0, {"actor": resolved, "ownership_type": "assigned_owner"})

        cleaned_owners: list[dict[str, str]] = []
        seen = set()
        for owner in owners:
            actor = self._normalize_actor(str(owner.get("actor", "")).strip(), alias_map)
            ownership_type = self._coerce(
                str(owner.get("ownership_type", "")).strip().lower(), OWNERSHIP_ENUM, "missing_owner"
            )
            if ownership_type != "missing_owner" and not actor:
                continue
            if ownership_type == "missing_owner" and actor:
                ownership_type = "assigned_owner"
            key = (actor, ownership_type)
            if key in seen:
                continue
            seen.add(key)
            cleaned_owners.append({"actor": actor, "ownership_type": ownership_type})

        rec["owners"] = cleaned_owners
        return rec

    def _normalize_commitment_text(self, sentence: str) -> str:
        lowered = sentence.lower().strip()
        if "i will mark" in lowered or "i will list" in lowered or "phasing" in lowered:
            return "Prepare a question list and phased project view for the next meeting."
        if "we will meet" in lowered or "meet again" in lowered:
            return "Hold a follow-up meeting to continue project clarification."
        if "yes" in lowered and "move forward" in lowered:
            return "Move forward with the project and proceed with deeper clarification."
        if "you will" in lowered and ("representative" in lowered or "responsible" in lowered):
            return "Act as the on-ground representative and execution lead in India."
        return sentence.strip()

    def _extract_commitment_actor(
        self,
        sentence: str,
        commitment_type: str,
        executive_primary: str,
        primary_actor: str,
        alias_map: dict[str, str],
    ) -> str:
        lowered = sentence.lower()
        if commitment_type == "implied_commitment" and "you will" in lowered:
            return executive_primary or primary_actor
        if commitment_type == "requested_commitment":
            if "can you" in lowered or "would you consider" in lowered:
                return executive_primary or primary_actor
            return ""
        if commitment_type == "explicit_commitment":
            if "i will" in lowered or "i'll" in lowered:
                ordered_candidates: list[str] = []
                for name in [primary_actor, executive_primary]:
                    if name and name not in ordered_candidates:
                        ordered_candidates.append(name)
                for name in ordered_candidates:
                    if re.search(rf"\b{re.escape(name)}\b", sentence, flags=re.IGNORECASE):
                        return self._normalize_actor(name, alias_map)
                return ""
            if "we will" in lowered or "we'll" in lowered:
                return ""

        ordered_candidates: list[str] = []
        for name in [executive_primary, primary_actor]:
            if name and name not in ordered_candidates:
                ordered_candidates.append(name)
        for name in ordered_candidates:
            if name and re.search(rf"\b{re.escape(name)}\b", sentence, flags=re.IGNORECASE):
                return self._normalize_actor(name, alias_map)
        return ""

    def _append_commitment_if_new(
        self,
        bucket: list[dict[str, str]],
        actor: str,
        commitment: str,
        commitment_type: str,
        status: str,
        confidence: str,
    ) -> None:
        actor = actor.strip()
        commitment = commitment.strip()
        if not actor and not commitment:
            return
        key = (actor.lower(), commitment.lower(), commitment_type)
        for existing in bucket:
            ekey = (
                str(existing.get("actor", "")).lower(),
                str(existing.get("commitment", "")).lower(),
                str(existing.get("commitment_type", "")),
            )
            if key == ekey:
                return
        bucket.append(
            {
                "actor": actor,
                "commitment": commitment,
                "commitment_type": commitment_type,
                "status": status,
                "confidence": confidence,
            }
        )

    def _repair_commitments(
        self,
        rec: dict[str, Any],
        transcript_clean: str,
        executive_primary: str,
        primary_actor: str,
        alias_map: dict[str, str],
    ) -> dict[str, Any]:
        repaired: list[dict[str, str]] = []
        for item in rec.get("commitments", []):
            if not isinstance(item, dict):
                continue
            actor = self._normalize_actor(str(item.get("actor", "")).strip(), alias_map)
            commitment = str(item.get("commitment", "")).strip()
            ctype = self._coerce(
                str(item.get("commitment_type", "")).strip().lower(),
                COMMITMENT_TYPE_ENUM,
                "unresolved_commitment",
            )
            status = self._coerce(
                str(item.get("status", "")).strip().lower(), COMMITMENT_STATUS_ENUM, "unresolved"
            )
            confidence = self._coerce(str(item.get("confidence", "")).strip().lower(), HML, "low")
            if not actor and not commitment:
                continue
            self._append_commitment_if_new(repaired, actor, commitment, ctype, status, confidence)

        for sentence in self._collect_record_sentences(rec, transcript_clean):
            lowered = sentence.lower()
            has_implied = any(pattern in lowered for pattern in IMPLIED_PATTERNS)
            has_conditional = any(pattern in lowered for pattern in CONDITIONAL_MARKERS)
            if has_implied:
                actor = self._extract_commitment_actor(
                    sentence,
                    "implied_commitment",
                    executive_primary,
                    primary_actor,
                    alias_map,
                )
                self._append_commitment_if_new(
                    repaired,
                    actor,
                    self._normalize_commitment_text(sentence),
                    "implied_commitment",
                    "accepted",
                    "medium",
                )

            if any(pattern in lowered for pattern in REQUESTED_PATTERNS):
                actor = self._extract_commitment_actor(
                    sentence,
                    "requested_commitment",
                    executive_primary,
                    primary_actor,
                    alias_map,
                )
                self._append_commitment_if_new(
                    repaired,
                    actor,
                    self._normalize_commitment_text(sentence),
                    "requested_commitment",
                    "open",
                    "medium",
                )

            if any(pattern in lowered for pattern in EXPLICIT_PATTERNS) and not has_implied and not has_conditional:
                actor = self._extract_commitment_actor(
                    sentence,
                    "explicit_commitment",
                    executive_primary,
                    primary_actor,
                    alias_map,
                )
                confidence = "medium" if any(marker in lowered for marker in UNCERTAINTY_MARKERS) else "high"
                self._append_commitment_if_new(
                    repaired,
                    actor,
                    self._normalize_commitment_text(sentence),
                    "explicit_commitment",
                    "accepted",
                    confidence,
                )

            if has_conditional and any(pattern in lowered for pattern in REQUESTED_PATTERNS + EXPLICIT_PATTERNS):
                actor = self._extract_commitment_actor(
                    sentence,
                    "requested_commitment",
                    executive_primary,
                    primary_actor,
                    alias_map,
                )
                self._append_commitment_if_new(
                    repaired,
                    actor,
                    self._normalize_commitment_text(sentence),
                    "requested_commitment",
                    "unresolved",
                    "low",
                )

        if not repaired:
            text = " ".join(self._collect_record_sentences(rec, transcript_clean)).lower()
            if "will" in text or "responsible" in text:
                self._append_commitment_if_new(
                    repaired,
                    "",
                    "Clarify commitment ownership and acceptance.",
                    "unresolved_commitment",
                    "unresolved",
                    "low",
                )

        rec["commitments"] = repaired
        return rec

    def _extract_timeline_signals(self, rec: dict[str, Any], transcript_clean: str) -> dict[str, Any]:
        signals = [s for s in rec.get("timeline_signals", []) if isinstance(s, dict)]
        seen = {
            (
                str(s.get("signal_type", "")).strip().lower(),
                str(s.get("raw_reference", "")).strip().lower(),
            )
            for s in signals
            if str(s.get("raw_reference", "")).strip()
        }

        transcript_sentences = self._split_sentences(transcript_clean)
        sentences = self._collect_record_sentences(rec, transcript_clean)
        statement = str(rec.get("statement", "")).lower()
        if "meeting" in statement or "meet" in statement:
            for sentence in self._split_sentences(transcript_clean):
                lowered = sentence.lower()
                if "meet" in lowered or "class" in lowered:
                    sentences.append(sentence)

        for sentence in sentences:
            for pattern, signal_type, confidence in TIME_PATTERNS:
                for match in re.finditer(pattern, sentence, flags=re.IGNORECASE):
                    raw = sentence[match.start() : match.end()].strip()
                    key = (signal_type, raw.lower())
                    if raw and key not in seen:
                        seen.add(key)
                        signals.append(
                            {
                                "signal_type": signal_type,
                                "raw_reference": raw,
                                "confidence": confidence,
                            }
                        )

        record_text = " ".join(
            [str(rec.get("statement", ""))]
            + [str(e) for e in rec.get("evidence", []) if isinstance(e, str)]
            + [str(c.get("commitment", "")) for c in rec.get("commitments", []) if isinstance(c, dict)]
        ).lower()
        if not signals and any(
            k in record_text for k in ["start", "phase", "meeting", "meet", "implementation", "project"]
        ):
            for sentence in transcript_sentences:
                for pattern, signal_type, confidence in TIME_PATTERNS:
                    for match in re.finditer(pattern, sentence, flags=re.IGNORECASE):
                        raw = sentence[match.start() : match.end()].strip()
                        key = (signal_type, raw.lower())
                        if raw and key not in seen:
                            seen.add(key)
                            signals.append(
                                {
                                    "signal_type": signal_type,
                                    "raw_reference": raw,
                                    "confidence": confidence,
                                }
                            )
                if len(signals) >= 4:
                    break

        if ("meeting" in statement or "meet" in statement) and not any(
            "5:30" in str(s.get("raw_reference", "")) for s in signals if isinstance(s, dict)
        ):
            for sentence in transcript_sentences:
                for pattern, signal_type, confidence in (
                    (r"\btuesday\s+after\s+5:30\b", "followup_marker", "high"),
                    (r"\bafter\s+5:30\b", "followup_marker", "high"),
                ):
                    for match in re.finditer(pattern, sentence, flags=re.IGNORECASE):
                        raw = sentence[match.start() : match.end()].strip()
                        key = (signal_type, raw.lower())
                        if raw and key not in seen:
                            seen.add(key)
                            signals.append(
                                {
                                    "signal_type": signal_type,
                                    "raw_reference": raw,
                                    "confidence": confidence,
                                }
                            )

        if any(k in record_text for k in ["start", "project", "implementation"]) and not any(
            "june" in str(s.get("raw_reference", "")).lower() for s in signals if isinstance(s, dict)
        ):
            for sentence in transcript_sentences:
                for pattern, signal_type, confidence in (
                    (r"\bmaybe\s+june\b", "start_window", "medium"),
                    (r"\bfirst\s+june\b", "start_window", "medium"),
                ):
                    for match in re.finditer(pattern, sentence, flags=re.IGNORECASE):
                        raw = sentence[match.start() : match.end()].strip()
                        key = (signal_type, raw.lower())
                        if raw and key not in seen:
                            seen.add(key)
                            signals.append(
                                {
                                    "signal_type": signal_type,
                                    "raw_reference": raw,
                                    "confidence": confidence,
                                }
                            )

        rec["timeline_signals"] = signals
        return rec

    def _enforce_missing_owner_gap(self, rec: dict[str, Any]) -> dict[str, Any]:
        primary_owner = str(rec.get("primary_owner", "")).strip()
        owners = [o for o in rec.get("owners", []) if isinstance(o, dict)]
        gaps = [g for g in rec.get("decision_gaps", []) if isinstance(g, dict)]
        if primary_owner:
            owners = [
                o
                for o in owners
                if not (
                    o.get("ownership_type") == "missing_owner"
                    and not str(o.get("actor", "")).strip()
                )
            ]
            if not any(
                str(o.get("actor", "")).strip() == primary_owner
                and o.get("ownership_type") in {"assigned_owner", "shared_owner"}
                for o in owners
            ):
                owners.insert(0, {"actor": primary_owner, "ownership_type": "assigned_owner"})
            gaps = [
                g
                for g in gaps
                if str(g.get("gap_type", "")).lower() != "missing_owner"
                and "accountable owner" not in str(g.get("question", "")).lower()
            ]
            rec["owners"] = owners
            rec["decision_gaps"] = gaps
            return rec

        if not any(o.get("ownership_type") == "missing_owner" for o in owners):
            owners.append({"actor": "", "ownership_type": "missing_owner"})
        if not any(
            str(g.get("gap_type", "")).lower() == "missing_owner"
            or "accountable owner" in str(g.get("question", "")).lower()
            for g in gaps
        ):
            gaps.append(
                {
                    "gap_type": "missing_owner",
                    "criticality": "high",
                    "question": "Who is the accountable owner for this decision?",
                }
            )
        rec["owners"] = owners
        rec["decision_gaps"] = gaps
        return rec

    def _coerce_state_from_evidence(self, rec: dict[str, Any]) -> dict[str, Any]:
        text = " ".join(
            [str(rec.get("statement", ""))]
            + [str(e) for e in rec.get("evidence", []) if isinstance(e, str)]
            + [str(c.get("commitment", "")) for c in rec.get("commitments", []) if isinstance(c, dict)]
        ).lower()
        if self._has_open_high(rec):
            rec["state"] = "blocked"
            return rec
        has_pending = any(marker in text for marker in PENDING_MARKERS)
        has_weak = any(marker in text for marker in UNCERTAINTY_MARKERS)
        certainty_class = str(rec.get("certainty_class", "")).upper()
        has_explicit = any(
            isinstance(c, dict) and c.get("commitment_type") == "explicit_commitment"
            for c in rec.get("commitments", [])
        )
        if has_pending:
            rec["state"] = "pending"
        elif has_weak or certainty_class == "UNCERTAIN":
            rec["state"] = "tentative"
        elif certainty_class == "CONDITIONAL":
            rec["state"] = "tentative"
        elif has_explicit:
            rec["state"] = "confirmed"
        elif rec.get("state") not in STATE_ENUM:
            rec["state"] = "tentative"
        return rec

    def _enforce_confirmed_rule(self, rec: dict[str, Any]) -> dict[str, Any]:
        if rec.get("state") != "confirmed":
            return rec
        text = " ".join(
            [str(rec.get("statement", ""))]
            + [str(e) for e in rec.get("evidence", []) if isinstance(e, str)]
        ).lower()
        has_explicit = any(
            c.get("commitment_type") == "explicit_commitment"
            for c in rec.get("commitments", [])
            if isinstance(c, dict)
        )
        if not has_explicit:
            rec["state"] = "blocked" if self._has_open_high(rec) else "tentative"
            return rec
        if str(rec.get("certainty_class", "")).upper() in {"UNCERTAIN", "CONDITIONAL"}:
            rec["state"] = "tentative"
            return rec
        if not is_semantically_supportive(
            str(rec.get("statement", "")),
            str(rec.get("evidence_span", "")),
            claim_type="decision",
        ):
            rec["state"] = "tentative"
            return rec
        if any(marker in text for marker in PENDING_MARKERS):
            rec["state"] = "pending"
        elif any(marker in text for marker in UNCERTAINTY_MARKERS):
            rec["state"] = "tentative"
        return rec

    def _enforce_blocked_rule(self, rec: dict[str, Any]) -> dict[str, Any]:
        if self._has_open_high(rec):
            rec["state"] = "blocked"
        return rec

    def _apply_decision_status(self, rec: dict[str, Any]) -> dict[str, Any]:
        text = " ".join(
            [str(rec.get("statement", ""))]
            + [str(e) for e in rec.get("evidence", []) if isinstance(e, str)]
            + [str(c.get("commitment", "")) for c in rec.get("commitments", []) if isinstance(c, dict)]
        ).lower()
        certainty_class = str(rec.get("certainty_class", "")).upper()
        if any(marker in text for marker in CONDITIONAL_MARKERS) or certainty_class == "CONDITIONAL":
            rec["decision_status"] = "conditional"
            if rec.get("state") == "confirmed":
                rec["state"] = "tentative"
            return rec
        if certainty_class == "UNCERTAIN":
            rec["decision_status"] = "tentative"
            if rec.get("state") == "confirmed":
                rec["state"] = "tentative"
            return rec
        if rec.get("state") == "blocked":
            rec["decision_status"] = "blocked"
        elif rec.get("state") == "confirmed":
            rec["decision_status"] = "confirmed"
        elif rec.get("state") in {"pending", "tentative"}:
            rec["decision_status"] = "tentative"
        else:
            rec["decision_status"] = "tentative"
        return rec

    def _has_open_high(self, rec: dict[str, Any]) -> bool:
        return any(
            d.get("status") == "open" and d.get("blocking_level") == "high"
            for d in rec.get("dependencies", [])
            if isinstance(d, dict)
        )

    def _calibrate_impact(self, rec: dict[str, Any]) -> dict[str, Any]:
        text = " ".join(
            [str(rec.get("statement", ""))]
            + [str(e) for e in rec.get("evidence", []) if isinstance(e, str)]
        ).lower()
        high_markers = (
            "center of excellence",
            "coe",
            "four years",
            "representative",
            "responsible",
            "authority",
            "governance",
            "funding",
            "revenue",
            "tuition",
            "india",
            "long-term project",
            "implementation",
        )
        medium_markers = (
            "next meeting",
            "meet again",
            "tuesday",
            "first of may",
            "first week",
            "phasing",
            "questions",
            "start with",
        )
        low_markers = ("whatsapp", "close from here", "thanks", "bon app")
        if any(marker in text for marker in low_markers):
            rec["impact_level"] = "low"
        elif any(marker in text for marker in high_markers):
            rec["impact_level"] = "high"
        elif any(marker in text for marker in medium_markers):
            rec["impact_level"] = "medium"
        else:
            rec["impact_level"] = "medium"
        if "meeting" in text and rec["impact_level"] == "high":
            rec["impact_level"] = "medium"
        if any(marker in text for marker in UNCERTAINTY_MARKERS) and rec["impact_level"] == "high":
            rec["impact_level"] = "medium"
        return rec

    def _apply_confidence_corrections(
        self,
        rec: dict[str, Any],
        alias_map: dict[str, str],
        known_values: set[str],
    ) -> dict[str, Any]:
        def rank(level: str) -> int:
            return {"low": 0, "medium": 1, "high": 2}.get(level, 1)

        def min_level(cur: str, cap: str) -> str:
            return cur if rank(cur) <= rank(cap) else cap

        def known(actor: str) -> bool:
            return bool(actor) and (actor.lower() in alias_map or actor.lower() in known_values)

        rec["confidence"] = self._coerce(str(rec.get("confidence", "")).lower(), HML, "medium")
        primary_owner = str(rec.get("primary_owner", "")).strip()
        if primary_owner and not known(primary_owner):
            rec["confidence"] = "low"
        if not primary_owner:
            rec["confidence"] = min_level(rec["confidence"], "medium")

        text = " ".join(
            [str(rec.get("statement", ""))]
            + [str(e) for e in rec.get("evidence", []) if isinstance(e, str)]
        ).lower()
        if any(marker in text for marker in UNCERTAINTY_MARKERS):
            rec["confidence"] = min_level(rec["confidence"], "medium")
        if any(marker in text for marker in PENDING_MARKERS):
            rec["confidence"] = min_level(rec["confidence"], "medium")

        commitment_types = {
            c.get("commitment_type")
            for c in rec.get("commitments", [])
            if isinstance(c, dict)
        }
        if commitment_types and commitment_types <= {"requested_commitment", "unresolved_commitment"}:
            rec["confidence"] = min_level(rec["confidence"], "medium")
            if commitment_types == {"unresolved_commitment"}:
                rec["confidence"] = "low"

        for cmt in rec.get("commitments", []):
            if not isinstance(cmt, dict):
                continue
            actor = str(cmt.get("actor", "")).strip()
            if actor and not known(actor):
                cmt["confidence"] = "low"
                rec["confidence"] = "low"
            if cmt.get("commitment_type") in {"requested_commitment", "unresolved_commitment"} and cmt.get("confidence") == "high":
                cmt["confidence"] = "medium"
        if rec.get("state") in {"tentative", "pending"}:
            rec["confidence"] = min_level(rec["confidence"], "medium")
        certainty_class = str(rec.get("certainty_class", "")).upper()
        if certainty_class in {"UNCERTAIN", "CONDITIONAL"}:
            rec["confidence"] = min_level(rec["confidence"], "medium")
        if certainty_class == "UNCERTAIN":
            rec["confidence"] = min_level(rec["confidence"], "low")
        owner_conf = float(rec.get("owner_confidence", 0.0) or 0.0)
        if owner_conf and owner_conf < 0.75:
            rec["confidence"] = min_level(rec["confidence"], "medium")
        return rec

    def _apply_truth_binding(self, rec: dict[str, Any], transcript_clean: str) -> dict[str, Any]:
        statement = str(rec.get("statement", "")).strip()
        evidence = [ev for ev in rec.get("evidence", []) if isinstance(ev, str)]
        binding = build_evidence_binding(
            statement,
            transcript_clean,
            preferred_spans=evidence,
            claim_type="decision",
        )
        if evidence and float(binding.get("evidence_confidence", 0.0)) < 0.5:
            binding = build_evidence_binding(
                evidence[0],
                transcript_clean,
                preferred_spans=evidence,
                claim_type="decision",
            )
        rec["support_level"] = binding["support_level"]
        rec["claim_strength"] = binding["claim_strength"]
        rec["evidence_span"] = binding["evidence_span"]
        rec["evidence_start_index"] = binding["evidence_start_index"]
        rec["evidence_end_index"] = binding["evidence_end_index"]
        rec["evidence_confidence"] = binding["evidence_confidence"]
        rec["certainty_class"] = self._infer_record_certainty(rec)
        if binding["evidence_span"] and binding["evidence_span"] in transcript_clean:
            rec["evidence"] = self._prefer_sentence_evidence([binding["evidence_span"]], transcript_clean)

        record_context = " ".join(self._collect_record_sentences(rec, transcript_clean))
        for owner in rec.get("owners", []):
            if not isinstance(owner, dict):
                continue
            actor = str(owner.get("actor", "")).strip()
            if not actor:
                owner["support_level"] = "WEAK_INFERENCE"
                owner["evidence_confidence"] = 0.0
                owner["actor_confidence"] = 0.0
                continue
            actor_supported = actor_present_in_transcript(actor, transcript_clean, alias_map={})
            _, actor_conf = resolve_actor_from_text(
                text=record_context,
                transcript=transcript_clean,
                alias_map={},
                preferred_actor=actor,
                fallback_actor="",
            )
            if actor_supported:
                actor_conf = max(float(actor_conf), 0.8)
            if not actor_supported:
                owner["actor"] = ""
                owner["ownership_type"] = "missing_owner"
            owner["support_level"] = SUPPORT_DIRECT if actor_supported else "WEAK_INFERENCE"
            owner["evidence_confidence"] = 1.0 if owner["support_level"] == SUPPORT_DIRECT else 0.4
            owner["actor_confidence"] = round(float(actor_conf if actor_supported else min(actor_conf, 0.49)), 3)

        repaired_commitments: list[dict[str, Any]] = []
        for cmt in rec.get("commitments", []):
            if not isinstance(cmt, dict):
                continue
            actor = str(cmt.get("actor", "")).strip()
            if actor and not actor_present_in_transcript(actor, transcript_clean, alias_map={}):
                cmt["actor"] = ""
            claim = str(cmt.get("commitment", "")).strip()
            pref = rec.get("evidence", [])
            cbind = build_evidence_binding(
                claim,
                transcript_clean,
                preferred_spans=pref if isinstance(pref, list) else [],
                claim_type="decision",
            )
            cmt["support_level"] = cbind["support_level"]
            cmt["claim_strength"] = cbind["claim_strength"]
            cmt["evidence_span"] = cbind["evidence_span"]
            cmt["evidence_start_index"] = cbind["evidence_start_index"]
            cmt["evidence_end_index"] = cbind["evidence_end_index"]
            cmt["evidence_confidence"] = cbind["evidence_confidence"]
            if str(cmt.get("actor", "")).strip() or claim:
                repaired_commitments.append(cmt)
        rec["commitments"] = repaired_commitments

        for dep in rec.get("dependencies", []):
            if not isinstance(dep, dict):
                continue
            dep_claim = f"{dep.get('type', '')} {dep.get('reason', '')}".strip()
            dep_type = str(dep.get("type", "")).strip().lower() or "governance_dependency"
            dbind = build_evidence_binding(
                dep_claim,
                transcript_clean,
                preferred_spans=rec.get("evidence", []),
                claim_type=f"dependency:{dep_type}",
            )
            dep["support_level"] = dbind["support_level"]
            dep["claim_strength"] = dbind["claim_strength"]
            dep["evidence_span"] = dbind["evidence_span"]
            dep["evidence_start_index"] = dbind["evidence_start_index"]
            dep["evidence_end_index"] = dbind["evidence_end_index"]
            dep["evidence_confidence"] = dbind["evidence_confidence"]
            if not is_semantically_supportive(dep_claim, str(dep.get("evidence_span", "")), claim_type=f"dependency:{dep_type}"):
                dep["evidence_confidence"] = min(float(dep.get("evidence_confidence", 0.0)), 0.49)
                dep["support_level"] = "WEAK_INFERENCE"
                dep["claim_strength"] = "weak"

        for sig in rec.get("timeline_signals", []):
            if not isinstance(sig, dict):
                continue
            raw_ref = str(sig.get("raw_reference", "")).strip()
            sbind = build_evidence_binding(
                raw_ref,
                transcript_clean,
                preferred_spans=[raw_ref],
                claim_type="timeline",
            )
            sig["support_level"] = sbind["support_level"]
            sig["claim_strength"] = sbind["claim_strength"]
            sig["evidence_span"] = sbind["evidence_span"]
            sig["evidence_start_index"] = sbind["evidence_start_index"]
            sig["evidence_end_index"] = sbind["evidence_end_index"]
            sig["evidence_confidence"] = sbind["evidence_confidence"]
        return rec

    def _infer_record_certainty(self, rec: dict[str, Any]) -> str:
        text = " ".join(
            [str(rec.get("statement", ""))]
            + [str(ev) for ev in rec.get("evidence", []) if isinstance(ev, str)]
            + [str(c.get("commitment", "")) for c in rec.get("commitments", []) if isinstance(c, dict)]
        ).lower()
        if any(marker in text for marker in CERTAINTY_CONDITIONAL_MARKERS):
            return "CONDITIONAL"
        if any(marker in text for marker in CERTAINTY_UNCERTAIN_MARKERS):
            return "UNCERTAIN"
        return "DIRECT"

    def _filter_weak_dependencies(self, rec: dict[str, Any], threshold: float) -> dict[str, Any]:
        filtered: list[dict[str, Any]] = []
        for dep in rec.get("dependencies", []):
            if not isinstance(dep, dict):
                continue
            conf = float(dep.get("evidence_confidence", 0.0) or 0.0)
            if conf < threshold:
                continue
            filtered.append(dep)
        rec["dependencies"] = filtered
        return rec

    def _enforce_owner_conservatism(self, rec: dict[str, Any], min_conf: float) -> dict[str, Any]:
        owners: list[dict[str, Any]] = []
        for owner in rec.get("owners", []):
            if not isinstance(owner, dict):
                continue
            actor = str(owner.get("actor", "")).strip()
            actor_conf = float(owner.get("actor_confidence", 0.0) or 0.0)
            if actor and actor_conf < min_conf:
                owner = dict(owner)
                owner["actor"] = ""
                owner["ownership_type"] = "missing_owner"
            owners.append(owner)
        rec["owners"] = owners
        primary_owner = str(rec.get("primary_owner", "")).strip()
        if primary_owner:
            matching = [
                o
                for o in owners
                if isinstance(o, dict) and str(o.get("actor", "")).strip() == primary_owner
            ]
            if not matching:
                rec["primary_owner"] = ""
            else:
                conf = float(matching[0].get("actor_confidence", 0.0) or 0.0)
                if conf < min_conf:
                    rec["primary_owner"] = ""
        rec["owner_confidence"] = max(
            [float(o.get("actor_confidence", 0.0) or 0.0) for o in owners if isinstance(o, dict)],
            default=0.0,
        )
        return rec

    def _decision_min_confidence_threshold(self) -> float:
        raw = str(os.getenv("DECISION_MIN_CONFIDENCE_SCORE", str(DECISION_DEFAULT_MIN_CONFIDENCE_SCORE))).strip()
        try:
            parsed = float(raw)
        except Exception:
            parsed = DECISION_DEFAULT_MIN_CONFIDENCE_SCORE
        return max(0.0, min(1.0, parsed))

    def _extract_evidence_snippets(self, rec: dict[str, Any], transcript_clean: str) -> list[str]:
        snippets: list[str] = []
        seen: set[str] = set()
        for ev in rec.get("evidence", []):
            if not isinstance(ev, str):
                continue
            text = ev.strip()
            if text and text in transcript_clean and text not in seen:
                snippets.append(text)
                seen.add(text)
        span = str(rec.get("evidence_span", "")).strip()
        if span and span in transcript_clean and span not in seen:
            snippets.append(span)
        preferred = self._prefer_sentence_evidence(snippets, transcript_clean)
        return [s for s in preferred if isinstance(s, str) and s.strip()]

    def _extract_source_timestamps(self, snippets: list[str], transcript_raw: str) -> list[str]:
        if not transcript_raw.strip() or not snippets:
            return []
        ts_pattern = re.compile(r"(?:\[\d{1,2}:\d{2}(?::\d{2})?\]|\b\d{1,2}:\d{2}(?::\d{2})?\b)")
        found: set[str] = set()
        for snippet in snippets:
            start = 0
            while True:
                idx = transcript_raw.find(snippet, start)
                if idx < 0:
                    break
                window_start = max(0, idx - 100)
                window_end = min(len(transcript_raw), idx + len(snippet) + 100)
                window = transcript_raw[window_start:window_end]
                for ts in ts_pattern.findall(window):
                    token = str(ts).strip("[] ")
                    if token:
                        found.add(token)
                start = idx + len(snippet)
        return sorted(found)

    def _is_multitopic_statement(self, statement: str) -> bool:
        lowered = statement.lower()
        families = 0
        for keywords in DECISION_TOPIC_FAMILIES.values():
            if any(keyword in lowered for keyword in keywords):
                families += 1
        separators = (
            lowered.count(" and ")
            + lowered.count(";")
            + lowered.count(",")
            + lowered.count(" also ")
        )
        return families >= 3 and separators >= 2

    def _is_specific_actionable_decision(self, rec: dict[str, Any]) -> bool:
        statement = str(rec.get("statement", "")).strip()
        if not statement:
            return False
        words = re.findall(r"[A-Za-z0-9]+", statement)
        if len(words) < 4 or len(words) > 80:
            return False
        lowered = statement.lower()
        if any(marker in lowered for marker in DECISION_GENERIC_MARKERS):
            return False
        has_action_verb = any(verb in lowered for verb in DECISION_ACTIONABLE_VERBS)
        has_explicit_decision = any(verb in lowered for verb in DECISION_EXPLICIT_VERBS)
        has_commitment = any(
            isinstance(cmt, dict)
            and str(cmt.get("commitment", "")).strip()
            and cmt.get("commitment_type") in {"explicit_commitment", "implied_commitment"}
            for cmt in rec.get("commitments", [])
        )
        has_owner = bool(str(rec.get("primary_owner", "")).strip())
        return (has_explicit_decision or has_action_verb) and (has_commitment or has_action_verb or has_owner)

    def _clean_clause(self, text: str) -> str:
        value = " ".join(str(text or "").strip().split())
        value = re.sub(r"^[,;:\-\s]+", "", value).strip()
        value = re.sub(r"\b(and|then)\b$", "", value, flags=re.IGNORECASE).strip()
        return value

    def _is_non_actionable_statement(self, statement: str, rec: dict[str, Any]) -> bool:
        lowered = statement.lower().strip()
        if not lowered:
            return True
        if "?" in lowered:
            return True
        if any(lowered.startswith(prefix) for prefix in ("what ", "why ", "how ", "when ", "where ", "who ")):
            return True
        if any(marker in lowered for marker in DECISION_NON_ACTIONABLE_MARKERS):
            return True
        if any(marker in lowered for marker in UNCERTAINTY_MARKERS) and not any(
            verb in lowered for verb in DECISION_ACTIONABLE_VERBS
        ):
            return True
        has_commitment = any(
            isinstance(cmt, dict)
            and cmt.get("commitment_type") in {"explicit_commitment", "implied_commitment"}
            and str(cmt.get("commitment", "")).strip()
            for cmt in rec.get("commitments", [])
        )
        has_action = any(verb in lowered for verb in DECISION_ACTIONABLE_VERBS)
        return not (has_commitment or has_action)

    def _split_record_actions(self, rec: dict[str, Any]) -> list[dict[str, Any]]:
        statement = str(rec.get("statement", "")).strip()
        if not statement:
            return [rec]
        normalized = re.sub(r"\s+", " ", statement)
        delimiters = re.split(r"\s*;\s*|\s+\band\b\s+", normalized, flags=re.IGNORECASE)
        parts = [self._clean_clause(part) for part in delimiters if self._clean_clause(part)]
        if len(parts) <= 1:
            return [rec]
        actionable_parts: list[str] = []
        for part in parts:
            lowered = part.lower()
            if self._is_non_actionable_statement(part, rec):
                continue
            if any(verb in lowered for verb in DECISION_ACTIONABLE_VERBS + DECISION_EXPLICIT_VERBS):
                actionable_parts.append(part)
        if len(actionable_parts) <= 1:
            return [rec]
        out: list[dict[str, Any]] = []
        for idx, part in enumerate(actionable_parts, start=1):
            cloned = dict(rec)
            cloned["statement"] = part
            base_id = str(rec.get("decision_id", "")).strip()
            if base_id:
                cloned["decision_id"] = f"{base_id}-S{idx}"
            out.append(cloned)
        return out

    def _refine_decision_text(self, statement: str) -> str:
        text = " ".join(str(statement or "").strip().split())
        if not text:
            return ""
        lowered = text.lower()
        replacements = (
            (r"^\s*we\s+will\s+", ""),
            (r"^\s*i\s+will\s+", ""),
            (r"^\s*let'?s\s+", ""),
            (r"^\s*there\s+is\s+a\s+need\s+to\s+", ""),
            (r"^\s*it\s+was\s+mentioned\s+that\s+", ""),
            (r"^\s*we\s+need\s+to\s+", ""),
        )
        for pattern, repl in replacements:
            text = re.sub(pattern, repl, text, flags=re.IGNORECASE).strip()
        lowered = text.lower()

        if lowered.startswith("meet "):
            text = f"Schedule {text}".strip()
        elif " meet " in f" {lowered} " and not lowered.startswith("schedule "):
            text = f"Schedule {text}".strip()
        elif " assign " in f" {lowered} " and not lowered.startswith("assign "):
            text = f"Assign {text}".strip()
        elif "move forward" in lowered and not lowered.startswith("proceed "):
            text = f"Proceed with {text}".strip()
        elif " start " in f" {lowered} " and not lowered.startswith("start "):
            text = f"Start {text}".strip()
        elif " finalize " in f" {lowered} " and not lowered.startswith("finalize "):
            text = f"Finalize {text}".strip()
        elif " define " in f" {lowered} " and not lowered.startswith("define "):
            text = f"Define {text}".strip()

        text = self._clean_clause(text)
        if not text:
            return ""
        text = text[0].upper() + text[1:] if len(text) > 1 else text.upper()
        if not text.endswith("."):
            text += "."
        return text

    def _is_generic_owner(self, name: str) -> bool:
        cleaned = str(name or "").strip().lower()
        if not cleaned:
            return True
        if cleaned in DECISION_OWNER_GENERIC:
            return True
        if re.fullmatch(r"(the\s+)?team", cleaned):
            return True
        return False

    def _resolve_decision_owner(self, rec: dict[str, Any], transcript_clean: str) -> tuple[str, str, float]:
        primary_owner = str(rec.get("primary_owner", "")).strip()
        if primary_owner and not self._is_generic_owner(primary_owner):
            if actor_present_in_transcript(primary_owner, transcript_clean, alias_map={}):
                return primary_owner, primary_owner, float(rec.get("owner_confidence", 1.0) or 1.0)

        owners = [o for o in rec.get("owners", []) if isinstance(o, dict)]
        for owner in owners:
            actor = str(owner.get("actor", "")).strip()
            if not actor or self._is_generic_owner(actor):
                continue
            if owner.get("ownership_type") not in {"assigned_owner", "shared_owner"}:
                continue
            actor_conf = float(owner.get("actor_confidence", 0.0) or 0.0)
            if actor_conf < 0.75:
                continue
            if actor_present_in_transcript(actor, transcript_clean, alias_map={}):
                return actor, actor, actor_conf

        for cmt in rec.get("commitments", []):
            if not isinstance(cmt, dict):
                continue
            actor = str(cmt.get("actor", "")).strip()
            if not actor or self._is_generic_owner(actor):
                continue
            if actor_present_in_transcript(actor, transcript_clean, alias_map={}):
                return actor, actor, float(cmt.get("actor_confidence", 0.85) or 0.85)

        return "", "Unassigned", 0.0

    def _score_decision_confidence(self, rec: dict[str, Any], evidence_snippets: list[str]) -> tuple[float, str]:
        statement = str(rec.get("statement", "")).strip().lower()
        merged = " ".join([statement] + [s.lower() for s in evidence_snippets])
        score = 0.2

        if any(verb in merged for verb in DECISION_EXPLICIT_VERBS):
            score += 0.35
        elif any(
            isinstance(cmt, dict) and cmt.get("commitment_type") == "explicit_commitment"
            for cmt in rec.get("commitments", [])
        ):
            score += 0.25

        evidence_count = len(evidence_snippets)
        if evidence_count >= 3:
            score += 0.25
        elif evidence_count == 2:
            score += 0.2
        elif evidence_count == 1:
            score += 0.1

        if self._is_specific_actionable_decision(rec):
            score += 0.2
        if not self._is_multitopic_statement(statement):
            score += 0.05

        support_level = str(rec.get("support_level", "")).strip()
        claim_strength = str(rec.get("claim_strength", "")).strip()
        if support_level == "WEAK_INFERENCE" or claim_strength == "weak":
            score -= 0.35

        if any(marker in merged for marker in UNCERTAINTY_MARKERS):
            score -= 0.2
        if any(marker in merged for marker in CONDITIONAL_MARKERS):
            score -= 0.1

        certainty_class = str(rec.get("certainty_class", "")).upper()
        if certainty_class in {"UNCERTAIN", "CONDITIONAL"}:
            score -= 0.1

        clamped = max(0.0, min(1.0, round(score, 3)))
        if clamped >= 0.75:
            return clamped, DECISION_LABEL_HIGH
        if clamped >= 0.5:
            return clamped, DECISION_LABEL_MEDIUM
        return clamped, DECISION_LABEL_LOW

    def _decision_dedup_key(self, rec: dict[str, Any]) -> str:
        decision_text = str(rec.get("decision_text", rec.get("statement", ""))).lower()
        owner = str(rec.get("owner", rec.get("primary_owner", ""))).lower()
        tokens = [
            tok
            for tok in re.findall(r"[a-z0-9]+", decision_text)
            if len(tok) >= 4 and tok not in {"that", "this", "with", "from", "were", "have"}
        ]
        token_str = " ".join(tokens[:14]) if tokens else decision_text.strip()
        return f"{owner}|{token_str}"

    def _resolve_conflicting_duplicates(
        self,
        records: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], int]:
        grouped: dict[str, list[dict[str, Any]]] = {}
        for rec in records:
            key = self._decision_dedup_key(rec)
            grouped.setdefault(key, []).append(rec)

        kept: list[dict[str, Any]] = []
        rejected = 0
        for key in sorted(grouped.keys()):
            rows = grouped[key]
            if len(rows) == 1:
                kept.append(rows[0])
                continue
            signatures = {
                (
                    str(row.get("state", "")).strip(),
                    str(row.get("decision_status", "")).strip(),
                )
                for row in rows
            }
            if len(signatures) > 1:
                rejected += len(rows)
                continue

            ordered = sorted(
                rows,
                key=lambda row: (
                    float(row.get("confidence_score", 0.0) or 0.0),
                    int(row.get("evidence_count", 0) or 0),
                    str(row.get("decision_id", "")).strip(),
                ),
                reverse=True,
            )
            kept.append(ordered[0])
            rejected += len(ordered) - 1
        return kept, rejected

    def _apply_decision_grounding(
        self,
        payload: dict[str, Any],
        transcript_clean: str,
        transcript_raw: str,
    ) -> tuple[dict[str, Any], dict[str, int | float]]:
        rows = payload.get("decision_records", [])
        if not isinstance(rows, list):
            rows = []
        stats: dict[str, int | float] = {
            "total_candidates": len(rows),
            "rejected_no_evidence": 0,
            "rejected_low_confidence": 0,
            "rejected_invalid": 0,
            "rejected_non_actionable": 0,
            "rejected_conflict": 0,
            "refined_count": 0,
            "split_count": 0,
            "owner_unassigned_count": 0,
            "final_accepted": 0,
            "min_confidence_threshold": self._decision_min_confidence_threshold(),
        }

        accepted: list[dict[str, Any]] = []
        threshold = float(stats["min_confidence_threshold"])
        for rec in rows:
            if not isinstance(rec, dict):
                stats["rejected_invalid"] += 1
                continue

            split_records = self._split_record_actions(rec)
            if len(split_records) > 1:
                stats["split_count"] += len(split_records) - 1
            for split_rec in split_records:
                evidence_snippets = self._extract_evidence_snippets(split_rec, transcript_clean)
                if not evidence_snippets:
                    stats["rejected_no_evidence"] += 1
                    continue

                if str(split_rec.get("support_level", "")).strip() == "WEAK_INFERENCE" or str(split_rec.get("claim_strength", "")).strip() == "weak":
                    stats["rejected_invalid"] += 1
                    continue
                if self._is_multitopic_statement(str(split_rec.get("statement", "")).strip()):
                    stats["rejected_invalid"] += 1
                    continue
                if self._is_non_actionable_statement(str(split_rec.get("statement", "")).strip(), split_rec):
                    stats["rejected_non_actionable"] += 1
                    continue
                if not self._is_specific_actionable_decision(split_rec):
                    stats["rejected_invalid"] += 1
                    continue

                confidence_score, confidence_label = self._score_decision_confidence(split_rec, evidence_snippets)
                if confidence_score < threshold or confidence_label == DECISION_LABEL_LOW:
                    stats["rejected_low_confidence"] += 1
                    continue

                refined_text = self._refine_decision_text(str(split_rec.get("statement", "")).strip())
                if not refined_text:
                    stats["rejected_non_actionable"] += 1
                    continue
                if refined_text != str(split_rec.get("statement", "")).strip():
                    stats["refined_count"] += 1

                resolved_primary_owner, owner_label, owner_conf = self._resolve_decision_owner(
                    split_rec,
                    transcript_clean,
                )

                enriched = dict(split_rec)
                enriched["decision_text"] = refined_text
                enriched["primary_owner"] = resolved_primary_owner
                enriched["owner"] = owner_label
                if not resolved_primary_owner:
                    stats["owner_unassigned_count"] += 1
                    enriched["owner_confidence"] = 0.0
                else:
                    enriched["owner_confidence"] = max(float(enriched.get("owner_confidence", 0.0) or 0.0), owner_conf)
                enriched = self._enforce_missing_owner_gap(enriched)
                enriched["evidence_snippets"] = evidence_snippets
                enriched["evidence_count"] = len(evidence_snippets)
                enriched["source_timestamps"] = self._extract_source_timestamps(evidence_snippets, transcript_raw)
                enriched["confidence_score"] = confidence_score
                enriched["confidence_label"] = confidence_label
                accepted.append(enriched)

        deduped, rejected_conflict = self._resolve_conflicting_duplicates(accepted)
        stats["rejected_conflict"] = rejected_conflict
        stats["final_accepted"] = len(deduped)
        payload["decision_records"] = deduped
        return payload, stats

    def _augment_dependencies(
        self,
        rec: dict[str, Any],
        executive: dict[str, Any],
        intelligence: dict[str, Any],
    ) -> dict[str, Any]:
        deps = [d for d in rec.get("dependencies", []) if isinstance(d, dict)]
        ex_struct = executive.get("execution_structure", {}) if isinstance(executive, dict) else {}
        bm = executive.get("business_model_clarity", {}) if isinstance(executive, dict) else {}
        families = {
            family
            for family in ["decisions", "risks", "action_plan", "roadmap", "deadlines", "stakeholders", "timeline_mentions"]
            if isinstance(intelligence.get(family, []), list) and len(intelligence.get(family, [])) > 0
        } if isinstance(intelligence, dict) else set()
        text = " ".join(
            [str(rec.get("statement", ""))]
            + [str(e) for e in rec.get("evidence", []) if isinstance(e, str)]
            + [str(c.get("commitment", "")) for c in rec.get("commitments", []) if isinstance(c, dict)]
        ).lower()
        has = {d.get("type") for d in deps if isinstance(d, dict)}
        exec_related = any(
            k in text
            for k in ["implementation", "coe", "program", "representative", "responsible", "india"]
        )
        if (
            ex_struct.get("authority_clarity") in {"partial", "undefined"}
            and exec_related
            and "authority_dependency" not in has
            and {"decisions", "risks"} & families
        ):
            deps.append({"type": "authority_dependency", "status": "open", "blocking_level": "high", "reason": ""})
        if (
            ex_struct.get("governance_clarity") in {"partial", "undefined"}
            and exec_related
            and "governance_dependency" not in has
            and {"decisions", "risks"} & families
        ):
            deps.append({"type": "governance_dependency", "status": "open", "blocking_level": ("high" if ex_struct.get("governance_clarity") == "undefined" else "medium"), "reason": ""})
        funding_related = any(k in text for k in ["fund", "revenue", "tuition", "fee", "profit", "finance", "phd"])
        if (
            funding_related
            and ("funding_dependency" not in has)
            and (bm.get("funding_logic") in {"partial", "undefined"} or bm.get("revenue_logic") in {"partial", "undefined"})
            and {"risks", "decisions"} & families
        ):
            deps.append({"type": "funding_dependency", "status": "open", "blocking_level": ("high" if (bm.get("funding_logic") == "undefined" or bm.get("revenue_logic") == "undefined") else "medium"), "reason": ""})
        has_signal = any(str(s.get("raw_reference", "")).strip() for s in rec.get("timeline_signals", []) if isinstance(s, dict))
        if (
            (has_signal or any(k in text for k in ["may", "june", "week", "tuesday", "timeline", "start"]))
            and "timeline_dependency" not in has
            and {"timeline_mentions", "deadlines", "roadmap"} & families
        ):
            deps.append({"type": "timeline_dependency", "status": "open", "blocking_level": "medium", "reason": ""})
        rec["dependencies"] = deps
        return rec

    def _enforce_dependency_reasons(self, rec: dict[str, Any]) -> dict[str, Any]:
        repaired = []
        for dep in rec.get("dependencies", []):
            if not isinstance(dep, dict):
                continue
            dep_type = self._coerce(
                str(dep.get("type", "")).strip().lower(), DEPENDENCY_TYPE_ENUM, "governance_dependency"
            )
            reason = str(dep.get("reason", "")).strip()
            if not reason:
                reason = DEPENDENCY_REASON_TEMPLATES[dep_type]
            repaired.append(
                {
                    "type": dep_type,
                    "status": self._coerce(
                        str(dep.get("status", "")).strip().lower(), DEPENDENCY_STATUS_ENUM, "open"
                    ),
                    "blocking_level": self._coerce(
                        str(dep.get("blocking_level", "")).strip().lower(), HML, "medium"
                    ),
                    "reason": reason,
                }
            )
        rec["dependencies"] = repaired
        return rec

    def _enforce_evidence(self, rec: dict[str, Any], transcript_clean: str) -> dict[str, Any]:
        valid = []
        seen = set()
        for ev in rec.get("evidence", []):
            if not isinstance(ev, str):
                continue
            t = ev.strip()
            if t and t in transcript_clean and t not in seen:
                valid.append(t)
                seen.add(t)
        if not valid:
            recovered = self._recover_evidence(rec, transcript_clean)
            if recovered:
                valid.append(recovered)
        if not valid:
            raise DecisionIntelligenceV2Error("Decision record is unsupported after evidence validation")
        rec["evidence"] = self._prefer_sentence_evidence(valid, transcript_clean)
        return rec

    def _prefer_sentence_evidence(self, evidence: list[str], transcript_clean: str) -> list[str]:
        sentences = self._split_sentences(transcript_clean)
        scored: list[tuple[int, str]] = []
        seen = set()
        for ev in evidence:
            best = ev
            best_len = len(ev)
            if len(re.findall(r"[A-Za-z0-9]+", ev)) >= 5:
                for sentence in sentences:
                    if ev in sentence and len(sentence) > best_len:
                        best = sentence
                        best_len = len(sentence)
            if best not in seen:
                seen.add(best)
                scored.append((best_len, best))
        scored.sort(key=lambda item: item[0], reverse=True)
        return [text for _, text in scored]

    def _recover_evidence(self, rec: dict[str, Any], transcript_clean: str) -> str:
        candidates: list[str] = []
        statement = str(rec.get("statement", "")).strip()
        if statement:
            candidates.append(statement)
        for c in rec.get("commitments", []):
            if isinstance(c, dict):
                text = str(c.get("commitment", "")).strip()
                if text:
                    candidates.append(text)
        for sig in rec.get("timeline_signals", []):
            if isinstance(sig, dict):
                text = str(sig.get("raw_reference", "")).strip()
                if text:
                    candidates.append(text)

        for phrase in candidates:
            if phrase and phrase in transcript_clean:
                return phrase

        sentences = [s.strip() for s in re.split(r"(?<=[.!?])\s+", transcript_clean) if s.strip()]
        best_sentence = ""
        best_score = 0
        for phrase in candidates:
            tokens = [
                tok
                for tok in re.findall(r"[A-Za-z0-9]+", phrase.lower())
                if len(tok) >= 4 and tok not in {"that", "this", "with", "from", "will", "have", "been", "were"}
            ]
            if not tokens:
                continue
            token_set = set(tokens)
            for sentence in sentences:
                sent_tokens = set(re.findall(r"[A-Za-z0-9]+", sentence.lower()))
                score = len(token_set & sent_tokens)
                if score > best_score:
                    best_score = score
                    best_sentence = sentence
        if best_score >= 2:
            return best_sentence
        return ""

    def _decision_id(self, rec: dict[str, Any]) -> str:
        statement = str(rec.get("statement", "")).strip()
        first_ev = rec.get("evidence", [""])[0] if rec.get("evidence") else ""
        digest = hashlib.sha1(f"{statement}|{first_ev}".encode("utf-8", errors="ignore")).hexdigest()[:10]
        return f"DEC-{digest}"

    def _validate_records(
        self,
        data: dict[str, Any],
        transcript_clean: str,
        alias_map: dict[str, str],
        executive_primary: str,
        primary_actor: str,
    ) -> None:
        for rec in data.get("decision_records", []):
            if rec.get("state") not in STATE_ENUM:
                raise DecisionIntelligenceV2Error("Invalid decision state")
            if rec.get("decision_status") not in DECISION_STATUS_ENUM:
                raise DecisionIntelligenceV2Error("Invalid decision_status")
            if rec.get("impact_level") not in HML:
                raise DecisionIntelligenceV2Error("Invalid impact_level")
            if rec.get("confidence") not in HML:
                raise DecisionIntelligenceV2Error("Invalid decision confidence")
            if str(rec.get("support_level", "")).strip() not in {
                "DIRECTLY_SUPPORTED",
                "ACCEPTABLE_INFERENCE",
                "WEAK_INFERENCE",
            }:
                raise DecisionIntelligenceV2Error("Invalid decision support_level")
            if str(rec.get("claim_strength", "")).strip() not in {"direct", "inferred", "weak"}:
                raise DecisionIntelligenceV2Error("Invalid decision claim_strength")
            if str(rec.get("certainty_class", "")).upper() not in {"UNCERTAIN", "CONDITIONAL", "DIRECT"}:
                raise DecisionIntelligenceV2Error("Invalid certainty_class")
            try:
                evidence_conf = float(rec.get("evidence_confidence", 0.0))
            except Exception as exc:
                raise DecisionIntelligenceV2Error("Invalid decision evidence_confidence") from exc
            if evidence_conf < 0.5:
                raise DecisionIntelligenceV2Error("Decision evidence_confidence below threshold")
            decision_text = str(rec.get("decision_text", "")).strip()
            if not decision_text:
                raise DecisionIntelligenceV2Error("decision_text is required after grounding")
            owner = str(rec.get("owner", "")).strip()
            primary_owner_value = str(rec.get("primary_owner", "")).strip()
            if owner and owner != "Unassigned" and owner != primary_owner_value:
                raise DecisionIntelligenceV2Error("owner must match primary_owner when provided")
            if owner == "Unassigned" and primary_owner_value:
                raise DecisionIntelligenceV2Error("Unassigned owner cannot coexist with primary_owner")
            snippets = rec.get("evidence_snippets", [])
            if not isinstance(snippets, list) or not snippets:
                raise DecisionIntelligenceV2Error("evidence_snippets must include at least one transcript snippet")
            for snippet in snippets:
                if not isinstance(snippet, str) or not snippet.strip() or snippet not in transcript_clean:
                    raise DecisionIntelligenceV2Error("evidence_snippets must be exact transcript substrings")
            if int(rec.get("evidence_count", 0) or 0) != len(snippets):
                raise DecisionIntelligenceV2Error("evidence_count must equal evidence_snippets length")
            timestamps = rec.get("source_timestamps", [])
            if not isinstance(timestamps, list):
                raise DecisionIntelligenceV2Error("source_timestamps must be a list")
            for ts in timestamps:
                if not isinstance(ts, str):
                    raise DecisionIntelligenceV2Error("source_timestamps must contain strings")
            try:
                conf_score = float(rec.get("confidence_score", 0.0))
            except Exception as exc:
                raise DecisionIntelligenceV2Error("confidence_score must be numeric") from exc
            if conf_score < 0.0 or conf_score > 1.0:
                raise DecisionIntelligenceV2Error("confidence_score must be in [0,1]")
            if str(rec.get("confidence_label", "")).strip() not in {
                DECISION_LABEL_HIGH,
                DECISION_LABEL_MEDIUM,
                DECISION_LABEL_LOW,
            }:
                raise DecisionIntelligenceV2Error("Invalid confidence_label")

            owners = rec.get("owners", [])
            for owner in owners:
                if not isinstance(owner, dict) or owner.get("ownership_type") not in OWNERSHIP_ENUM:
                    raise DecisionIntelligenceV2Error("Invalid ownership payload")
                actor = str(owner.get("actor", "")).strip()
                actor_conf = float(owner.get("actor_confidence", 0.0) or 0.0)
                if actor and actor.lower() in alias_map and actor != self._normalize_actor(actor, alias_map):
                    raise DecisionIntelligenceV2Error("Owner actor not normalized")
                if actor and not actor_present_in_transcript(actor, transcript_clean, alias_map):
                    raise DecisionIntelligenceV2Error("Owner actor is not supported by transcript evidence")
                if actor and actor_conf < 0.75:
                    raise DecisionIntelligenceV2Error("Owner assigned with actor_confidence below threshold")

            commitments = rec.get("commitments", [])
            for cmt in commitments:
                if not isinstance(cmt, dict):
                    raise DecisionIntelligenceV2Error("Invalid commitment payload")
                if cmt.get("commitment_type") not in COMMITMENT_TYPE_ENUM:
                    raise DecisionIntelligenceV2Error("Invalid commitment_type")
                if cmt.get("status") not in COMMITMENT_STATUS_ENUM:
                    raise DecisionIntelligenceV2Error("Invalid commitment status")
                if cmt.get("confidence") not in HML:
                    raise DecisionIntelligenceV2Error("Invalid commitment confidence")
                actor = str(cmt.get("actor", "")).strip()
                if actor and actor.lower() in alias_map and actor != self._normalize_actor(actor, alias_map):
                    raise DecisionIntelligenceV2Error("Commitment actor not normalized")
                if actor and not actor_present_in_transcript(actor, transcript_clean, alias_map):
                    raise DecisionIntelligenceV2Error("Commitment actor is not supported by transcript evidence")
                if not actor and not str(cmt.get("commitment", "")).strip():
                    raise DecisionIntelligenceV2Error("Commitment cannot have both empty actor and commitment")

            deps = rec.get("dependencies", [])
            for dep in deps:
                if not isinstance(dep, dict):
                    raise DecisionIntelligenceV2Error("Invalid dependency payload")
                if dep.get("type") not in DEPENDENCY_TYPE_ENUM:
                    raise DecisionIntelligenceV2Error("Invalid dependency type")
                if dep.get("status") not in DEPENDENCY_STATUS_ENUM:
                    raise DecisionIntelligenceV2Error("Invalid dependency status")
                if dep.get("blocking_level") not in HML:
                    raise DecisionIntelligenceV2Error("Invalid dependency blocking_level")
                if not str(dep.get("reason", "")).strip():
                    raise DecisionIntelligenceV2Error("Dependency reason cannot be empty")
                if float(dep.get("evidence_confidence", 0.0) or 0.0) < 0.6:
                    raise DecisionIntelligenceV2Error("Weak dependency leaked to final payload")

            for signal in rec.get("timeline_signals", []):
                if not isinstance(signal, dict):
                    raise DecisionIntelligenceV2Error("Invalid timeline signal payload")
                if signal.get("signal_type") not in TIMELINE_TYPE_ENUM:
                    raise DecisionIntelligenceV2Error("Invalid timeline signal type")
                if signal.get("confidence") not in HML:
                    raise DecisionIntelligenceV2Error("Invalid timeline signal confidence")

            evidence = rec.get("evidence", [])
            if not isinstance(evidence, list) or not evidence:
                raise DecisionIntelligenceV2Error("Each decision record must include evidence")
            for ev in evidence:
                if not isinstance(ev, str) or ev not in transcript_clean:
                    raise DecisionIntelligenceV2Error("Evidence must be exact transcript substring")
            span = str(rec.get("evidence_span", "")).strip()
            if span and span not in transcript_clean:
                raise DecisionIntelligenceV2Error("Decision evidence_span must be exact transcript substring")
            text = " ".join(
                [str(rec.get("statement", ""))]
                + [str(e) for e in evidence if isinstance(e, str)]
            ).lower()
            if self._contains_time_phrase(text) and not rec.get("timeline_signals", []):
                raise DecisionIntelligenceV2Error(
                    "Timeline signal missing despite explicit time phrase evidence"
                )

            if rec.get("state") == "confirmed":
                if not any(c.get("commitment_type") == "explicit_commitment" for c in commitments if isinstance(c, dict)):
                    raise DecisionIntelligenceV2Error("Confirmed decision has no explicit commitment")
                if any(marker in text for marker in PENDING_MARKERS + UNCERTAINTY_MARKERS):
                    raise DecisionIntelligenceV2Error("Confirmed decision contains unresolved markers")
            if any(marker in text for marker in CONDITIONAL_MARKERS) and rec.get("decision_status") == "confirmed":
                raise DecisionIntelligenceV2Error("Conditional contradiction cannot be marked confirmed")

            if rec.get("state") == "blocked" and not self._has_open_high(rec):
                raise DecisionIntelligenceV2Error("Blocked decision must include open high dependency")

            primary_owner = str(rec.get("primary_owner", "")).strip()
            missing_owner_gap = any(
                isinstance(g, dict)
                and (str(g.get("gap_type", "")).lower() == "missing_owner" or "accountable owner" in str(g.get("question", "")).lower())
                for g in rec.get("decision_gaps", [])
            )
            if not primary_owner:
                if not any(o.get("ownership_type") == "missing_owner" for o in owners if isinstance(o, dict)):
                    raise DecisionIntelligenceV2Error("Missing owner decision must include missing_owner ownership_type")
                if not missing_owner_gap:
                    raise DecisionIntelligenceV2Error("Missing owner decision must include missing_owner gap")
            else:
                if not any(isinstance(o, dict) and str(o.get("actor", "")).strip() == primary_owner for o in owners):
                    raise DecisionIntelligenceV2Error("primary_owner is not present in owners list")

    def _build_operational_summary(self, records: list[dict[str, Any]]) -> dict[str, Any]:
        summary = {
            "confirmed_count": 0,
            "tentative_count": 0,
            "pending_count": 0,
            "blocked_count": 0,
            "high_blockers": [],
            "missing_owners_count": 0,
            "open_dependencies_count": 0,
        }
        for rec in records:
            state = rec.get("state")
            if state in {"confirmed", "tentative", "pending", "blocked"}:
                summary[f"{state}_count"] += 1
            deps = rec.get("dependencies", [])
            has_open_high = False
            for dep in deps:
                if not isinstance(dep, dict):
                    continue
                if dep.get("status") == "open":
                    summary["open_dependencies_count"] += 1
                if dep.get("status") == "open" and dep.get("blocking_level") == "high":
                    has_open_high = True
            if state == "blocked" and has_open_high:
                statement = str(rec.get("statement", "")).strip() or str(rec.get("decision_id", "")).strip()
                if statement:
                    summary["high_blockers"].append(statement)
            owners = rec.get("owners", [])
            if (
                not str(rec.get("primary_owner", "")).strip()
                or any(isinstance(o, dict) and o.get("ownership_type") == "missing_owner" for o in owners)
            ):
                summary["missing_owners_count"] += 1
        return summary

    def _sort_records_deterministically(self, data: dict[str, Any]) -> dict[str, Any]:
        records = data.get("decision_records", [])
        if not isinstance(records, list):
            data["decision_records"] = []
            return data

        def owner_key(owner: dict[str, Any]) -> tuple[str, str]:
            return (
                str(owner.get("ownership_type", "")).lower(),
                str(owner.get("actor", "")).lower(),
            )

        def commitment_key(cmt: dict[str, Any]) -> tuple[str, str, str]:
            return (
                str(cmt.get("actor", "")).lower(),
                str(cmt.get("commitment_type", "")).lower(),
                str(cmt.get("commitment", "")).lower(),
            )

        def dependency_key(dep: dict[str, Any]) -> tuple[str, str, str, str]:
            return (
                str(dep.get("type", "")).lower(),
                str(dep.get("status", "")).lower(),
                str(dep.get("blocking_level", "")).lower(),
                str(dep.get("reason", "")).lower(),
            )

        def gap_key(gap: dict[str, Any]) -> tuple[str, str, str]:
            return (
                str(gap.get("gap_type", "")).lower(),
                str(gap.get("criticality", "")).lower(),
                str(gap.get("question", "")).lower(),
            )

        def signal_key(sig: dict[str, Any]) -> tuple[str, str]:
            return (
                str(sig.get("signal_type", "")).lower(),
                str(sig.get("raw_reference", "")).lower(),
            )

        normalized_records: list[dict[str, Any]] = []
        for rec in records:
            if not isinstance(rec, dict):
                continue
            out = dict(rec)
            out["owners"] = sorted(
                [x for x in out.get("owners", []) if isinstance(x, dict)],
                key=owner_key,
            )
            out["commitments"] = sorted(
                [x for x in out.get("commitments", []) if isinstance(x, dict)],
                key=commitment_key,
            )
            out["dependencies"] = sorted(
                [x for x in out.get("dependencies", []) if isinstance(x, dict)],
                key=dependency_key,
            )
            out["decision_gaps"] = sorted(
                [x for x in out.get("decision_gaps", []) if isinstance(x, dict)],
                key=gap_key,
            )
            out["timeline_signals"] = sorted(
                [x for x in out.get("timeline_signals", []) if isinstance(x, dict)],
                key=signal_key,
            )
            normalized_records.append(out)

        data["decision_records"] = sorted(
            normalized_records,
            key=lambda rec: (
                str(rec.get("decision_id", "")).strip().lower() or "~",
                str(rec.get("statement", "")).strip().lower(),
            ),
        )
        return data

    def _enforce_cross_artifact_consistency(
        self,
        payload: dict[str, Any],
        executive: dict[str, Any],
        intelligence: dict[str, Any],
    ) -> None:
        def _known_exec_actor_set(exec_payload: dict[str, Any]) -> set[str]:
            known: set[str] = set()
            if not isinstance(exec_payload, dict):
                return known
            power = exec_payload.get("power_structure", {})
            roles = exec_payload.get("role_clarity_assessment", [])
            if isinstance(power, dict):
                for key in [
                    "sponsor",
                    "strategic_authority",
                    "decision_makers",
                    "advisors",
                    "executors",
                    "implementation_owner",
                ]:
                    values = power.get(key, [])
                    if not isinstance(values, list):
                        continue
                    for value in values:
                        actor = str(value or "").strip().lower()
                        if actor:
                            known.add(actor)
            if isinstance(roles, list):
                for row in roles:
                    if not isinstance(row, dict):
                        continue
                    actor = str(row.get("actor", "")).strip().lower()
                    if actor:
                        known.add(actor)
            return known

        def _classify_owner_type(owner: str) -> str:
            text = str(owner or "").strip().lower()
            if not text:
                return "UNKNOWN"
            collective_markers = {
                "we",
                "team",
                "leadership",
                "management",
                "committee",
                "group",
                "everyone",
                "all",
            }
            if text in collective_markers or any(
                token in text
                for token in (
                    " leadership",
                    " team",
                    " committee",
                    " management",
                    " all ",
                    " we ",
                )
            ):
                return "COLLECTIVE"
            org_tokens = (
                "aivancity",
                "gtu",
                "university",
                "institution",
                "center of excellence",
                "coe",
                "company",
                "partner",
                "school",
                "faculty",
            )
            if any(token in text for token in org_tokens):
                return "ORGANIZATION"
            return "UNKNOWN"

        issues = validate_cross_artifact_consistency(executive, payload, intelligence=intelligence)
        if not issues:
            return
        # Deterministic reconciliation for owner mismatch and certainty contradiction.
        rows = payload.get("decision_records", [])
        if not isinstance(rows, list):
            raise DecisionIntelligenceV2Error("Cross-artifact consistency failure: invalid decision_records")
        certainty_anchors = self._build_intelligence_certainty_anchors(intelligence)
        anchor_corpus = self._build_intelligence_anchor_corpus(intelligence)
        known_exec_actors = _known_exec_actor_set(executive)
        kept_rows: list[dict[str, Any]] = []
        for rec in rows:
            if not isinstance(rec, dict):
                continue
            evidence_text = " ".join(str(x) for x in rec.get("evidence", []) if isinstance(x, str)).lower()
            if "not a yes" in evidence_text:
                rec["decision_status"] = "conditional"
                if rec.get("state") == "confirmed":
                    rec["state"] = "tentative"
            rec = self._filter_weak_dependencies(rec, threshold=0.6)
            rec = self._enforce_owner_conservatism(rec, min_conf=0.75)
            owner_value = str(rec.get("primary_owner", "")).strip()
            owner_norm = owner_value.lower()
            if owner_value and owner_norm not in known_exec_actors:
                owner_type = _classify_owner_type(owner_value)
                rec["owner_raw"] = owner_value
                rec["owner_type"] = owner_type.lower()
                rec["primary_owner"] = ""
                rec["owner_confidence"] = 0.0
                rec["missing_owner"] = True
                rec["missing_owner_reason"] = (
                    "non_resolvable_collective_or_organization"
                    if owner_type in {"ORGANIZATION", "COLLECTIVE"}
                    else "non_resolvable_unknown_owner"
                )
                rec["owners"] = [
                    {
                        "actor": "",
                        "ownership_type": "missing_owner",
                        "actor_confidence": 0.0,
                        "support_level": "WEAK_INFERENCE",
                        "evidence_confidence": 0.0,
                    }
                ]
                rec = self._enforce_missing_owner_gap(rec)
                print(
                    "[OWNER_NORMALIZATION] "
                    f'original="{owner_value}" '
                    'action="downgraded_to_missing_owner" '
                    'reason="not_in_executive_actor_map"'
                )
            cap = self._best_anchor_certainty(rec, certainty_anchors)
            if cap in {"UNCERTAIN", "CONDITIONAL"}:
                rec["certainty_class"] = cap
                if rec.get("state") == "confirmed":
                    rec["state"] = "tentative"
            if not self._is_record_anchored_to_intelligence(rec, anchor_corpus):
                continue
            claim = str(rec.get("statement", "")).strip()
            if not is_semantically_supportive(claim, str(rec.get("evidence_span", "")), claim_type="decision"):
                continue
            kept_rows.append(rec)
        payload["decision_records"] = kept_rows
        unresolved = validate_cross_artifact_consistency(executive, payload, intelligence=intelligence)
        if unresolved:
            joined = "; ".join(unresolved[:5])
            raise DecisionIntelligenceV2Error(f"Cross-artifact consistency failure: {joined}")

    def _validate_final(self, payload: dict[str, Any]) -> None:
        if not isinstance(payload, dict):
            raise DecisionIntelligenceV2Error("Final payload must be an object")
        if "decision_records" not in payload or "operational_summary" not in payload:
            raise DecisionIntelligenceV2Error("Top-level keys missing")
        if not isinstance(payload["decision_records"], list):
            raise DecisionIntelligenceV2Error("decision_records must be a list")
        summary = payload["operational_summary"]
        if not isinstance(summary, dict):
            raise DecisionIntelligenceV2Error("operational_summary must be an object")
        for key in [
            "confirmed_count",
            "tentative_count",
            "pending_count",
            "blocked_count",
            "high_blockers",
            "missing_owners_count",
            "open_dependencies_count",
        ]:
            if key not in summary:
                raise DecisionIntelligenceV2Error(f"Missing operational_summary.{key}")
        for key in ["confirmed_count", "tentative_count", "pending_count", "blocked_count", "missing_owners_count", "open_dependencies_count"]:
            if not isinstance(summary[key], int):
                raise DecisionIntelligenceV2Error(f"operational_summary.{key} must be int")
        if not isinstance(summary["high_blockers"], list):
            raise DecisionIntelligenceV2Error("operational_summary.high_blockers must be list")

    def _write_artifacts_fail_closed(
        self,
        output_path: Path,
        metadata_path: Path,
        payload: dict[str, Any],
        metadata: dict[str, Any],
    ) -> None:
        output_tmp = output_path.with_suffix(output_path.suffix + ".tmp")
        meta_tmp = metadata_path.with_suffix(metadata_path.suffix + ".tmp")
        cleanup = [output_tmp, meta_tmp, output_path, metadata_path]
        try:
            output_tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
            meta_tmp.write_text(json.dumps(metadata, indent=2, ensure_ascii=False), encoding="utf-8")
            os.replace(output_tmp, output_path)
            os.replace(meta_tmp, metadata_path)
        except Exception as exc:
            for p in cleanup:
                if p.exists():
                    try:
                        p.unlink()
                    except Exception:
                        pass
            raise DecisionIntelligenceV2Error(f"Failed to write decision intelligence artifacts: {exc}") from exc
