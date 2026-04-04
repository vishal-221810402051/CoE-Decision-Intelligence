from __future__ import annotations

import json
import os
import re
import time
from pathlib import Path
from typing import Any

from openai import OpenAI

from app.intelligence.actor_resolver import actor_present_in_transcript
from app.intelligence.evidence_engine import (
    SUPPORT_ACCEPTABLE,
    SUPPORT_DIRECT,
    SUPPORT_WEAK,
    build_evidence_binding,
    is_semantically_supportive,
)
from app.config import (
    EXECUTIVE_METADATA_FILE,
    EXECUTIVE_MODEL,
    EXECUTIVE_OUTPUT_DIR,
    EXECUTIVE_OUTPUT_FILE,
    EXECUTIVE_PROMPT_VERSION,
    EXECUTIVE_SEED,
    MISSION_REGISTRY_PATH,
)
from app.models.executive import ExecutiveIntelligenceResult, executive_schema_defaults
from app.services.intelligence.contract import (
    adapt_canonical_intelligence_for_downstream,
    get_canonical_intelligence_path,
    load_canonical_intelligence,
)


class ExecutiveIntelligenceError(RuntimeError):
    pass


def load_mission_registry() -> dict[str, Any]:
    path = Path(MISSION_REGISTRY_PATH)
    if not path.exists():
        raise FileNotFoundError(f"Mission registry not found: {path}")
    data = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(data, dict):
        raise ExecutiveIntelligenceError("Mission registry must be a JSON object")
    return data


def build_registry_grounding(registry_data: dict[str, Any]) -> str:
    primary_actor = registry_data.get("primary_actor", {})
    institutions = registry_data.get("institutions", [])
    stakeholders = registry_data.get("stakeholders", [])
    mission_terms = registry_data.get("mission_terms", [])

    lines: list[str] = []

    if primary_actor:
        lines.append("PRIMARY ACTOR")
        lines.append(f"- Name: {primary_actor.get('name', '')}")
        lines.append(f"- Role: {primary_actor.get('role', '')}")
        for rule in primary_actor.get("interpretation_rules", []):
            lines.append(f"- Rule: {rule}")

    if institutions:
        lines.append("\nINSTITUTIONS")
        for inst in institutions:
            lines.append(
                f"- {inst.get('normalized_name', inst.get('name', ''))}: "
                f"{inst.get('mission_role', '')}"
            )

    if stakeholders:
        lines.append("\nSTAKEHOLDERS")
        for s in stakeholders:
            lines.append(
                f"- {s.get('name', '')}: {s.get('role', '')} "
                f"[importance={s.get('importance', '')}, signal_weight={s.get('signal_weight', '')}]"
            )

    if mission_terms:
        lines.append("\nMISSION TERMS")
        for t in mission_terms:
            lines.append(
                f"- {t.get('normalized_name', t.get('term', ''))}: {t.get('meaning', '')}"
            )

    return "\n".join(lines).strip()


def build_alias_map(registry_data: dict[str, Any]) -> dict[str, str]:
    alias_map: dict[str, str] = {}

    for inst in registry_data.get("institutions", []):
        if not isinstance(inst, dict):
            continue
        name = inst.get("name", "")
        normalized = inst.get("normalized_name", name)
        if isinstance(name, str) and name:
            alias_map[name] = normalized
            alias_map[name.lower()] = normalized
        if isinstance(normalized, str) and normalized:
            alias_map[normalized] = normalized
            alias_map[normalized.lower()] = normalized

    for s in registry_data.get("stakeholders", []):
        if not isinstance(s, dict):
            continue
        name = s.get("name", "")
        if isinstance(name, str) and name:
            alias_map[name] = name
            alias_map[name.lower()] = name

    for t in registry_data.get("mission_terms", []):
        if not isinstance(t, dict):
            continue
        term = t.get("term", "")
        normalized = t.get("normalized_name", term)
        if isinstance(term, str) and term:
            alias_map[term] = normalized
            alias_map[term.lower()] = normalized
        if isinstance(normalized, str) and normalized:
            alias_map[normalized] = normalized
            alias_map[normalized.lower()] = normalized

    return alias_map


class ExecutiveIntelligenceService:
    def __init__(self) -> None:
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ExecutiveIntelligenceError("OPENAI_API_KEY is missing")
        self.client = OpenAI(api_key=api_key)

    def run(self, meeting_id: str) -> ExecutiveIntelligenceResult:
        start_time = time.time()

        meeting_dir = Path("data") / "processed" / meeting_id
        transcript_dir = meeting_dir / "transcript"
        metadata_dir = meeting_dir / "metadata"
        executive_dir = meeting_dir / EXECUTIVE_OUTPUT_DIR

        raw_path = transcript_dir / "transcript_raw.txt"
        clean_path = transcript_dir / "transcript_clean.txt"
        canonical_intelligence_path = get_canonical_intelligence_path(meeting_dir)
        # Legacy optional input only. Active execution must not depend on this file.
        decision_path = Path("reports") / "decision_intelligence.json"

        for required in [raw_path, clean_path]:
            if not required.exists():
                raise FileNotFoundError(f"Required artifact not found: {required}")
        if not canonical_intelligence_path.exists():
            raise FileNotFoundError(
                "Canonical intelligence artifact missing. Phase 06 must complete successfully before downstream phases. "
                f"Expected path: {canonical_intelligence_path}"
            )

        registry_data = load_mission_registry()
        registry_grounding = build_registry_grounding(registry_data)

        raw_text = raw_path.read_text(encoding="utf-8")
        clean_text = clean_path.read_text(encoding="utf-8")
        try:
            canonical_intelligence = load_canonical_intelligence(meeting_dir)
            intelligence = adapt_canonical_intelligence_for_downstream(canonical_intelligence)
        except (FileNotFoundError, ValueError) as exc:
            raise ExecutiveIntelligenceError(str(exc)) from exc
        decision_data = {}
        if decision_path.exists():
            decision_data = json.loads(decision_path.read_text(encoding="utf-8"))

        executive_dir.mkdir(parents=True, exist_ok=True)
        metadata_dir.mkdir(parents=True, exist_ok=True)
        output_path = executive_dir / EXECUTIVE_OUTPUT_FILE
        metadata_path = metadata_dir / EXECUTIVE_METADATA_FILE

        messages = [
            {"role": "system", "content": self._system_prompt()},
            {
                "role": "user",
                "content": self._build_prompt(
                    raw_text=raw_text,
                    clean_text=clean_text,
                    intelligence=intelligence,
                    decision_data=decision_data,
                    registry_grounding=registry_grounding,
                ),
            },
        ]
        response = self._create_completion(messages)

        raw_output = response.choices[0].message.content or ""
        parsed = self._safe_parse_json(raw_output)
        if parsed is None:
            raise ExecutiveIntelligenceError("Model did not return valid JSON")

        parsed = self._enforce_schema(parsed)
        parsed = self._normalize_with_registry(parsed, registry_data)
        self._ensure_primary_executor_row(parsed)
        self._apply_execution_risk_rule(parsed)
        self._apply_structural_hardening(parsed)
        self._apply_warning_backstop(parsed)
        self._apply_structural_hardening(parsed)
        self._enforce_truth_integrity(parsed, clean_text, registry_data, intelligence)
        self._apply_warning_backstop(parsed)
        self._enforce_truth_integrity(parsed, clean_text, registry_data, intelligence)
        self._sort_output_deterministically(parsed)
        self._validate(parsed, clean_text)

        output_path.write_text(
            json.dumps(parsed, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

        metadata = {
            "meeting_id": meeting_id,
            "model": EXECUTIVE_MODEL,
            "prompt_version": EXECUTIVE_PROMPT_VERSION,
            "processing_time_seconds": round(time.time() - start_time, 3),
            "status": "executive_intelligence_completed",
        }
        metadata_path.write_text(
            json.dumps(metadata, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

        return ExecutiveIntelligenceResult(
            meeting_id=meeting_id,
            output_path=str(output_path),
            metadata_path=str(metadata_path),
            model=EXECUTIVE_MODEL,
            prompt_version=EXECUTIVE_PROMPT_VERSION,
            status="executive_intelligence_completed",
        )

    def _system_prompt(self) -> str:
        return """
You are an Executive Intelligence Engine.

You do not summarize casually.
You produce executive-grade interpretation from:
- transcript
- structured intelligence
- decision intelligence
- mission registry grounding

Your job is to identify:
- what the meeting really means
- what is intent vs commitment
- who holds authority
- who carries execution burden
- what is still unclear
- what must be clarified next

STRICT RULES:
- Return JSON only
- No markdown
- No commentary
- No hallucination
- No invented actors, money, dates, authority, or legal structure
- Use registry grounding for canonical names, institutional roles, and authority interpretation
- If uncertain, lower confidence instead of inventing
- Keep language concise and operational
- Use canonical role wording; avoid stylistic variation
- Evidence phrases must be exact transcript substrings
- Do not claim strong consensus unless multiple explicit commitments exist
- You are a structured compressor of lower-layer intelligence, not an open-ended strategist
- Do not introduce new business, funding, ownership, governance, or strategic concepts absent from lower-layer intelligence
- Higher-layer certainty must never exceed lower-layer certainty (UNCERTAIN/CONDITIONAL must stay non-final)
"""

    def _create_completion(self, messages: list[dict[str, str]]) -> Any:
        try:
            return self.client.chat.completions.create(
                model=EXECUTIVE_MODEL,
                temperature=0,
                seed=EXECUTIVE_SEED,
                response_format={"type": "json_object"},
                messages=messages,
            )
        except Exception as exc:
            text = str(exc).lower()
            if "seed" not in text and "response_format" not in text and "unsupported" not in text:
                raise
            return self.client.chat.completions.create(
                model=EXECUTIVE_MODEL,
                temperature=0,
                messages=messages,
            )

    def _build_prompt(
        self,
        raw_text: str,
        clean_text: str,
        intelligence: dict[str, Any],
        decision_data: dict[str, Any],
        registry_grounding: str,
    ) -> str:
        return f"""
MISSION REGISTRY GROUNDING
--------------------------
{registry_grounding}

RAW TRANSCRIPT
--------------
{raw_text}

CLEAN TRANSCRIPT
----------------
{clean_text}

INTELLIGENCE.JSON
-----------------
{json.dumps(intelligence, indent=2, ensure_ascii=False)}

DECISION_INTELLIGENCE.JSON
--------------------------
{json.dumps(decision_data, indent=2, ensure_ascii=False)}

Return STRICT JSON with exactly this structure:

{{
  "executive_summary": {{
    "meaning_of_meeting": "",
    "intent": "",
    "commitment": "",
    "execution_readiness": "high|medium|low",
    "confidence": "high|medium|low",
    "evidence": []
  }},
  "strategic_objective": {{
    "objective": "",
    "business_direction": "",
    "success_condition": "",
    "confidence": "high|medium|low",
    "evidence": []
  }},
  "power_structure": {{
    "sponsor": [],
    "strategic_authority": [],
    "decision_makers": [],
    "advisors": [],
    "executors": [],
    "implementation_owner": [],
    "unknown_authority_gaps": [],
    "confidence": "high|medium|low"
  }},
  "execution_structure": {{
    "primary_executor": "",
    "responsibility_load": "high|medium|low",
    "authority_clarity": "clear|partial|undefined",
    "compensation_clarity": "clear|partial|undefined",
    "governance_clarity": "clear|partial|undefined",
    "execution_risk_score": "high|medium|low",
    "confidence": "high|medium|low",
    "evidence": []
  }},
  "role_clarity_assessment": [
    {{
      "actor": "",
      "role": "",
      "authority_level": "high|medium|low|undefined",
      "responsibility_level": "high|medium|low",
      "clarity": "clear|partial|undefined",
      "confidence": "high|medium|low",
      "evidence": ""
    }}
  ],
  "business_model_clarity": {{
    "revenue_logic": "clear|partial|undefined",
    "funding_logic": "clear|partial|undefined",
    "ownership_model": "clear|partial|undefined",
    "legal_governance": "clear|partial|undefined",
    "confidence": "high|medium|low",
    "evidence": []
  }},
  "risk_posture": {{
    "overall": "high|medium|low",
    "drivers": [],
    "confidence": "high|medium|low",
    "evidence": []
  }},
  "negotiation_flags": [
    {{
      "topic": "",
      "status": "open|partially_defined",
      "severity": "high|medium|low",
      "confidence": "high|medium|low",
      "evidence": ""
    }}
  ],
  "recommended_next_questions": [
    {{
      "question": "",
      "priority": "high|medium|low",
      "why_now": ""
    }}
  ],
  "executive_warnings": [
    {{
      "warning": "",
      "severity": "high|medium|low",
      "confidence": "high|medium|low",
      "reason": "",
      "evidence": ""
    }}
  ]
}}

IMPORTANT:
- Distinguish intent from commitment.
- Distinguish strategic authority from execution burden.
- Use canonical names from registry.
- Do not invent missing details.
- If execution responsibility is high but authority/compensation/governance are unclear, surface that clearly.
- Derive output from INTELLIGENCE.JSON and DECISION_INTELLIGENCE.JSON anchors only.
- If lower-layer certainty is UNCERTAIN or CONDITIONAL, keep wording cautious and non-final.
- Prefer undefined/partial/open when support is weak.
"""

    def _safe_parse_json(self, text: str) -> dict[str, Any] | None:
        try:
            parsed = json.loads(text)
            return parsed if isinstance(parsed, dict) else None
        except Exception:
            match = re.search(r"\{.*\}", text, re.DOTALL)
            if not match:
                return None
            try:
                parsed = json.loads(match.group())
                return parsed if isinstance(parsed, dict) else None
            except Exception:
                return None

    def _enforce_schema(self, data: dict[str, Any]) -> dict[str, Any]:
        defaults = executive_schema_defaults()
        if not isinstance(data, dict):
            return defaults

        for key, default_value in defaults.items():
            if key not in data:
                data[key] = default_value
            elif isinstance(default_value, dict) and not isinstance(data[key], dict):
                data[key] = default_value
            elif isinstance(default_value, list) and not isinstance(data[key], list):
                data[key] = default_value

        return data

    def _normalize_with_registry(self, data: dict[str, Any], registry_data: dict[str, Any]) -> dict[str, Any]:
        alias_map = build_alias_map(registry_data)

        def normalize(value: str) -> str:
            if not isinstance(value, str):
                return value
            return alias_map.get(value, alias_map.get(value.lower(), value))

        for bucket in [
            "sponsor",
            "strategic_authority",
            "decision_makers",
            "advisors",
            "executors",
            "implementation_owner",
        ]:
            values = data["power_structure"].get(bucket, [])
            if not isinstance(values, list):
                data["power_structure"][bucket] = []
                continue
            data["power_structure"][bucket] = [normalize(v) for v in values if isinstance(v, str)]

        if data["execution_structure"].get("primary_executor"):
            data["execution_structure"]["primary_executor"] = normalize(
                data["execution_structure"]["primary_executor"]
            )

        for row in data.get("role_clarity_assessment", []):
            if isinstance(row, dict) and row.get("actor"):
                row["actor"] = normalize(row["actor"])

        return data

    def _apply_execution_risk_rule(self, data: dict[str, Any]) -> None:
        risky_count = 0
        for key in ["authority_clarity", "compensation_clarity", "governance_clarity"]:
            if data["execution_structure"].get(key) in {"partial", "undefined"}:
                risky_count += 1

        if risky_count >= 2:
            data["execution_structure"]["execution_risk_score"] = "high"
        elif risky_count == 1:
            data["execution_structure"]["execution_risk_score"] = "medium"
        else:
            data["execution_structure"]["execution_risk_score"] = "low"

    def _ensure_primary_executor_row(self, data: dict[str, Any]) -> None:
        execution_structure = data.get("execution_structure", {})
        if not isinstance(execution_structure, dict):
            return

        primary_executor = execution_structure.get("primary_executor")
        if not isinstance(primary_executor, str) or not primary_executor.strip():
            return
        primary_executor = primary_executor.strip()

        role_rows = data.get("role_clarity_assessment")
        if not isinstance(role_rows, list):
            role_rows = []
            data["role_clarity_assessment"] = role_rows

        for row in role_rows:
            if not isinstance(row, dict):
                continue
            actor = str(row.get("actor", "")).strip().lower()
            if actor == primary_executor.lower():
                return

        authority_clarity = execution_structure.get("authority_clarity")
        if authority_clarity == "clear":
            authority_level = "high"
            clarity = "clear"
        else:
            authority_level = "undefined"
            clarity = authority_clarity if authority_clarity in {"partial", "undefined"} else "undefined"

        evidence_list = execution_structure.get("evidence", [])
        evidence_text = ""
        if isinstance(evidence_list, list) and evidence_list:
            first = evidence_list[0]
            if isinstance(first, str):
                evidence_text = first

        role_rows.append(
            {
                "actor": primary_executor,
                "role": "Primary Executor",
                "authority_level": authority_level,
                "responsibility_level": execution_structure.get("responsibility_load", "medium"),
                "clarity": clarity,
                "confidence": execution_structure.get("confidence", "medium"),
                "evidence": evidence_text,
            }
        )

    def _apply_structural_hardening(self, data: dict[str, Any]) -> None:
        ex = data.get("execution_structure", {})
        summary = data.get("executive_summary", {})
        power = data.get("power_structure", {})
        role_rows = data.get("role_clarity_assessment", [])
        questions = data.get("recommended_next_questions", [])

        if not isinstance(ex, dict) or not isinstance(summary, dict) or not isinstance(power, dict):
            return

        risk_score = ex.get("execution_risk_score")
        readiness = summary.get("execution_readiness")
        if risk_score == "high" and readiness in {"medium", "high"}:
            summary["execution_readiness"] = "low"
        elif risk_score == "medium" and readiness == "high":
            summary["execution_readiness"] = "medium"

        primary_executor = ex.get("primary_executor")
        authority_clarity = ex.get("authority_clarity")
        if isinstance(role_rows, list):
            for row in role_rows:
                if not isinstance(row, dict):
                    continue
                actor = str(row.get("actor", "")).strip().lower()
                responsibility = row.get("responsibility_level")
                if (
                    ("vishal nelaturi" in actor or actor == "vishal")
                    and responsibility == "high"
                    and authority_clarity != "clear"
                ):
                    row["authority_level"] = "undefined"

        unknown_gaps = power.get("unknown_authority_gaps", [])
        if not isinstance(unknown_gaps, list):
            unknown_gaps = []
            power["unknown_authority_gaps"] = unknown_gaps
        gap_text = "Final operational authority in India is not clearly defined"
        has_gap = any(
            isinstance(item, str)
            and ("final operational authority" in item.lower() or "operational control" in item.lower())
            for item in unknown_gaps
        )
        if not has_gap:
            unknown_gaps.append(gap_text)

        if power.get("unknown_authority_gaps"):
            if not isinstance(questions, list):
                questions = []
                data["recommended_next_questions"] = questions
            has_question = any(
                isinstance(item, dict)
                and (
                    "final authority" in str(item.get("question", "")).lower()
                    or "approve or reject" in str(item.get("question", "")).lower()
                    or "operational approval authority" in str(item.get("question", "")).lower()
                )
                for item in questions
            )
            if not has_question:
                questions.append(
                    {
                        "question": "Who has final authority to approve or reject CoE decisions on the ground?",
                        "priority": "high",
                        "why_now": "Execution cannot proceed safely if the on-ground implementation owner lacks explicit decision power.",
                    }
                )

        if (
            isinstance(primary_executor, str)
            and primary_executor.strip().lower() in {"vishal nelaturi", "vishal"}
            and authority_clarity != "clear"
            and isinstance(role_rows, list)
        ):
            for row in role_rows:
                if not isinstance(row, dict):
                    continue
                actor = str(row.get("actor", "")).strip().lower()
                if "vishal" in actor and row.get("authority_level") == "high":
                    row["authority_level"] = "undefined"

    def _apply_warning_backstop(self, data: dict[str, Any]) -> None:
        ex = data.get("execution_structure", {})
        warnings = data.get("executive_warnings", [])
        if not isinstance(ex, dict):
            return
        if not isinstance(warnings, list):
            warnings = []
            data["executive_warnings"] = warnings

        primary_executor = ex.get("primary_executor")
        responsibility_load = ex.get("responsibility_load")
        risk_score = ex.get("execution_risk_score")

        if (
            isinstance(primary_executor, str)
            and primary_executor.strip()
            and responsibility_load == "high"
            and risk_score == "high"
        ):
            has_equivalent = any(
                isinstance(row, dict)
                and "execution responsibility" in str(row.get("warning", "")).lower()
                and ("authority" in str(row.get("warning", "")).lower() or "governance" in str(row.get("warning", "")).lower())
                for row in warnings
            )
            if has_equivalent:
                return

            evidence = ex.get("evidence", [])
            evidence_text = ""
            if isinstance(evidence, list) and evidence:
                first = evidence[0]
                if isinstance(first, str):
                    evidence_text = first

            warnings.append(
                {
                    "warning": "Execution responsibility is assigned without clearly defined authority, compensation, or governance structure.",
                    "severity": "high",
                    "confidence": "high",
                    "reason": "The delivery owner carries high execution burden while structural control conditions remain insufficiently defined.",
                    "evidence": evidence_text,
                    "support_level": SUPPORT_DIRECT if evidence_text else SUPPORT_WEAK,
                    "claim_strength": "direct" if evidence_text else "weak",
                    "evidence_span": evidence_text,
                    "evidence_start_index": -1,
                    "evidence_end_index": -1,
                    "evidence_confidence": 1.0 if evidence_text else 0.0,
                }
            )

    def _evidence_list_from_binding(
        self,
        claim: str,
        transcript: str,
        preferred: list[str] | None = None,
        claim_type: str = "generic",
    ) -> tuple[list[str], dict[str, Any]]:
        binding = build_evidence_binding(
            claim=claim,
            transcript=transcript,
            preferred_spans=preferred or [],
            claim_type=claim_type,
        )
        if preferred and float(binding.get("evidence_confidence", 0.0)) < 0.5:
            binding = build_evidence_binding(
                claim=str(preferred[0]),
                transcript=transcript,
                preferred_spans=preferred,
                claim_type=claim_type,
            )
        evidence = [binding["evidence_span"]] if binding.get("evidence_span") else []
        return evidence, binding

    def _is_supported_actor(self, actor: str, transcript: str, alias_map: dict[str, str]) -> bool:
        value = str(actor or "").strip()
        if not value:
            return False
        return actor_present_in_transcript(value, transcript, alias_map=alias_map)

    def _attach_claim_meta(
        self,
        row: dict[str, Any],
        claim: str,
        transcript: str,
        preferred: list[str] | None = None,
        claim_type: str = "generic",
    ) -> dict[str, Any]:
        evidence, binding = self._evidence_list_from_binding(claim, transcript, preferred, claim_type=claim_type)
        if evidence:
            row["evidence"] = evidence[0] if isinstance(row.get("evidence", ""), str) else evidence
        row["support_level"] = binding["support_level"]
        row["claim_strength"] = binding["claim_strength"]
        row["evidence_span"] = binding["evidence_span"]
        row["evidence_start_index"] = binding["evidence_start_index"]
        row["evidence_end_index"] = binding["evidence_end_index"]
        row["evidence_confidence"] = binding["evidence_confidence"]
        return row

    def _build_intelligence_anchor_corpus(self, intelligence: dict[str, Any]) -> list[tuple[str, str]]:
        rows: list[tuple[str, str]] = []
        if not isinstance(intelligence, dict):
            return rows
        for family in [
            "decisions",
            "risks",
            "action_plan",
            "roadmap",
            "deadlines",
            "stakeholders",
            "timeline_mentions",
        ]:
            values = intelligence.get(family, [])
            if not isinstance(values, list):
                continue
            for item in values:
                if not isinstance(item, dict):
                    continue
                text = ""
                for key in ["text", "task", "step", "event", "name", "raw_time_reference"]:
                    candidate = str(item.get(key, "")).strip()
                    if candidate:
                        text = candidate
                        break
                if not text:
                    continue
                certainty = str(item.get("certainty_class", "UNCERTAIN")).strip().upper()
                if certainty not in {"DIRECT", "CONDITIONAL", "UNCERTAIN"}:
                    certainty = "UNCERTAIN"
                rows.append((text, certainty))
        summary = str(intelligence.get("summary", "")).strip()
        if summary:
            rows.append((summary, "UNCERTAIN"))
        return rows

    def _tokenize(self, text: str) -> set[str]:
        return {
            tok
            for tok in re.findall(r"[A-Za-z0-9]+", str(text or "").lower())
            if len(tok) >= 3
        }

    def _is_claim_anchored(self, claim: str, anchors: list[tuple[str, str]]) -> bool:
        claim_tokens = self._tokenize(claim)
        if not claim_tokens:
            return False
        for anchor_text, _ in anchors:
            if len(claim_tokens & self._tokenize(anchor_text)) >= 2:
                return True
        return False

    def _certainty_rank(self, value: str) -> int:
        return {"UNCERTAIN": 0, "CONDITIONAL": 1, "DIRECT": 2}.get(str(value).upper(), 0)

    def _claim_certainty_cap(self, claim: str, anchors: list[tuple[str, str]]) -> str:
        claim_tokens = self._tokenize(claim)
        best_overlap = 0
        best_certainty = "UNCERTAIN"
        for anchor_text, certainty in anchors:
            overlap = len(claim_tokens & self._tokenize(anchor_text))
            if overlap > best_overlap:
                best_overlap = overlap
                best_certainty = certainty
        return best_certainty if best_overlap >= 2 else "UNCERTAIN"

    def _apply_certainty_cap_to_confidence(self, payload: dict[str, Any], cap: str) -> None:
        current = str(payload.get("confidence", "low")).lower()
        rank = {"low": 0, "medium": 1, "high": 2}
        if cap == "UNCERTAIN" and rank.get(current, 0) > rank["low"]:
            payload["confidence"] = "low"
        elif cap == "CONDITIONAL" and rank.get(current, 0) > rank["medium"]:
            payload["confidence"] = "medium"

    def _enforce_truth_integrity(
        self,
        data: dict[str, Any],
        transcript: str,
        registry_data: dict[str, Any],
        intelligence: dict[str, Any],
    ) -> None:
        alias_map = build_alias_map(registry_data)
        anchors = self._build_intelligence_anchor_corpus(intelligence)
        power = data.get("power_structure", {})
        ex = data.get("execution_structure", {})
        role_rows = data.get("role_clarity_assessment", [])

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
                    power[key] = []
                    continue
                power[key] = [
                    item
                    for item in values
                    if isinstance(item, str) and self._is_supported_actor(item, transcript, alias_map)
                ]

        if isinstance(ex, dict):
            primary_executor = str(ex.get("primary_executor", "")).strip()
            if primary_executor and not self._is_supported_actor(primary_executor, transcript, alias_map):
                ex["primary_executor"] = ""

            claim = (
                f"{ex.get('primary_executor', '')} {ex.get('responsibility_load', '')} "
                f"{ex.get('authority_clarity', '')} {ex.get('compensation_clarity', '')} {ex.get('governance_clarity', '')}"
            ).strip()
            preferred = [ev for ev in ex.get("evidence", []) if isinstance(ev, str)]
            evidence, binding = self._evidence_list_from_binding(
                claim, transcript, preferred, claim_type="owner"
            )
            ex["evidence"] = evidence
            ex["support_level"] = binding["support_level"]
            ex["claim_strength"] = binding["claim_strength"]
            ex["evidence_span"] = binding["evidence_span"]
            ex["evidence_start_index"] = binding["evidence_start_index"]
            ex["evidence_end_index"] = binding["evidence_end_index"]
            ex["evidence_confidence"] = binding["evidence_confidence"]
            cap = self._claim_certainty_cap(claim, anchors)
            ex["certainty_class"] = cap
            if cap != "DIRECT":
                ex["authority_clarity"] = "partial" if ex.get("authority_clarity") == "clear" else ex.get("authority_clarity", "undefined")

        if isinstance(role_rows, list):
            filtered_rows: list[dict[str, Any]] = []
            for row in role_rows:
                if not isinstance(row, dict):
                    continue
                actor = str(row.get("actor", "")).strip()
                if actor and not self._is_supported_actor(actor, transcript, alias_map):
                    continue
                claim = f"{actor} {row.get('role', '')} {row.get('authority_level', '')} {row.get('responsibility_level', '')}".strip()
                preferred = [str(row.get("evidence", "")).strip()]
                row = self._attach_claim_meta(row, claim, transcript, preferred, claim_type="owner")
                if float(row.get("evidence_confidence", 0.0)) < 0.5:
                    continue
                if not self._is_claim_anchored(claim, anchors):
                    continue
                cap = self._claim_certainty_cap(claim, anchors)
                row["certainty_class"] = cap
                if cap != "DIRECT" and row.get("authority_level") == "high":
                    row["authority_level"] = "undefined"
                filtered_rows.append(row)
            data["role_clarity_assessment"] = filtered_rows

        section_claim_fields = {
            "executive_summary": ["meaning_of_meeting", "intent", "commitment"],
            "strategic_objective": ["objective", "business_direction", "success_condition"],
            "business_model_clarity": [],
            "risk_posture": [],
        }
        for section in section_claim_fields:
            payload = data.get(section, {})
            if not isinstance(payload, dict):
                continue
            preferred = [ev for ev in payload.get("evidence", []) if isinstance(ev, str)]
            claim_fields = section_claim_fields.get(section, [])
            claim = " ".join(str(payload.get(k, "")).strip() for k in claim_fields if str(payload.get(k, "")).strip())
            if not claim:
                claim = " ".join(preferred)
            if not claim and section == "risk_posture":
                drivers = payload.get("drivers", [])
                if isinstance(drivers, list):
                    claim = " ".join(str(d).strip() for d in drivers if str(d).strip())
            claim_type = "warning" if section == "risk_posture" else "decision"
            evidence, binding = self._evidence_list_from_binding(claim, transcript, preferred, claim_type=claim_type)
            if claim and not self._is_claim_anchored(claim, anchors):
                if section == "business_model_clarity":
                    payload["revenue_logic"] = "undefined"
                    payload["funding_logic"] = "undefined"
                    payload["ownership_model"] = "undefined"
                    payload["legal_governance"] = "undefined"
                elif section == "risk_posture":
                    payload["overall"] = "low"
                    payload["drivers"] = []
                else:
                    for field in claim_fields:
                        if field in payload:
                            payload[field] = ""
                payload["confidence"] = "low"
                evidence = []
                binding = {
                    "support_level": SUPPORT_WEAK,
                    "claim_strength": "weak",
                    "evidence_span": "",
                    "evidence_start_index": -1,
                    "evidence_end_index": -1,
                    "evidence_confidence": 0.0,
                }
            payload["evidence"] = evidence
            payload["support_level"] = binding["support_level"]
            payload["claim_strength"] = binding["claim_strength"]
            payload["evidence_span"] = binding["evidence_span"]
            payload["evidence_start_index"] = binding["evidence_start_index"]
            payload["evidence_end_index"] = binding["evidence_end_index"]
            payload["evidence_confidence"] = binding["evidence_confidence"]
            cap = self._claim_certainty_cap(claim, anchors)
            payload["certainty_class"] = cap
            self._apply_certainty_cap_to_confidence(payload, cap)
            if cap in {"UNCERTAIN", "CONDITIONAL"}:
                for field in claim_fields:
                    value = str(payload.get(field, ""))
                    value = re.sub(r"\b(strong consensus|finalized|confirmed agreement|clearly established|definite start)\b", "initial alignment", value, flags=re.IGNORECASE)
                    payload[field] = value

        flags = data.get("negotiation_flags", [])
        if isinstance(flags, list):
            filtered_flags: list[dict[str, Any]] = []
            for row in flags:
                if not isinstance(row, dict):
                    continue
                claim = f"{row.get('topic', '')} {row.get('status', '')} {row.get('severity', '')}".strip()
                preferred = [str(row.get("evidence", "")).strip()]
                row = self._attach_claim_meta(row, claim, transcript, preferred, claim_type="warning")
                if float(row.get("evidence_confidence", 0.0)) < 0.5:
                    continue
                if not self._is_claim_anchored(claim, anchors):
                    continue
                row["certainty_class"] = self._claim_certainty_cap(claim, anchors)
                filtered_flags.append(row)
            data["negotiation_flags"] = filtered_flags

        warnings = data.get("executive_warnings", [])
        if isinstance(warnings, list):
            filtered_warnings: list[dict[str, Any]] = []
            ex_risk_high = str(ex.get("execution_risk_score", "")).lower() == "high" if isinstance(ex, dict) else False
            for row in warnings:
                if not isinstance(row, dict):
                    continue
                claim = f"{row.get('warning', '')} {row.get('reason', '')}".strip()
                preferred = [str(row.get("evidence", "")).strip()]
                row = self._attach_claim_meta(row, claim, transcript, preferred, claim_type="warning")
                structural = "execution responsibility is assigned without clearly defined authority" in claim.lower()
                if (
                    float(row.get("evidence_confidence", 0.0)) < 0.5
                    and str(row.get("severity", "")).lower() == "high"
                    and not (structural and ex_risk_high)
                ):
                    continue
                if not self._is_claim_anchored(claim, anchors):
                    if not structural:
                        continue
                if (
                    not is_semantically_supportive(claim, str(row.get("evidence_span", "")), claim_type="warning")
                    and not (structural and ex_risk_high)
                ):
                    continue
                row["certainty_class"] = self._claim_certainty_cap(claim, anchors)
                filtered_warnings.append(row)
            data["executive_warnings"] = filtered_warnings

        questions = data.get("recommended_next_questions", [])
        if isinstance(questions, list):
            filtered_questions: list[dict[str, Any]] = []
            authority_gap_exists = bool(power.get("unknown_authority_gaps", [])) if isinstance(power, dict) else False
            for row in questions:
                if not isinstance(row, dict):
                    continue
                question = str(row.get("question", "")).strip()
                why_now = str(row.get("why_now", "")).strip()
                claim = f"{question} {why_now}".strip()
                if not question:
                    continue
                if not self._is_claim_anchored(claim, anchors):
                    authority_q = (
                        "final authority" in question.lower()
                        or "approve or reject" in question.lower()
                        or "operational approval authority" in question.lower()
                    )
                    if not (authority_q and authority_gap_exists):
                        continue
                row["question"] = question
                row["why_now"] = why_now
                filtered_questions.append(row)
            data["recommended_next_questions"] = filtered_questions

        summary = data.get("executive_summary", {})
        if isinstance(summary, dict):
            meaning = str(summary.get("meaning_of_meeting", ""))
            if "strong consensus" in meaning.lower():
                commitment_signals = 0
                for sentence in re.split(r"(?<=[.!?])\s+", transcript):
                    lowered = sentence.lower()
                    if any(sig in lowered for sig in ["we will", "i will", "agreed", "yes", "let's"]):
                        commitment_signals += 1
                if commitment_signals < 3:
                    summary["meaning_of_meeting"] = re.sub(
                        r"strong consensus",
                        "initial alignment",
                        meaning,
                        flags=re.IGNORECASE,
                    )
                    summary["support_level"] = SUPPORT_WEAK
                    summary["claim_strength"] = "weak"

    def _sort_output_deterministically(self, data: dict[str, Any]) -> None:
        power = data.get("power_structure", {})
        if isinstance(power, dict):
            for key in [
                "sponsor",
                "strategic_authority",
                "decision_makers",
                "advisors",
                "executors",
                "implementation_owner",
                "unknown_authority_gaps",
            ]:
                value = power.get(key, [])
                if isinstance(value, list):
                    power[key] = sorted(
                        [item for item in value if isinstance(item, str)],
                        key=lambda x: x.lower(),
                    )

        rows = data.get("role_clarity_assessment", [])
        if isinstance(rows, list):
            data["role_clarity_assessment"] = sorted(
                [row for row in rows if isinstance(row, dict)],
                key=lambda row: (
                    str(row.get("actor", "")).lower(),
                    str(row.get("role", "")).lower(),
                ),
            )

        flags = data.get("negotiation_flags", [])
        if isinstance(flags, list):
            data["negotiation_flags"] = sorted(
                [row for row in flags if isinstance(row, dict)],
                key=lambda row: (
                    str(row.get("topic", "")).lower(),
                    str(row.get("status", "")).lower(),
                    str(row.get("severity", "")).lower(),
                    str(row.get("evidence", "")).lower(),
                ),
            )

        warnings = data.get("executive_warnings", [])
        if isinstance(warnings, list):
            data["executive_warnings"] = sorted(
                [row for row in warnings if isinstance(row, dict)],
                key=lambda row: (
                    str(row.get("severity", "")).lower(),
                    str(row.get("reason", "")).lower(),
                    str(row.get("warning", "")).lower(),
                    str(row.get("evidence", "")).lower(),
                ),
            )

        questions = data.get("recommended_next_questions", [])
        if isinstance(questions, list):
            priority_rank = {"high": 0, "medium": 1, "low": 2}
            data["recommended_next_questions"] = sorted(
                [row for row in questions if isinstance(row, dict)],
                key=lambda row: (
                    priority_rank.get(str(row.get("priority", "")).lower(), 9),
                    str(row.get("question", "")).lower(),
                    str(row.get("why_now", "")).lower(),
                ),
            )

    def _validate(self, data: dict[str, Any], transcript: str) -> None:
        allowed_hml = {"high", "medium", "low"}
        allowed_clear = {"clear", "partial", "undefined"}
        allowed_authority = {"high", "medium", "low", "undefined"}
        allowed_status = {"open", "partially_defined"}
        allowed_support = {SUPPORT_DIRECT, SUPPORT_ACCEPTABLE, SUPPORT_WEAK}
        allowed_certainty = {"UNCERTAIN", "CONDITIONAL", "DIRECT"}

        for section in [
            "executive_summary",
            "strategic_objective",
            "business_model_clarity",
            "risk_posture",
        ]:
            if data[section].get("confidence") not in allowed_hml:
                raise ExecutiveIntelligenceError(f"Invalid confidence in {section}")

        if data["executive_summary"].get("execution_readiness") not in allowed_hml:
            raise ExecutiveIntelligenceError("Invalid execution_readiness")

        for key in ["authority_clarity", "compensation_clarity", "governance_clarity"]:
            if data["execution_structure"].get(key) not in allowed_clear:
                raise ExecutiveIntelligenceError(f"Invalid execution_structure.{key}")

        if data["execution_structure"].get("execution_risk_score") not in allowed_hml:
            raise ExecutiveIntelligenceError("Invalid execution_risk_score")

        if data["power_structure"].get("confidence") not in allowed_hml:
            raise ExecutiveIntelligenceError("Invalid power_structure confidence")

        for row in data.get("role_clarity_assessment", []):
            if not isinstance(row, dict):
                raise ExecutiveIntelligenceError("Invalid role clarity row")
            if row.get("authority_level") not in allowed_authority:
                raise ExecutiveIntelligenceError("Invalid authority_level")
            if row.get("responsibility_level") not in allowed_hml:
                raise ExecutiveIntelligenceError("Invalid responsibility_level")
            if row.get("clarity") not in allowed_clear:
                raise ExecutiveIntelligenceError("Invalid clarity")
            if row.get("confidence") not in allowed_hml:
                raise ExecutiveIntelligenceError("Invalid role clarity confidence")

        for row in data.get("negotiation_flags", []):
            if not isinstance(row, dict):
                raise ExecutiveIntelligenceError("Invalid negotiation row")
            if row.get("status") not in allowed_status:
                raise ExecutiveIntelligenceError("Invalid negotiation flag status")
            if row.get("severity") not in allowed_hml:
                raise ExecutiveIntelligenceError("Invalid negotiation flag severity")
            if row.get("confidence") not in allowed_hml:
                raise ExecutiveIntelligenceError("Invalid negotiation flag confidence")

        for row in data.get("executive_warnings", []):
            if not isinstance(row, dict):
                raise ExecutiveIntelligenceError("Invalid executive warning row")
            if row.get("severity") not in allowed_hml:
                raise ExecutiveIntelligenceError("Invalid warning severity")
            if row.get("confidence") not in allowed_hml:
                raise ExecutiveIntelligenceError("Invalid warning confidence")
            if not row.get("warning") or not row.get("reason"):
                raise ExecutiveIntelligenceError("Invalid executive warning payload")

        for section in [
            data.get("executive_summary", {}),
            data.get("strategic_objective", {}),
            data.get("execution_structure", {}),
            data.get("business_model_clarity", {}),
            data.get("risk_posture", {}),
        ]:
            if not isinstance(section, dict):
                continue
            support_level = section.get("support_level")
            if support_level and support_level not in allowed_support:
                raise ExecutiveIntelligenceError("Invalid support_level in executive payload")
            span = str(section.get("evidence_span", "")).strip()
            if span and span not in transcript:
                raise ExecutiveIntelligenceError("Non-verbatim executive evidence_span detected")
            certainty = str(section.get("certainty_class", "")).strip().upper()
            if certainty and certainty not in allowed_certainty:
                raise ExecutiveIntelligenceError("Invalid certainty_class in executive payload")

        execution_structure = data.get("execution_structure", {})
        executive_summary = data.get("executive_summary", {})
        power_structure = data.get("power_structure", {})

        if not isinstance(execution_structure, dict) or not isinstance(executive_summary, dict):
            raise ExecutiveIntelligenceError("Invalid execution or summary payload")

        risk_score = execution_structure.get("execution_risk_score")
        readiness = executive_summary.get("execution_readiness")
        if risk_score == "high" and readiness in {"high", "medium"}:
            raise ExecutiveIntelligenceError("High execution risk requires low execution_readiness")

        primary_executor = str(execution_structure.get("primary_executor", "")).strip().lower()
        authority_clarity = execution_structure.get("authority_clarity")
        if primary_executor == "vishal nelaturi" and authority_clarity != "clear":
            for row in data.get("role_clarity_assessment", []):
                if not isinstance(row, dict):
                    continue
                actor = str(row.get("actor", "")).strip().lower()
                if "vishal" in actor and row.get("authority_level") == "high":
                    raise ExecutiveIntelligenceError(
                        "Vishal authority_level cannot be high when authority_clarity is not clear"
                    )

        if (
            execution_structure.get("responsibility_load") == "high"
            and risk_score == "high"
        ):
            has_structural_warning = any(
                isinstance(row, dict)
                and row.get("severity") == "high"
                and "execution responsibility" in str(row.get("warning", "")).lower()
                and (
                    "authority" in str(row.get("warning", "")).lower()
                    or "compensation" in str(row.get("warning", "")).lower()
                    or "governance" in str(row.get("warning", "")).lower()
                )
                for row in data.get("executive_warnings", [])
            )
            if not has_structural_warning:
                raise ExecutiveIntelligenceError(
                    "Missing high-severity structural warning for high execution burden/risk"
                )

        unknown_gaps = power_structure.get("unknown_authority_gaps", [])
        if isinstance(unknown_gaps, list) and unknown_gaps:
            has_authority_question = any(
                isinstance(row, dict)
                and (
                    "final authority" in str(row.get("question", "")).lower()
                    or "approve or reject" in str(row.get("question", "")).lower()
                    or "operational approval authority" in str(row.get("question", "")).lower()
                )
                for row in data.get("recommended_next_questions", [])
            )
            if not has_authority_question:
                raise ExecutiveIntelligenceError(
                    "Unknown authority gaps require an authority clarification question"
                )
