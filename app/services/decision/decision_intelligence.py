from __future__ import annotations

import hashlib
import json
import os
import re
import time
from pathlib import Path
from typing import Any

from openai import OpenAI

from app.config import (
    DECISION_V2_METADATA_FILE,
    DECISION_V2_MODEL,
    DECISION_V2_OUTPUT_DIR,
    DECISION_V2_OUTPUT_FILE,
    DECISION_V2_PROMPT_VERSION,
    MISSION_REGISTRY_PATH,
)
from app.models.decision import DecisionIntelligenceResult, decision_v2_schema_defaults


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
        intelligence_path = transcript_dir / "intelligence.json"
        executive_path = executive_dir / "executive_intelligence.json"
        legacy_path = Path("reports") / "decision_intelligence.json"
        for req in [raw_path, clean_path, intelligence_path, executive_path]:
            if not req.exists():
                raise FileNotFoundError(f"Required artifact not found: {req}")

        registry = load_mission_registry()
        registry_grounding = build_registry_grounding(registry)
        alias_map = build_alias_map(registry)
        raw_text = raw_path.read_text(encoding="utf-8")
        clean_text = clean_path.read_text(encoding="utf-8")
        intelligence = json.loads(intelligence_path.read_text(encoding="utf-8"))
        executive = json.loads(executive_path.read_text(encoding="utf-8"))
        legacy = json.loads(legacy_path.read_text(encoding="utf-8")) if legacy_path.exists() else {}

        response = self.client.chat.completions.create(
            model=DECISION_V2_MODEL,
            temperature=0,
            messages=[
                {"role": "system", "content": self._system_prompt()},
                {
                    "role": "user",
                    "content": self._build_prompt(clean_text, intelligence, executive, registry_grounding, legacy),
                },
            ],
        )
        parsed = self._safe_parse_json(response.choices[0].message.content or "")
        if parsed is None:
            raise DecisionIntelligenceV2Error("Model did not return valid JSON")

        payload = self._enforce_schema(parsed)
        payload = self._normalize_with_registry(payload, alias_map)
        payload = self._harden_records(payload, clean_text, executive, alias_map)
        self._validate_records(payload, clean_text, alias_map)
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
            },
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
            "Blocked decision means execution cannot proceed due to unresolved dependency."
        )

    def _build_prompt(
        self,
        transcript_clean: str,
        intelligence: dict[str, Any],
        executive: dict[str, Any],
        registry_grounding: str,
        legacy_decision: dict[str, Any],
    ) -> str:
        return f"""MISSION REGISTRY GROUNDING\n{registry_grounding}\n\nTRANSCRIPT_CLEAN\n{transcript_clean}\n\nINTELLIGENCE.JSON\n{json.dumps(intelligence, ensure_ascii=False, indent=2)}\n\nEXECUTIVE_INTELLIGENCE.JSON\n{json.dumps(executive, ensure_ascii=False, indent=2)}\n\nLEGACY_DECISION_INTELLIGENCE\n{json.dumps(legacy_decision, ensure_ascii=False, indent=2)}\n\nReturn JSON with exact top-level keys: decision_records, operational_summary.\nFor each decision record use fields: decision_id, statement, state, impact_level, confidence, primary_owner, owners, commitments, dependencies, decision_gaps, timeline_signals, evidence.\nEnums:\nstate=confirmed|tentative|pending|blocked\nownership_type=assigned_owner|shared_owner|missing_owner\ncommitment_type=explicit_commitment|implied_commitment|requested_commitment|unresolved_commitment\ncommitment status=open|accepted|unresolved\ndependency type=governance_dependency|authority_dependency|funding_dependency|timeline_dependency|partner_dependency\ndependency status=open|partially_resolved|resolved\ntimeline signal_type=start_window|deadline_hint|followup_marker\nRules: confirmed requires explicit commitment; blocked requires unresolved high blocker; no invented details; timeline signals remain raw."""

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
                    "impact_level": self._coerce(str(rec.get("impact_level", "")).strip().lower(), HML, "medium"),
                    "confidence": self._coerce(str(rec.get("confidence", "")).strip().lower(), HML, "medium"),
                    "primary_owner": str(rec.get("primary_owner", "")).strip(),
                    "owners": self._normalize_owners(rec.get("owners", [])),
                    "commitments": self._normalize_commitments(rec.get("commitments", [])),
                    "dependencies": self._normalize_dependencies(rec.get("dependencies", [])),
                    "decision_gaps": self._normalize_gaps(rec.get("decision_gaps", [])),
                    "timeline_signals": self._normalize_signals(rec.get("timeline_signals", [])),
                    "evidence": self._str_list(rec.get("evidence", [])),
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
            actor = str(item.get("actor", "")).strip()
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
                    "actor": str(item.get("actor", "")).strip(),
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

    def _harden_records(
        self,
        data: dict[str, Any],
        transcript_clean: str,
        executive: dict[str, Any],
        alias_map: dict[str, str],
    ) -> dict[str, Any]:
        known_values = {v.lower() for v in alias_map.values() if isinstance(v, str)}
        records = []
        dropped_unsupported = 0
        for rec in data["decision_records"]:
            rec = self._augment_dependencies(rec, executive)
            rec = self._enforce_primary_owner(rec)
            rec = self._enforce_missing_owner_gap(rec)
            rec = self._enforce_confirmed_rule(rec)
            rec = self._enforce_blocked_rule(rec)
            rec = self._apply_unknown_actor_confidence(rec, alias_map, known_values)
            try:
                rec = self._enforce_evidence(rec, transcript_clean)
            except DecisionIntelligenceV2Error:
                dropped_unsupported += 1
                continue
            if not rec.get("decision_id"):
                rec["decision_id"] = self._decision_id(rec)
            records.append(rec)
        if dropped_unsupported and not records:
            raise DecisionIntelligenceV2Error(
                "All candidate decision records were unsupported after evidence validation"
            )
        data["decision_records"] = records
        return data

    def _enforce_primary_owner(self, rec: dict[str, Any]) -> dict[str, Any]:
        owners = rec.get("owners", [])
        assigned = [o.get("actor", "").strip() for o in owners if o.get("ownership_type") == "assigned_owner" and o.get("actor", "").strip()]
        shared = [o.get("actor", "").strip() for o in owners if o.get("ownership_type") == "shared_owner" and o.get("actor", "").strip()]
        rec["primary_owner"] = assigned[0] if assigned else (shared[0] if shared else "")
        return rec

    def _enforce_missing_owner_gap(self, rec: dict[str, Any]) -> dict[str, Any]:
        if str(rec.get("primary_owner", "")).strip():
            return rec
        owners = rec.get("owners", [])
        if not owners:
            owners.append({"actor": "", "ownership_type": "missing_owner"})
        elif not any(o.get("ownership_type") == "missing_owner" for o in owners if isinstance(o, dict)):
            owners.append({"actor": "", "ownership_type": "missing_owner"})
        gaps = rec.get("decision_gaps", [])
        if not any(
            isinstance(g, dict)
            and (
                str(g.get("gap_type", "")).lower() == "missing_owner"
                or "accountable owner" in str(g.get("question", "")).lower()
            )
            for g in gaps
        ):
            gaps.append({"gap_type": "missing_owner", "criticality": "high", "question": "Who is the accountable owner for this decision?"})
        rec["owners"] = owners
        rec["decision_gaps"] = gaps
        return rec

    def _enforce_confirmed_rule(self, rec: dict[str, Any]) -> dict[str, Any]:
        if rec.get("state") != "confirmed":
            return rec
        has_explicit = any(c.get("commitment_type") == "explicit_commitment" for c in rec.get("commitments", []) if isinstance(c, dict))
        if not has_explicit:
            rec["state"] = "blocked" if self._has_open_high(rec) else "tentative"
        return rec

    def _enforce_blocked_rule(self, rec: dict[str, Any]) -> dict[str, Any]:
        if self._has_open_high(rec):
            rec["state"] = "blocked"
        return rec

    def _has_open_high(self, rec: dict[str, Any]) -> bool:
        return any(d.get("status") == "open" and d.get("blocking_level") == "high" for d in rec.get("dependencies", []) if isinstance(d, dict))

    def _apply_unknown_actor_confidence(
        self,
        rec: dict[str, Any],
        alias_map: dict[str, str],
        known_values: set[str],
    ) -> dict[str, Any]:
        def known(actor: str) -> bool:
            return bool(actor) and (actor.lower() in alias_map or actor.lower() in known_values)

        primary_owner = str(rec.get("primary_owner", "")).strip()
        if primary_owner and not known(primary_owner):
            rec["confidence"] = "low"

        for cmt in rec.get("commitments", []):
            if not isinstance(cmt, dict):
                continue
            actor = str(cmt.get("actor", "")).strip()
            if actor and not known(actor):
                cmt["confidence"] = "low"
                rec["confidence"] = "low"
        return rec

    def _augment_dependencies(self, rec: dict[str, Any], executive: dict[str, Any]) -> dict[str, Any]:
        deps = rec.get("dependencies", [])
        ex_struct = executive.get("execution_structure", {}) if isinstance(executive, dict) else {}
        bm = executive.get("business_model_clarity", {}) if isinstance(executive, dict) else {}
        text = " ".join(
            [str(rec.get("statement", ""))]
            + rec.get("evidence", [])
            + [str(c.get("commitment", "")) for c in rec.get("commitments", []) if isinstance(c, dict)]
        ).lower()
        has = {d.get("type") for d in deps if isinstance(d, dict)}
        exec_related = any(k in text for k in ["implementation", "coe", "program", "representative"])
        if ex_struct.get("authority_clarity") in {"partial", "undefined"} and exec_related and "authority_dependency" not in has:
            deps.append({"type": "authority_dependency", "status": "open", "blocking_level": "high", "reason": "Operational authority remains unclear for execution ownership."})
        if ex_struct.get("governance_clarity") in {"partial", "undefined"} and exec_related and "governance_dependency" not in has:
            deps.append({"type": "governance_dependency", "status": "open", "blocking_level": ("high" if ex_struct.get("governance_clarity") == "undefined" else "medium"), "reason": "Governance structure for execution control is not fully defined."})
        funding_related = any(k in text for k in ["fund", "revenue", "tuition", "fee", "profit", "finance", "phd"])
        if funding_related and ("funding_dependency" not in has) and (bm.get("funding_logic") in {"partial", "undefined"} or bm.get("revenue_logic") in {"partial", "undefined"}):
            deps.append({"type": "funding_dependency", "status": "open", "blocking_level": ("high" if (bm.get("funding_logic") == "undefined" or bm.get("revenue_logic") == "undefined") else "medium"), "reason": "Funding/revenue model terms are not fully resolved."})
        has_signal = any(str(s.get("raw_reference", "")).strip() for s in rec.get("timeline_signals", []) if isinstance(s, dict))
        if (has_signal or any(k in text for k in ["may", "june", "week", "tuesday", "timeline", "start"])) and "timeline_dependency" not in has:
            deps.append({"type": "timeline_dependency", "status": "open", "blocking_level": "medium", "reason": "Timeline references exist but execution timing remains unresolved."})
        rec["dependencies"] = deps
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
        rec["evidence"] = valid
        return rec

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

    def _validate_records(self, data: dict[str, Any], transcript_clean: str, alias_map: dict[str, str]) -> None:
        for rec in data.get("decision_records", []):
            if rec.get("state") not in STATE_ENUM:
                raise DecisionIntelligenceV2Error("Invalid decision state")
            if rec.get("impact_level") not in HML:
                raise DecisionIntelligenceV2Error("Invalid impact_level")
            if rec.get("confidence") not in HML:
                raise DecisionIntelligenceV2Error("Invalid decision confidence")

            owners = rec.get("owners", [])
            for owner in owners:
                if not isinstance(owner, dict) or owner.get("ownership_type") not in OWNERSHIP_ENUM:
                    raise DecisionIntelligenceV2Error("Invalid ownership payload")
                actor = str(owner.get("actor", "")).strip()
                if actor and actor.lower() in alias_map and actor != self._normalize_actor(actor, alias_map):
                    raise DecisionIntelligenceV2Error("Owner actor not normalized")

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

            if rec.get("state") == "confirmed":
                if not any(c.get("commitment_type") == "explicit_commitment" for c in commitments if isinstance(c, dict)):
                    raise DecisionIntelligenceV2Error("Confirmed decision has no explicit commitment")

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
