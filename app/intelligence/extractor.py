from __future__ import annotations

import json
import hashlib
import re
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

MISSION_REGISTRY_PATH = Path("data/context/mission_registry.json")
MISSION_CONTEXT_PROMPT_PATH = Path("app/prompts/mission_context.txt")

USER_PROMPT_TEMPLATE = """
Extract structured decision intelligence from this transcript chunk.

Important:
- Extract only what is supported by the transcript.
- If something is discussed but a key value is missing, capture it under missing_details.
- Do not invent numbers, percentages, salary ranges, ownership, or dates.

Transcript chunk:
----------------
{chunk}
----------------
"""

PLACEHOLDER_PATTERNS = [
    "x percent",
    "tbd",
    "some amount",
    "some percentage",
    "unknown percentage",
    "not sure",
    "etc.",
]

GENERIC_LOW_VALUE_PATTERNS = [
    "misalignment",
    "lack of clarity",
    "communication issue",
]

UNRESOLVED_MARKERS = [
    "not decided",
    "not decided yet",
    "to be decided",
    "tbd",
    "not clear",
    "unclear",
    "we need to decide",
    "we need to define",
    "we need to finalize",
    "we will decide",
    "we'll decide",
    "we need to check",
    "to confirm",
    "need confirmation",
    "not finalized",
    "pending",
    "later",
    "we will see",
    "not sure",
    "maybe",
    "or",
]

DOMAIN_REGISTRY: dict[str, dict[str, Any]] = {
    "compensation": {
        "trigger_keywords": ["salary", "compensation", "stipend", "pay", "package"],
        "missing_field_candidates": ["salary", "compensation", "stipend", "pay package"],
        "severity": "high",
        "critical": True,
        "reason_code": "compensation_undefined",
    },
    "funding": {
        "trigger_keywords": ["funding", "finance", "budget", "cost", "tuition", "revenue share"],
        "missing_field_candidates": ["funding terms", "budget", "revenue share percentage"],
        "severity": "high",
        "critical": True,
        "reason_code": "funding_model_undefined",
    },
    "ownership": {
        "trigger_keywords": ["ownership", "owner", "accountable", "who decides", "responsible"],
        "missing_field_candidates": ["ownership", "accountability owner", "decision owner"],
        "severity": "high",
        "critical": True,
        "reason_code": "ownership_undefined",
    },
    "reporting": {
        "trigger_keywords": ["reporting", "authority", "supervise", "report to", "governance"],
        "missing_field_candidates": ["reporting line", "authority level", "governance authority"],
        "severity": "high",
        "critical": True,
        "reason_code": "reporting_undefined",
    },
    "timeline": {
        "trigger_keywords": ["start", "date", "timeline", "milestone", "week", "month"],
        "missing_field_candidates": ["start date", "milestone date", "timeline"],
        "severity": "medium",
        "critical": True,
        "reason_code": "timeline_undefined",
    },
    "legal": {
        "trigger_keywords": ["mou", "contract", "legal", "agreement", "approval"],
        "missing_field_candidates": ["contract type", "mou status", "approval authority"],
        "severity": "high",
        "critical": True,
        "reason_code": "legal_structure_undefined",
    },
}

_client: OpenAI | None = None


def _get_client() -> OpenAI:
    global _client
    if _client is None:
        _client = OpenAI()
    return _client


def _load_mission_registry() -> dict[str, Any]:
    if not MISSION_REGISTRY_PATH.exists():
        raise FileNotFoundError(f"Mission registry missing: {MISSION_REGISTRY_PATH}")

    data = json.loads(MISSION_REGISTRY_PATH.read_text(encoding="utf-8-sig"))
    if not isinstance(data, dict):
        raise ValueError("Mission registry must be a JSON object")
    return data


def _build_mission_context_block(registry: dict[str, Any]) -> str:
    primary_actor = registry.get("primary_actor", {})
    institutions = registry.get("institutions", [])
    stakeholders = registry.get("stakeholders", [])
    mission_terms = registry.get("mission_terms", [])

    lines: list[str] = ["MISSION CONTEXT", ""]
    lines.append("Primary mission actor:")
    lines.append(f"- {primary_actor.get('name', '')}")
    lines.append(f"- Role: {primary_actor.get('role', '')}")
    lines.append(
        "- Treat Vishal as the primary implementation agent unless the transcript explicitly assigns responsibility to someone else."
    )
    lines.append("")

    lines.append("Institutions:")
    for institution in institutions:
        if isinstance(institution, dict):
            normalized_name = str(institution.get("normalized_name", "")).strip()
            mission_role = str(institution.get("mission_role", "")).strip()
            if normalized_name and mission_role:
                lines.append(f"- {normalized_name} = {mission_role}")
    lines.append("")

    lines.append("High-importance stakeholders:")
    for stakeholder in stakeholders:
        if not isinstance(stakeholder, dict):
            continue
        if str(stakeholder.get("signal_weight", "")).lower() != "high":
            continue
        name = str(stakeholder.get("name", "")).strip()
        importance = str(stakeholder.get("importance", "")).strip().lower()
        if not name:
            continue
        if "strategic" in importance:
            authority = "strategic authority"
        elif "academic" in importance:
            authority = "academic authority"
        elif "executive" in importance:
            authority = "executive authority"
        else:
            authority = importance or "authority"
        lines.append(f"- {name} = {authority}")
    lines.append("")

    lines.append("Mission concept:")
    for term in mission_terms:
        if not isinstance(term, dict):
            continue
        term_name = str(term.get("term", "")).strip()
        normalized_name = str(term.get("normalized_name", "")).strip()
        meaning = str(term.get("meaning", "")).strip()
        if term_name and normalized_name and meaning:
            lines.append(f"- {term_name} ({normalized_name}) = {meaning}")
    lines.append("")

    lines.append("Entity normalization rules:")
    lines.append('- Normalize Aivancity consistently as "Aivancity"')
    lines.append('- Normalize Gujarat Technological University as "GTU"')
    lines.append('- Normalize Center of Excellence / CoE as "Center of Excellence (CoE)"')
    lines.append("- Keep people, organizations, locations, and initiatives separate")
    lines.append("- Do not duplicate entities because of spelling variations")
    lines.append("")

    lines.append("Interpretation rules:")
    actor_rules = primary_actor.get("interpretation_rules", [])
    if isinstance(actor_rules, list):
        for rule in actor_rules:
            if isinstance(rule, str) and rule.strip():
                lines.append(f"- {rule.strip()}")

    for institution in institutions:
        if isinstance(institution, dict):
            rules = institution.get("interpretation_rules", [])
            if isinstance(rules, list):
                for rule in rules:
                    if isinstance(rule, str) and rule.strip():
                        lines.append(f"- {rule.strip()}")

    return "\n".join(lines).strip()


def _load_mission_context_block(registry: dict[str, Any]) -> str:
    block = _build_mission_context_block(registry)

    if MISSION_CONTEXT_PROMPT_PATH.exists():
        _ = MISSION_CONTEXT_PROMPT_PATH.read_text(encoding="utf-8")

    return block


MISSION_REGISTRY = _load_mission_registry()
MISSION_CONTEXT_BLOCK = _load_mission_context_block(MISSION_REGISTRY)

SYSTEM_PROMPT = MISSION_CONTEXT_BLOCK + """

You are an expert Decision Intelligence Extraction Engine for CoE-Decision-Intelligence.

Your job is to extract ONLY information that is explicitly supported by the transcript.

Return STRICT JSON with exactly these keys:
- decisions
- risks
- action_items
- suggestions
- key_points
- entities
- missing_details

Entities MUST be structured as:
{
  "people": [],
  "organizations": [],
  "locations": [],
  "initiatives": []
}

Definitions:

1. decisions
Concrete commitments, directions, or choices made in the conversation.

2. risks
Specific, contextual risks, blockers, uncertainties, or failure conditions tied to a concrete scenario.

3. action_items
Concrete tasks, next steps, or responsibilities that someone should do.

4. suggestions
Recommendations, proposals, or improvement ideas discussed.

5. key_points
Important discussion points useful for understanding the conversation.

6. entities
Important people, organizations, locations, and initiatives.

7. missing_details
Critical details that were discussed but not specified clearly.

STRICT RULES:
- DO NOT hallucinate.
- DO NOT invent values.
- DO NOT use placeholders such as X percent, TBD, some amount, or some percentage.
- If a value, number, percentage, date, owner, or amount is not explicitly stated, do not fabricate it.
- Either omit it or record it under missing_details if it is important and clearly discussed.

DECISION FILTER RULE:
Only include a decision if:
- there is a clear commitment, agreement, or direction
- OR responsibility is explicitly assigned

RISK QUALITY RULE:
Reject generic risks. Every risk must include a concrete scenario condition and consequence.

MISSING DETAILS PRIORITY RULE:
Focus especially on missing:
- financial terms (salary, funding, revenue share)
- role definition (responsibility, reporting)
- timeline (start date, milestones)
- ownership (who is accountable)

If these are discussed but not clearly defined, they MUST appear in missing_details.

OUTPUT RULES:
- Return JSON only
- No markdown
- No preamble
- No explanation
"""


def chunk_text(text: str, max_tokens: int = 1200) -> list[str]:
    words = text.split()
    chunks: list[str] = []

    for i in range(0, len(words), max_tokens):
        chunks.append(" ".join(words[i : i + max_tokens]))

    return chunks


def safe_parse_json(text: str) -> dict[str, Any] | None:
    try:
        parsed = json.loads(text)
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if match:
            try:
                parsed = json.loads(match.group())
                return parsed if isinstance(parsed, dict) else None
            except Exception:
                return None
        return None


def _empty_entities() -> dict[str, list[str]]:
    return {
        "people": [],
        "organizations": [],
        "locations": [],
        "initiatives": [],
    }


def _stable_missing_detail_id(item: dict[str, Any]) -> str:
    payload = {
        "topic": str(item.get("topic", "")).strip().lower(),
        "missing_field": str(item.get("missing_field", "")).strip().lower(),
        "context": str(item.get("context", "")).strip().lower(),
        "evidence": str(item.get("evidence", "")).strip(),
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    digest = hashlib.sha1(encoded).hexdigest()[:12]
    return f"md-{digest}"


def _normalize_missing_details_with_ids(values: Any) -> list[dict[str, Any]]:
    if not isinstance(values, list):
        return []

    normalized: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for item in values:
        if not isinstance(item, dict):
            continue
        row = dict(item)
        item_id = str(row.get("id", "")).strip() or _stable_missing_detail_id(row)
        if item_id in seen_ids:
            continue
        row["id"] = item_id
        seen_ids.add(item_id)
        normalized.append(row)
    return normalized


def enforce_schema(data: Any) -> dict[str, Any]:
    schema: dict[str, Any] = {
        "decisions": [],
        "risks": [],
        "action_items": [],
        "suggestions": [],
        "key_points": [],
        "entities": _empty_entities(),
        "missing_details": [],
        "inferred_risks": [],
        "critical_gaps": [],
    }

    if not isinstance(data, dict):
        return schema

    # If model returns flat entities list, capture as raw_entities for normalization.
    raw_entities: list[Any] = []
    entities_value = data.get("entities")
    if isinstance(entities_value, list):
        raw_entities.extend(entities_value)
        data["entities"] = _empty_entities()
    elif isinstance(entities_value, dict):
        normalized_entities = _empty_entities()
        for key in normalized_entities:
            values = entities_value.get(key, [])
            if isinstance(values, list):
                normalized_entities[key] = values
            else:
                normalized_entities[key] = []
        data["entities"] = normalized_entities
    else:
        data["entities"] = _empty_entities()

    existing_raw = data.get("raw_entities", [])
    if isinstance(existing_raw, list):
        raw_entities.extend(existing_raw)
    data["raw_entities"] = raw_entities

    for key, default in schema.items():
        if key == "entities":
            continue
        if key not in data:
            data[key] = default
        elif not isinstance(data[key], list):
            data[key] = default

    data["missing_details"] = _normalize_missing_details_with_ids(data["missing_details"])

    return data


def _dedupe_sorted(values: list[str]) -> list[str]:
    return sorted({v.strip() for v in values if isinstance(v, str) and v.strip()})


def normalize_entities(
    raw_entities: list[Any], mission_context: dict[str, Any]
) -> dict[str, list[str]]:
    structured = _empty_entities()

    stakeholders = mission_context.get("stakeholders", [])
    institutions = mission_context.get("institutions", [])
    mission_terms = mission_context.get("mission_terms", [])
    primary_actor = mission_context.get("primary_actor", {})

    registry_people: dict[str, str] = {}
    registry_orgs: dict[str, str] = {}
    registry_inits: dict[str, str] = {}

    if isinstance(primary_actor, dict):
        name = str(primary_actor.get("name", "")).strip()
        if name:
            registry_people[name.lower()] = name

    for stakeholder in stakeholders:
        if isinstance(stakeholder, dict):
            name = str(stakeholder.get("name", "")).strip()
            if name:
                registry_people[name.lower()] = name

    for institution in institutions:
        if isinstance(institution, dict):
            n1 = str(institution.get("normalized_name", "")).strip()
            n2 = str(institution.get("name", "")).strip()
            if n1:
                registry_orgs[n1.lower()] = n1
            if n2 and n1:
                registry_orgs[n2.lower()] = n1
            elif n2:
                registry_orgs[n2.lower()] = n2

    for term in mission_terms:
        if isinstance(term, dict):
            normalized_name = str(term.get("normalized_name", "")).strip()
            term_name = str(term.get("term", "")).strip()
            canonical = (
                "Center of Excellence (CoE)"
                if (normalized_name.lower() == "coe" or "center of excellence" in term_name.lower())
                else (normalized_name or term_name)
            )
            if normalized_name:
                registry_inits[normalized_name.lower()] = canonical
            if term_name:
                registry_inits[term_name.lower()] = canonical

    alias_map: dict[str, tuple[str, str]] = {
        "ivan city": ("Aivancity", "organizations"),
        "ivancity": ("Aivancity", "organizations"),
        "aivancity": ("Aivancity", "organizations"),
        "gujarat technological university": ("GTU", "organizations"),
        "gtu": ("GTU", "organizations"),
        "gto": ("GTU", "organizations"),
        "coe": ("Center of Excellence (CoE)", "initiatives"),
        "center of excellence": ("Center of Excellence (CoE)", "initiatives"),
    }

    known_locations = {
        "india": "India",
        "gujarat": "Gujarat",
        "france": "France",
        "paris": "Paris",
        "nepal": "Nepal",
        "bangladesh": "Bangladesh",
        "sri lanka": "Sri Lanka",
    }
    org_keywords = ["university", "school", "institute", "company", "center", "coe"]

    def _add(bucket: str, value: str) -> None:
        if value and value not in structured[bucket]:
            structured[bucket].append(value)

    queue: list[Any] = list(raw_entities)
    while queue:
        item = queue.pop(0)
        if isinstance(item, dict):
            for bucket in ["people", "organizations", "locations", "initiatives"]:
                values = item.get(bucket, [])
                if isinstance(values, list):
                    queue.extend(values)
            continue
        if not isinstance(item, str):
            continue

        value = item.strip()
        if not value:
            continue
        lowered = value.lower()

        if lowered in alias_map:
            canonical, bucket = alias_map[lowered]
            _add(bucket, canonical)
            continue
        if lowered in registry_people:
            _add("people", registry_people[lowered])
            continue
        if lowered in registry_orgs:
            _add("organizations", registry_orgs[lowered])
            continue
        if lowered in registry_inits:
            _add("initiatives", registry_inits[lowered])
            continue
        if lowered in known_locations:
            _add("locations", known_locations[lowered])
            continue

        if any(keyword in lowered for keyword in org_keywords):
            if "center of excellence" in lowered or lowered == "coe":
                _add("initiatives", "Center of Excellence (CoE)")
            else:
                _add("organizations", value)
            continue

        tokens = value.split()
        if len(tokens) <= 4 and any(ch.isupper() for ch in value):
            if all(token[:1].isupper() for token in tokens if token):
                _add("people", value)
                continue

        _add("organizations", value)

    for bucket in structured:
        structured[bucket] = _dedupe_sorted(structured[bucket])

    return structured


def contains_bad_pattern(text: str) -> bool:
    lowered = text.lower().strip()
    return any(p in lowered for p in PLACEHOLDER_PATTERNS)


def is_too_generic(text: str) -> bool:
    lowered = text.lower().strip()
    return any(p in lowered for p in GENERIC_LOW_VALUE_PATTERNS)


def has_commitment_signals(text: str) -> bool:
    signals = [
        "we will",
        "we'll",
        "let's",
        "yes",
        "agreed",
        "next step",
        "you will",
        "i will",
        "we should proceed",
        "confirmed",
        "we meet",
    ]
    lowered = text.lower()
    return any(signal in lowered for signal in signals)


def has_missing_domain_signals(text: str) -> bool:
    domains = {
        "financial": ["salary", "revenue", "share", "funding", "fees"],
        "role": ["role", "responsibility", "reporting", "authority"],
        "timeline": ["start", "date", "timeline", "month", "week"],
        "ownership": ["who decides", "who is responsible", "ownership"],
    }
    lowered = text.lower()
    return any(keyword in lowered for values in domains.values() for keyword in values)


def _has_risk_scenario_structure(text: str, mission_context: dict[str, Any]) -> bool:
    lowered = text.lower()

    condition_markers = ["if ", "when ", "because", "due to", "unless", "without"]
    consequence_markers = [
        "slow",
        "delay",
        "fail",
        "risk",
        "loss",
        "move to another",
        "not",
        "unclear",
    ]

    actor_terms: list[str] = ["project", "team", "partner", "representative", "initiative"]
    for stakeholder in mission_context.get("stakeholders", []):
        if isinstance(stakeholder, dict):
            name = str(stakeholder.get("name", "")).strip().lower()
            if name:
                actor_terms.append(name)
    for institution in mission_context.get("institutions", []):
        if isinstance(institution, dict):
            n1 = str(institution.get("normalized_name", "")).strip().lower()
            n2 = str(institution.get("name", "")).strip().lower()
            if n1:
                actor_terms.append(n1)
            if n2:
                actor_terms.append(n2)
    actor_terms.extend(["aivancity", "gtu", "india", "gujarat", "vishal", "laurent", "coe"])

    has_actor = any(term and term in lowered for term in actor_terms)
    has_condition = any(marker in lowered for marker in condition_markers)
    has_consequence = any(marker in lowered for marker in consequence_markers)
    return has_actor and has_condition and has_consequence


def _complete_missing_detail_item(item: dict[str, Any]) -> dict[str, str] | None:
    topic = str(item.get("topic", "")).strip() or "Unspecified critical detail"
    missing_field = str(item.get("missing_field", "")).strip() or "unspecified"
    context = (
        str(item.get("context", "")).strip()
        or "Discussed in transcript but not clearly specified."
    )
    evidence = str(item.get("evidence", "")).strip()
    importance = str(item.get("importance", "")).strip().lower() or "medium"

    if not evidence:
        return None
    if importance not in {"high", "medium", "low"}:
        importance = "medium"
    if contains_bad_pattern(topic) or contains_bad_pattern(context):
        return None

    output: dict[str, str] = {
        "topic": topic,
        "missing_field": missing_field,
        "context": context,
        "importance": importance,
        "evidence": evidence,
    }
    existing_id = str(item.get("id", "")).strip()
    output["id"] = existing_id or _stable_missing_detail_id(output)
    domain = str(item.get("domain", "")).strip()
    if domain:
        output["domain"] = domain
    reason_code = str(item.get("reason_code", "")).strip()
    if reason_code:
        output["reason_code"] = reason_code
    return output


def filter_low_quality(data: dict[str, Any]) -> dict[str, Any]:
    cleaned: dict[str, Any] = {
        "decisions": [],
        "risks": [],
        "action_items": [],
        "suggestions": [],
        "key_points": [],
        "entities": _empty_entities(),
        "missing_details": [],
        "raw_entities": [],
    }

    enforced = enforce_schema(data)

    entity_inputs: list[Any] = []
    raw_entities = enforced.get("raw_entities", [])
    if isinstance(raw_entities, list):
        entity_inputs.extend(raw_entities)

    entities_obj = enforced.get("entities", _empty_entities())
    if isinstance(entities_obj, dict):
        for key in ["people", "organizations", "locations", "initiatives"]:
            values = entities_obj.get(key, [])
            if isinstance(values, list):
                entity_inputs.extend(values)

    cleaned["entities"] = normalize_entities(entity_inputs, MISSION_REGISTRY)
    print("[DEBUG] entity normalization applied")

    for key in ["decisions", "risks", "action_items", "suggestions", "key_points"]:
        values = enforced.get(key, [])
        if not isinstance(values, list):
            continue
        for item in values:
            if not isinstance(item, str):
                continue
            value = item.strip()
            if len(value) < 5:
                continue
            if contains_bad_pattern(value):
                continue
            if key == "risks":
                if is_too_generic(value):
                    continue
                if not _has_risk_scenario_structure(value, MISSION_REGISTRY):
                    continue
            cleaned[key].append(value)

    missing_values = enforced.get("missing_details", [])
    if isinstance(missing_values, list):
        for item in missing_values:
            if not isinstance(item, dict):
                continue
            completed = _complete_missing_detail_item(item)
            if completed is not None:
                cleaned["missing_details"].append(completed)

    cleaned["decisions"] = _dedupe_sorted(cleaned["decisions"])
    cleaned["risks"] = _dedupe_sorted(cleaned["risks"])
    cleaned["action_items"] = _dedupe_sorted(cleaned["action_items"])
    cleaned["suggestions"] = _dedupe_sorted(cleaned["suggestions"])
    cleaned["key_points"] = _dedupe_sorted(cleaned["key_points"])

    return enforce_schema(cleaned)


def _call_model(system_prompt: str, user_prompt: str, model: str = "gpt-5.3") -> str:
    try:
        response = _get_client().chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0,
        )
    except Exception as exc:
        if "model_not_found" in str(exc) or "`gpt-5.3` does not exist" in str(exc):
            response = _get_client().chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=0,
            )
        else:
            raise

    return response.choices[0].message.content or ""


def extract_chunk(chunk: str, model: str = "gpt-5.3") -> dict[str, Any]:
    user_prompt = USER_PROMPT_TEMPLATE.format(chunk=chunk)
    raw = _call_model(SYSTEM_PROMPT, user_prompt, model=model)

    parsed = safe_parse_json(raw)
    if parsed is None:
        print("[WARN] Failed to parse chunk")
        return enforce_schema({})

    return filter_low_quality(enforce_schema(parsed))


def _extract_backstop_array(
    chunks: list[str],
    category_key: str,
    override_instruction: str,
    model: str = "gpt-5.3",
) -> list[Any]:
    collected: list[Any] = []
    system_prompt = SYSTEM_PROMPT + "\n\n" + override_instruction

    for chunk in chunks:
        user_prompt = USER_PROMPT_TEMPLATE.format(chunk=chunk)
        raw = _call_model(system_prompt, user_prompt, model=model)
        parsed = safe_parse_json(raw)
        if not isinstance(parsed, dict):
            continue

        values = parsed.get(category_key, [])
        if not isinstance(values, list):
            continue

        if category_key == "missing_details":
            for item in values:
                if isinstance(item, dict):
                    completed = _complete_missing_detail_item(item)
                    if completed is not None:
                        collected.append(completed)
        else:
            for item in values:
                if isinstance(item, str) and item.strip() and not contains_bad_pattern(item):
                    collected.append(item.strip())

    return collected


def extract_with_retry(
    chunk: str,
    retries: int = 2,
    model: str = "gpt-5.3",
    require_decisions: bool = False,
    require_missing_details: bool = False,
) -> dict[str, Any]:
    last = enforce_schema({})
    for attempt in range(retries + 1):
        result = extract_chunk(chunk, model=model)
        last = result

        decisions_ok = (not require_decisions) or bool(result.get("decisions"))
        missing_ok = (not require_missing_details) or bool(result.get("missing_details"))

        if decisions_ok and missing_ok:
            return result

        if attempt < retries:
            print(
                f"[DEBUG] retry chunk={attempt + 1} decisions_ok={decisions_ok} missing_ok={missing_ok}"
            )

    return last


def merge_results(results: list[dict[str, Any]]) -> dict[str, Any]:
    final_text_sets: dict[str, set[str]] = {
        "decisions": set(),
        "risks": set(),
        "action_items": set(),
        "suggestions": set(),
        "key_points": set(),
    }
    final_entities_sets: dict[str, set[str]] = {
        "people": set(),
        "organizations": set(),
        "locations": set(),
        "initiatives": set(),
    }
    missing_details_set: set[str] = set()

    for r in results:
        data = filter_low_quality(enforce_schema(r))

        for key in final_text_sets:
            values = data.get(key, [])
            if isinstance(values, list):
                for item in values:
                    if isinstance(item, str) and item.strip():
                        final_text_sets[key].add(item.strip())

        entities = data.get("entities", _empty_entities())
        if isinstance(entities, dict):
            for bucket in final_entities_sets:
                values = entities.get(bucket, [])
                if isinstance(values, list):
                    for value in values:
                        if isinstance(value, str) and value.strip():
                            final_entities_sets[bucket].add(value.strip())

        missing_details = data.get("missing_details", [])
        if isinstance(missing_details, list):
            for item in missing_details:
                if isinstance(item, dict):
                    missing_details_set.add(
                        json.dumps(item, ensure_ascii=False, sort_keys=True)
                    )

    merged: dict[str, Any] = {
        key: sorted(values) for key, values in final_text_sets.items()
    }
    merged["entities"] = {
        key: sorted(values) for key, values in final_entities_sets.items()
    }

    missing_details: list[dict[str, str]] = []
    for item in sorted(missing_details_set):
        try:
            parsed = json.loads(item)
            if isinstance(parsed, dict):
                completed = _complete_missing_detail_item(parsed)
                if completed is not None:
                    missing_details.append(completed)
        except Exception:
            continue

    merged["missing_details"] = missing_details
    return enforce_schema(merged)


def _merge_text_arrays(base: list[str], extra: list[Any]) -> list[str]:
    merged = [item for item in base if isinstance(item, str)]
    for item in extra:
        if isinstance(item, str):
            candidate = item.strip()
            if candidate and not contains_bad_pattern(candidate):
                merged.append(candidate)
    return _dedupe_sorted(merged)


def _merge_missing_details(
    base: list[dict[str, str]], extra: list[Any]
) -> list[dict[str, str]]:
    combined: list[dict[str, str]] = []
    for item in base:
        if isinstance(item, dict):
            completed = _complete_missing_detail_item(item)
            if completed is not None:
                combined.append(completed)

    for item in extra:
        if isinstance(item, dict):
            completed = _complete_missing_detail_item(item)
            if completed is not None:
                combined.append(completed)

    dedup: dict[str, dict[str, str]] = {}
    for item in combined:
        row = dict(item)
        row["id"] = row.get("id", "") or _stable_missing_detail_id(row)
        key = row["id"]
        dedup[key] = row

    return [dedup[key] for key in sorted(dedup.keys())]


def split_sentences(text: str) -> list[str]:
    return [s.strip() for s in re.split(r"(?<=[.!?])\s+", text) if s.strip()]


def has_partial_specification(sentence: str) -> bool:
    s = sentence.lower()
    return any(x in s for x in ["%", "percent", "may", "or", "option", "either"])


def is_domain_discussed_with_uncertainty(sentence: str, triggers: list[str]) -> bool:
    s = sentence.lower()
    has_domain = any(t.lower() in s for t in triggers)
    has_uncertainty = any(m in s for m in UNRESOLVED_MARKERS)
    return has_domain and (has_uncertainty or has_partial_specification(s))


def domain_discussed_safely(transcript: str, triggers: list[str]) -> bool:
    for sentence in split_sentences(transcript):
        if is_domain_discussed_with_uncertainty(sentence, triggers):
            return True
    return False


def find_strict_evidence(transcript: str, triggers: list[str]) -> str:
    for sentence in split_sentences(transcript):
        if is_domain_discussed_with_uncertainty(sentence, triggers):
            return sentence
    return ""


def _detect_domain_from_missing_detail(item: dict[str, Any]) -> str:
    searchable = " ".join(
        [
            str(item.get("topic", "")),
            str(item.get("missing_field", "")),
            str(item.get("context", "")),
        ]
    ).lower()
    for domain, cfg in DOMAIN_REGISTRY.items():
        candidates = cfg.get("missing_field_candidates", [])
        if any(candidate.lower() in searchable for candidate in candidates):
            return domain
        if any(trigger.lower() in searchable for trigger in cfg.get("trigger_keywords", [])):
            return domain
    return ""


def detect_domain_coverage(
    transcript: str, missing_details: list[dict[str, Any]]
) -> dict[str, dict[str, Any]]:
    coverage: dict[str, dict[str, Any]] = {}

    for domain, cfg in DOMAIN_REGISTRY.items():
        triggers = [t.lower() for t in cfg.get("trigger_keywords", [])]
        discussed = domain_discussed_safely(transcript, triggers)
        represented_ids: list[str] = []
        for detail in missing_details:
            if not isinstance(detail, dict):
                continue
            detail_domain = _detect_domain_from_missing_detail(detail)
            if detail_domain == domain:
                represented_ids.append(str(detail.get("id", "")).strip())
        coverage[domain] = {
            "discussed": discussed,
            "represented": bool(represented_ids),
            "represented_ids": [rid for rid in represented_ids if rid],
            "config": cfg,
        }

    return coverage


def synthesize_missing_details_from_coverage(
    transcript: str, coverage: dict[str, dict[str, Any]]
) -> list[dict[str, str]]:
    synthesized: list[dict[str, str]] = []
    for domain, info in coverage.items():
        discussed = bool(info.get("discussed"))
        represented = bool(info.get("represented"))
        cfg = info.get("config", {})
        if not discussed or represented or not isinstance(cfg, dict):
            continue

        triggers = cfg.get("trigger_keywords", [])
        if not isinstance(triggers, list):
            continue
        if not domain_discussed_safely(transcript, triggers):
            continue

        evidence = find_strict_evidence(transcript, triggers)
        if not evidence:
            continue

        missing_candidates = cfg.get("missing_field_candidates", [])
        missing_field = (
            str(missing_candidates[0]).strip()
            if isinstance(missing_candidates, list) and missing_candidates
            else f"{domain} detail"
        )
        severity = str(cfg.get("severity", "medium")).strip().lower()
        importance = "high" if severity == "high" else "medium"

        item = {
            "id": "",
            "topic": f"{domain.title()} detail for CoE execution",
            "missing_field": missing_field,
            "context": "Discussed in transcript but not clearly specified.",
            "importance": importance,
            "evidence": evidence,
            "domain": domain,
            "reason_code": str(cfg.get("reason_code", f"{domain}_undefined")).strip(),
        }
        completed = _complete_missing_detail_item(item)
        if completed is None:
            continue
        completed["domain"] = domain
        completed["reason_code"] = item["reason_code"]
        completed["id"] = _stable_missing_detail_id(completed)
        synthesized.append(completed)

    return synthesized


def infer_risk_text(missing_detail: dict[str, Any]) -> str:
    missing_field = str(missing_detail.get("missing_field", "")).strip()
    topic = str(missing_detail.get("topic", "")).strip() or "Execution topic"
    if missing_field:
        return (
            f"Unresolved {missing_field} for {topic.lower()} can delay execution and weaken accountability."
        )
    return "Unresolved execution-critical detail can delay execution and weaken accountability."


def build_inferred_risks(
    missing_details: list[dict[str, Any]], transcript: str
) -> list[dict[str, str]]:
    risks: list[dict[str, str]] = []
    seen_ids: set[str] = set()
    for item in missing_details:
        if not isinstance(item, dict):
            continue
        item_id = str(item.get("id", "")).strip() or _stable_missing_detail_id(item)
        if item_id in seen_ids:
            continue
        evidence = str(item.get("evidence", "")).strip()
        if not evidence or evidence not in transcript:
            continue

        domain = _detect_domain_from_missing_detail(item)
        cfg = DOMAIN_REGISTRY.get(domain, {})
        severity = str(cfg.get("severity", "medium")).strip().lower()
        if severity not in {"high", "medium", "low"}:
            severity = "medium"

        rule_id = f"phase08_1_{domain or 'general'}_inferred_risk_v1"
        risks.append(
            {
                "text": infer_risk_text(item),
                "severity": severity,
                "source_missing_detail_id": item_id,
                "rule_id": rule_id,
                "evidence": evidence,
            }
        )
        seen_ids.add(item_id)

    return risks


def build_critical_gaps(missing_details: list[dict[str, Any]]) -> list[dict[str, Any]]:
    critical: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in missing_details:
        if not isinstance(item, dict):
            continue
        item_id = str(item.get("id", "")).strip() or _stable_missing_detail_id(item)
        domain = _detect_domain_from_missing_detail(item)
        cfg = DOMAIN_REGISTRY.get(domain, {})
        high_importance = str(item.get("importance", "")).strip().lower() == "high"
        is_critical = bool(cfg.get("critical", False))
        if not (high_importance or is_critical):
            continue
        if item_id in seen:
            continue
        row = dict(item)
        row["id"] = item_id
        row["reason_code"] = str(
            row.get("reason_code", "") or cfg.get("reason_code", f"{domain or 'general'}_undefined")
        ).strip()
        critical.append(row)
        seen.add(item_id)

    return critical


def _fallback_decisions_from_transcript(text: str) -> list[str]:
    markers = ["we will", "we'll", "next step", "you will", "we need", "we meet"]
    excluded = ["yes, no, maybe", "etc", "between french", "because we need to go deeper"]
    decisions: list[str] = []
    for sentence in split_sentences(text):
        lowered = sentence.lower()
        if not any(marker in lowered for marker in markers):
            continue
        if any(ex in lowered for ex in excluded):
            continue
        if len(sentence) < 25:
            continue
        cleaned = sentence.lstrip(", ").strip()
        if cleaned.lower().startswith(("and ", "because ", "between ")):
            continue
        decisions.append(cleaned)
    return _dedupe_sorted(decisions)[:8]


def _fallback_missing_details_from_transcript(text: str) -> list[dict[str, str]]:
    candidates = [
        {
            "topic": "Revenue-sharing model for CoE",
            "missing_field": "percentage",
            "importance": "high",
            "keywords": ["x percent", "revenue", "tuition fees"],
        },
        {
            "topic": "Role governance for implementation lead",
            "missing_field": "reporting structure",
            "importance": "high",
            "keywords": ["responsible", "authority", "reporting"],
        },
        {
            "topic": "Execution timeline",
            "missing_field": "confirmed start date",
            "importance": "high",
            "keywords": ["first of may", "first june", "week of may", "tuesday"],
        },
        {
            "topic": "Financial support model",
            "missing_field": "funding terms",
            "importance": "medium",
            "keywords": ["finance a phd", "funding", "fees"],
        },
    ]

    results: list[dict[str, str]] = []
    sentences = split_sentences(text)
    for candidate in candidates:
        evidence = ""
        for sentence in sentences:
            lowered = sentence.lower()
            if any(keyword in lowered for keyword in candidate["keywords"]):
                evidence = sentence
                break
        if not evidence:
            continue
        item = {
            "topic": candidate["topic"],
            "missing_field": candidate["missing_field"],
            "context": "Discussed in transcript but not explicitly finalized.",
            "importance": candidate["importance"],
            "evidence": evidence,
        }
        completed = _complete_missing_detail_item(item)
        if completed is not None:
            results.append(completed)

    return _merge_missing_details([], results)


def _fallback_risks_from_transcript(text: str) -> list[str]:
    risks: list[str] = []
    for sentence in split_sentences(text):
        lowered = sentence.lower()
        if "if we're not there" in lowered and "slow" in lowered:
            risks.append(sentence.strip())
            continue
        if "if in three years" in lowered and "another partner" in lowered:
            risks.append(sentence.strip())
            continue
        if (
            ("if " in lowered or "when " in lowered)
            and ("slow" in lowered or "not" in lowered or "another partner" in lowered)
            and ("project" in lowered or "partner" in lowered or "we" in lowered)
        ):
            risks.append(sentence.strip())
    return _dedupe_sorted(risks)[:5]


def enforce_mission_context_entities(
    entities: dict[str, Any], transcript: str, mission_context: dict[str, Any]
) -> dict[str, list[str]]:
    normalized = _empty_entities()
    if isinstance(entities, dict):
        for key in normalized:
            values = entities.get(key, [])
            if isinstance(values, list):
                normalized[key] = [v for v in values if isinstance(v, str) and v.strip()]

    lowered = transcript.lower()

    if "laurent" in lowered and "Laurent Chebassier" not in normalized["people"]:
        normalized["people"].append("Laurent Chebassier")

    if "aivancity" in lowered and "Aivancity" not in normalized["organizations"]:
        normalized["organizations"].append("Aivancity")

    if (
        "gtu" in lowered or "gujarat" in lowered
    ) and "GTU" not in normalized["organizations"]:
        normalized["organizations"].append("GTU")

    for stakeholder in mission_context.get("stakeholders", []):
        if isinstance(stakeholder, dict):
            name = str(stakeholder.get("name", "")).strip()
            if name and name.lower().split()[0] in lowered and name not in normalized["people"]:
                normalized["people"].append(name)

    for institution in mission_context.get("institutions", []):
        if isinstance(institution, dict):
            canonical = str(institution.get("normalized_name", "")).strip()
            raw_name = str(institution.get("name", "")).strip().lower()
            if canonical and canonical.lower() in lowered and canonical not in normalized["organizations"]:
                normalized["organizations"].append(canonical)
            elif canonical and raw_name and raw_name in lowered and canonical not in normalized["organizations"]:
                normalized["organizations"].append(canonical)

    for key in normalized:
        normalized[key] = _dedupe_sorted(normalized[key])

    return normalized


def _validate_phase081_integrity(payload: dict[str, Any], transcript: str) -> None:
    missing_details = payload.get("missing_details", [])
    if not isinstance(missing_details, list):
        raise RuntimeError("Phase 08.1 validation failed: missing_details must be an array")

    missing_ids: set[str] = set()
    for item in missing_details:
        if not isinstance(item, dict):
            continue
        item_id = str(item.get("id", "")).strip()
        if item_id:
            missing_ids.add(item_id)

    inferred_risks = payload.get("inferred_risks", [])
    if not isinstance(inferred_risks, list):
        raise RuntimeError("Phase 08.1 validation failed: inferred_risks must be an array")

    for risk in inferred_risks:
        if not isinstance(risk, dict):
            raise RuntimeError("Phase 08.1 validation failed: inferred_risk item must be object")
        evidence = str(risk.get("evidence", "")).strip()
        source_id = str(risk.get("source_missing_detail_id", "")).strip()
        if not evidence or evidence not in transcript:
            raise RuntimeError(
                "Phase 08.1 validation failed: inferred_risk evidence is not verbatim transcript substring"
            )
        if not source_id or source_id not in missing_ids:
            raise RuntimeError(
                "Phase 08.1 validation failed: inferred_risk source_missing_detail_id is invalid"
            )

    critical_gaps = payload.get("critical_gaps", [])
    if not isinstance(critical_gaps, list):
        raise RuntimeError("Phase 08.1 validation failed: critical_gaps must be an array")

    for gap in critical_gaps:
        if not isinstance(gap, dict):
            raise RuntimeError("Phase 08.1 validation failed: critical_gap item must be object")
        gap_id = str(gap.get("id", "")).strip()
        reason_code = str(gap.get("reason_code", "")).strip()
        if not gap_id or gap_id not in missing_ids:
            raise RuntimeError(
                "Phase 08.1 validation failed: critical_gap id must map to existing missing_detail"
            )
        if not reason_code:
            raise RuntimeError("Phase 08.1 validation failed: critical_gap reason_code is required")


def run_intelligence(session_path: str, max_tokens: int = 1200) -> dict[str, Any]:
    session_dir = Path(session_path)
    clean_path = session_dir / "transcript_clean.txt"

    if not clean_path.exists():
        raise FileNotFoundError(f"transcript_clean.txt not found: {clean_path}")

    text = clean_path.read_text(encoding="utf-8")
    if not text.strip():
        raise ValueError("transcript_clean.txt is empty")

    commitment_detected = has_commitment_signals(text)
    missing_domain_detected = has_missing_domain_signals(text)
    print(f"[DEBUG] commitment detected: {commitment_detected}")
    print(f"[DEBUG] missing domain detected: {missing_domain_detected}")

    chunks = chunk_text(text, max_tokens=max_tokens)

    results: list[dict[str, Any]] = []
    for chunk in chunks:
        chunk_commitment = has_commitment_signals(chunk)
        chunk_missing_domain = has_missing_domain_signals(chunk)
        results.append(
            extract_with_retry(
                chunk,
                require_decisions=chunk_commitment,
                require_missing_details=chunk_missing_domain,
            )
        )

    final = merge_results(results)

    if commitment_detected and not final.get("decisions"):
        print("[DEBUG] decision retry triggered")
        decision_override = """
STRICT OVERRIDE:
Extract ONLY decisions based on commitment, agreement, direction, or responsibility.
Return decisions array only.
"""
        retry_decisions = _extract_backstop_array(chunks, "decisions", decision_override)
        final["decisions"] = _merge_text_arrays(final.get("decisions", []), retry_decisions)
        if not final["decisions"]:
            print("[DEBUG] decision fallback triggered")
            final["decisions"] = _merge_text_arrays(
                final.get("decisions", []), _fallback_decisions_from_transcript(text)
            )

    if not final.get("risks"):
        print("[DEBUG] risk fallback triggered")
        final["risks"] = _merge_text_arrays(final.get("risks", []), _fallback_risks_from_transcript(text))

    if missing_domain_detected and not final.get("missing_details"):
        print("[DEBUG] missing_details retry triggered")
        missing_override = """
STRICT OVERRIDE:
Extract ONLY missing critical details:
- salary
- funding
- revenue share
- role clarity
- reporting
- ownership
- timeline

Return missing_details only.
"""
        retry_missing = _extract_backstop_array(
            chunks, "missing_details", missing_override
        )
        final["missing_details"] = _merge_missing_details(
            final.get("missing_details", []), retry_missing
        )
        if not final["missing_details"]:
            print("[DEBUG] missing_details fallback triggered")
            final["missing_details"] = _merge_missing_details(
                final.get("missing_details", []),
                _fallback_missing_details_from_transcript(text),
            )

    # Phase 08.1 deterministic domain coverage hardening.
    final = enforce_schema(final)
    coverage = detect_domain_coverage(text, final.get("missing_details", []))
    synthesized_missing = synthesize_missing_details_from_coverage(text, coverage)
    if synthesized_missing:
        final["missing_details"] = _merge_missing_details(
            final.get("missing_details", []), synthesized_missing
        )

    final = enforce_schema(final)
    final["inferred_risks"] = build_inferred_risks(final.get("missing_details", []), text)
    final["critical_gaps"] = build_critical_gaps(final.get("missing_details", []))

    final["entities"] = enforce_mission_context_entities(
        final.get("entities", _empty_entities()), text, MISSION_REGISTRY
    )

    out_payload = enforce_schema(final)
    _validate_phase081_integrity(out_payload, text)
    out_payload.pop("raw_entities", None)
    out_path = session_dir / "intelligence.json"
    out_path.write_text(json.dumps(out_payload, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"[OK] Intelligence extracted: {out_path}")
    return out_payload
