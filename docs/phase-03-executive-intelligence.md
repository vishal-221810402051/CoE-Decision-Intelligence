# Phase 03 - Executive Intelligence

## Objective
Generate executive-grade interpretation from:
- transcript_raw.txt
- transcript_clean.txt
- intelligence.json
- decision_intelligence.json
- mission_registry.json

## Outputs
- executive/executive_intelligence.json
- metadata/executive_metadata.json

## Grounding Rules
- Use mission_registry.json as the single source of truth for names, roles, institutions, and mission semantics
- Normalize aliases deterministically
- Do not invent compensation, authority, ownership, or legal specifics

## Validation
- Schema valid
- Confidence fields present
- Registry-grounded names normalized
- High execution risk triggers warning if clarity remains undefined
