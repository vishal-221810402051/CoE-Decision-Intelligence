# Phase 09 - Decision Intelligence v2

## Objective
Convert existing meeting artifacts into an operational decision ledger for execution tracking, ownership, blockers, and follow-up.

## Inputs
- `transcript_raw.txt`
- `transcript_clean.txt`
- `intelligence/intelligence.json` (canonical Phase 06 artifact)
- `executive/executive_intelligence.json`
- `mission_registry.json`
- optional: `reports/decision_intelligence.json` (backward compatibility)

## Outputs
- `decision/decision_intelligence_v2.json`
- `metadata/decision_v2_metadata.json`

## Decision States
- `confirmed`
- `tentative`
- `pending`
- `blocked`

## Commitment Types
- `explicit_commitment`
- `implied_commitment`
- `requested_commitment`
- `unresolved_commitment`

## Ownership Logic
- Derive deterministic `primary_owner` from owners list.
- If no owner exists, enforce `missing_owner` plus high-criticality gap question.

## Dependency Logic
- Supported dependencies:
  - `governance_dependency`
  - `authority_dependency`
  - `funding_dependency`
  - `timeline_dependency`
  - `partner_dependency`
- Open + high blocker forces decision state to `blocked`.

## Validation Rules
- Top-level schema keys must exist.
- All enums must be valid.
- Each decision record must have non-empty verbatim evidence from `transcript_clean.txt`.
- `confirmed` requires explicit commitment.
- `blocked` requires at least one open high dependency.
- Missing owner requires `missing_owner` marker and owner gap.
