# Phase 09.3 - Determinism and Regression

## Objective
Harden repeated-run reliability for professional meetings by enforcing deterministic output behavior and automated drift detection.

## Scope
- Deterministic output tightening in Executive and Decision layers
- Repeated-run drift checks for canonical artifacts
- Benchmark regression suite with golden normalized snapshots

## Artifacts Compared
- `data/processed/<meeting_id>/intelligence/intelligence.json`
- `data/processed/<meeting_id>/executive/executive_intelligence.json`
- `data/processed/<meeting_id>/decision/decision_intelligence_v2.json`

## Critical Drift (Hard Fail)
- Owner drift for the same decision
- Decision state drift for the same decision
- Evidence drift (missing/changed for operational records)
- Dependency status/blocking drift
- Missing dependency reason
- Blocked/open-high consistency violations
- Schema or contract validation failures

## Soft Drift (Warning)
- Executive summary wording changes
- Strategic objective wording changes
- Recommended question wording changes when operational meaning remains stable
- Commitment text wording changes when actor/type/status remains unchanged
- Decision record count drift by +/-1

## Repeated-Run Protocol
1. Run Executive + Decision services `N` times (`REGRESSION_REPEAT_RUNS`)
2. Normalize artifacts for each run
3. Compare run-1 baseline to run-2..run-N
4. Emit report:
   - `data/processed/<meeting_id>/regression/repeat_run_report.json`
5. Emit normalized snapshots:
   - `data/processed/<meeting_id>/regression/normalized/run_XX/*.normalized.json`

## Benchmark Protocol
1. Define enabled meetings in `benchmarks/manifest.json`
2. Run suite runner
3. Compare normalized current artifacts against:
   - `benchmarks/golden/<meeting_id>/*.normalized.json`
4. Emit suite report:
   - `benchmarks/reports/latest_regression_report.json`

## Acceptance Thresholds
- 100% schema pass
- 100% contract pass
- 100% evidence validity
- 0 critical drift on repeated runs
- >=95% stable decision-record count
- >=95% stable owner resolution
- 100% blocked/open-high rule consistency

Soft narrative drift is allowed only if operational fields remain stable.

