# Benchmarks and Golden Snapshots

## Purpose
Benchmark regression validates repeated-run stability and drift behavior for professional meeting intelligence artifacts.

## Manifest
Edit `benchmarks/manifest.json` and add meeting entries:

```json
{
  "meeting_id": "MTG-...",
  "category": "strategy-heavy",
  "difficulty": "medium",
  "enabled": true
}
```

## Categories
- `strategy-heavy`
- `execution-heavy`
- `governance-heavy`
- `finance-revenue-model`
- `timeline-followup`
- `noisy-professional`

Target benchmark pack: minimum 10 meetings.

## Golden Snapshot Policy
Golden files are normalized snapshots, not raw artifacts:

`benchmarks/golden/<meeting_id>/`
- `intelligence.normalized.json`
- `executive.normalized.json`
- `decision.normalized.json`

Golden files are not auto-overwritten by standard runs.
Update golden baselines only through explicit manual action during controlled review.

## Drift Rules
- Critical drift: hard fail
- Soft drift: warning only

Critical/soft definitions are documented in:
`docs/phase-09.3-determinism-and-regression.md`

