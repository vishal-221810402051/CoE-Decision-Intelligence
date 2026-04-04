# Phase 06 - Decision Intelligence Extraction

## Objective
Convert cleaned transcript text into deterministic, transcript-grounded intelligence JSON.

## Input
`data/processed/<meeting_id>/transcript/transcript_clean.txt`

## Outputs
- `data/processed/<meeting_id>/intelligence/intelligence.json`
- `data/processed/<meeting_id>/metadata/intelligence_metadata.json`

Canonical intelligence contract path:
- `data/processed/<meeting_id>/intelligence/intelligence.json`

## Extraction Flow
- Pass A: chunk-level extraction per category
- Pass B: global consolidation and summary generation from extracted items only

## Chunking
- paragraph-aware
- chunk size: 6000 chars
- overlap: 300 chars
- stable ordering

## Validation Gates
- strict schema validation
- evidence must be verbatim in transcript
- all top-level keys required
- summary must be non-empty
- fail closed on any validation failure

## Retry Policy
- Pass A retries: 2
- Pass B retries: 2
- backoff seconds: 2, 5

## Run
```powershell
python scripts/test_intelligence_extraction.py "<meeting_id>"
```

## Expected
- `intelligence/intelligence.json` created
- `metadata/intelligence_metadata.json` created
- metadata contains model, prompt version, chunk stats, retries used, status
