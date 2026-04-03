# CoE Decision Intelligence

Meeting intelligence system for CoE mission analysis.

## Setup

```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
streamlit run app/main.py
```

## Phase 1 Smoke Test

Run audio intake service directly:

```bash
python scripts/test_audio_intake.py "C:\path\to\meeting.m4a"
```

Expected result:
- meeting folder created under `data/processed/`
- original file copied to `source/`
- `metadata/intake.json` created

## Validation Steps

## Test A - Valid `.m4a`
### Input
A real phone-exported `.m4a`

### Command
```powershell
python scripts/test_audio_intake.py "C:\path\to\meeting.m4a"
```

Expected output:
- `Audio intake completed successfully.`
- `meeting_id=...`
- `status=intake_completed`

Filesystem should contain:
- `data/processed/<meeting_id>/source/original.m4a`
- `data/processed/<meeting_id>/metadata/intake.json`
- `data/processed/<meeting_id>/logs/`

Pass criteria:
- all paths exist and metadata is correct

## Test B - Valid `.mp3`
### Command
```powershell
python scripts/test_audio_intake.py "C:\path\to\meeting.mp3"
```

Expected:
- stored file is `original.mp3`

## Test C - Valid `.wav`
### Command
```powershell
python scripts/test_audio_intake.py "C:\path\to\meeting.wav"
```

Expected:
- stored file is `original.wav`

## Test D - Invalid Extension
### Input
Example: `.txt`

### Command
```powershell
python scripts/test_audio_intake.py "C:\path\to\notes.txt"
```

Expected:
- clear failure message containing unsupported format

Pass criteria:
- no meeting folder created

## Test E - Missing File
### Command
```powershell
python scripts/test_audio_intake.py "C:\path\to\missing.m4a"
```

Expected:
- clear `FileNotFoundError` path message

Pass criteria:
- no partial intake folder created

## Optional Automated Test

Install `pytest` only if you want it now. Since Phase 0 intentionally kept dependencies minimal, direct smoke validation is enough for this phase.

If you want `pytest`, add it to `requirements.txt`, install, and run:

```bash
pytest tests/test_audio_intake_smoke.py -q
```

## Common Errors and Fixes

`ModuleNotFoundError: No module named 'app'`
- run from repo root:

```powershell
cd C:\Users\Vishal\OneDrive\Desktop\Projects\CoE-Decision-Intelligence
python scripts/test_audio_intake.py "C:\path\to\meeting.m4a"
```

`Unsupported audio format`
- accepted extensions: `.m4a`, `.mp3`, `.wav`

`Source audio file not found`
- use an absolute Windows path in quotes

`FileExistsError`
- unlikely due to UUID suffix; rerun once if it happens

## Phase 2 Audio Normalization

Normalize Phase 01 source audio into:
- `data/processed/<meeting_id>/normalized/audio.wav`
- `data/processed/<meeting_id>/metadata/normalization.json`

Normalization spec:
- mono
- 16000 Hz
- PCM 16-bit (`pcm_s16le`)

Overwrite policy:
- `replace_existing_output` (existing `normalized/audio.wav` is replaced)

Run:

```powershell
python scripts/test_audio_normalization.py "<meeting_id>"
```

Validate output audio format:

```powershell
ffprobe -v error -select_streams a:0 -show_entries stream=codec_name,sample_rate,channels -of default=nw=1:nk=1 "data/processed/<meeting_id>/normalized/audio.wav"
```

Expected ffprobe values:
- `pcm_s16le`
- `16000`
- `1`

## Phase 6 Decision Intelligence Extraction

Convert cleaned transcript text into structured, transcript-grounded intelligence.

Input:
- `data/processed/<meeting_id>/transcript/transcript_clean.txt`

Outputs:
- `data/processed/<meeting_id>/intelligence/intelligence.json`
- `data/processed/<meeting_id>/metadata/intelligence_metadata.json`

Run:

```powershell
python scripts/test_intelligence_extraction.py "<meeting_id>"
```

Validation:
- strict schema output
- verbatim evidence snippets only
- deterministic multi-pass extraction
- fail-closed on invalid JSON, schema, evidence, or summary

## Phase 3 Executive Intelligence

Generate executive-grade interpretation from transcript and prior intelligence layers.

Inputs:
- `data/processed/<meeting_id>/transcript/transcript_raw.txt`
- `data/processed/<meeting_id>/transcript/transcript_clean.txt`
- `data/processed/<meeting_id>/transcript/intelligence.json`
- `reports/decision_intelligence.json` (or meeting-local decision file when present)
- `data/context/mission_registry.json`

Outputs:
- `data/processed/<meeting_id>/executive/executive_intelligence.json`
- `data/processed/<meeting_id>/metadata/executive_metadata.json`

Run:

```powershell
python scripts/test_executive_intelligence.py "<meeting_id>"
```
