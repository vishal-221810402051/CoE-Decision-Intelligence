import json
import os
from pathlib import Path
import re

from openai import OpenAI

from app.config import (
    CLEANUP_CHUNK_SIZE,
    CLEANUP_METADATA,
    CLEANUP_MIN_RATIO,
    CLEANUP_MODEL,
    TRANSCRIPT_CLEAN,
    TRANSCRIPT_RAW,
    TRANSFORMATION_MODE,
)
from app.models.cleanup import CleanupResult

CLEANUP_PROMPT = """
You are a transcript cleanup engine.

STRICT RULES:
- Preserve meaning exactly.
- Do NOT summarize.
- Do NOT add information.
- Do NOT interpret.
- Do NOT change intent.

STRUCTURE RULES:
- Preserve speaker labels exactly if present.
- Do NOT merge speaker turns.
- Do NOT infer speakers.

DISFLUENCY RULES:
- You may remove: "uh", "um", "you know"
- You MUST NOT remove: "maybe", "probably", "I think", "perhaps"

OUTPUT RULES:
- Return ONLY cleaned transcript text
- No headings
- No bullets
- No commentary
- No markdown

TEXT:
==========
{input_text}
==========
"""


def chunk_text(text, size):
    return [text[i:i + size] for i in range(0, len(text), size)]


def repair_cleanup_artifacts(text: str) -> str:
    # join words broken by newline in the middle of alphabetic tokens
    text = re.sub(r'(?<=[A-Za-z])\n(?=[a-z])', '', text)

    # convert line-wrap newline between normal words into a space
    text = re.sub(r'(?<=[a-z])\n(?=[A-Z][a-z])', ' ', text)
    text = re.sub(r'(?<=[a-z])\n(?=[a-z])', ' ', text)

    # normalize spaces but preserve paragraph breaks
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)

    return text.strip()


class TranscriptCleanupService:

    @staticmethod
    def cleanup_meeting(meeting_id: str):
        meeting_path = Path("data") / "processed" / meeting_id
        input_path = meeting_path / "transcript" / TRANSCRIPT_RAW
        output_path = meeting_path / "transcript" / TRANSCRIPT_CLEAN
        metadata_path = meeting_path / "metadata" / CLEANUP_METADATA

        if not meeting_path.exists():
            raise FileNotFoundError(f"Meeting folder not found: {meeting_id}")
        if not input_path.exists():
            raise FileNotFoundError(f"Raw transcript not found: {input_path}")
        if not os.getenv("OPENAI_API_KEY"):
            raise RuntimeError("OPENAI_API_KEY is missing in environment")

        with open(input_path, "r", encoding="utf-8") as f:
            raw_text = f.read()

        client = OpenAI()
        chunks = chunk_text(raw_text, CLEANUP_CHUNK_SIZE)
        cleaned_chunks = []

        try:
            for chunk in chunks:
                response = client.chat.completions.create(
                    model=CLEANUP_MODEL,
                    messages=[
                        {"role": "user", "content": CLEANUP_PROMPT.format(input_text=chunk)}
                    ],
                    temperature=0
                )
                cleaned_chunks.append(response.choices[0].message.content or "")

            cleaned_text = "\n".join(cleaned_chunks)
            cleaned_text = repair_cleanup_artifacts(cleaned_text)

            if "Here is" in cleaned_text or "Output:" in cleaned_text:
                raise RuntimeError("Sanitization failed")

            if len(cleaned_text) < CLEANUP_MIN_RATIO * len(raw_text):
                raise RuntimeError("Integrity check failed")

            if cleaned_text == "":
                raise RuntimeError("Empty cleanup output")

            with open(output_path, "w", encoding="utf-8") as f:
                f.write(cleaned_text)

            metadata = {
                "meeting_id": meeting_id,
                "input_path": str(input_path),
                "output_path": str(output_path),
                "model": CLEANUP_MODEL,
                "chunked": len(chunks) > 1,
                "chunk_count": len(chunks),
                "transformation_mode": TRANSFORMATION_MODE,
                "status": "cleanup_completed",
            }

            with open(metadata_path, "w", encoding="utf-8") as f:
                json.dump(metadata, f, indent=2)

            return CleanupResult(
                meeting_id=meeting_id,
                input_path=str(input_path),
                output_path=str(output_path),
                metadata_path=str(metadata_path),
                model=CLEANUP_MODEL,
                chunked=len(chunks) > 1,
                chunk_count=len(chunks),
                transformation_mode=TRANSFORMATION_MODE,
                status="cleanup_completed",
            )
        except Exception:
            if output_path.exists():
                output_path.unlink()
            if metadata_path.exists():
                metadata_path.unlink()
            raise
