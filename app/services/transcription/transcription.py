import os
from datetime import datetime
from openai import OpenAI

from app.config import (
    TRANSCRIPT_DIR_NAME,
    TRANSCRIPT_RAW_FILE_NAME,
    TRANSCRIPTION_METADATA_FILE_NAME,
    TRANSCRIPTION_MODEL
)

BASE_PROCESSED_DIR = "data/processed"


class TranscriptionService:

    def __init__(self):
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise RuntimeError("OPENAI_API_KEY is missing in environment")
        self.client = OpenAI(api_key=api_key)

    def transcribe_meeting(self, meeting_id: str):
        meeting_path = os.path.join(BASE_PROCESSED_DIR, meeting_id)

        if not os.path.exists(meeting_path):
            raise FileNotFoundError(f"Meeting folder not found: {meeting_id}")

        audio_path = os.path.join(meeting_path, "normalized", "audio.wav")

        if not os.path.exists(audio_path):
            raise FileNotFoundError("Normalized audio not found")

        if not os.path.isfile(audio_path):
            raise ValueError("Normalized audio path is not a file")

        transcript_dir = os.path.join(meeting_path, TRANSCRIPT_DIR_NAME)
        os.makedirs(transcript_dir, exist_ok=True)

        transcript_file_path = os.path.join(transcript_dir, TRANSCRIPT_RAW_FILE_NAME)
        metadata_path = os.path.join(
            meeting_path, "metadata", TRANSCRIPTION_METADATA_FILE_NAME
        )

        started_at = datetime.utcnow().isoformat()

        try:
            with open(audio_path, "rb") as audio_file:
                response = self.client.audio.transcriptions.create(
                    model=TRANSCRIPTION_MODEL,
                    file=audio_file
                )

            transcript_text = response.text

            if not transcript_text or transcript_text.strip() == "":
                raise RuntimeError("Empty transcription received")

            # Write transcript
            with open(transcript_file_path, "w", encoding="utf-8") as f:
                f.write(transcript_text)

            completed_at = datetime.utcnow().isoformat()

            metadata = {
                "meeting_id": meeting_id,
                "model": TRANSCRIPTION_MODEL,
                "input_path": audio_path,
                "output_text_path": transcript_file_path,
                "started_at": started_at,
                "completed_at": completed_at,
                "status": "transcription_completed"
            }

            with open(metadata_path, "w", encoding="utf-8") as f:
                import json
                json.dump(metadata, f, indent=4)

            return transcript_file_path

        except Exception as e:
            # cleanup if partial file exists
            if os.path.exists(transcript_file_path):
                os.remove(transcript_file_path)

            raise RuntimeError(f"Transcription failed: {str(e)}")
