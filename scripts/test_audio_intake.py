from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.services.audio import AudioIntakeService


def main() -> None:
    parser = argparse.ArgumentParser(description="Smoke test for audio intake service.")
    parser.add_argument("source", help="Path to source audio file (.m4a, .mp3, .wav)")
    args = parser.parse_args()

    service = AudioIntakeService()
    result = service.intake_audio(args.source)

    print("Audio intake completed successfully.")
    print(f"meeting_id={result.meeting_id}")
    print(f"meeting_dir={result.meeting_dir}")
    print(f"original_audio_path={result.original_audio_path}")
    print(f"intake_metadata_path={result.intake_metadata_path}")
    print(f"status={result.status}")


if __name__ == "__main__":
    main()
