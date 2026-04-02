import re
from pathlib import Path

FILLER_WORDS = [
    "um",
    "uh",
    "you know",
    "like",
    "basically",
    "actually",
    "so",
    "well",
    "okay",
    "right",
]


def remove_fillers(text: str) -> str:
    pattern = r"\b(" + "|".join(FILLER_WORDS) + r")\b"
    return re.sub(pattern, "", text, flags=re.IGNORECASE)


def remove_noise(text: str) -> str:
    # Remove [noise], (laughs), etc.
    return re.sub(r"\[.*?\]|\(.*?\)", "", text)


def normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def fix_repetitions(text: str) -> str:
    # Remove repeated words: "we we need" -> "we need"
    return re.sub(r"\b(\w+)( \1\b)+", r"\1", text, flags=re.IGNORECASE)


def basic_punctuation(text: str) -> str:
    # Step 1: Normalize encoding artifacts
    text = text.replace("\u00e2\u20ac\u00a6", "...")
    text = text.replace("\u2026", "...")

    # Step 2: Split ONLY on strong pauses (not commas)
    sentences = re.split(r"[.!?]+", text)

    clean_sentences = []
    for s in sentences:
        s = s.strip()

        if len(s) < 5:
            continue

        # Preserve uppercase abbreviations
        s = s[0].upper() + s[1:]

        clean_sentences.append(s)

    if not clean_sentences:
        return ""
    return ". ".join(clean_sentences) + "."


def fix_acronyms(text: str) -> str:
    replacements = {
        "ai": "AI",
        "phd": "PhD",
        "gtu": "GTU",
        "ivancity": "Aivancity",
    }

    for k, v in replacements.items():
        text = re.sub(rf"\b{re.escape(k)}\b", v, text, flags=re.IGNORECASE)

    return text


def clean_transcript(raw_text: str) -> str:
    text = raw_text

    text = remove_noise(text)
    text = remove_fillers(text)
    text = fix_repetitions(text)
    text = normalize_whitespace(text)
    text = fix_acronyms(text)
    text = basic_punctuation(text)

    return text


def process_transcript(session_path: str) -> None:
    session_dir = Path(session_path)

    raw_file = session_dir / "transcript_raw.txt"
    clean_file = session_dir / "transcript_clean.txt"

    if not raw_file.exists():
        raise FileNotFoundError("transcript_raw.txt not found")

    raw_text = raw_file.read_text(encoding="utf-8")

    cleaned = clean_transcript(raw_text)

    clean_file.write_text(cleaned, encoding="utf-8")

    print(f"[OK] Clean transcript saved: {clean_file}")
