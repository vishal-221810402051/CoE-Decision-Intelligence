from __future__ import annotations

import base64
import mimetypes
import os
from email.message import EmailMessage
from pathlib import Path
from typing import Any

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build


GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.send"]
GMAIL_TOKEN_PATH = Path("google_gmail_token.json")
DEFAULT_RECIPIENT = "vishalnelaturi@gmail.com"


def _client_secret_candidates() -> list[Path]:
    env_path = str(os.getenv("GMAIL_CLIENT_SECRET_PATH", "")).strip()
    candidates: list[Path] = []
    if env_path:
        candidates.append(Path(env_path))
    candidates.append(Path("google_gmail_client_secret.json"))
    candidates.append(
        Path(
            "client_secret_983650636859-vvfogqihu0i195ko8f2sbr8bg89hmgae.apps.googleusercontent.com.json"
        )
    )
    candidates.extend(sorted(Path(".").glob("client_secret_*.json")))
    unique: list[Path] = []
    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate.resolve()) if candidate.exists() else str(candidate)
        if key in seen:
            continue
        seen.add(key)
        unique.append(candidate)
    return unique


def _resolve_client_secret_path() -> Path:
    for candidate in _client_secret_candidates():
        if candidate.exists() and candidate.is_file():
            return candidate
    raise FileNotFoundError(
        "Missing Gmail OAuth client file. Set GMAIL_CLIENT_SECRET_PATH or place a client_secret_*.json in project root."
    )


def _get_gmail_service() -> Any:
    creds: Credentials | None = None
    if GMAIL_TOKEN_PATH.exists():
        creds = Credentials.from_authorized_user_file(str(GMAIL_TOKEN_PATH), GMAIL_SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(str(_resolve_client_secret_path()), GMAIL_SCOPES)
            creds = flow.run_local_server(port=0)
        GMAIL_TOKEN_PATH.write_text(creds.to_json(), encoding="utf-8")

    return build("gmail", "v1", credentials=creds)


def send_pdf_email(meeting_id: str, pdf_path: Path, recipient: str = DEFAULT_RECIPIENT) -> dict[str, Any]:
    meeting_key = str(meeting_id).strip()
    resolved_pdf = Path(pdf_path).resolve()
    if not resolved_pdf.exists() or not resolved_pdf.is_file():
        raise FileNotFoundError(f"PDF not found: {resolved_pdf}")

    message = EmailMessage()
    message["To"] = recipient
    message["Subject"] = f"CoE Meeting Report - {meeting_key}"
    message.set_content(
        "\n".join(
            [
                f"Meeting ID: {meeting_key}",
                "",
                "Attached is the generated CoE meeting report PDF.",
                "This email was sent from the laptop backend.",
            ]
        )
    )

    mime_type, _ = mimetypes.guess_type(str(resolved_pdf))
    if not mime_type:
        mime_type = "application/pdf"
    maintype, subtype = mime_type.split("/", 1)
    message.add_attachment(
        resolved_pdf.read_bytes(),
        maintype=maintype,
        subtype=subtype,
        filename=resolved_pdf.name,
    )

    encoded = base64.urlsafe_b64encode(message.as_bytes()).decode("utf-8")
    service = _get_gmail_service()
    sent = service.users().messages().send(userId="me", body={"raw": encoded}).execute()
    message_id = str(sent.get("id", "")).strip()

    return {
        "meeting_id": meeting_key,
        "recipient": recipient,
        "status": "sent",
        "provider": "gmail",
        "message_id": message_id,
    }
