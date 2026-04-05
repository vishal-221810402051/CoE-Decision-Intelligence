from __future__ import annotations

from pathlib import Path
from typing import Any

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build


SCOPES = ["https://www.googleapis.com/auth/calendar"]
CLIENT_SECRET_PATH = Path("google_calendar_client_secret.json")
TOKEN_PATH = Path("google_calendar_token.json")


def get_google_calendar_service() -> Any:
    """
    Build and return an authenticated Google Calendar service client.

    First run opens a local OAuth flow and stores a refreshable token.
    Subsequent runs reuse google_calendar_token.json.
    """
    creds: Credentials | None = None

    if TOKEN_PATH.exists():
        creds = Credentials.from_authorized_user_file(str(TOKEN_PATH), SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not CLIENT_SECRET_PATH.exists():
                raise FileNotFoundError(
                    f"Missing OAuth client file: {CLIENT_SECRET_PATH.resolve()}"
                )
            flow = InstalledAppFlow.from_client_secrets_file(
                str(CLIENT_SECRET_PATH),
                SCOPES,
            )
            creds = flow.run_local_server(port=0)

        TOKEN_PATH.write_text(creds.to_json(), encoding="utf-8")

    return build("calendar", "v3", credentials=creds)
