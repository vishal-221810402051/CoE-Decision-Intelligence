from __future__ import annotations

from typing import Any

from app.services.calendar.google_auth import get_google_calendar_service


def create_google_calendar_event(event: dict[str, Any], calendar_id: str = "primary") -> dict[str, Any]:
    try:
        service = get_google_calendar_service()
        created = service.events().insert(calendarId=calendar_id, body=event).execute()
        return {
            "success": True,
            "event_id": str(created.get("id", "")).strip(),
            "calendar_id": calendar_id,
            "raw": created,
        }
    except Exception as exc:
        return {
            "success": False,
            "error": str(exc),
            "calendar_id": calendar_id,
        }

