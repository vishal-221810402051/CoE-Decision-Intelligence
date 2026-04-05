from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.ui.repository import list_meetings
from app.ui.components import render_status_badge
from app.ui.status_model import compute_stage_status


def _is_fully_completed(status: dict[str, str]) -> bool:
    return all(value == "completed" for value in status.values())


def _set_open_meeting(meeting_id: str) -> None:
    st.session_state["selected_meeting_id"] = meeting_id
    try:
        st.switch_page("pages/05_Meeting_Detail.py")
    except Exception:
        st.info("Open '05_Meeting_Detail' from the sidebar and select the meeting.")


def main() -> None:
    st.set_page_config(page_title="Home", layout="wide")
    st.title("Home")
    st.caption("Professional operator overview")

    meetings = list_meetings()
    rows: list[dict[str, str]] = []
    complete_count = 0

    for item in meetings:
        meeting_id = str(item.get("meeting_id", "")).strip()
        if not meeting_id:
            continue
        stage = compute_stage_status(meeting_id)
        if _is_fully_completed(stage):
            complete_count += 1
        rows.append(
            {
                "Meeting ID": meeting_id,
                "Created At": str(item.get("created_at", "")),
                "Intake": stage["intake"],
                "Transcription": stage["transcription"],
                "Intelligence": stage["intelligence"],
                "Executive": stage["executive"],
                "Decision": stage["decision"],
            }
        )

    total = len(rows)
    partial = max(total - complete_count, 0)

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Meetings", total)
    col2.metric("Completed Meetings", complete_count)
    col3.metric("Partial Meetings", partial)

    st.divider()
    st.subheader("Recent Meetings")
    if not rows:
        st.info("No meetings found in data/processed.")
        return

    header = st.columns([2.2, 1.2, 1.2, 1.2, 1.2, 1.2, 1.1])
    header[0].markdown("**Meeting ID**")
    header[1].markdown("**Intake**")
    header[2].markdown("**Transcription**")
    header[3].markdown("**Intelligence**")
    header[4].markdown("**Executive**")
    header[5].markdown("**Decision**")
    header[6].markdown("**Open**")

    for row in rows:
        cols = st.columns([2.2, 1.2, 1.2, 1.2, 1.2, 1.2, 1.1])
        meeting_id = row["Meeting ID"]
        cols[0].write(meeting_id)
        with cols[1]:
            render_status_badge("intake", row["Intake"])
        with cols[2]:
            render_status_badge("transcription", row["Transcription"])
        with cols[3]:
            render_status_badge("intelligence", row["Intelligence"])
        with cols[4]:
            render_status_badge("executive", row["Executive"])
        with cols[5]:
            render_status_badge("decision", row["Decision"])
        if cols[6].button("Open", key=f"home_open_{meeting_id}"):
            _set_open_meeting(meeting_id)


if __name__ == "__main__":
    main()
