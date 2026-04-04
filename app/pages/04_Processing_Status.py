from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.ui.repository import list_meetings
from app.ui.status_model import compute_stage_status
from app.ui.components import render_status_badge


def main() -> None:
    st.set_page_config(page_title="Processing Status", layout="wide")
    st.title("Processing Status")
    st.caption("Filesystem-derived stage completion matrix")

    meetings = list_meetings()
    if not meetings:
        st.info("No meetings found in data/processed.")
        return

    header = st.columns([2.3, 1, 1, 1, 1, 1, 1, 1])
    header[0].markdown("**Meeting**")
    header[1].markdown("**Intake**")
    header[2].markdown("**Norm**")
    header[3].markdown("**Trans**")
    header[4].markdown("**Clean**")
    header[5].markdown("**Intel**")
    header[6].markdown("**Exec**")
    header[7].markdown("**Decision**")

    for item in meetings:
        meeting_id = str(item.get("meeting_id", "")).strip()
        if not meeting_id:
            continue
        stage = compute_stage_status(meeting_id)
        row = st.columns([2.3, 1, 1, 1, 1, 1, 1, 1])
        row[0].write(meeting_id)
        with row[1]:
            render_status_badge("intake", stage["intake"])
        with row[2]:
            render_status_badge("normalization", stage["normalization"])
        with row[3]:
            render_status_badge("transcription", stage["transcription"])
        with row[4]:
            render_status_badge("cleanup", stage["cleanup"])
        with row[5]:
            render_status_badge("intelligence", stage["intelligence"])
        with row[6]:
            render_status_badge("executive", stage["executive"])
        with row[7]:
            render_status_badge("decision", stage["decision"])


if __name__ == "__main__":
    main()
