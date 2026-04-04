from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.config import config
from app.ui.repository import list_meetings
from app.ui.status_model import compute_stage_status


def _is_fully_completed(status: dict[str, str]) -> bool:
    return all(value == "completed" for value in status.values())


def main() -> None:
    st.set_page_config(page_title="CoE Decision Intelligence", layout="wide")
    st.title("CoE Decision Intelligence Operator Console")
    st.caption("Professional filesystem-driven dashboard")

    left, right = st.columns([2, 1])
    with left:
        st.write(f"Environment: `{config.APP_ENV}`")
        st.write(f"Data path: `{config.DATA_PATH}`")
    with right:
        st.info(
            "Use sidebar pages: Home, Meeting Intake, Source Documents, "
            "Processing Status, Meeting Detail."
        )

    meetings = list_meetings()
    statuses = [
        compute_stage_status(str(item.get("meeting_id", "")).strip())
        for item in meetings
        if str(item.get("meeting_id", "")).strip()
    ]
    completed = sum(1 for status in statuses if _is_fully_completed(status))
    total = len(statuses)
    partial = max(total - completed, 0)

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Meetings", total)
    col2.metric("Completed Meetings", completed)
    col3.metric("Partial Meetings", partial)

    st.divider()
    st.subheader("Current Scope")
    st.write("- Audio intake")
    st.write("- Processed meeting browsing")
    st.write("- Stage status visibility")
    st.write("- Meeting detail artifact viewing")
    st.write("- Source docs and report PDF as optional/future-ready")


if __name__ == "__main__":
    main()

