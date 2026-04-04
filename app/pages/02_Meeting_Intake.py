from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.config import config
from app.ui.actions import intake_audio_upload


def main() -> None:
    st.set_page_config(page_title="Meeting Intake", layout="wide")
    st.title("Meeting Intake")
    st.caption("Upload audio and create a meeting workspace (intake only).")

    allowed_types = [ext.replace(".", "") for ext in sorted(config.ALLOWED_AUDIO_EXTENSIONS)]
    uploaded_file = st.file_uploader(
        "Upload audio file",
        type=allowed_types,
        accept_multiple_files=False,
    )

    if "last_created_meeting_id" not in st.session_state:
        st.session_state["last_created_meeting_id"] = ""

    if st.button("Create Meeting", type="primary"):
        result = intake_audio_upload(uploaded_file)
        if result.get("ok"):
            meeting_id = str(result.get("meeting_id", "")).strip()
            st.session_state["last_created_meeting_id"] = meeting_id
            st.success(f"Meeting Created: {meeting_id}")
            st.write(f"Stored audio path: `{result.get('stored_audio_path', '')}`")
        else:
            st.error(str(result.get("message", "Meeting intake failed.")))

    created = str(st.session_state.get("last_created_meeting_id", "")).strip()
    if created:
        col1, col2 = st.columns([1, 3])
        with col1:
            if st.button("Open Meeting", key="open_created_meeting"):
                st.session_state["selected_meeting_id"] = created
                try:
                    st.switch_page("pages/05_Meeting_Detail.py")
                except Exception:
                    st.info("Open '05_Meeting_Detail' from sidebar and select the meeting.")
        with col2:
            st.info(f"Latest created meeting: `{created}`")


if __name__ == "__main__":
    main()

