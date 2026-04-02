from __future__ import annotations

import streamlit as st

from app.config import config

ALLOWED_TYPES = ["m4a", "mp3", "wav"]


def main() -> None:
    st.set_page_config(page_title="CoE Decision Intelligence", layout="wide")
    st.title("CoE Decision Intelligence System")
    st.write(f"Environment: {config.APP_ENV}")
    st.write(f"Data Path: {config.DATA_PATH}")

    st.subheader("Phase 1 — Audio Intake")
    uploaded_file = st.file_uploader(
        "Upload meeting audio",
        type=ALLOWED_TYPES,
        accept_multiple_files=False,
    )

    if uploaded_file is not None:
        st.info(
            "Phase 1 service implementation is filesystem-ready. "
            "UI-triggered persistence wiring can be added after service validation."
        )
        st.write(f"Selected file: {uploaded_file.name}")


if __name__ == "__main__":
    main()
