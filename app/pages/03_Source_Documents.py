from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.config import (
    ALLOWED_DOCUMENT_EXTENSIONS,
    ALLOWED_DOCUMENT_ROLES,
    ALLOWED_DOCUMENT_SCOPES,
    config,
)
from app.ui.actions import source_doc_upload
from app.ui.repository import list_meetings, safe_read_json


def _list_mission_docs() -> list[dict[str, str]]:
    root = config.DATA_PATH / "context_docs"
    if not root.exists():
        return []
    rows: list[dict[str, str]] = []
    for folder in sorted(root.iterdir(), key=lambda p: p.name, reverse=True):
        if not folder.is_dir():
            continue
        metadata = safe_read_json(folder / "metadata" / "document_intake.json")
        if isinstance(metadata, dict):
            rows.append(
                {
                    "doc_id": str(metadata.get("doc_id", folder.name)),
                    "scope": str(metadata.get("scope", "mission")),
                    "role": str(metadata.get("document_role", "")),
                    "file": str(metadata.get("source_file_name", "")),
                    "created_at": str(metadata.get("created_at", "")),
                }
            )
    return rows


def _list_meeting_docs(meeting_id: str) -> list[dict[str, str]]:
    root = config.PROCESSED_PATH / meeting_id / "docs"
    if not root.exists():
        return []
    rows: list[dict[str, str]] = []
    for folder in sorted(root.iterdir(), key=lambda p: p.name, reverse=True):
        if not folder.is_dir():
            continue
        metadata = safe_read_json(folder / "metadata" / "document_intake.json")
        if isinstance(metadata, dict):
            rows.append(
                {
                    "doc_id": str(metadata.get("doc_id", folder.name)),
                    "scope": str(metadata.get("scope", "meeting")),
                    "role": str(metadata.get("document_role", "")),
                    "file": str(metadata.get("source_file_name", "")),
                    "created_at": str(metadata.get("created_at", "")),
                }
            )
    return rows


def main() -> None:
    st.set_page_config(page_title="Source Documents", layout="wide")
    st.title("Source Documents")
    st.caption("Optional future context documents. Not required for normal workflow.")

    mission_docs = _list_mission_docs()
    meetings = list_meetings()
    meeting_ids = [str(row.get("meeting_id", "")).strip() for row in meetings if str(row.get("meeting_id", "")).strip()]

    total_meeting_docs = 0
    for meeting_id in meeting_ids:
        total_meeting_docs += len(_list_meeting_docs(meeting_id))

    if not mission_docs and total_meeting_docs == 0:
        st.info("No source documents uploaded yet.")

    with st.expander("Optional upload", expanded=False):
        allowed_types = [ext.replace(".", "") for ext in sorted(ALLOWED_DOCUMENT_EXTENSIONS)]
        uploaded = st.file_uploader(
            "Optional future context documents",
            type=allowed_types,
            accept_multiple_files=False,
            key="source_doc_upload",
        )
        col1, col2, col3 = st.columns(3)
        with col1:
            scope = st.selectbox("Scope", sorted(ALLOWED_DOCUMENT_SCOPES))
        with col2:
            role = st.selectbox("Document role", sorted(ALLOWED_DOCUMENT_ROLES))
        with col3:
            linked = None
            if scope == "meeting":
                if meeting_ids:
                    linked = st.selectbox("Linked meeting", meeting_ids)
                else:
                    st.warning("No meetings available for meeting scope.")

        if st.button("Upload Source Document"):
            result = source_doc_upload(
                uploaded_file=uploaded,
                scope=scope,
                linked_meeting_id=linked,
                document_role=role,
            )
            if result.get("ok"):
                st.success(f"Uploaded document: {result.get('doc_id', '')}")
            else:
                st.error(str(result.get("message", "Source document upload failed.")))

    st.subheader("Mission Scope Documents")
    if mission_docs:
        st.dataframe(mission_docs, use_container_width=True, hide_index=True)
    else:
        st.caption("No mission-scoped source docs.")

    st.subheader("Meeting Scope Documents")
    if meeting_ids:
        selected = st.selectbox("Meeting", meeting_ids, key="docs_meeting_selector")
        rows = _list_meeting_docs(selected)
        if rows:
            st.dataframe(rows, use_container_width=True, hide_index=True)
        else:
            st.caption("No meeting-scoped source docs for selected meeting.")
    else:
        st.caption("No meetings available.")


if __name__ == "__main__":
    main()

