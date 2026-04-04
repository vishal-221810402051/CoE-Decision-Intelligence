from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.ui.components import (
    is_valid_pdf,
    render_json_panel,
    render_pdf_panel,
    render_status_strip,
    render_text_panel,
)
from app.ui.repository import (
    find_report_pdf,
    get_artifact_paths,
    list_meeting_metadata,
    list_meeting_source_pdfs,
    list_meetings,
    safe_read_json,
    safe_read_text,
)
from app.ui.status_model import compute_stage_status


def _meeting_ids() -> list[str]:
    rows = list_meetings()
    return [str(item.get("meeting_id", "")).strip() for item in rows if str(item.get("meeting_id", "")).strip()]


def main() -> None:
    st.set_page_config(page_title="Meeting Detail", layout="wide")
    st.title("Meeting Detail")
    st.caption("Canonical artifacts and metadata viewer")

    meeting_ids = _meeting_ids()
    if not meeting_ids:
        st.info("No meetings found in data/processed.")
        return

    selected_state = str(st.session_state.get("selected_meeting_id", "")).strip()
    index = meeting_ids.index(selected_state) if selected_state in meeting_ids else 0
    meeting_id = st.selectbox("Meeting ID", meeting_ids, index=index)
    st.session_state["selected_meeting_id"] = meeting_id

    status_map = compute_stage_status(meeting_id)
    render_status_strip(status_map)

    paths = get_artifact_paths(meeting_id)
    metadata_map = list_meeting_metadata(meeting_id)

    tabs = st.tabs(
        [
            "Raw Transcript",
            "Clean Transcript",
            "Intelligence",
            "Executive",
            "Decision",
            "PDF",
            "Metadata",
        ]
    )

    with tabs[0]:
        render_text_panel(
            safe_read_text(paths["raw_transcript"]),
            "Raw Transcript",
        )

    with tabs[1]:
        render_text_panel(
            safe_read_text(paths["clean_transcript"]),
            "Clean Transcript",
        )

    with tabs[2]:
        render_json_panel(
            safe_read_json(paths["intelligence"]),
            "Intelligence (canonical)",
        )

    with tabs[3]:
        render_json_panel(
            safe_read_json(paths["executive"]),
            "Executive Intelligence",
        )

    with tabs[4]:
        render_json_panel(
            safe_read_json(paths["decision"]),
            "Decision Intelligence v2",
        )

    with tabs[5]:
        st.subheader("Report PDF")
        report_pdf = find_report_pdf(meeting_id)
        if report_pdf is None:
            st.info("Report PDF generation not implemented yet.")
        elif report_pdf.exists():
            render_pdf_panel(report_pdf, "Report PDF Preview")
        else:
            st.info("Report PDF generation not implemented yet.")

        st.divider()
        st.subheader("Source Document PDFs")
        source_pdfs = list_meeting_source_pdfs(meeting_id)
        if not source_pdfs:
            st.info("No source document PDFs uploaded for this meeting.")
        else:
            valid_pdfs = [path for path in source_pdfs if is_valid_pdf(path)]
            invalid_pdfs = [path for path in source_pdfs if path not in valid_pdfs]

            if valid_pdfs:
                options = [str(path.relative_to(paths["meeting_dir"])) for path in valid_pdfs]
                selected = st.selectbox("Select source PDF", options)
                selected_index = options.index(selected)
                render_pdf_panel(valid_pdfs[selected_index], "Source Document PDF")

            if invalid_pdfs:
                st.warning("Some uploaded PDF files are invalid.")
                for path in invalid_pdfs:
                    st.write(f"- `{path.relative_to(paths['meeting_dir'])}` (Invalid PDF file)")

            if not valid_pdfs and invalid_pdfs:
                st.info("No PDF available for this meeting yet.")

    with tabs[6]:
        st.subheader("Metadata")
        if not metadata_map:
            st.info("No metadata files available.")
        for name, payload in metadata_map.items():
            with st.expander(name, expanded=False):
                if isinstance(payload, dict):
                    st.json(payload)
                else:
                    st.info("Metadata unavailable or invalid.")


if __name__ == "__main__":
    main()

