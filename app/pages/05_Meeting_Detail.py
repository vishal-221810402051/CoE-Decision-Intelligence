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
from app.ui.actions import (
    generate_meeting_report,
    get_processing_mode_state,
    save_processing_mode_state,
)
from app.ui.repository import (
    find_report_pdf,
    get_artifact_paths,
    list_meeting_metadata,
    list_meeting_source_docs,
    list_meeting_source_pdfs,
    list_meetings,
    safe_read_json,
    safe_read_text,
)
from app.ui.status_model import compute_stage_status


def _meeting_ids() -> list[str]:
    rows = list_meetings()
    return [str(item.get("meeting_id", "")).strip() for item in rows if str(item.get("meeting_id", "")).strip()]


def _render_mode_chip(mode: str) -> None:
    normalized = str(mode).strip().lower()
    if normalized == "transcript_plus_docs":
        label = "TRANSCRIPT + DOCS"
        bg = "#1f3b73"
        fg = "#dbeafe"
    else:
        label = "TRANSCRIPT ONLY"
        bg = "#14532d"
        fg = "#dcfce7"
    st.markdown(
        (
            "<span style='display:inline-block;padding:4px 10px;border-radius:999px;"
            f"font-size:12px;font-weight:700;background:{bg};color:{fg};'>{label}</span>"
        ),
        unsafe_allow_html=True,
    )


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
    source_docs = list_meeting_source_docs(meeting_id)

    st.divider()
    st.subheader("Processing Mode")
    mode_state = get_processing_mode_state(meeting_id)
    _render_mode_chip(mode_state.get("processing_mode", "transcript_only"))
    mode_options = ["transcript_only", "transcript_plus_docs"]
    mode_labels = {
        "transcript_only": "Transcript-only",
        "transcript_plus_docs": "Transcript + Source Docs",
    }
    mode_index = mode_options.index(mode_state.get("processing_mode", "transcript_only")) if mode_state.get("processing_mode", "transcript_only") in mode_options else 0

    with st.form("processing_mode_form", clear_on_submit=False):
        selected_mode = st.selectbox(
            "Mode",
            options=mode_options,
            index=mode_index,
            format_func=lambda item: mode_labels.get(item, item),
        )

        available_doc_ids = [str(item.get("doc_id", "")).strip() for item in source_docs if str(item.get("doc_id", "")).strip()]
        selected_doc_ids: list[str] = []
        if selected_mode == "transcript_plus_docs":
            if available_doc_ids:
                label_map: dict[str, str] = {}
                for item in source_docs:
                    doc_id = str(item.get("doc_id", "")).strip()
                    if not doc_id:
                        continue
                    role = str(item.get("document_role", "")).strip()
                    name = str(item.get("source_file_name", "")).strip()
                    label_map[doc_id] = f"{doc_id} | {role or 'unknown_role'} | {name or 'unknown_file'}"
                defaults = [doc_id for doc_id in mode_state.get("selected_source_doc_ids", []) if doc_id in available_doc_ids]
                selected_doc_ids = st.multiselect(
                    "Selected Source Docs",
                    options=available_doc_ids,
                    default=defaults,
                    format_func=lambda item: label_map.get(item, item),
                    help="Transcript + Source Docs mode requires valid selected PDFs.",
                )
            else:
                st.warning("No meeting-scoped source docs available.")
        save_mode = st.form_submit_button("Save Processing Mode")

    if save_mode:
        save_result = save_processing_mode_state(
            meeting_id=meeting_id,
            mode=selected_mode,
            selected_doc_ids=selected_doc_ids,
        )
        if save_result.get("ok"):
            st.success(str(save_result.get("message", "Processing mode saved.")))
            st.rerun()
        else:
            st.error(str(save_result.get("message", "Failed to save processing mode.")))

    if st.button("Generate Report", key=f"generate_report_{meeting_id}"):
        generation_result = generate_meeting_report(meeting_id)
        if generation_result.get("ok"):
            message = str(generation_result.get("message", "Report generation completed."))
            lowered = message.lower()
            if "failed" in lowered or "unavailable" in lowered:
                st.warning(message)
            else:
                st.success(message)
        else:
            st.error(str(generation_result.get("message", "Report generation failed.")))
        st.rerun()

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
        report_metadata = safe_read_json(paths["report_metadata"])
        if isinstance(report_metadata, dict):
            meta_mode = str(report_metadata.get("processing_mode", mode_state.get("processing_mode", "transcript_only"))).strip()
            _render_mode_chip(meta_mode)
            report_status = str(report_metadata.get("status", "")).strip()
            if report_status == "completed":
                st.success("Report status: completed")
            elif report_status == "blocked":
                st.warning("Report status: blocked")
            elif report_status:
                st.error(f"Report status: {report_status}")

            report_version = str(report_metadata.get("report_version", "")).strip()
            generated_at = str(report_metadata.get("generated_at", "")).strip()
            if report_version:
                st.caption(f"Report Version: {report_version}")
            if generated_at:
                st.caption(f"Generated At: {generated_at}")

            doc_summary = report_metadata.get("doc_validation_summary")
            if isinstance(doc_summary, dict):
                supported = int(doc_summary.get("supported", 0))
                not_found = int(doc_summary.get("not_found", 0))
                unclear = int(doc_summary.get("unclear", 0))
                col_a, col_b, col_c = st.columns(3)
                col_a.metric("Supported", supported)
                col_b.metric("Not Found", not_found)
                col_c.metric("Unclear", unclear)

            report_error = str(report_metadata.get("error", "")).strip()
            if report_error:
                st.error(report_error)
            with st.expander("Report Metadata", expanded=False):
                st.json(report_metadata)

        report_html = paths["report_html"]
        html_text = safe_read_text(report_html)
        if isinstance(html_text, str) and html_text.strip():
            st.download_button(
                "Download HTML",
                data=html_text.encode("utf-8"),
                file_name=report_html.name,
                mime="text/html",
            )

        report_pdf = find_report_pdf(meeting_id)
        if report_pdf is None:
            if isinstance(report_metadata, dict) and str(report_metadata.get("status", "")).strip() == "completed":
                pdf_status = str(report_metadata.get("pdf_status", "")).strip().lower()
                if pdf_status in {"failed", "unavailable"}:
                    st.warning("PDF generation failed, HTML available.")
                else:
                    st.info("Report generated (HTML payload available), but no PDF was produced in this environment.")
            else:
                st.info("Report not generated yet.")
        elif report_pdf.exists():
            render_pdf_panel(report_pdf, "Report PDF Preview")
        else:
            st.info("Report not generated yet.")

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
