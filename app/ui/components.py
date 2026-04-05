from __future__ import annotations

import base64
import json
from pathlib import Path
from typing import Any

import streamlit as st


def render_status_badge(label: str, status: str) -> None:
    normalized = str(status).strip().lower()
    if normalized == "completed":
        bg = "#0f5132"
        fg = "#d1e7dd"
    elif normalized in {"blocked", "failed"}:
        bg = "#842029"
        fg = "#f8d7da"
    elif normalized == "pending":
        bg = "#664d03"
        fg = "#fff3cd"
    elif normalized == "unknown":
        bg = "#374151"
        fg = "#e5e7eb"
    elif normalized == "missing":
        bg = "#7f1d1d"
        fg = "#fee2e2"
    else:
        bg = "#4b5563"
        fg = "#f3f4f6"

    st.markdown(
        (
            "<div style='display:flex;align-items:center;gap:8px;'>"
            f"<span style='font-size:12px;color:#9ca3af;'>{label}</span>"
            "<span style='display:inline-block;padding:2px 10px;border-radius:999px;"
            f"font-size:12px;font-weight:600;background:{bg};color:{fg};'>{normalized.upper()}</span>"
            "</div>"
        ),
        unsafe_allow_html=True,
    )


def render_status_strip(status_map: dict[str, str]) -> None:
    if not status_map:
        st.info("No status information available.")
        return

    keys = [
        "intake",
        "normalization",
        "transcription",
        "cleanup",
        "intelligence",
        "executive",
        "decision",
    ]
    cols = st.columns(len(keys))
    for idx, key in enumerate(keys):
        with cols[idx]:
            render_status_badge(key, status_map.get(key, "unknown"))


def render_json_panel(data: dict[str, Any] | None, title: str) -> None:
    st.subheader(title)
    if not isinstance(data, dict):
        st.info("Artifact not available.")
        return
    with st.expander("View JSON", expanded=True):
        st.json(data)


def render_text_panel(text: str | None, title: str) -> None:
    st.subheader(title)
    if not isinstance(text, str) or not text.strip():
        st.info("Artifact not available.")
        return
    st.text_area("Content", value=text, height=560)


def is_valid_pdf(file_path: Path) -> bool:
    try:
        if not file_path.exists() or not file_path.is_file():
            return False
        with file_path.open("rb") as file:
            header = file.read(5)
        return header == b"%PDF-"
    except Exception:
        return False


def render_pdf_panel(file_path: Path, title: str) -> None:
    st.subheader(title)
    if not file_path.exists() or not file_path.is_file():
        st.info("PDF file not available.")
        return

    try:
        pdf_bytes = file_path.read_bytes()
    except Exception as exc:
        st.warning(f"Unable to read PDF: {exc}")
        return

    st.download_button(
        "Download PDF",
        data=pdf_bytes,
        file_name=file_path.name,
        mime="application/pdf",
    )

    if not is_valid_pdf(file_path):
        st.warning("Invalid PDF file (not a real PDF binary).")
        return

    payload = base64.b64encode(pdf_bytes).decode("utf-8")
    encoded_payload = json.dumps(payload)
    html = (
        "<div id='pdf-host' style='width:100%;height:860px;'></div>"
        "<script>"
        f"const b64 = {encoded_payload};"
        "function toBytes(value){"
        "  const binary = atob(value);"
        "  const size = binary.length;"
        "  const bytes = new Uint8Array(size);"
        "  for(let i=0;i<size;i++){bytes[i]=binary.charCodeAt(i);} "
        "  return bytes;"
        "}"
        "try {"
        "  const bytes = toBytes(b64);"
        "  const blob = new Blob([bytes], {type:'application/pdf'});"
        "  const url = URL.createObjectURL(blob);"
        "  const iframe = document.createElement('iframe');"
        "  iframe.src = url;"
        "  iframe.style.width = '100%';"
        "  iframe.style.height = '860px';"
        "  iframe.style.border = 'none';"
        "  const host = document.getElementById('pdf-host');"
        "  host.innerHTML = '';"
        "  host.appendChild(iframe);"
        "} catch (error) {"
        "  const host = document.getElementById('pdf-host');"
        "  host.innerHTML = '<div style=\"padding:12px;border:1px solid #666;border-radius:8px;\">PDF preview unavailable in this browser. Use Download PDF.</div>';"
        "}"
        "</script>"
    )
    st.components.v1.html(html, height=880, scrolling=False)
