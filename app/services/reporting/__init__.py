from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from html import escape
from pathlib import Path
from typing import Any

from app.config import (
    REPORT_DIR_NAME,
    REPORT_HTML_FILE,
    REPORT_METADATA_FILE,
    REPORT_PAYLOAD_FILE,
    REPORT_PDF_FILE,
    config,
)
from app.services.processing_mode import get_processing_mode


class ReportGenerationError(Exception):
    def __init__(self, message: str, *, status: str = "failed") -> None:
        super().__init__(message)
        self.status = status


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _meeting_dir(meeting_id: str) -> Path:
    return config.PROCESSED_PATH / str(meeting_id).strip()


def _report_paths(meeting_id: str) -> dict[str, Path]:
    report_dir = _meeting_dir(meeting_id) / REPORT_DIR_NAME
    return {
        "report_dir": report_dir,
        "report_payload": report_dir / REPORT_PAYLOAD_FILE,
        "report_html": report_dir / REPORT_HTML_FILE,
        "report_pdf": report_dir / REPORT_PDF_FILE,
        "report_metadata": report_dir / REPORT_METADATA_FILE,
    }


def _read_required_text(path: Path, label: str) -> str:
    if not path.exists() or not path.is_file():
        raise ReportGenerationError(f"Missing required artifact: {label}", status="failed")
    content = path.read_text(encoding="utf-8")
    if not content.strip():
        raise ReportGenerationError(f"Required artifact is empty: {label}", status="failed")
    return content


def _read_required_json(path: Path, label: str) -> dict[str, Any]:
    if not path.exists() or not path.is_file():
        raise ReportGenerationError(f"Missing required artifact: {label}", status="failed")
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception as exc:
        raise ReportGenerationError(f"Invalid JSON in {label}: {exc}", status="failed") from exc
    if not isinstance(payload, dict) or not payload:
        raise ReportGenerationError(f"Required JSON artifact is empty: {label}", status="failed")
    return payload


def _is_valid_pdf(path: Path) -> bool:
    try:
        if not path.exists() or not path.is_file():
            return False
        with path.open("rb") as handle:
            return handle.read(5) == b"%PDF-"
    except Exception:
        return False


def _resolve_selected_docs(meeting_id: str, selected_doc_ids: list[str]) -> list[dict[str, str]]:
    meeting_docs_root = _meeting_dir(meeting_id) / "docs"
    if not meeting_docs_root.exists() or not meeting_docs_root.is_dir():
        raise ReportGenerationError(
            "Source-document validation is enabled, but no meeting docs folder exists.",
            status="blocked",
        )

    unique_doc_ids: list[str] = []
    for doc_id in selected_doc_ids:
        normalized = str(doc_id).strip()
        if normalized and normalized not in unique_doc_ids:
            unique_doc_ids.append(normalized)

    if not unique_doc_ids:
        raise ReportGenerationError(
            "Source-document validation is enabled, but no source docs were selected.",
            status="blocked",
        )

    resolved: list[dict[str, str]] = []
    for doc_id in unique_doc_ids:
        doc_root = meeting_docs_root / doc_id
        source_dir = doc_root / "source"
        if not source_dir.exists() or not source_dir.is_dir():
            raise ReportGenerationError(
                f"Selected source doc is missing source folder: {doc_id}",
                status="blocked",
            )
        pdf_paths = sorted([path for path in source_dir.glob("*.pdf") if path.is_file()])
        if not pdf_paths:
            raise ReportGenerationError(
                f"Selected source doc does not contain a PDF: {doc_id}",
                status="blocked",
            )
        pdf_path = pdf_paths[0]
        if not _is_valid_pdf(pdf_path):
            raise ReportGenerationError(
                f"Selected source doc is not a valid PDF binary: {doc_id}",
                status="blocked",
            )
        resolved.append(
            {
                "doc_id": doc_id,
                "pdf_path": str(pdf_path),
                "relative_pdf_path": str(pdf_path.relative_to(_meeting_dir(meeting_id))),
            }
        )
    return resolved


def _to_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    return []


def build_report_payload(meeting_id: str, mode: str) -> dict[str, Any]:
    meeting_dir = _meeting_dir(meeting_id)

    transcript_clean = _read_required_text(
        meeting_dir / "transcript" / "transcript_clean.txt",
        "transcript/transcript_clean.txt",
    )
    intelligence = _read_required_json(
        meeting_dir / "intelligence" / "intelligence.json",
        "intelligence/intelligence.json",
    )
    executive = _read_required_json(
        meeting_dir / "executive" / "executive_intelligence.json",
        "executive/executive_intelligence.json",
    )
    decision = _read_required_json(
        meeting_dir / "decision" / "decision_intelligence_v2.json",
        "decision/decision_intelligence_v2.json",
    )

    executive_summary = ""
    if isinstance(executive.get("executive_summary"), dict):
        executive_summary = str(
            executive.get("executive_summary", {}).get("meaning_of_meeting", "")
        ).strip()
    if not executive_summary:
        executive_summary = str(intelligence.get("summary", "")).strip()

    key_decisions: list[dict[str, Any]] = []
    for record in _to_list(decision.get("decision_records")):
        if not isinstance(record, dict):
            continue
        statement = str(record.get("statement", "")).strip()
        if not statement:
            continue
        key_decisions.append(
            {
                "statement": statement,
                "state": str(record.get("state", "")).strip(),
                "primary_owner": str(record.get("primary_owner", "")).strip(),
            }
        )

    timeline_mentions = _to_list(intelligence.get("timeline_mentions"))
    timeline_text = "; ".join(str(item).strip() for item in timeline_mentions if str(item).strip())

    notes = f"Transcript length: {len(transcript_clean)} characters."

    source_docs_used: list[str] = []
    doc_validation: dict[str, Any] | None = None
    if mode == "transcript_plus_docs":
        mode_payload = get_processing_mode(meeting_id)
        selected_ids = mode_payload.get("selected_source_doc_ids", [])
        resolved_docs = _resolve_selected_docs(meeting_id, selected_ids if isinstance(selected_ids, list) else [])
        source_docs_used = [item["doc_id"] for item in resolved_docs]
        doc_validation = {
            "docs_used": source_docs_used,
            "validation_status": "basic_v1",
        }

    return {
        "meeting_id": meeting_id,
        "processing_mode": mode,
        "generated_at": _now_iso(),
        "sections": {
            "executive_summary": executive_summary,
            "key_decisions": key_decisions,
            "risks": _to_list(intelligence.get("risks")),
            "actions": _to_list(intelligence.get("action_plan")),
            "timeline": timeline_text,
            "notes": notes,
            "doc_validation": doc_validation,
        },
        "source_docs_used": source_docs_used,
    }


def _render_list(value: list[Any]) -> str:
    if not value:
        return "<li>None</li>"
    chunks: list[str] = []
    for item in value:
        if isinstance(item, dict):
            chunks.append(f"<li><pre>{escape(json.dumps(item, ensure_ascii=False, indent=2))}</pre></li>")
        else:
            chunks.append(f"<li>{escape(str(item))}</li>")
    return "".join(chunks)


def generate_html_report(payload: dict[str, Any]) -> str:
    sections = payload.get("sections", {}) if isinstance(payload, dict) else {}
    if not isinstance(sections, dict):
        sections = {}

    title = f"Meeting Report - {payload.get('meeting_id', '')}"
    return (
        "<!doctype html>"
        "<html><head><meta charset='utf-8'><title>"
        f"{escape(title)}"
        "</title></head><body style='font-family:Arial,sans-serif;margin:24px;'>"
        f"<h1>{escape(title)}</h1>"
        f"<p><strong>Generated At:</strong> {escape(str(payload.get('generated_at', '')))}</p>"
        f"<p><strong>Processing Mode:</strong> {escape(str(payload.get('processing_mode', '')))}</p>"
        f"<h2>Executive Summary</h2><p>{escape(str(sections.get('executive_summary', '')))}</p>"
        f"<h2>Key Decisions</h2><ul>{_render_list(_to_list(sections.get('key_decisions')))}</ul>"
        f"<h2>Risks</h2><ul>{_render_list(_to_list(sections.get('risks')))}</ul>"
        f"<h2>Actions</h2><ul>{_render_list(_to_list(sections.get('actions')))}</ul>"
        f"<h2>Timeline</h2><p>{escape(str(sections.get('timeline', '')))}</p>"
        f"<h2>Notes</h2><p>{escape(str(sections.get('notes', '')))}</p>"
        f"<h2>Source Document Validation</h2><pre>{escape(json.dumps(sections.get('doc_validation'), ensure_ascii=False, indent=2))}</pre>"
        f"<h2>Source Docs Used</h2><ul>{_render_list(_to_list(payload.get('source_docs_used')))}</ul>"
        "</body></html>"
    )


def generate_pdf_from_html(html: str, output_path: Path) -> dict[str, Any]:
    try:
        from reportlab.lib.pagesizes import LETTER
        from reportlab.pdfgen import canvas
    except Exception:
        return {
            "ok": False,
            "reason": "unavailable",
            "error": "PDF library unavailable. HTML report generated only.",
        }

    try:
        plain = re.sub(r"<[^>]+>", " ", html)
        plain = re.sub(r"\s+", " ", plain).strip()
        lines: list[str] = []
        chunk = 95
        for idx in range(0, len(plain), chunk):
            lines.append(plain[idx : idx + chunk])
        if not lines:
            lines = ["Report generated."]

        output_path.parent.mkdir(parents=True, exist_ok=True)
        pdf = canvas.Canvas(str(output_path), pagesize=LETTER)
        width, height = LETTER
        y = height - 40
        for line in lines:
            pdf.drawString(36, y, line)
            y -= 14
            if y < 40:
                pdf.showPage()
                y = height - 40
        pdf.save()
        return {"ok": True, "reason": "generated", "error": None}
    except Exception as exc:
        return {
            "ok": False,
            "reason": "failed",
            "error": f"PDF generation failed: {exc}",
        }


def write_report_files(meeting_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    paths = _report_paths(meeting_id)
    paths["report_dir"].mkdir(parents=True, exist_ok=True)

    paths["report_payload"].write_text(
        json.dumps(payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    html = generate_html_report(payload)
    paths["report_html"].write_text(html, encoding="utf-8")

    pdf_result = generate_pdf_from_html(html, paths["report_pdf"])
    return {
        "paths": paths,
        "pdf_result": pdf_result,
    }


def _write_metadata(meeting_id: str, metadata: dict[str, Any]) -> Path:
    paths = _report_paths(meeting_id)
    paths["report_dir"].mkdir(parents=True, exist_ok=True)
    paths["report_metadata"].write_text(
        json.dumps(metadata, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return paths["report_metadata"]


def generate_report(meeting_id: str) -> dict[str, Any]:
    mode_payload = get_processing_mode(meeting_id)
    mode = str(mode_payload.get("processing_mode", "transcript_only")).strip().lower()
    if mode not in {"transcript_only", "transcript_plus_docs"}:
        mode = "transcript_only"

    metadata: dict[str, Any] = {
        "status": "failed",
        "processing_mode": mode,
        "generated_at": _now_iso(),
        "source_docs_used": [],
        "error": None,
    }

    try:
        payload = build_report_payload(meeting_id, mode)
        write_result = write_report_files(meeting_id, payload)
        pdf_result = write_result.get("pdf_result", {})
        metadata["source_docs_used"] = payload.get("source_docs_used", [])

        if isinstance(pdf_result, dict) and pdf_result.get("ok") is False:
            reason = str(pdf_result.get("reason", "")).strip()
            if reason == "failed":
                metadata["status"] = "failed"
                metadata["error"] = str(pdf_result.get("error", "PDF generation failed.")).strip()
            else:
                metadata["status"] = "completed"
                metadata["error"] = None
        else:
            metadata["status"] = "completed"
            metadata["error"] = None
    except ReportGenerationError as exc:
        metadata["status"] = exc.status
        metadata["error"] = str(exc)
    except Exception as exc:
        metadata["status"] = "failed"
        metadata["error"] = f"Unexpected report generation error: {exc}"

    metadata_path = _write_metadata(meeting_id, metadata)
    return {
        "meeting_id": meeting_id,
        "status": metadata["status"],
        "metadata_path": str(metadata_path),
        "error": metadata["error"],
    }
