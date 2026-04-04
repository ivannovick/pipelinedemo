"""
Shared PDF stream message handling (decode, extract text, disk, Postgres).
Used by the scheduled Airflow consumer DAG (replaces the long-running stream-processor container).
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
import re
from io import BytesIO
from pathlib import Path

import psycopg
from pypdf import PdfReader

log = logging.getLogger(__name__)

_UNSAFE = re.compile(r"[^A-Za-z0-9._-]+")


def sanitize_filename(name: str) -> str:
    base = Path(name).name
    base = _UNSAFE.sub("_", base)
    return base[:200] if base else "document.pdf"


def extract_pdf_text(data: bytes) -> str:
    reader = PdfReader(BytesIO(data))
    parts = [(p.extract_text() or "") for p in reader.pages]
    return "\n".join(parts).strip()


def decode_message_body(body: bytes) -> tuple[bytes, str, str | None]:
    """
    Returns (pdf_bytes, original_file_name, source_file_url).
    Supports: raw PDF bytes, or JSON envelope from the publisher DAG.
    """
    if len(body) >= 4 and body[:4] == b"%PDF":
        return body, "stream_document.pdf", None
    try:
        doc = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise ValueError(f"not PDF and not JSON: {e}") from e
    b64 = doc.get("pdf_base64")
    if not isinstance(b64, str):
        raise ValueError("JSON envelope missing pdf_base64")
    raw = base64.b64decode(b64)
    name = doc.get("original_file_name") or "document.pdf"
    url = doc.get("source_file_url")
    if isinstance(url, str):
        return raw, str(name), url
    return raw, str(name), None


def process_after_stream_read(
    body: bytes,
    stream: str,
    offset: int,
) -> None:
    """After a message is read from the stream: decode, persist, INSERT."""
    pdf_bytes, orig_name, source_url = decode_message_body(body)
    if not pdf_bytes:
        raise ValueError("empty pdf payload")

    text = extract_pdf_text(pdf_bytes)
    if not text:
        raise ValueError("no extractable text from PDF")
    # Postgres TEXT rejects embedded NUL bytes; some PDFs yield them from extract_text().
    text = text.replace("\x00", "")

    db_url = os.environ["DATABASE_URL"]
    local_root = Path(os.environ.get("LOCAL_PDF_STORAGE", "/data/pdfs"))
    local_root.mkdir(parents=True, exist_ok=True)
    host_base = os.environ.get("PDF_HOST_BASE_DIR", "").strip().rstrip("/\\")

    h = hashlib.sha256(f"{stream}\0{offset}\0{orig_name}".encode()).hexdigest()[:16]
    stored_name = f"{h}_{sanitize_filename(orig_name)}"
    dest = local_root / stored_name
    dest.write_bytes(pdf_bytes)
    container_path = str(dest)
    if host_base:
        local_pc_path = str(Path(host_base) / stored_name)
    else:
        local_pc_path = f"data/pdfs/{stored_name}"

    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pdf_documents (
                    source_file_url,
                    original_file_name,
                    stored_file_name,
                    container_path,
                    local_pc_archive_path,
                    extracted_text,
                    source_stream,
                    stream_offset
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (source_stream, stream_offset) DO NOTHING
                """,
                (
                    source_url or "",
                    orig_name,
                    stored_name,
                    container_path,
                    local_pc_path,
                    text,
                    stream,
                    offset,
                ),
            )
        conn.commit()
    log.info("Processed stream=%s offset=%s -> %s", stream, offset, stored_name)
