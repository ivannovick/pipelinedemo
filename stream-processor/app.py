"""
RabbitMQ Streams consumer: for each message, ONLY after the payload is read from
the stream, decode PDF bytes, extract text, write to LOCAL_PDF_STORAGE, and INSERT
into Postgres. No parsing or disk I/O before stream delivery completes.
"""

from __future__ import annotations

import asyncio
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
from rstream import AMQPMessage, Consumer, amqp_decoder
from rstream.constants import ConsumerOffsetSpecification, OffsetType

log = logging.getLogger("stream-processor")
logging.basicConfig(level=logging.INFO)

_UNSAFE = re.compile(r"[^A-Za-z0-9._-]+")


def _sanitize_filename(name: str) -> str:
    base = Path(name).name
    base = _UNSAFE.sub("_", base)
    return base[:200] if base else "document.pdf"


def _extract_pdf_text(data: bytes) -> str:
    reader = PdfReader(BytesIO(data))
    parts = [(p.extract_text() or "") for p in reader.pages]
    return "\n".join(parts).strip()


def _decode_message_body(body: bytes) -> tuple[bytes, str, str | None]:
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
    """Runs only after the consumer has received the stream message."""
    pdf_bytes, orig_name, source_url = _decode_message_body(body)
    if not pdf_bytes:
        raise ValueError("empty pdf payload")

    text = _extract_pdf_text(pdf_bytes)
    if not text:
        raise ValueError("no extractable text from PDF")

    db_url = os.environ["DATABASE_URL"]
    local_root = Path(os.environ.get("LOCAL_PDF_STORAGE", "/data/pdfs"))
    local_root.mkdir(parents=True, exist_ok=True)
    host_base = os.environ.get("PDF_HOST_BASE_DIR", "").strip().rstrip("/\\")

    h = hashlib.sha256(f"{stream}\0{offset}\0{orig_name}".encode()).hexdigest()[:16]
    stored_name = f"{h}_{_sanitize_filename(orig_name)}"
    dest = local_root / stored_name
    dest.write_bytes(pdf_bytes)
    container_path = str(dest)
    if host_base:
        local_pc_path = str(Path(host_base) / stored_name)
    else:
        # Bind mount in compose: ./data/pdfs -> /data/pdfs (path relative to project root on host)
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


async def on_message(message: AMQPMessage | bytes, context) -> None:
    if isinstance(message, AMQPMessage):
        body = message.body
    else:
        body = message
    if not body:
        log.warning("empty body at offset %s", context.offset)
        return
    sub_name = os.environ.get("STREAM_CONSUMER_NAME", "pdf-processor")
    try:
        await asyncio.to_thread(
            process_after_stream_read,
            body,
            context.stream,
            context.offset,
        )
        if sub_name:
            await context.consumer.store_offset(
                context.stream, sub_name, context.offset
            )
    except Exception:
        log.exception("failed at offset %s (not storing offset)", context.offset)


async def run_consumer() -> None:
    host = os.environ.get("STREAM_HOST", os.environ.get("RABBITMQ_HOST", "rabbitmq"))
    port = int(os.environ.get("STREAM_PORT", "5552"))
    user = os.environ["RABBITMQ_USER"]
    pwd = os.environ["RABBITMQ_PASS"]
    vhost = os.environ.get("RABBITMQ_VHOST", "/")
    stream_name = os.environ.get("STREAM_NAME", "pdf.jobs")
    sub_name = os.environ.get("STREAM_CONSUMER_NAME", "pdf-processor")
    start = os.environ.get("STREAM_READ_OFFSET", "next").lower()
    offset_type = OffsetType.NEXT if start == "next" else OffsetType.FIRST

    consumer = Consumer(
        host=host,
        port=port,
        vhost=vhost,
        username=user,
        password=pwd,
        connection_name="pdf-stream-processor",
    )
    await consumer.start()
    await consumer.create_stream(stream_name, exists_ok=True)
    await consumer.subscribe(
        stream=stream_name,
        callback=on_message,
        decoder=amqp_decoder,
        subscriber_name=sub_name,
        offset_specification=ConsumerOffsetSpecification(offset_type, None),
    )
    log.info(
        "Subscribed to stream %r as %r (offset=%s)",
        stream_name,
        sub_name,
        start,
    )
    await consumer.run()


def main() -> None:
    asyncio.run(run_consumer())


if __name__ == "__main__":
    main()
