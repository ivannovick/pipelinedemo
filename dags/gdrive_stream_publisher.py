"""
Ingest only: read PDFs from Google Drive (dlt) and publish to RabbitMQ Streams.
Does not parse PDFs or write Postgres/disk — the stream-processor service does that
after it consumes each message.
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os

from dlt.sources.filesystem import filesystem
from rstream import AMQPMessage, Producer

log = logging.getLogger(__name__)


async def publish_pdfs_to_stream() -> None:
    bucket = os.environ["GDRIVE_BUCKET_URL"]
    stream_name = os.environ.get("STREAM_NAME", "pdf.jobs")
    host = os.environ.get("STREAM_HOST", "rabbitmq")
    port = int(os.environ.get("STREAM_PORT", "5552"))
    user = os.environ["RABBITMQ_USER"]
    pwd = os.environ["RABBITMQ_PASS"]
    vhost = os.environ.get("RABBITMQ_VHOST", "/")

    producer = Producer(
        host=host,
        port=port,
        vhost=vhost,
        username=user,
        password=pwd,
    )
    await producer.start()
    await producer.create_stream(stream_name, exists_ok=True)

    src = filesystem(bucket_url=bucket, file_glob="**/*.pdf")
    n = 0
    try:
        for item in src:
            name = str(item.get("file_name", ""))
            if not name.lower().endswith(".pdf"):
                continue
            try:
                data = item.read_bytes()
            except Exception:
                log.exception("read_bytes failed for %s", item.get("file_url"))
                continue
            envelope = {
                "original_file_name": name,
                "source_file_url": str(item.get("file_url", "")),
                "pdf_base64": base64.b64encode(data).decode("ascii"),
            }
            body = json.dumps(envelope).encode("utf-8")
            await producer.send(
                stream=stream_name,
                message=AMQPMessage(body=body),
            )
            n += 1
            log.info("Published to stream %s: %s (%d bytes)", stream_name, name, len(data))
    finally:
        await producer.close()

    if n == 0:
        log.warning("No PDFs published (none found or unreadable).")
    else:
        log.info("Published %d message(s) to stream %s", n, stream_name)


def run_publish_pdfs_to_stream() -> None:
    asyncio.run(publish_pdfs_to_stream())
