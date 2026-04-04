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
from pathlib import Path

from dlt.common.configuration.specs.gcp_credentials import GcpServiceAccountCredentials
from dlt.sources.filesystem import filesystem
from rstream import AMQPMessage, Producer

log = logging.getLogger(__name__)

# In Docker, ./dlt is mounted at /opt/airflow/.dlt
_DEFAULT_SA_JSON = Path("/opt/airflow/.dlt/service_account.json")


def _credentials_from_service_account_json(path: Path) -> GcpServiceAccountCredentials:
    data = json.loads(path.read_text(encoding="utf-8"))
    if data.get("type") != "service_account":
        raise ValueError(f"{path}: expected type service_account")
    creds = GcpServiceAccountCredentials()
    creds["client_email"] = data["client_email"]
    creds["private_key"] = data["private_key"]
    creds["project_id"] = data["project_id"]
    return creds


def _load_gdrive_credentials() -> GcpServiceAccountCredentials | None:
    paths: list[Path] = []
    env = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if env:
        paths.append(Path(env))
    paths.append(_DEFAULT_SA_JSON)
    for path in paths:
        try:
            if path.is_file():
                log.info("Using Google credentials from %s", path)
                return _credentials_from_service_account_json(path)
        except OSError:
            continue
    return None


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

    gcp_creds = _load_gdrive_credentials()
    if gcp_creds is not None:
        src = filesystem(
            bucket_url=bucket,
            file_glob="**/*.pdf",
            credentials=gcp_creds,
        )
    else:
        log.info(
            "No service_account.json or GOOGLE_APPLICATION_CREDENTIALS; "
            "using dlt secrets.toml [sources.filesystem.credentials]"
        )
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
