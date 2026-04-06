"""After gdrive_publish_to_stream: confirm RabbitMQ stream has PDF job JSON messages.

Uses a no-name subscriber and FIRST offset (peek only; does not advance consumer offsets).

Exit codes: 0 if at least MIN_STREAM_JOBS valid envelopes found, 1 otherwise.

Run in Airflow container:
  docker compose exec airflow python /opt/airflow/tools/validate_pdf_stream.py
"""

from __future__ import annotations

import asyncio
import json
import os
import sys

from rstream import Consumer, amqp_decoder
from rstream.constants import ConsumerOffsetSpecification, OffsetType


def _valid_envelope(body: bytes) -> bool:
    if not body or (len(body) >= 4 and body[:4] == b"%PDF"):
        return False
    try:
        doc = json.loads(body.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return False
    b64 = doc.get("pdf_base64")
    name = doc.get("original_file_name")
    return isinstance(b64, str) and len(b64) > 0 and isinstance(name, str)


async def main() -> int:
    host = os.environ.get("STREAM_HOST", os.environ.get("RABBITMQ_HOST", "rabbitmq"))
    port = int(os.environ.get("STREAM_PORT", "5552"))
    user = os.environ["RABBITMQ_USER"]
    pwd = os.environ["RABBITMQ_PASS"]
    vhost = os.environ.get("RABBITMQ_VHOST", "/")
    stream = os.environ.get("STREAM_NAME", "pdf.jobs")
    min_jobs = int(os.environ.get("MIN_STREAM_JOBS", "1"))
    limit = int(os.environ.get("VALIDATE_PEEK_LIMIT", "500"))
    timeout = float(os.environ.get("VALIDATE_PEEK_TIMEOUT_SEC", "45"))

    valid = 0
    scanned = 0

    async def on_message(message, context) -> None:
        nonlocal valid, scanned
        body = message.body if hasattr(message, "body") else message
        scanned += 1
        if isinstance(body, (bytes, bytearray)) and _valid_envelope(bytes(body)):
            valid += 1
        if scanned >= limit or valid >= min_jobs:
            context.consumer.stop()

    consumer = Consumer(
        host=host,
        port=port,
        vhost=vhost,
        username=user,
        password=pwd,
        connection_name="validate-pdf-stream",
    )
    await consumer.start()
    try:
        await consumer.subscribe(
            stream=stream,
            callback=on_message,
            decoder=amqp_decoder,
            subscriber_name=None,
            offset_specification=ConsumerOffsetSpecification(OffsetType.FIRST, None),
            initial_credit=min(500, max(50, limit * 2)),
        )
        run_task = asyncio.create_task(consumer.run())
        done, _ = await asyncio.wait({run_task}, timeout=timeout)
        if not done:
            consumer.stop()
        await run_task
    finally:
        await consumer.close()

    print(
        f"stream={stream!r} valid_pdf_job_messages={valid} messages_scanned={scanned} "
        f"(need>={min_jobs})"
    )
    if valid >= min_jobs:
        return 0
    print(
        "Validation failed: no (or not enough) JSON jobs with pdf_base64 in the stream. "
        "Check GDRIVE_BUCKET_URL, credentials, and gdrive_publish_to_stream logs.",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
