"""One-shot: publish a local PDF to RabbitMQ Stream (same envelope as gdrive_publish_to_stream).

Run inside the Airflow container (has rstream), e.g.:
  docker compose exec airflow python /opt/airflow/tools/publish_stream_once.py /opt/airflow/testdata/sample.pdf
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
import sys
from pathlib import Path

from rstream import AMQPMessage, Producer


async def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: publish_stream_once.py /path/to/file.pdf", file=sys.stderr)
        sys.exit(1)
    path = Path(sys.argv[1])
    if not path.is_file():
        print(f"Not a file: {path}", file=sys.stderr)
        sys.exit(1)

    data = path.read_bytes()
    stream_name = os.environ.get("STREAM_NAME", "pdf.jobs")
    host = os.environ.get("STREAM_HOST", os.environ.get("RABBITMQ_HOST", "rabbitmq"))
    port = int(os.environ.get("STREAM_PORT", "5552"))
    user = os.environ["RABBITMQ_USER"]
    pwd = os.environ["RABBITMQ_PASS"]
    vhost = os.environ.get("RABBITMQ_VHOST", "/")

    envelope = {
        "original_file_name": path.name,
        "source_file_url": f"file://{path.resolve()}",
        "pdf_base64": base64.b64encode(data).decode("ascii"),
    }
    body = json.dumps(envelope).encode("utf-8")

    producer = Producer(
        host=host,
        port=port,
        vhost=vhost,
        username=user,
        password=pwd,
    )
    await producer.start()
    try:
        await producer.create_stream(stream_name, exists_ok=True)
        await producer.send(stream=stream_name, message=AMQPMessage(body=body))
        print(f"Published {path.name} ({len(data)} bytes) to stream {stream_name!r}")
    finally:
        await producer.close()


if __name__ == "__main__":
    asyncio.run(main())
