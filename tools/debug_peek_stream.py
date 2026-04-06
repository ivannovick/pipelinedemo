"""One-shot: scan pdf.jobs from FIRST with no subscriber name (no offset tracking). For debugging."""

from __future__ import annotations

import asyncio
import json
import os
import sys

from rstream import Consumer, amqp_decoder
from rstream.constants import ConsumerOffsetSpecification, OffsetType


async def main() -> None:
    host = os.environ.get("STREAM_HOST", os.environ.get("RABBITMQ_HOST", "rabbitmq"))
    port = int(os.environ.get("STREAM_PORT", "5552"))
    user = os.environ["RABBITMQ_USER"]
    pwd = os.environ["RABBITMQ_PASS"]
    vhost = os.environ.get("RABBITMQ_VHOST", "/")
    stream = os.environ.get("STREAM_NAME", "pdf.jobs")
    limit = int(os.environ.get("DEBUG_PEEK_LIMIT", "40"))
    timeout = float(os.environ.get("DEBUG_PEEK_TIMEOUT_SEC", "30"))

    rows: list[tuple[int, str, int]] = []

    async def on_message(message, context) -> None:
        body = message.body if hasattr(message, "body") else message
        if not body:
            rows.append((context.offset, "(empty)", 0))
            return
        if len(body) >= 4 and body[:4] == b"%PDF":
            rows.append((context.offset, "(raw PDF bytes)", len(body)))
        else:
            try:
                doc = json.loads(body.decode("utf-8"))
                name = str(doc.get("original_file_name", "?"))
                b64 = doc.get("pdf_base64") or ""
                rows.append((context.offset, name, len(b64) if isinstance(b64, str) else 0))
            except Exception as exc:
                rows.append((context.offset, f"(json err: {exc})", len(body)))
        if len(rows) >= limit:
            context.consumer.stop()

    consumer = Consumer(
        host=host,
        port=port,
        vhost=vhost,
        username=user,
        password=pwd,
        connection_name="debug-stream-peek",
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

    print(f"stream={stream!r} broker_queue_messages≈(see rabbitmqctl) scanned={len(rows)}")
    for off, name, hint in rows:
        print(f"  offset={off:>4}  file={name!r}  base64_or_body_hint={hint}")
    # Exit 1 if expected Drive trio not all present by filename
    want = {"bcom-2023.pdf", "bcom-2024.pdf", "broadcom-202510K.pdf"}
    found = {name for _, name, _ in rows if name in want}
    missing = want - found
    if missing:
        print(f"NOTE: expected filenames not seen in scan: {sorted(missing)}", file=sys.stderr)


if __name__ == "__main__":
    asyncio.run(main())
