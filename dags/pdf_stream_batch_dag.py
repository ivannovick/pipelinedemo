"""
Airflow: poll RabbitMQ Stream on a schedule, process PDF jobs (same behavior as the
former stream-processor container). Uses durable consumer name + store_offset.
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime as pendulum_datetime
from rstream import AMQPMessage, Consumer, amqp_decoder
from rstream.constants import ConsumerOffsetSpecification, OffsetType

from pdf_stream_processing import process_after_stream_read

log = logging.getLogger(__name__)


async def _consume_batch_async() -> None:
    host = os.environ.get("STREAM_HOST", os.environ.get("RABBITMQ_HOST", "rabbitmq"))
    port = int(os.environ.get("STREAM_PORT", "5552"))
    user = os.environ["RABBITMQ_USER"]
    pwd = os.environ["RABBITMQ_PASS"]
    vhost = os.environ.get("RABBITMQ_VHOST", "/")
    stream_name = os.environ.get("STREAM_NAME", "pdf.jobs")
    sub_name = os.environ.get("STREAM_CONSUMER_NAME", "pdf-processor")
    start = os.environ.get("STREAM_READ_OFFSET", "next").lower()

    poll_sec = float(os.environ.get("STREAM_BATCH_POLL_SECONDS", "50"))
    max_msgs = int(os.environ.get("STREAM_BATCH_MAX_MESSAGES", "200"))

    processed = {"count": 0}

    consumer = Consumer(
        host=host,
        port=port,
        vhost=vhost,
        username=user,
        password=pwd,
        connection_name="airflow-pdf-batch-consumer",
    )
    await consumer.start()
    try:
        await consumer.create_stream(stream_name, exists_ok=True)

        # OffsetType.NEXT alone skips all messages already in the stream when the broker has no
        # stored offset (tail-only). For batch catch-up, resume from store_offset+1 or FIRST.
        if start == "first":
            offset_spec = ConsumerOffsetSpecification(OffsetType.FIRST, None)
        elif start == "next" and sub_name:
            try:
                last = await consumer.query_offset(stream_name, sub_name)
                next_off = int(last) + 1
                offset_spec = ConsumerOffsetSpecification(OffsetType.OFFSET, next_off)
                log.info(
                    "Consumer %r resuming %r at OFFSET %s (stored was %s)",
                    sub_name,
                    stream_name,
                    next_off,
                    last,
                )
            except Exception as exc:
                log.warning(
                    "No stored offset for %r on %r (%s); reading from FIRST",
                    sub_name,
                    stream_name,
                    exc,
                )
                offset_spec = ConsumerOffsetSpecification(OffsetType.FIRST, None)
        else:
            offset_spec = ConsumerOffsetSpecification(OffsetType.NEXT, None)

        async def on_message(message: AMQPMessage | bytes, context) -> None:
            if isinstance(message, AMQPMessage):
                body = message.body
            else:
                body = message
            if not body:
                log.warning("empty body at offset %s", context.offset)
                return
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
                processed["count"] += 1
                if processed["count"] >= max_msgs:
                    context.consumer.stop()
            except Exception:
                log.exception("failed at offset %s (not storing offset)", context.offset)

        await consumer.subscribe(
            stream=stream_name,
            callback=on_message,
            decoder=amqp_decoder,
            subscriber_name=sub_name,
            offset_specification=offset_spec,
            initial_credit=min(500, max(10, max_msgs)),
        )
        log.info(
            "Batch subscribe stream=%r consumer=%r start=%s spec=%s poll=%ss max_msgs=%s",
            stream_name,
            sub_name,
            start,
            offset_spec,
            poll_sec,
            max_msgs,
        )

        # Do not use wait_for() here: on timeout it cancels run(), so consumer.stop() never
        # completes cleanly. asyncio.wait leaves the task running until we call stop().
        run_task = asyncio.create_task(consumer.run())
        done, pending = await asyncio.wait({run_task}, timeout=poll_sec)
        if pending:
            consumer.stop()
        await run_task
    finally:
        await consumer.close()

    n = processed["count"]
    log.info("Batch consumer finished, processed %s message(s)", n)


def run_pdf_stream_batch() -> None:
    asyncio.run(_consume_batch_async())


with DAG(
    dag_id="pdf_stream_batch_consumer",
    default_args={"owner": "airflow"},
    schedule=timedelta(minutes=1),
    start_date=pendulum_datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["rabbitmq-stream", "pdf", "postgres"],
) as dag:
    PythonOperator(
        task_id="consume_pdf_jobs_from_stream",
        python_callable=run_pdf_stream_batch,
    )
