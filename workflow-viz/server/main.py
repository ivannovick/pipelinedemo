"""
Serve Clarity static UI + Postgres-backed API for pdf_documents listing and full-text view.
"""

from __future__ import annotations

import base64
import json
import os
import urllib.error
import urllib.parse
import urllib.request
from decimal import Decimal
from html import escape
from pathlib import Path
from typing import Any
from uuid import UUID

import psycopg
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from psycopg import errors as pg_errors
from psycopg import sql

DATABASE_URL = os.environ["DATABASE_URL"]
DBT_SCHEMA = os.environ.get("DBT_SCHEMA", "dbt_analytics")
STATIC_DIR = Path(os.environ.get("STATIC_ROOT", "/app/static")).resolve()

# When WORKFLOW_VIZ_DAG_SOURCE_BASE is unset (e.g. local uvicorn), still offer GitHub file links.
_DEFAULT_DAG_SOURCE_BASE = "https://github.com/ivannovick/pipelinedemo/blob/main/dags"

SILVER_MARTS: tuple[tuple[str, str], ...] = (
    ("stg_pdf_documents", "id"),
    ("pdf_documents_enriched", "id"),
    ("pdf_documents_fts", "id"),
)

BRONZE_SCHEMA = "public"
BRONZE_TABLE = "pdf_documents"

RABBITMQ_MANAGEMENT_URL = os.environ.get("RABBITMQ_MANAGEMENT_URL", "http://rabbitmq:15672").rstrip("/")
RABBITMQ_API_USER = os.environ.get("RABBITMQ_USER", "guest")
RABBITMQ_API_PASS = os.environ.get("RABBITMQ_PASS", "guest")
RABBITMQ_VHOST = os.environ.get("RABBITMQ_VHOST", "/")
STREAM_QUEUE_NAME = os.environ.get("STREAM_NAME", "pdf.jobs")
STREAM_RESET_SECRET = os.environ.get("WORKFLOW_VIZ_STREAM_RESET_SECRET", "").strip()

app = FastAPI(title="workflow-viz", docs=False, redoc_url=None)


def _rabbitmq_mgmt_request(
    method: str,
    path: str,
    *,
    data: dict[str, Any] | None = None,
    timeout: float = 15.0,
) -> tuple[int, bytes]:
    """HTTP call to RabbitMQ management plugin (path starts with /api/)."""
    url = f"{RABBITMQ_MANAGEMENT_URL}{path}"
    payload = json.dumps(data).encode("utf-8") if data is not None else None
    req = urllib.request.Request(url, data=payload, method=method.upper())
    tok = base64.b64encode(f"{RABBITMQ_API_USER}:{RABBITMQ_API_PASS}".encode()).decode("ascii")
    req.add_header("Authorization", f"Basic {tok}")
    if payload is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.getcode() or 200, resp.read()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read()


def _queue_api_path() -> str:
    v = urllib.parse.quote(RABBITMQ_VHOST, safe="")
    q = urllib.parse.quote(STREAM_QUEUE_NAME, safe="")
    return f"/api/queues/{v}/{q}"


@app.get("/api/stream/metrics")
def stream_metrics() -> dict[str, Any]:
    """
    Stream queue stats from RabbitMQ management API (messages, bytes, consumers).
    """
    path = _queue_api_path()
    code, body = _rabbitmq_mgmt_request("GET", path)
    base: dict[str, Any] = {
        "ok": code == 200,
        "stream_name": STREAM_QUEUE_NAME,
        "vhost": RABBITMQ_VHOST,
        "management_base": RABBITMQ_MANAGEMENT_URL,
        "exists": False,
        "error": None,
    }
    if code == 404:
        base["exists"] = False
        base["queue_name"] = STREAM_QUEUE_NAME
        base["vhost"] = RABBITMQ_VHOST
        base["messages"] = 0
        base["messages_ready"] = 0
        base["message_bytes"] = 0
        base["memory_bytes"] = None
        base["segments"] = None
        base["payload_bytes_in_api"] = False
        base["stream_payload_bytes_not_in_management_api"] = False
        base["consumers"] = 0
        base["state"] = None
        base["queue_type"] = None
        base["consumer_details"] = []
        return base
    if code != 200:
        err_txt = body.decode("utf-8", errors="replace")[:500]
        base["ok"] = False
        base["error"] = f"RabbitMQ management HTTP {code}: {err_txt}"
        return base
    try:
        qj = json.loads(body.decode("utf-8"))
    except json.JSONDecodeError as exc:
        base["ok"] = False
        base["error"] = f"Invalid JSON from management API: {exc}"
        return base
    args = qj.get("arguments") or {}
    qtype = args.get("x-queue-type") or qj.get("type")
    consumers = qj.get("consumers")
    consumer_details = qj.get("consumer_details") or []
    messages = int(qj.get("messages") or 0)
    messages_ready = int(qj.get("messages_ready") or messages)
    # Stream queues often omit message_*_bytes in GET /api/queues (only classic queues fill them).
    mb_raw = qj.get("message_bytes")
    mbr_raw = qj.get("message_bytes_ready")
    message_bytes = int(mb_raw) if mb_raw is not None else 0
    if message_bytes == 0 and mbr_raw is not None:
        message_bytes = int(mbr_raw)
    payload_bytes_reported = mb_raw is not None or mbr_raw is not None
    memory = qj.get("memory")
    memory_bytes = int(memory) if memory is not None else None
    segments = qj.get("segments")
    segments_int = int(segments) if segments is not None else None
    qname = qj.get("name") or STREAM_QUEUE_NAME
    vhost_name = qj.get("vhost") or RABBITMQ_VHOST
    stream_limitation = (qtype == "stream" or args.get("x-queue-type") == "stream") and not payload_bytes_reported
    base.update(
        {
            "exists": True,
            "queue_name": qname,
            "vhost": vhost_name,
            "messages": messages,
            "messages_ready": messages_ready,
            "messages_unacked": int(qj.get("messages_unacknowledged") or 0),
            "message_bytes": message_bytes,
            "message_bytes_ready": int(qj.get("message_bytes_ready") or 0)
            if qj.get("message_bytes_ready") is not None
            else 0,
            "message_bytes_unacked": int(qj.get("message_bytes_unacknowledged") or 0)
            if qj.get("message_bytes_unacknowledged") is not None
            else 0,
            "payload_bytes_in_api": payload_bytes_reported,
            "memory_bytes": memory_bytes,
            "segments": segments_int,
            "consumers": int(consumers) if consumers is not None else len(consumer_details),
            "state": qj.get("state"),
            "queue_type": qtype,
            "durable": qj.get("durable"),
            "stream_payload_bytes_not_in_management_api": stream_limitation,
            "consumer_details": [
                {
                    "consumer_tag": c.get("consumer_tag"),
                    "channel_details": (c.get("channel_details") or {}).get("name"),
                }
                for c in consumer_details[:20]
            ],
        }
    )
    return base


class StreamClearBody(BaseModel):
    secret: str = Field(default="", max_length=256)


@app.post("/api/stream/clear")
def stream_clear(body: StreamClearBody) -> dict[str, Any]:
    """
    DELETE the stream queue (all retained messages). Next publisher run recreates it.
    Requires WORKFLOW_VIZ_STREAM_RESET_SECRET and matching JSON body {"secret": "..."}.
    """
    if not STREAM_RESET_SECRET:
        raise HTTPException(
            status_code=503,
            detail="Stream reset disabled: set WORKFLOW_VIZ_STREAM_RESET_SECRET in the workflow-viz service.",
        )
    if body.secret != STREAM_RESET_SECRET:
        raise HTTPException(status_code=403, detail="Invalid secret.")

    path = _queue_api_path()
    code, resp_body = _rabbitmq_mgmt_request("DELETE", path)
    if code in (204, 404):
        return {
            "ok": True,
            "deleted": code == 204,
            "detail": "Stream queue deleted." if code == 204 else "Queue did not exist (already empty or never created).",
        }
    err_txt = resp_body.decode("utf-8", errors="replace")[:500]
    raise HTTPException(status_code=502, detail=f"RabbitMQ management HTTP {code}: {err_txt}")

# Full bronze row shape; cap extracted_text size for the list endpoint (full text via /text/{id}).
LIST_SQL = """
SELECT
    id,
    source_file_url,
    original_file_name,
    stored_file_name,
    container_path,
    local_pc_archive_path,
    left(coalesce(extracted_text, ''), 8000) AS extracted_text,
    source_stream,
    stream_offset,
    created_at
FROM public.pdf_documents
ORDER BY id DESC
LIMIT 500
"""

TEXT_SQL = """
SELECT original_file_name, extracted_text
FROM public.pdf_documents
WHERE id = %s
"""


def _json_cell(name: str, value: Any) -> Any:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, (bytes, memoryview)):
        return f"<{len(value)} bytes>"
    if isinstance(value, (int, float, bool)):
        return value
    if isinstance(value, str):
        if name == "search_vector" and len(value) > 160:
            return value[:160] + "…"
        return value
    s = str(value)
    if name == "search_vector" and len(s) > 160:
        return s[:160] + "…"
    return s


def _row_to_dict(columns: list[str], row: tuple[Any, ...]) -> dict[str, Any]:
    return {c: _json_cell(c, v) for c, v in zip(columns, row, strict=True)}


@app.get("/api/pdf-documents")
def list_pdf_documents() -> dict:
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(LIST_SQL)
                cols = [d.name for d in cur.description] if cur.description else []
                rows = cur.fetchall()
    except psycopg.Error as exc:
        raise HTTPException(status_code=503, detail=f"database error: {exc}") from exc

    items = []
    for row in rows:
        d = _row_to_dict(cols, row)
        if d.get("created_at") is not None and hasattr(d["created_at"], "isoformat"):
            d["created_at"] = d["created_at"].isoformat()
        doc_id = d.get("id")
        if doc_id is not None:
            d["full_text_path"] = f"/text/{doc_id}"
        items.append(d)
    return {
        "schema": BRONZE_SCHEMA,
        "table": BRONZE_TABLE,
        "columns": cols,
        "items": items,
        "extracted_text_note": "List caps extracted_text at 8000 chars; use Full text for the full column.",
    }


@app.get("/api/silver/marts")
def list_silver_marts() -> dict:
    """dbt models in DBT_SCHEMA (default dbt_analytics)."""
    result: dict[str, dict[str, Any]] = {}
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                for table, order_col in SILVER_MARTS:
                    try:
                        query = sql.SQL(
                            "SELECT * FROM {}.{} ORDER BY {} DESC NULLS LAST LIMIT 500"
                        ).format(
                            sql.Identifier(DBT_SCHEMA),
                            sql.Identifier(table),
                            sql.Identifier(order_col),
                        )
                        cur.execute(query)
                        cols = [d.name for d in cur.description] if cur.description else []
                        raw = cur.fetchall()
                        rows = [_row_to_dict(cols, r) for r in raw]
                        result[table] = {
                            "schema": DBT_SCHEMA,
                            "columns": cols,
                            "rows": rows,
                            "error": None,
                        }
                    except pg_errors.UndefinedTable:
                        result[table] = {
                            "schema": DBT_SCHEMA,
                            "columns": [],
                            "rows": [],
                            "error": "Table not found — run the dbt DAG or check DBT_SCHEMA.",
                        }
                    except psycopg.Error as exc:
                        result[table] = {
                            "schema": DBT_SCHEMA,
                            "columns": [],
                            "rows": [],
                            "error": str(exc),
                        }
    except psycopg.Error as exc:
        raise HTTPException(status_code=503, detail=f"database error: {exc}") from exc

    return {"marts": result, "schema": DBT_SCHEMA}


@app.get("/api/config")
def api_config() -> dict[str, Any]:
    """UI links: WORKFLOW_VIZ_DAG_SOURCE_BASE = GitHub .../blob/<branch>/dags (no trailing slash)."""
    raw = os.environ.get("WORKFLOW_VIZ_DAG_SOURCE_BASE")
    if raw is None or not str(raw).strip():
        dag_base = _DEFAULT_DAG_SOURCE_BASE
    else:
        dag_base = str(raw).strip()
    return {
        "dag_source_base": dag_base.rstrip("/"),
        "airflow_ui_url": os.environ.get("WORKFLOW_VIZ_AIRFLOW_UI_URL", "").rstrip("/"),
        "rabbitmq_ui_url": os.environ.get("WORKFLOW_VIZ_RABBITMQ_UI_URL", "").rstrip("/"),
        "stream_clear_enabled": bool(STREAM_RESET_SECRET),
        "stream_name": STREAM_QUEUE_NAME,
        "rabbitmq_vhost": RABBITMQ_VHOST,
    }


@app.get("/api/search")
def api_search(q: str = Query("", max_length=400)) -> dict[str, Any]:
    """
    Full-text search over extracted text when pdf_documents_fts exists; else ILIKE on bronze table.
    """
    q = (q or "").strip()
    if not q:
        return {"mode": "empty", "items": [], "detail": "Enter a search query."}

    fts_query = sql.SQL(
        "SELECT d.id, d.original_file_name, "
        "ts_rank_cd(f.search_vector, query) AS rank, "
        "ts_headline('english', left(coalesce(d.extracted_text, ''), 500000), query, "
        "'StartSel=>>, MaxWords=35, MinWords=3') AS headline "
        "FROM {}.pdf_documents_fts f "
        "INNER JOIN public.pdf_documents d ON d.id = f.id "
        "CROSS JOIN LATERAL websearch_to_tsquery('english', {}) AS query "
        "WHERE f.search_vector @@ query "
        "ORDER BY rank DESC NULLS LAST LIMIT 40"
    ).format(sql.Identifier(DBT_SCHEMA), sql.Literal(q))

    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(fts_query)
                    cols = [d.name for d in cur.description]
                    raw = cur.fetchall()
                    items = [_row_to_dict(cols, r) for r in raw]
                    return {"mode": "fts", "items": items}
                except (pg_errors.UndefinedTable, pg_errors.UndefinedColumn):
                    detail = "FTS mart not built yet; substring match on bronze."
                except psycopg.Error:
                    detail = "FTS query could not run; substring match on bronze."

                pat = f"%{q}%"
                cur.execute(
                    """
                    SELECT id, original_file_name, NULL::double precision AS rank,
                           left(coalesce(extracted_text, ''), 400) AS headline
                    FROM public.pdf_documents
                    WHERE extracted_text ILIKE %s OR original_file_name ILIKE %s
                    ORDER BY id DESC
                    LIMIT 40
                    """,
                    (pat, pat),
                )
                cols = [d.name for d in cur.description]
                raw = cur.fetchall()
                items = [_row_to_dict(cols, r) for r in raw]
                out: dict[str, Any] = {"mode": "ilike", "items": items}
                if detail:
                    out["detail"] = detail
                return out
    except psycopg.Error as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.get("/text/{doc_id}", response_class=HTMLResponse)
def full_extracted_text(doc_id: int) -> HTMLResponse:
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(TEXT_SQL, (doc_id,))
                row = cur.fetchone()
    except psycopg.Error as exc:
        raise HTTPException(status_code=503, detail=f"database error: {exc}") from exc

    if not row:
        raise HTTPException(status_code=404, detail="document not found")

    name, text = row[0] or "document.pdf", row[1] or ""
    safe_name = escape(name)
    safe_text = escape(text)

    page = f"""<!DOCTYPE html>
<html lang="en" cds-theme="light">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Extracted text — {safe_name}</title>
  <link rel="stylesheet" href="https://unpkg.com/@cds/core@6.9.2/global.min.css" crossorigin="anonymous" />
  <link rel="stylesheet" href="https://unpkg.com/@cds/city@1.1.0/css/bundles/default.min.css" crossorigin="anonymous" />
  <link rel="stylesheet" href="/styles.css" />
  <style>
    .text-page {{ max-width: 960px; margin: 0 auto; padding: var(--cds-global-layout-space-xl); }}
    .text-page pre {{
      white-space: pre-wrap; word-break: break-word;
      font-family: var(--cds-global-typography-monospace-font-family);
      font-size: var(--cds-global-typography-secondary-font-size);
      line-height: 1.5;
      margin-top: var(--cds-global-layout-space-md);
      padding: var(--cds-global-layout-space-md);
      background: var(--cds-alias-object-container-background-tint);
      border-radius: var(--cds-alias-object-border-radius-100);
    }}
    .text-page .back {{ margin-bottom: var(--cds-global-layout-space-md); }}
  </style>
</head>
<body cds-text="body">
  <div class="text-page">
    <p class="back"><a href="/" cds-text="link">← Back to pipeline</a></p>
    <h1 cds-text="title">{safe_name}</h1>
    <p cds-text="secondary">Full <code>extracted_text</code> from <code>public.pdf_documents</code> (id={doc_id}).</p>
    <pre>{safe_text}</pre>
  </div>
</body>
</html>"""
    return HTMLResponse(page)


if not STATIC_DIR.is_dir():
    raise RuntimeError(f"STATIC_DIR is not a directory: {STATIC_DIR}")

app.mount("/", StaticFiles(directory=str(STATIC_DIR), html=True), name="static")
