"""
Serve Clarity static UI + Postgres-backed API for pdf_documents listing and full-text view.
"""

from __future__ import annotations

import os
from decimal import Decimal
from html import escape
from pathlib import Path
from typing import Any
from uuid import UUID

import psycopg
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from psycopg import errors as pg_errors
from psycopg import sql

DATABASE_URL = os.environ["DATABASE_URL"]
DBT_SCHEMA = os.environ.get("DBT_SCHEMA", "dbt_analytics")
STATIC_DIR = Path(os.environ.get("STATIC_ROOT", "/app/static")).resolve()

SILVER_MARTS: tuple[tuple[str, str], ...] = (
    ("stg_pdf_documents", "id"),
    ("pdf_documents_enriched", "id"),
    ("pdf_documents_fts", "id"),
)

BRONZE_SCHEMA = "public"
BRONZE_TABLE = "pdf_documents"

app = FastAPI(title="workflow-viz", docs=False, redoc_url=None)

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
def api_config() -> dict[str, str]:
    """UI links: set WORKFLOW_VIZ_DAG_SOURCE_BASE to GitHub blob URL prefix (no trailing slash)."""
    return {
        "dag_source_base": os.environ.get("WORKFLOW_VIZ_DAG_SOURCE_BASE", "").rstrip("/"),
        "airflow_ui_url": os.environ.get("WORKFLOW_VIZ_AIRFLOW_UI_URL", "").rstrip("/"),
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
