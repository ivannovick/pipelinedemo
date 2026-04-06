/**
 * Pipeline viz: flow, DAG links, Bronze/Silver tables, search.
 */

const STEPS = [
  "step-drive",
  "step-publish-dag",
  "step-publisher-lib",
  "step-stream",
  "step-consumer-dag",
  "step-processing-lib",
  "step-postgres",
  "step-disk",
  "step-dbt-dag",
  "step-silver-layer",
];

const SILVER_MAP = [
  { key: "stg_pdf_documents", slug: "stg", kind: "view" },
  { key: "pdf_documents_enriched", slug: "enriched", kind: "table" },
  { key: "pdf_documents_fts", slug: "fts", kind: "table" },
];

/** Matches server default when env is blank or /api/config fails. */
const DEFAULT_DAG_SOURCE_BASE =
  "https://github.com/ivannovick/pipelinedemo/blob/main/dags";

const DAGS = [
  {
    id: "gdrive_publish_to_stream",
    file: "gdrive_publish_to_stream.py",
    role: "Drive → RabbitMQ stream",
  },
  {
    id: "pdf_stream_batch_consumer",
    file: "pdf_stream_batch_dag.py",
    role: "Stream → Postgres + disk",
  },
  {
    id: "dbt_run_pdf_archive",
    file: "dbt_run_pdf_archive.py",
    role: "dbt transforms (Silver)",
  },
];

const MODULES = [
  {
    file: "gdrive_stream_publisher.py",
    role: "dlt ingest + publish (used by gdrive DAG)",
  },
  {
    file: "pdf_stream_processing.py",
    role: "Decode PDF, INSERT bronze (used by batch DAG)",
  },
];

let appConfig = {
  dag_source_base: DEFAULT_DAG_SOURCE_BASE,
  airflow_ui_url: "",
  rabbitmq_ui_url: "",
  stream_clear_enabled: false,
  stream_name: "pdf.jobs",
  rabbitmq_vhost: "/",
};

const STREAM_BYTES_BAR_CAP = 50 * 1024 * 1024;
/** Bar full at this many messages (visual scale only). */
const STREAM_MESSAGES_BAR_CAP = 50;

function formatBytes(n) {
  if (n == null || Number.isNaN(n)) return "—";
  const x = Number(n);
  if (x === 0) return "0 B";
  const u = ["B", "KiB", "MiB", "GiB"];
  let i = 0;
  let v = x;
  while (v >= 1024 && i < u.length - 1) {
    v /= 1024;
    i += 1;
  }
  return `${v < 10 && i > 0 ? v.toFixed(1) : Math.round(v)} ${u[i]}`;
}

function setActive(id) {
  STEPS.forEach((sid) => {
    const el = document.getElementById(sid);
    if (el) el.dataset.active = sid === id ? "true" : "false";
  });
}

function wireStepFocus() {
  STEPS.forEach((id) => {
    const el = document.getElementById(id);
    if (!el) return;
    el.addEventListener("mouseenter", () => setActive(id));
    el.addEventListener("focusin", () => setActive(id));
  });
  document.querySelector("#section-pipeline")?.addEventListener("mouseleave", () => {
    STEPS.forEach((sid) => {
      const el = document.getElementById(sid);
      if (el) el.dataset.active = "false";
    });
  });
}

function wireThemeToggle() {
  const btn = document.getElementById("theme-toggle");
  if (!btn) return;
  const root = document.documentElement;
  btn.addEventListener("click", () => {
    const next = root.getAttribute("cds-theme") === "dark" ? "light" : "dark";
    root.setAttribute("cds-theme", next);
    btn.textContent = next === "dark" ? "Light mode" : "Dark mode";
  });
}

function cellText(value) {
  const td = document.createElement("td");
  if (value == null || value === "") {
    td.textContent = "—";
  } else {
    td.textContent = String(value);
  }
  return td;
}

function cellTrunc(value, maxLen) {
  const td = document.createElement("td");
  const s = value == null || value === "" ? "" : String(value);
  if (!s) {
    td.textContent = "—";
    return td;
  }
  if (s.length <= maxLen) {
    td.textContent = s;
    return td;
  }
  td.textContent = `${s.slice(0, maxLen)}…`;
  td.title = s;
  return td;
}

function cellCode(s) {
  const td = document.createElement("td");
  const code = document.createElement("code");
  code.textContent = s;
  td.appendChild(code);
  return td;
}

function sourceLinkCell(base, filename) {
  const td = document.createElement("td");
  if (!base) {
    td.textContent = "—";
    return td;
  }
  const a = document.createElement("a");
  a.href = `${base.replace(/\/$/, "")}/${filename}`;
  a.target = "_blank";
  a.rel = "noopener noreferrer";
  a.textContent = "Open";
  a.title = a.href;
  a.setAttribute("cds-text", "link");
  td.appendChild(a);
  return td;
}

function escapeHtml(s) {
  const d = document.createElement("div");
  d.textContent = s;
  return d.innerHTML;
}

async function loadConfig() {
  try {
    const r = await fetch("/api/config");
    if (r.ok) {
      const j = await r.json();
      appConfig = {
        dag_source_base: (
          String(j.dag_source_base ?? "").trim() || DEFAULT_DAG_SOURCE_BASE
        ).replace(/\/$/, ""),
        airflow_ui_url: (j.airflow_ui_url || "").replace(/\/$/, ""),
        rabbitmq_ui_url: (j.rabbitmq_ui_url || "").replace(/\/$/, ""),
        stream_clear_enabled: Boolean(j.stream_clear_enabled),
        stream_name: j.stream_name || "pdf.jobs",
        rabbitmq_vhost: j.rabbitmq_vhost != null ? String(j.rabbitmq_vhost) : "/",
      };
    }
  } catch (_) {
    /* keep defaults */
  }
  renderDagSection();
  applyStreamSectionConfig();
}

function applyStreamSectionConfig() {
  const rmWrap = document.getElementById("rabbitmq-ui-wrap");
  if (rmWrap) {
    if (appConfig.rabbitmq_ui_url) {
      rmWrap.hidden = false;
      const u = escapeHtml(appConfig.rabbitmq_ui_url);
      rmWrap.innerHTML = `RabbitMQ management UI: <a href="${u}" target="_blank" rel="noopener">${u}</a> (queues → stream <code>${escapeHtml(appConfig.stream_name)}</code>)`;
    } else {
      rmWrap.hidden = true;
      rmWrap.textContent = "";
    }
  }
  const nameEl = document.getElementById("stream-name-label");
  if (nameEl) nameEl.textContent = appConfig.stream_name;
  const vhostEl = document.getElementById("stream-vhost-label");
  if (vhostEl) vhostEl.textContent = appConfig.rabbitmq_vhost || "/";
  const clearPanel = document.getElementById("stream-clear-panel");
  if (clearPanel) clearPanel.hidden = !appConfig.stream_clear_enabled;
}

function renderDagSection() {
  const base = appConfig.dag_source_base;
  const hint = document.getElementById("dag-links-hint");
  const airflowWrap = document.getElementById("airflow-ui-wrap");
  if (hint) {
    if (!base) {
      hint.hidden = false;
      hint.textContent =
        "Set WORKFLOW_VIZ_DAG_SOURCE_BASE in Compose (GitHub blob URL to your dags/ folder, no trailing slash) to enable “View source” links.";
    } else {
      hint.hidden = true;
    }
  }
  if (airflowWrap) {
    if (appConfig.airflow_ui_url) {
      airflowWrap.hidden = false;
      const u = escapeHtml(appConfig.airflow_ui_url);
      airflowWrap.innerHTML = `Airflow UI: <a href="${u}" target="_blank" rel="noopener">${u}</a>`;
    } else {
      airflowWrap.hidden = true;
    }
  }

  const dagBody = document.getElementById("dag-tbody");
  const modBody = document.getElementById("module-tbody");
  if (!dagBody || !modBody) return;
  dagBody.replaceChildren();
  for (const d of DAGS) {
    const tr = document.createElement("tr");
    tr.appendChild(cellText(d.id));
    tr.appendChild(cellCode(d.file));
    tr.appendChild(cellText(d.role));
    tr.appendChild(sourceLinkCell(base, d.file));
    dagBody.appendChild(tr);
  }
  modBody.replaceChildren();
  for (const m of MODULES) {
    const tr = document.createElement("tr");
    tr.appendChild(cellCode(m.file));
    tr.appendChild(cellText(m.role));
    tr.appendChild(sourceLinkCell(base, m.file));
    modBody.appendChild(tr);
  }
}

function setSilverMartTitles(schema) {
  for (const { key, slug, kind } of SILVER_MAP) {
    const el = document.getElementById(`silver-title-${slug}`);
    if (el) {
      el.innerHTML = `<code>${escapeHtml(schema)}.${escapeHtml(key)}</code> <span class="mart-kind">${kind}</span>`;
    }
  }
}

/** Bronze: columns from API (full table shape) + link to full extracted_text. */
function renderBronzeTable(thead, tbody, columns, items) {
  thead.replaceChildren();
  tbody.replaceChildren();

  const truncCols = (name) =>
    /url|path|vector|text/i.test(name) && !/^id$/i.test(name);

  const hr = document.createElement("tr");
  for (const c of columns) {
    const th = document.createElement("th");
    th.textContent = c;
    hr.appendChild(th);
  }
  const thLink = document.createElement("th");
  thLink.textContent = "Full text";
  hr.appendChild(thLink);
  thead.appendChild(hr);

  const ncol = columns.length + 1;

  if (items.length === 0) {
    const tr = document.createElement("tr");
    const td = document.createElement("td");
    td.colSpan = Math.max(ncol, 1);
    td.className = "doc-table-placeholder";
    td.textContent = "No rows yet.";
    tr.appendChild(td);
    tbody.appendChild(tr);
    return;
  }

  for (const item of items) {
    const tr = document.createElement("tr");
    for (const col of columns) {
      const v = item[col];
      if (truncCols(col) && v != null && String(v).length > 96) {
        tr.appendChild(cellTrunc(v, 92));
      } else {
        tr.appendChild(cellText(v));
      }
    }
    const linkTd = document.createElement("td");
    const a = document.createElement("a");
    a.href = item.full_text_path || `/text/${item.id}`;
    a.textContent = "Open";
    a.setAttribute("cds-text", "link");
    linkTd.appendChild(a);
    tr.appendChild(linkTd);
    tbody.appendChild(tr);
  }
}

function renderDynamicTable(thead, tbody, columns, rows) {
  thead.replaceChildren();
  tbody.replaceChildren();
  const ncol = columns.length;
  if (ncol === 0) {
    const tr = document.createElement("tr");
    const td = document.createElement("td");
    td.colSpan = 1;
    td.className = "doc-table-placeholder";
    td.textContent = "No columns.";
    tr.appendChild(td);
    tbody.appendChild(tr);
    return;
  }
  const hr = document.createElement("tr");
  for (const c of columns) {
    const th = document.createElement("th");
    th.textContent = c;
    hr.appendChild(th);
  }
  thead.appendChild(hr);

  if (rows.length === 0) {
    const tr = document.createElement("tr");
    const td = document.createElement("td");
    td.colSpan = ncol;
    td.className = "doc-table-placeholder";
    td.textContent = "No rows.";
    tr.appendChild(td);
    tbody.appendChild(tr);
    return;
  }

  const truncCols = (name) =>
    /url|path|vector|text/i.test(name) && !/^id$/i.test(name);

  for (const row of rows) {
    const tr = document.createElement("tr");
    for (const col of columns) {
      const v = row[col];
      if (truncCols(col) && v != null && String(v).length > 48) {
        tr.appendChild(cellTrunc(v, 44));
      } else {
        tr.appendChild(cellText(v));
      }
    }
    tbody.appendChild(tr);
  }
}

async function loadBronze() {
  const thead = document.getElementById("bronze-thead");
  const tbody = document.getElementById("docs-tbody");
  const errEl = document.getElementById("docs-error");
  const hint = document.getElementById("bronze-schema-hint");
  if (!thead || !tbody || !errEl) return;

  errEl.hidden = true;
  errEl.textContent = "";
  thead.replaceChildren();
  tbody.replaceChildren();
  const loading = document.createElement("tr");
  const loadingTd = document.createElement("td");
  loadingTd.className = "doc-table-placeholder";
  loadingTd.textContent = "Loading…";
  loading.appendChild(loadingTd);
  tbody.appendChild(loading);

  try {
    const res = await fetch("/api/pdf-documents");
    if (!res.ok) {
      const detail = await res.json().catch(() => ({}));
      let msg = `${res.status} ${res.statusText}`;
      if (detail.detail != null) {
        msg =
          typeof detail.detail === "string"
            ? detail.detail
            : JSON.stringify(detail.detail);
      }
      throw new Error(msg);
    }
    const data = await res.json();
    const schema = data.schema || "public";
    const table = data.table || "pdf_documents";
    const columns = data.columns || [];
    const items = data.items || [];

    if (hint) {
      const note = data.extracted_text_note
        ? ` ${escapeHtml(data.extracted_text_note)}`
        : "";
      hint.innerHTML = `<code>${escapeHtml(schema)}.${escapeHtml(table)}</code> — stream consumer landing table; columns match Postgres.${note}`;
    }

    renderBronzeTable(thead, tbody, columns, items);
  } catch (e) {
    thead.replaceChildren();
    tbody.replaceChildren();
    const tr = document.createElement("tr");
    const td = document.createElement("td");
    td.className = "doc-table-placeholder";
    td.textContent = "Could not load Bronze table.";
    tr.appendChild(td);
    tbody.appendChild(tr);
    errEl.textContent =
      e instanceof Error ? e.message : "Request failed. Is Postgres up and DATABASE_URL set?";
    errEl.hidden = false;
  }
}

async function loadSilver() {
  const globalErr = document.getElementById("silver-global-error");
  const hint = document.getElementById("silver-schema-hint");
  if (!globalErr) return;

  globalErr.hidden = true;
  globalErr.textContent = "";

  for (const { slug } of SILVER_MAP) {
    const thead = document.getElementById(`silver-thead-${slug}`);
    const tbody = document.getElementById(`silver-tbody-${slug}`);
    const errP = document.getElementById(`silver-error-${slug}`);
    if (thead && tbody) {
      thead.replaceChildren();
      tbody.replaceChildren();
      const tr = document.createElement("tr");
      const td = document.createElement("td");
      td.className = "doc-table-placeholder";
      td.textContent = "Loading…";
      tr.appendChild(td);
      tbody.appendChild(tr);
    }
    if (errP) {
      errP.hidden = true;
      errP.textContent = "";
    }
  }

  try {
    const res = await fetch("/api/silver/marts");
    if (!res.ok) {
      const detail = await res.json().catch(() => ({}));
      let msg = `${res.status} ${res.statusText}`;
      if (detail.detail != null) {
        msg =
          typeof detail.detail === "string"
            ? detail.detail
            : JSON.stringify(detail.detail);
      }
      throw new Error(msg);
    }
    const data = await res.json();
    const schema = data.schema || "dbt_analytics";
    setSilverMartTitles(schema);
    if (hint) {
      hint.innerHTML = `Schema <code>${escapeHtml(schema)}</code> — dbt models over <code>public.pdf_documents</code>; build with DAG <code>dbt_run_pdf_archive</code>.`;
    }

    const marts = data.marts || {};
    for (const { key, slug } of SILVER_MAP) {
      const thead = document.getElementById(`silver-thead-${slug}`);
      const tbody = document.getElementById(`silver-tbody-${slug}`);
      const errP = document.getElementById(`silver-error-${slug}`);
      const payload = marts[key];
      if (!thead || !tbody) continue;
      if (!payload) {
        renderDynamicTable(thead, tbody, [], []);
        if (errP) {
          errP.textContent = "Missing from API response.";
          errP.hidden = false;
        }
        continue;
      }
      if (payload.error) {
        thead.replaceChildren();
        tbody.replaceChildren();
        const tr = document.createElement("tr");
        const td = document.createElement("td");
        td.className = "doc-table-placeholder";
        td.textContent = "—";
        tr.appendChild(td);
        tbody.appendChild(tr);
        if (errP) {
          errP.textContent = payload.error;
          errP.hidden = false;
        }
      } else {
        renderDynamicTable(thead, tbody, payload.columns || [], payload.rows || []);
      }
    }
  } catch (e) {
    globalErr.textContent =
      e instanceof Error ? e.message : "Could not load Silver marts.";
    globalErr.hidden = false;
    for (const { slug } of SILVER_MAP) {
      const thead = document.getElementById(`silver-thead-${slug}`);
      const tbody = document.getElementById(`silver-tbody-${slug}`);
      if (thead && tbody) {
        thead.replaceChildren();
        tbody.replaceChildren();
      }
    }
  }
}

async function loadAllData() {
  await Promise.all([loadBronze(), loadSilver(), loadStreamMetrics()]);
}

async function loadStreamMetrics() {
  const errEl = document.getElementById("stream-metrics-error");
  const loading = document.getElementById("stream-metrics-loading");
  const grid = document.getElementById("stream-metrics-grid");
  const vizBytes = document.getElementById("stream-bytes-viz");
  const barBytes = document.getElementById("stream-bytes-viz-bar");
  const capBytes = document.getElementById("stream-bytes-viz-caption");
  const vizMsgs = document.getElementById("stream-messages-viz");
  const barMsgs = document.getElementById("stream-messages-viz-bar");
  const capMsgs = document.getElementById("stream-messages-viz-caption");
  const payloadNote = document.getElementById("stream-payload-note");
  const consWrap = document.getElementById("stream-consumers-wrap");
  const consTbody = document.getElementById("stream-consumers-tbody");

  if (!grid || !loading) return;

  if (errEl) {
    errEl.hidden = true;
    errEl.textContent = "";
  }
  if (payloadNote) {
    payloadNote.hidden = true;
    payloadNote.textContent = "";
  }
  loading.hidden = false;
  grid.hidden = true;
  if (vizBytes) vizBytes.hidden = true;
  if (vizMsgs) vizMsgs.hidden = true;
  if (consWrap) consWrap.hidden = true;
  grid.replaceChildren();

  try {
    const res = await fetch("/api/stream/metrics");
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      const d = data.detail;
      throw new Error(typeof d === "string" ? d : res.statusText);
    }

    loading.hidden = true;
    grid.hidden = false;

    const nameLabel = document.getElementById("stream-name-label");
    const vhostLabel = document.getElementById("stream-vhost-label");
    if (nameLabel && data.queue_name) nameLabel.textContent = data.queue_name;
    if (vhostLabel && data.vhost != null && data.vhost !== "") vhostLabel.textContent = data.vhost;

    if (data.error) {
      if (errEl) {
        errEl.textContent = data.error;
        errEl.hidden = false;
      }
    }

    const limitation = Boolean(data.stream_payload_bytes_not_in_management_api);
    const bytesReported = Number(data.message_bytes) || 0;
    const byteTileVal = limitation
      ? "not in API (streams)"
      : formatBytes(data.message_bytes);

    const tiles = [
      ["Stream / queue name", data.queue_name != null ? String(data.queue_name) : "—"],
      ["Virtual host", data.vhost != null ? String(data.vhost) : "—"],
      ["Queue exists", data.exists ? "yes" : "no"],
      ["Queue type", data.queue_type != null ? String(data.queue_type) : "—"],
      ["Segments", data.segments != null ? String(data.segments) : "—"],
      ["Broker RAM (queue)", data.memory_bytes != null ? formatBytes(data.memory_bytes) : "—"],
      ["Messages (total)", data.messages != null ? String(data.messages) : "—"],
      ["Messages ready", data.messages_ready != null ? String(data.messages_ready) : "—"],
      ["Payload bytes (API)", byteTileVal],
      ["Consumers", data.consumers != null ? String(data.consumers) : "—"],
      ["State", data.state != null ? String(data.state) : "—"],
    ];

    for (const [label, val] of tiles) {
      const card = document.createElement("div");
      card.className = "metric-card";
      const lb = document.createElement("p");
      lb.className = "metric-card__label";
      lb.textContent = label;
      const vl = document.createElement("p");
      vl.className = "metric-card__value";
      vl.textContent = val;
      card.appendChild(lb);
      card.appendChild(vl);
      grid.appendChild(card);
    }

    const msgs = Number(data.messages) || 0;
    if (vizMsgs && barMsgs && capMsgs) {
      vizMsgs.hidden = false;
      const pctM = Math.min(100, (msgs / STREAM_MESSAGES_BAR_CAP) * 100);
      barMsgs.style.width = `${pctM}%`;
      capMsgs.textContent = `${msgs} message(s) — bar scale max ${STREAM_MESSAGES_BAR_CAP} messages (visual only; not disk size).`;
    }

    if (vizBytes && barBytes && capBytes) {
      vizBytes.hidden = false;
      if (limitation || bytesReported === 0) {
        barBytes.style.width = "0%";
        capBytes.textContent = limitation
          ? "RabbitMQ does not include payload message_bytes for stream queues in GET /api/queues — counts above are accurate; disk size is not exposed here."
          : `${formatBytes(bytesReported)} — bar scale max ${formatBytes(STREAM_BYTES_BAR_CAP)}.`;
      } else {
        const pct = Math.min(100, (bytesReported / STREAM_BYTES_BAR_CAP) * 100);
        barBytes.style.width = `${pct}%`;
        capBytes.textContent = `${formatBytes(bytesReported)} — bar scale max ${formatBytes(STREAM_BYTES_BAR_CAP)} (visual only).`;
      }
    }

    if (payloadNote && limitation) {
      payloadNote.hidden = false;
      payloadNote.textContent =
        "Why 0 bytes? For x-queue-type: stream, the management API still returns message counts, but typically omits message_bytes. Use Segments and message counts as backlog signals; queue RAM is Erlang process memory, not total PDF payload size.";
    }

    const details = data.consumer_details || [];
    if (consWrap && consTbody) {
      consTbody.replaceChildren();
      if (details.length > 0) {
        consWrap.hidden = false;
        for (const c of details) {
          const tr = document.createElement("tr");
          tr.appendChild(cellText(c.consumer_tag));
          tr.appendChild(cellText(c.channel_details));
          consTbody.appendChild(tr);
        }
      }
    }
  } catch (e) {
    loading.hidden = true;
    grid.hidden = false;
    const card = document.createElement("div");
    card.className = "metric-card metric-card--wide";
    const p = document.createElement("p");
    p.className = "metric-card__value";
    p.textContent = e instanceof Error ? e.message : "Failed to load stream metrics.";
    card.appendChild(p);
    grid.appendChild(card);
    if (errEl) {
      errEl.textContent = e instanceof Error ? e.message : String(e);
      errEl.hidden = false;
    }
  }
}

function wireStreamSection() {
  const refreshBtn = document.getElementById("refresh-stream-metrics");
  if (refreshBtn) refreshBtn.addEventListener("click", () => loadStreamMetrics());

  const form = document.getElementById("stream-clear-form");
  const result = document.getElementById("stream-clear-result");
  if (form) {
    form.addEventListener("submit", async (ev) => {
      ev.preventDefault();
      if (result) {
        result.hidden = true;
        result.textContent = "";
      }
      const input = document.getElementById("stream-clear-secret");
      const secret = input?.value ?? "";
      try {
        const res = await fetch("/api/stream/clear", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ secret }),
        });
        const data = await res.json().catch(() => ({}));
        if (!res.ok) {
          const d = data.detail;
          throw new Error(typeof d === "string" ? d : res.statusText);
        }
        if (result) {
          result.hidden = false;
          result.textContent = data.detail || "Done.";
        }
        if (input) input.value = "";
        await loadStreamMetrics();
      } catch (e) {
        if (result) {
          result.hidden = false;
          result.textContent = e instanceof Error ? e.message : String(e);
        }
      }
    });
  }
}

function wireRefresh() {
  const btn = document.getElementById("refresh-data");
  if (btn) {
    btn.addEventListener("click", () => loadAllData());
  }
}

async function runSearch(ev) {
  if (ev) ev.preventDefault();
  const input = document.getElementById("search-query");
  const meta = document.getElementById("search-meta");
  const err = document.getElementById("search-error");
  const tbody = document.getElementById("search-tbody");
  const q = input?.value?.trim() ?? "";
  if (err) {
    err.hidden = true;
    err.textContent = "";
  }
  if (meta) {
    meta.hidden = true;
    meta.textContent = "";
  }
  if (!tbody) return;

  if (!q) {
    tbody.replaceChildren();
    const tr = document.createElement("tr");
    const td = document.createElement("td");
    td.colSpan = 5;
    td.className = "doc-table-placeholder";
    td.textContent = "Enter a query and press Search.";
    tr.appendChild(td);
    tbody.appendChild(tr);
    return;
  }

  tbody.replaceChildren();
  const loading = document.createElement("tr");
  const ltd = document.createElement("td");
  ltd.colSpan = 5;
  ltd.className = "doc-table-placeholder";
  ltd.textContent = "Searching…";
  loading.appendChild(ltd);
  tbody.appendChild(loading);

  try {
    const res = await fetch(`/api/search?${new URLSearchParams({ q })}`);
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      const d = data.detail;
      throw new Error(typeof d === "string" ? d : res.statusText);
    }

    if (data.mode === "empty") {
      if (meta) {
        meta.hidden = false;
        meta.textContent = data.detail || "Enter a search query.";
      }
      tbody.replaceChildren();
      const tr = document.createElement("tr");
      const td = document.createElement("td");
      td.colSpan = 5;
      td.className = "doc-table-placeholder";
      td.textContent = "—";
      tr.appendChild(td);
      tbody.appendChild(tr);
      return;
    }

    if (meta) {
      meta.hidden = false;
      if (data.detail) {
        meta.textContent = `${data.mode === "fts" ? "Full-text (GIN)." : "ILIKE."} ${data.detail}`;
      } else {
        meta.textContent =
          data.mode === "fts"
            ? "Full-text search (websearch_to_tsquery + GIN index)."
            : "Substring match on filename or extracted_text.";
      }
    }

    const items = data.items || [];
    tbody.replaceChildren();
    if (items.length === 0) {
      const tr = document.createElement("tr");
      const td = document.createElement("td");
      td.colSpan = 5;
      td.className = "doc-table-placeholder";
      td.textContent = "No matches.";
      tr.appendChild(td);
      tbody.appendChild(tr);
      return;
    }

    for (const row of items) {
      const tr = document.createElement("tr");
      tr.appendChild(cellText(row.id));
      tr.appendChild(cellText(row.original_file_name));
      const rankVal =
        row.rank != null && row.rank !== ""
          ? Number(row.rank).toFixed(4)
          : "—";
      tr.appendChild(cellText(rankVal));
      const snip = document.createElement("td");
      snip.className = "search-snippet";
      snip.textContent = row.headline ?? "—";
      tr.appendChild(snip);
      const linkTd = document.createElement("td");
      const a = document.createElement("a");
      a.href = `/text/${row.id}`;
      a.textContent = "Open";
      a.setAttribute("cds-text", "link");
      linkTd.appendChild(a);
      tr.appendChild(linkTd);
      tbody.appendChild(tr);
    }
  } catch (e) {
    tbody.replaceChildren();
    const tr = document.createElement("tr");
    const td = document.createElement("td");
    td.colSpan = 5;
    td.className = "doc-table-placeholder";
    td.textContent = "Search failed.";
    tr.appendChild(td);
    tbody.appendChild(tr);
    if (err) {
      err.textContent = e instanceof Error ? e.message : String(e);
      err.hidden = false;
    }
  }
}

function wireSearch() {
  const form = document.getElementById("search-form");
  if (form) {
    form.addEventListener("submit", runSearch);
  }
}

document.addEventListener("DOMContentLoaded", async () => {
  wireStepFocus();
  wireThemeToggle();
  wireRefresh();
  wireStreamSection();
  wireSearch();
  await loadConfig();
  setSilverMartTitles("dbt_analytics");
  await loadAllData();
});
