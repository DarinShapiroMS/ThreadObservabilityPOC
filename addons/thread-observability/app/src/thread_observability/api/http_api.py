"""Core HTTP API for Thread Observability add-on.

Serves a lightweight status dashboard at ``/`` (Ingress entry-point) plus
JSON endpoints under ``/v1/...`` for programmatic access.
"""

import asyncio
import contextlib
import logging
from datetime import UTC, datetime
from pathlib import Path

import httpx
from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from . import supervisor_client
from ..config import get_config
from ..health import build_health_snapshot
from ..pipeline import nodes as nodes_mod
from ..pipeline import otbr_adapter
from ..pipeline import reasoner as reasoner_mod
from ..pipeline import seed as seed_mod
from ..pipeline import topology as topology_mod
from ..storage import influx_store as ts_store
from ..storage.sqlite_store import get_store

log = logging.getLogger(__name__)

ADDON_VERSION = "0.9.3"
LOG_PATH = Path("/data/thread-observability/addon.log")


def _utc_now() -> str:
    return datetime.now(tz=UTC).isoformat()


def _tail_log(n: int = 80) -> list[str]:
    if not LOG_PATH.exists():
        return []
    try:
        return LOG_PATH.read_text(errors="replace").splitlines()[-n:]
    except OSError:
        return []


DASHBOARD_HTML = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Thread Observability</title>
<style>
  :root { color-scheme: light dark; }
  body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
         margin: 0; padding: 1.5rem; background: var(--bg, #f6f7f9); color: var(--fg, #111); }
  @media (prefers-color-scheme: dark) {
    body { --bg: #1c1c1e; --fg: #f2f2f7; }
    .card { background: #2c2c2e !important; border-color: #3a3a3c !important; }
    pre { background: #000 !important; color: #c8e1ff !important; }
    code { background: #3a3a3c !important; }
  }
  h1 { margin: 0 0 .25rem; font-size: 1.4rem; }
  h2 { margin: 0 0 .5rem; font-size: 1rem; text-transform: uppercase; letter-spacing: .05em; opacity: .7; }
  .sub { opacity: .65; font-size: .85rem; margin-bottom: 1.25rem; }
  .grid { display: grid; gap: 1rem; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); }
  .card { background: #fff; border: 1px solid #e5e7eb; border-radius: 10px; padding: 1rem; }
  .card.wide { grid-column: 1 / -1; }
  .kv { display: grid; grid-template-columns: max-content 1fr; gap: .25rem 1rem; font-size: .9rem; }
  .kv dt { opacity: .65; }
  .kv dd { margin: 0; font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
           overflow-wrap: anywhere; }
  .pill { display: inline-block; padding: .15rem .55rem; border-radius: 999px; font-size: .75rem; font-weight: 600; }
  .pill.ok { background: #d1fae5; color: #065f46; }
  .pill.warn { background: #fef3c7; color: #92400e; }
  .pill.err { background: #fee2e2; color: #991b1b; }
  pre { background: #0d1117; color: #c9d1d9; padding: .75rem; border-radius: 6px;
        font-size: .78rem; line-height: 1.35; max-height: 280px; overflow: auto; margin: 0; }
  code { background: #eef0f3; padding: .05rem .3rem; border-radius: 4px; font-size: .85em; }
  .links a { display: inline-block; margin-right: .75rem; font-size: .85rem; }
  button { font: inherit; padding: .35rem .8rem; border-radius: 6px; border: 1px solid #d1d5db;
           background: #fff; cursor: pointer; }
  button:hover { background: #f3f4f6; }
  .row { display: flex; gap: .5rem; align-items: center; justify-content: space-between; margin-bottom: .75rem; }
  .muted { opacity: .55; font-size: .8rem; }
</style>
</head>
<body>
  <h1>Thread Observability <span class="muted" id="version"></span></h1>
  <div class="sub">Status dashboard &middot; auto-refresh every 5&nbsp;s &middot;
    <span id="last-refresh">never refreshed</span>
  </div>

  <div class="grid">
    <div class="card">
      <div class="row"><h2>Add-on (Supervisor)</h2><span id="addon-pill" class="pill warn">loading</span></div>
      <dl class="kv" id="addon-kv"><dt>state</dt><dd>&hellip;</dd></dl>
    </div>

    <div class="card">
      <div class="row"><h2>Services</h2><span id="svc-pill" class="pill warn">loading</span></div>
      <dl class="kv">
        <dt>core (this page)</dt><dd id="core-state">running</dd>
        <dt>mcp (port 8100)</dt><dd id="mcp-state">&hellip;</dd>
      </dl>
    </div>

    <div class="card">
      <div class="row"><h2>Thread Network</h2><span id="net-pill" class="pill warn">loading</span></div>
      <dl class="kv">
        <dt>nodes</dt><dd id="n-nodes">&mdash;</dd>
        <dt>links</dt><dd id="n-links">&mdash;</dd>
        <dt>healthy / stale / offline</dt><dd id="n-status">&mdash;</dd>
        <dt>active issues</dt><dd id="n-issues">&mdash;</dd>
        <dt>data age</dt><dd id="n-age">&mdash;</dd>
      </dl>
      <div class="muted" style="margin-top:.5rem" id="net-hint">Awaiting events. Use “Seed demo” to populate.</div>
    </div>

    <div class="card">
      <div class="row"><h2>Active Issues</h2><span id="iss-pill" class="pill ok">0</span></div>
      <div id="issues-list" class="muted">none</div>
    </div>

    <div class="card">
      <div class="row"><h2>Storage</h2><span id="store-pill" class="pill warn">loading</span></div>
      <dl class="kv" id="store-kv"></dl>
    </div>

    <div class="card">
      <div class="row"><h2>OTBR Ingestion</h2><span id="ing-pill" class="pill warn">loading</span></div>
      <dl class="kv" id="ing-kv"></dl>
      <div class="links" style="margin-top:.5rem">
        <button onclick="doPost('v1/ingest/run')">Ingest now</button>
        <button onclick="listCandidates()">List OTBR add-ons</button>
      </div>
      <pre id="ing-out" class="muted" style="margin-top:.5rem;max-height:140px">(no action yet)</pre>
    </div>

    <div class="card wide">
      <h2>Thread Nodes</h2>
      <table style="width:100%;border-collapse:collapse;font-size:.9rem">
        <thead style="border-bottom:1px solid #d1d5db">
          <tr>
            <th style="text-align:left;padding:.5rem"># ID</th>
            <th style="text-align:left;padding:.5rem">Name</th>
            <th style="text-align:left;padding:.5rem">Role</th>
            <th style="text-align:center;padding:.5rem">RSSI</th>
            <th style="text-align:center;padding:.5rem">LQI</th>
            <th style="text-align:center;padding:.5rem">Status</th>
            <th style="text-align:left;padding:.5rem">Last Seen</th>
          </tr>
        </thead>
        <tbody id="nodes-tbody">
          <tr><td colspan="7" class="muted" style="padding:.5rem">loading…</td></tr>
        </tbody>
      </table>
    </div>

    <div class="card wide">
      <h2>Dev Actions</h2>
      <div class="links">
        <button onclick="doPost('v1/dev/seed')">Seed demo topology</button>
        <button onclick="doPost('v1/reasoner/run')">Run reasoner</button>
        <button onclick="doPost('v1/ingest/run')">Run OTBR ingest</button>
        <button onclick="refresh()">Refresh now</button>
      </div>
      <pre id="action-out" class="muted" style="margin-top:.5rem;max-height:120px">(no action run yet)</pre>
    </div>

    <div class="card wide">
      <div class="row">
        <h2>Recent logs</h2>
        <button onclick="refresh()">Refresh now</button>
      </div>
      <pre id="logs">loading&hellip;</pre>
    </div>

    <div class="card wide">
      <h2>Endpoints</h2>
      <div class="links">
        <a href="v1/health/snapshot" target="_blank">/v1/health/snapshot</a>
        <a href="v1/issues/active" target="_blank">/v1/issues/active</a>
        <a href="v1/topology" target="_blank">/v1/topology</a>
        <a href="v1/dev/status" target="_blank">/v1/dev/status</a>
        <a href="health" target="_blank">/health</a>
      </div>
      <div class="muted" style="margin-top:.75rem">
        MCP JSON-RPC: <code>POST http://&lt;ha-host&gt;:8100/mcp</code> &middot;
        tools include <code>ha_get_addon_state</code>, <code>ha_get_addon_logs</code>,
        <code>ha_rebuild_addon</code>, <code>get_recent_logs</code>.
      </div>
    </div>
  </div>

<script>
async function fetchJSON(u) {
  const r = await fetch(u, {cache:'no-store'});
  if (!r.ok) throw new Error(u + ' -> ' + r.status);
  return r.json();
}
function setPill(el, kind, text) { el.className = 'pill ' + kind; el.textContent = text; }
async function doPost(url) {
  const out = document.getElementById('action-out');
  out.textContent = 'POST ' + url + ' …';
  out.className = '';
  try {
    const r = await fetch(url, {method:'POST', cache:'no-store'});
    const j = await r.json();
    out.textContent = JSON.stringify(j, null, 2);
  } catch (e) {
    out.textContent = 'Error: ' + e.message;
  }
  refresh();
}
function fmtKV(parent, obj) {
  parent.innerHTML = '';
  for (const [k,v] of Object.entries(obj)) {
    const dt = document.createElement('dt'); dt.textContent = k;
    const dd = document.createElement('dd');
    dd.textContent = v === null || v === undefined ? '—' : (typeof v === 'object' ? JSON.stringify(v) : String(v));
    parent.append(dt, dd);
  }
}
async function refresh() {
  document.getElementById('last-refresh').textContent = 'refreshed ' + new Date().toLocaleTimeString();
  try {
    const s = await fetchJSON('v1/dev/status');
    document.getElementById('version').textContent = 'v' + (s.addon_version || '?');

    const a = s.supervisor || {};
    if (a.error) {
      setPill(document.getElementById('addon-pill'), 'err', 'unreachable');
      fmtKV(document.getElementById('addon-kv'), {error: a.error});
    } else {
      const sum = a.summary || {};
      const state = (sum.state || 'unknown').toLowerCase();
      setPill(document.getElementById('addon-pill'),
              state === 'started' ? 'ok' : (state === 'stopped' ? 'err' : 'warn'),
              state);
      fmtKV(document.getElementById('addon-kv'), {
        version: sum.version, latest: sum.version_latest,
        update_available: sum.update_available, boot: sum.boot,
        watchdog: sum.watchdog, ingress: sum.ingress,
      });
    }

    try {
      const m = await fetchJSON('v1/dev/mcp-health');
      document.getElementById('mcp-state').textContent = m.ok ? 'running' : ('error: ' + (m.detail || m.status_code));
      setPill(document.getElementById('svc-pill'), m.ok ? 'ok' : 'err', m.ok ? 'healthy' : 'degraded');
    } catch (e) {
      document.getElementById('mcp-state').textContent = 'probe failed';
      setPill(document.getElementById('svc-pill'), 'warn', 'partial');
    }

    const h = s.health || {}, t = s.topology || {}, i = s.issues || {};
    const sum = h.summary || {};
    document.getElementById('n-nodes').textContent = (t.nodes || []).length;
    document.getElementById('n-links').textContent = (t.links || []).length;
    document.getElementById('n-status').textContent =
      (sum.healthy_nodes ?? 0) + ' / ' + (sum.stale_nodes ?? 0) + ' / ' + (sum.offline_nodes ?? 0);
    document.getElementById('n-issues').textContent = i.count ?? (i.issues || []).length;
    document.getElementById('n-age').textContent =
      h.data_age_seconds === null || h.data_age_seconds === undefined
        ? '—' : (Math.round(h.data_age_seconds) + ' s');
    const overall = (h.status || 'unknown');
    setPill(document.getElementById('net-pill'),
            overall === 'ok' ? 'ok' : (overall === 'critical' ? 'err' : 'warn'),
            overall);
    if ((t.nodes || []).length > 0) {
      document.getElementById('net-hint').textContent =
        'computed at ' + (t.computed_at || '—') + ' (freshness ' + (t.freshness_minutes ?? '?') + ' min)';
    }

    const issues = (i.issues || []);
    const issBox = document.getElementById('issues-list');
    const issPill = document.getElementById('iss-pill');
    if (issues.length === 0) {
      issPill.className = 'pill ok'; issPill.textContent = '0';
      issBox.className = 'muted'; issBox.textContent = 'none';
    } else {
      const crit = issues.filter(x => x.severity === 'crit').length;
      issPill.className = 'pill ' + (crit ? 'err' : 'warn');
      issPill.textContent = issues.length + (crit ? ' (' + crit + ' crit)' : '');
      issBox.className = '';
      issBox.innerHTML = issues.slice(0, 8).map(function(x) {
        const sev = '<span class="pill ' + (x.severity === 'crit' ? 'err' : 'warn') + '">'
                    + x.severity + '</span>';
        const eui = x.eui64 ? ' <code>' + x.eui64 + '</code>' : '';
        return '<div style="margin:.25rem 0">' + sev + ' <b>' + x.kind + '</b>' + eui
               + ' <span class="muted">#' + x.id + ' &middot; ' + x.opened_at + '</span></div>';
      }).join('');
    }

    document.getElementById('logs').textContent =
      (s.recent_logs || []).join('\\n') || '(no log entries yet)';

    const st = s.storage || {};
    const ts = s.timeseries || {};
    if (st.error) {
      setPill(document.getElementById('store-pill'), 'err', 'error');
      fmtKV(document.getElementById('store-kv'), {error: st.error});
    } else {
      const rc = st.row_counts || {};
      const sizeKB = ((st.size_bytes || 0) / 1024).toFixed(1) + ' KB';
      setPill(document.getElementById('store-pill'), 'ok',
              'schema v' + (st.schema_version ?? '?'));
      fmtKV(document.getElementById('store-kv'), {
        db_path: st.db_path,
        size: sizeKB,
        nodes: rc.nodes, events: rc.events, issues: rc.issues,
        timeseries: ts.backend || '?',
        newest_event: st.events_newest || '—',
      });
    }

    const ing = s.ingestion || {};
    const ingPill = document.getElementById('ing-pill');
    if (ing.error) {
      setPill(ingPill, 'err', 'error');
    } else if (!ing.slug) {
      setPill(ingPill, 'warn', 'no slug');
    } else if (ing.last_error) {
      setPill(ingPill, 'warn', 'errors');
    } else {
      setPill(ingPill, 'ok', 'active');
    }
    fmtKV(document.getElementById('ing-kv'), {
      slug: ing.slug || '(autodiscover)',
      lines_processed: ing.position,
      events_total: ing.events_total,
      last_event_ts: ing.last_event_ts || '—',
      last_run_at: ing.last_run_at || '—',
      last_error: ing.last_error || '—',
    });

    const allNodes = s.all_nodes || [];
    const tbody = document.getElementById('nodes-tbody');
    if (allNodes.length === 0) {
      tbody.innerHTML = '<tr><td colspan="7" class="muted" style="padding:.5rem">(no nodes yet; use Seed demo or enable OTBR ingestion)</td></tr>';
    } else {
      tbody.innerHTML = allNodes.map(function(n) {
        const statusPill = '<span class="pill ' + (n.status === 'healthy' ? 'ok' : n.status === 'stale' ? 'warn' : 'err') + '">'
                          + (n.status || '?') + '</span>';
        const lastSeen = n.last_seen ? new Date(n.last_seen).toLocaleString() : '—';
        const sig = (n.signal_strength || {});
        const rssi = sig.rssi !== null && sig.rssi !== undefined ? sig.rssi + ' dBm' : '—';
        const lqi = sig.lqi !== null && sig.lqi !== undefined ? sig.lqi : '—';
        return '<tr style="border-bottom:1px solid #e5e7eb">'
               + '<td style="padding:.5rem"><code style="font-size:.8em">' + (n.eui64 || '?').slice(-4).toUpperCase() + '</code></td>'
               + '<td style="padding:.5rem">' + (n.friendly_name || n.display_name || '?') + '</td>'
               + '<td style="padding:.5rem">' + (n.role || '?') + '</td>'
               + '<td style="text-align:center;padding:.5rem">' + rssi + '</td>'
               + '<td style="text-align:center;padding:.5rem">' + lqi + '</td>'
               + '<td style="text-align:center;padding:.5rem">' + statusPill + '</td>'
               + '<td style="padding:.5rem;font-size:.8em">' + lastSeen + '</td>'
               + '</tr>';
      }).join('');
    }
  } catch (e) {
    document.getElementById('logs').textContent = 'Error: ' + e.message;
  }
}
async function listCandidates() {
  const out = document.getElementById('ing-out');
  out.textContent = 'GET v1/ingest/candidates …';
  try {
    const j = await fetchJSON('v1/ingest/candidates');
    out.textContent = JSON.stringify(j, null, 2);
  } catch (e) { out.textContent = 'Error: ' + e.message; }
}
refresh();
setInterval(refresh, 5000);
</script>
</body>
</html>
"""


@contextlib.asynccontextmanager
async def _lifespan(app: FastAPI):
    """Start/stop background scheduler tasks alongside the FastAPI app."""
    cfg = get_config()
    interval = int(getattr(cfg.scheduler, "ingestion_interval_seconds", 10))
    task = asyncio.create_task(otbr_adapter.run_forever(interval_seconds=interval),
                               name="otbr-ingest-loop")
    log.info("background scheduler started (otbr ingest every %ss)", interval)
    try:
        yield
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task
        log.info("background scheduler stopped")


def create_core_app() -> FastAPI:
    """Create the core FastAPI application."""
    app = FastAPI(
        title="Thread Observability Core API",
        version=ADDON_VERSION,
        lifespan=_lifespan,
    )

    @app.get("/", response_class=HTMLResponse)
    def dashboard() -> HTMLResponse:
        return HTMLResponse(DASHBOARD_HTML)

    @app.get("/api")
    def api_root() -> dict[str, str]:
        return {"service": "core", "name": "thread-observability", "version": ADDON_VERSION}

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok", "service": "core", "checked_at": _utc_now()}

    @app.get("/v1/health/snapshot")
    def health_snapshot() -> dict[str, object]:
        try:
            return build_health_snapshot()
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc), "computed_at": _utc_now()}

    @app.get("/v1/issues/active")
    def list_active_issues() -> dict[str, object]:
        try:
            issues = get_store().list_active_issues()
            return {"count": len(issues), "issues": issues, "computed_at": _utc_now()}
        except Exception as exc:  # noqa: BLE001
            return {"count": 0, "issues": [], "error": str(exc), "computed_at": _utc_now()}

    @app.get("/v1/topology")
    def topology_snapshot() -> dict[str, object]:
        try:
            return topology_mod.build_topology()
        except Exception as exc:  # noqa: BLE001
            return {"nodes": [], "links": [], "error": str(exc), "computed_at": _utc_now()}

    @app.post("/v1/reasoner/run")
    def reasoner_run() -> dict[str, object]:
        try:
            return reasoner_mod.run_reasoner()
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    @app.post("/v1/dev/seed")
    def dev_seed() -> dict[str, object]:
        try:
            seeded = seed_mod.seed_demo_topology()
            reasoned = reasoner_mod.run_reasoner()
            return {"seeded": seeded, "reasoner": reasoned}
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    @app.get("/v1/dev/status")
    async def dev_status() -> dict[str, object]:
        try:
            sup: dict[str, object] = await supervisor_client.get_addon_info()
        except Exception as exc:  # noqa: BLE001
            sup = {"error": str(exc)}
        try:
            storage = get_store().stats()
        except Exception as exc:  # noqa: BLE001
            storage = {"error": str(exc)}
        try:
            ts_health = await ts_store.timeseries_health()
        except Exception as exc:  # noqa: BLE001
            ts_health = {"backend": "unknown", "error": str(exc)}
        try:
            cfg = get_config().model_dump()
            if cfg.get("influx", {}).get("token"):
                cfg["influx"]["token"] = "***"
        except Exception as exc:  # noqa: BLE001
            cfg = {"error": str(exc)}
        try:
            ingestion = otbr_adapter.get_state()
        except Exception as exc:  # noqa: BLE001
            ingestion = {"error": str(exc)}
        try:
            all_nodes = nodes_mod.list_nodes_enriched(include_signal_strength=True)
        except Exception as exc:  # noqa: BLE001
            all_nodes = []
        return {
            "addon_version": ADDON_VERSION,
            "checked_at": _utc_now(),
            "supervisor": sup,
            "health": health_snapshot(),
            "issues": list_active_issues(),
            "topology": topology_snapshot(),
            "recent_logs": _tail_log(80),
            "storage": storage,
            "timeseries": ts_health,
            "config": cfg,
            "ingestion": ingestion,
            "all_nodes": all_nodes,
        }

    @app.get("/v1/dev/mcp-health")
    async def dev_mcp_health() -> dict[str, object]:
        try:
            async with httpx.AsyncClient(timeout=3.0) as client:
                r = await client.get("http://127.0.0.1:8100/health")
            return {"ok": r.status_code == 200, "status_code": r.status_code}
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "detail": str(exc)}

    # -- OTBR ingestion (Phase 2.5) ---------------------------------------

    @app.get("/v1/ingest/state")
    def ingest_state() -> dict[str, object]:
        try:
            return otbr_adapter.get_state()
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    @app.get("/v1/ingest/candidates")
    async def ingest_candidates() -> dict[str, object]:
        try:
            cands = await otbr_adapter.list_candidates()
            return {"count": len(cands), "candidates": cands}
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc), "candidates": []}

    @app.post("/v1/ingest/run")
    async def ingest_run() -> dict[str, object]:
        try:
            return await otbr_adapter.ingest_once()
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    @app.post("/v1/ingest/slug")
    async def ingest_set_slug(payload: dict[str, str]) -> dict[str, object]:
        slug = (payload or {}).get("slug", "").strip()
        if not slug:
            return {"error": "slug required"}
        try:
            return otbr_adapter.set_slug(slug)
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    @app.get("/v1/ingest/debug")
    async def ingest_debug() -> dict[str, object]:
        """Debug endpoint: fetch raw OTBR logs to inspect format."""
        try:
            ingest_st = otbr_adapter.get_state()
            slug = ingest_st.get("slug")
            if not slug:
                return {"error": "no OTBR slug configured"}
            # Fetch latest 50 lines from the OTBR addon
            logs = await supervisor_client.get_addon_logs(slug=slug, lines=50)
            return {
                "slug": slug,
                "log_line_count": len(logs),
                "sample_lines": logs[-10:] if logs else [],
                "raw_sample": "\n".join(logs[-20:]) if logs else "",
            }
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    # -- Node metadata (Phase 3) ------------------------------------------

    @app.get("/v1/nodes/all")
    def nodes_list() -> dict[str, object]:
        try:
            nodes = nodes_mod.list_nodes_enriched(include_signal_strength=True)
            return {"count": len(nodes), "nodes": nodes}
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc), "nodes": []}

    @app.get("/v1/nodes/{eui64}")
    def nodes_get(eui64: str) -> dict[str, object]:
        try:
            return nodes_mod.get_node_summary(eui64, include_signal_strength=True)
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    @app.post("/v1/nodes/{eui64}/friendly-name")
    def nodes_set_name(eui64: str, payload: dict[str, str]) -> dict[str, object]:
        name = (payload or {}).get("name", "").strip()
        if not name:
            return {"error": "name required"}
        try:
            ok = get_store().set_node_friendly_name(eui64, name)
            if not ok:
                return {"error": f"node {eui64} not found"}
            return nodes_mod.get_node_summary(eui64, include_signal_strength=True)
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    return app
