# Databricks notebook source
# bp_config: widgets + runtime lookups (no hardcoded ids/paths)
import os, sys
from databricks.sdk import WorkspaceClient

W = WorkspaceClient()

# ── Widgets (one per setting; defaults mirror bundle variables) ──
_WIDGETS = {
    "catalog": "chevron_fpa",
    "schema": "finance",
    "volume": "raw",
    "company": "Meridian Energy, Inc.",
    "app_name": "chevron-fpa-planner",
    "warehouse_name": "chevron-fpa-warehouse",
    "lakebase_project": "chevron-fpa",
    "lakebase_branch": "production",
    "pg_database": "planning_db",
    "lakebase_catalog": "chevron_fpa_lakebase",
    "genie_space_title": "Meridian Energy FP&A Assistant",
    "endpoint_name": "chevron-fpa-forecast",
    "pipeline_name": "chevron_fpa_gl_pipeline",
    "model_name": "chevron_fpa.finance.energy_forecast",
    "remove_after": "2027-04-30",
    "remove_after_uc": "20270430",
}
for _k, _v in _WIDGETS.items():
    dbutils.widgets.text(_k, _v, _k)

# ── Project root (derived from this notebook's own path, not hardcoded) ──
try:
    _nb = str(dbutils.notebook.entry_point.getDbutils().notebook()
              .getContext().notebookPath().get())
    _root = os.path.dirname(os.path.dirname(_nb))
except Exception:
    _root = ""
APP_DIR = f"/Workspace{_root}/app" if _root else ""

def cfg(k):
    """Get a widget value."""
    return dbutils.widgets.get(k)

def fc():
    """Fully-qualified catalog.schema."""
    return f"{cfg('catalog')}.{cfg('schema')}"

def vpath(*parts):
    """Volume path with optional sub-paths."""
    base = f"/Volumes/{cfg('catalog')}/{cfg('schema')}/{cfg('volume')}"
    return "/".join([base] + list(parts))

def warehouse_id():
    """Resolve warehouse name → id via the SDK."""
    name = cfg("warehouse_name")
    for wh in W.warehouses.list():
        if wh.name == name:
            return wh.id
    raise ValueError(f"Warehouse '{name}' not found")

def app_sp_id():
    """Resolve app name → service principal client id via the SDK."""
    a = W.apps.get(cfg("app_name"))
    return a.service_principal_client_id

def genie_space_id():
    """Resolve Genie space by title, iterating through ALL pages."""
    title = cfg("genie_space_title")
    found, tk = [], None
    while True:
        r = W.genie.list_spaces(page_token=tk)
        found += [s.space_id for s in r.spaces or [] if s.title == title]
        tk = r.next_page_token
        if not tk: break
    if not found:
        raise ValueError(f"Genie space '{title}' not found")
    if len(found) > 1:
        raise ValueError(f"Multiple Genie spaces '{title}': {found}")
    return found[0]

def pg_connect():
    """Connect to Lakebase Postgres via OAuth credential (token valid 1 h)."""
    import psycopg
    proj, branch = cfg("lakebase_project"), cfg("lakebase_branch")
    parent = f"projects/{proj}/branches/{branch}"
    for e in W.postgres.list_endpoints(parent=parent):
        host = e.status.hosts.host
        tok = W.postgres.generate_database_credential(endpoint=e.name).token
        user = W.current_user.me().user_name
        return psycopg.connect(
            host=host, port=5432, dbname=cfg("pg_database"),
            user=user, password=tok, sslmode="require",
        )
    raise ValueError("No Lakebase endpoint found")

def import_app():
    """Add app/ to sys.path so notebooks can import forecast_engine, etc."""
    if APP_DIR and APP_DIR not in sys.path:
        sys.path.insert(0, APP_DIR)
