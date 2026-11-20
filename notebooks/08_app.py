# Databricks notebook source
# MAGIC %pip install -q "psycopg[binary]"

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %run ./bp_config

# COMMAND ----------
import requests, time
import_app()
from plan_store import save_cells

# COMMAND ----------
# App status (deployed by the bundle; this task only checks it)
app = W.apps.get(cfg("app_name"))
print(f"App: {app.name}, app status: {app.app_status.state.value}, compute: {app.compute_status.state.value}")
print(f"Active deployment: {app.active_deployment.deployment_id} ({app.active_deployment.status.state.value})")

# COMMAND ----------
# EVIDENCE: HTTP smoke test of every page (workspace OAuth headers)
print("=== PHASE 8 EVIDENCE ===")
hdr = W.config.authenticate()
for p in ["/","/pl","/bs","/cf","/plan","/forecast","/ask","/audit"]:
    try:
        r = requests.get(f"{app.url}{p}", headers=hdr, timeout=30, allow_redirects=False)
        print(f"  {p}: HTTP {r.status_code}")
    except Exception as e:
        print(f"  {p}: {str(e)[:60]}")

# COMMAND ----------
# EVIDENCE: edit round-trip through the app's own write path (plan_store.save_cells)
class _Pool:  # notebook stand-in for the app's PgPool
    def __init__(self): self.c = pg_connect(); self.c.autocommit = True
    def get(self): return self.c
pool = _Pool()
key = dict(period="2027-06", account_id="4000", cost_center_id="CC200")
amt = round(75e6 + time.time() % 1000, 2)  # new value each run so the edit is audited
n = save_cells(pool, "BUD", [{**key, "amount": amt}], W.current_user.me().user_name)
print(f"  save_cells changed {n} cell(s): BUD 2027-06 4000/CC200 = ${amt:,.2f}")
flt = "period='2027-06' AND account_id='4000' AND cost_center_id='CC200' AND scenario_id='BUD'"
uc = spark.sql(f"SELECT CAST(amount AS DOUBLE) FROM {fc()}.v_combined WHERE {flt}").collect()[0][0]
print(f"  UC v_combined reads ${uc:,.2f} (match: {abs(uc - amt) < 0.01})")
au = spark.sql(f"SELECT action, CAST(new_amount AS DOUBLE), changed_at FROM {fc()}.v_plan_audit WHERE {flt} ORDER BY id DESC LIMIT 1").collect()[0]
print(f"  Latest audit row: {au[0]} ${au[1]:,.2f} at {au[2]}")
pool.c.close()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Decision / Why / Trade-off
# MAGIC **Decision:** Dash app with 8 pages, OAuth via SDK Config(), pooled psycopg
# MAGIC with token refresh, Chevron theme from theme.py.
# MAGIC **Why:** Dash is the most Python-native multi-page framework for Databricks Apps;
# MAGIC OAuth avoids PAT rotation; pooled connections reduce latency; the Chevron theme
# MAGIC keeps colors consistent and accessible.
# MAGIC **Trade-off:** Dash is heavier than Flask for a simple API, but the interactive
# MAGIC planning grid and forecast sliders justify the framework.
