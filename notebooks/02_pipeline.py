# Databricks notebook source
# MAGIC %run ./bp_config

# COMMAND ----------
import time

# COMMAND ----------
# Find pipeline by name, start update, wait for completion
pid = None
for p in W.pipelines.list_pipelines():
    if p.name == cfg("pipeline_name"):
        pid = p.pipeline_id
        break
if not pid:
    raise ValueError(f"Pipeline '{cfg('pipeline_name')}' not found. Deploy the bundle first.")
print(f"Pipeline ID: {pid}")

upd = W.pipelines.start_update(pipeline_id=pid, full_refresh=False)
print(f"Update: {upd.update_id}")

# COMMAND ----------
# Wait for completion (poll every 15s)
_t0 = time.time()
while True:
    info = W.pipelines.get_update(pipeline_id=pid, update_id=upd.update_id)
    state = info.update.state.value  # enum -> str
    print(f"  state={state}", flush=True)
    if state in ("COMPLETED", "FAILED", "CANCELED"):
        break
    if time.time() - _t0 > 1800: raise TimeoutError("Pipeline update > 30 min")
    time.sleep(15)

if state != "COMPLETED":
    raise RuntimeError(f"Pipeline update ended with state={state}")
print(f"Pipeline update completed: {state}")

# COMMAND ----------
# EVIDENCE: Row counts
print("=== ROW COUNTS ===")
_expected = {
    "bronze_gl_actuals": 1221, "bronze_dim_account": 34,
    "bronze_dim_cost_center": 10, "bronze_dim_time": 36,
    "bronze_dim_scenario": 5,
    "silver_actuals": 1218, "fact_actuals": 1218,
    "dim_account": 34, "dim_cost_center": 10,
    "dim_time": 36, "dim_scenario": 5,
}
_all_pass = True
for tbl, exp in _expected.items():
    act = spark.sql(f"SELECT COUNT(*) AS n FROM {fc()}.{tbl}").collect()[0]["n"]
    mark = "PASS" if act == exp else "FAIL"
    if act != exp:
        _all_pass = False
    print(f"  {mark} {tbl}: {act} (expect {exp})")

# COMMAND ----------
# EVIDENCE: Dropped rows (3 bad rows: invalid period, null amount, unknown account)
print("=== DROPPED ROW CHECKS ===")
bad_period = spark.sql(
    f"SELECT COUNT(*) FROM {fc()}.silver_actuals WHERE period = '2026-13'"
).collect()[0][0]
print(f"  Bad period (2026-13) in silver (expect 0): {bad_period}")
null_amt = spark.sql(
    f"SELECT COUNT(*) FROM {fc()}.silver_actuals WHERE amount IS NULL"
).collect()[0][0]
print(f"  Null amount in silver (expect 0): {null_amt}")
unknown_acct = spark.sql(
    f"SELECT COUNT(*) FROM {fc()}.silver_actuals WHERE account_id = '9999'"
).collect()[0][0]
print(f"  Unknown account (9999) in silver (expect 0): {unknown_acct}")
br = spark.sql(f"SELECT COUNT(*) FROM {fc()}.bronze_gl_actuals").collect()[0][0]
sv = spark.sql(f"SELECT COUNT(*) FROM {fc()}.silver_actuals").collect()[0][0]
print(f"  Total dropped: {br - sv} (expect 3)")

# COMMAND ----------
# EVIDENCE: Expectation failures from event log
print("=== EXPECTATION FAILURES ===")
try:
    ev = spark.sql(f"""
        SELECT e.dataset, e.name, SUM(e.passed_records) AS passed, SUM(e.failed_records) AS failed
        FROM (SELECT explode(from_json(details:flow_progress:data_quality:expectations,
          'array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>')) e
          FROM event_log(TABLE({fc()}.silver_actuals)) WHERE event_type = 'flow_progress')
        WHERE e.dataset LIKE '%silver_actuals' GROUP BY ALL ORDER BY 2""")
    ev.show(truncate=False)
except Exception as e:
    print(f"  Event log query (optional): {e}")

# COMMAND ----------
# EVIDENCE SUMMARY
print("=== PHASE 2 EVIDENCE ===")
ft = spark.sql(f"SELECT COUNT(*) FROM {fc()}.fact_actuals").collect()[0][0]
da = spark.sql(f"SELECT COUNT(*) FROM {fc()}.dim_account").collect()[0][0]
dcc = spark.sql(f"SELECT COUNT(*) FROM {fc()}.dim_cost_center").collect()[0][0]
dt = spark.sql(f"SELECT COUNT(*) FROM {fc()}.dim_time").collect()[0][0]
ds = spark.sql(f"SELECT COUNT(*) FROM {fc()}.dim_scenario").collect()[0][0]
print(f"Bronze: {br} | Silver: {sv} | Fact: {ft}")
print(f"Dims: {da}/{dcc}/{dt}/{ds} (account/cc/time/scenario)")
print(f"Dropped: {br - sv} (3 bad rows: invalid period, null amount, unknown account)")
print(f"State: {state}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Decision / Why / Trade-off
# MAGIC **Decision:** Gold dims declared before silver; silver LEFT JOINs dim_account.
# MAGIC **Why:** Silver joins dim_account for the known_account constraint to catch
# MAGIC account 9999. LEFT JOIN (not INNER) ensures the constraint fires instead of
# MAGIC silently dropping via join. QUALIFY dedup keeps latest ingest on all bronze.
# MAGIC **Trade-off:** LEFT JOIN adds minor overhead vs INNER JOIN, but is necessary
# MAGIC for the DQ evidence to show exactly 1 failure per DROP rule.
