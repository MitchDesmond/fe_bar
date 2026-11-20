# Databricks notebook source
# MAGIC %run ./bp_config

# COMMAND ----------
import json, uuid

# COMMAND ----------
# Create UC views exposing Lakebase tables for Genie (Genie rejects Lakebase catalog)
lcat = cfg("lakebase_catalog")
spark.sql(f"""
CREATE OR REPLACE VIEW {fc()}.v_plan_drivers AS
SELECT scenario_id, key, value, updated_at
FROM {lcat}.planning.plan_assumptions
""")
spark.sql(f"""
CREATE OR REPLACE VIEW {fc()}.v_plan_audit AS
SELECT id, period, account_id, cost_center_id, scenario_id, action,
       old_amount, new_amount, changed_by, changed_at
FROM {lcat}.planning.audit_log
""")
print("Created v_plan_drivers and v_plan_audit views")

# COMMAND ----------
# Build serialized Genie space JSON (v2)
tables = sorted([
    f"{fc()}.consolidated_pnl", f"{fc()}.consolidated_bs", f"{fc()}.consolidated_cf",
    f"{fc()}.fact_actuals", f"{fc()}.dim_account", f"{fc()}.dim_cost_center",
    f"{fc()}.dim_time", f"{fc()}.dim_scenario", f"{fc()}.v_combined",
    f"{fc()}.v_plan_drivers", f"{fc()}.v_plan_audit", f"{fc()}.forecast_model",
])
instructions = """Meridian Energy, Inc. is a fictional upstream oil & gas producer (Permian, Gulf of Mexico, Appalachia).
Chart of accounts: 4000 crude, 4100 gas, 4200 NGLs, 5000 LOE, 5100/5200 labor, 6000-6900 opex (6500 G&T, 6600 DD&A, 6700 ARO accretion, 6900 severance), 7000 interest, 8000 tax.
Scenario vocabulary: ACT = actuals (read-only through 2026-09), BUD = budget FY2027 ($75 WTI / $3.50 HH), FCST = rolling forecast, FC1 = Q4 2026 forecast, WIF = what-if high price (+30% oil and gas).
Period rules: YYYY-MM format, actuals through 2026-09, plans from 2026-10.
Always use consolidated_pnl for P&L questions. Use consolidated_bs for balance sheet and consolidated_cf for cash flow.
Energy metrics: LOE (5000) per revenue dollar is a key efficiency metric. Compute as SUM(5000)/SUM(4000+4100+4200) per period.
Formatting: present amounts in $M with 1 decimal. When comparing scenarios, show the difference and % difference.
Cost centers: CC200 Permian, CC210 Gulf of Mexico, CC220 Appalachia, CC300/CC600 midstream, CC100/CC500 field, CC400/CC410/CC700 corporate."""

hid = lambda: uuid.uuid4().hex
QUESTIONS = ["What is the FY2025 revenue?", "Show EBITDA margin by month for 2025",
    "What is the LOE per revenue dollar for the Permian cost center?",
    "Compare BUD vs FCST revenue for FY2027", "What are the key drivers in the WIF scenario?"]
SQL = {
    "What is the FY2025 revenue?": f"SELECT SUM(revenue)/1e6 AS revenue_musd FROM {fc()}.consolidated_pnl WHERE period BETWEEN '2025-01' AND '2025-12' AND scenario_id = 'ACT'",
    "Compare BUD vs FCST revenue for FY2027": f"SELECT scenario_id, SUM(revenue)/1e6 AS revenue_musd FROM {fc()}.consolidated_pnl WHERE period BETWEEN '2027-01' AND '2027-12' AND scenario_id IN ('BUD','FCST') GROUP BY scenario_id",
}
# Genie serialized space v2
space_json = json.dumps({"version": 2,
    "config": {"sample_questions": sorted([{"id": hid(), "question": [q]} for q in QUESTIONS], key=lambda x: x["id"])},
    "data_sources": {"tables": [{"identifier": t} for t in tables]},
    "instructions": {"text_instructions": [{"id": hid(), "content": [l + "\n" for l in instructions.splitlines()]}],
        "example_question_sqls": sorted([{"id": hid(), "question": [q], "sql": [s]} for q, s in SQL.items()], key=lambda x: x["id"])}})

# COMMAND ----------
# Create or update Genie space
wh_id = warehouse_id()
title = cfg("genie_space_title")
try:
    space_id = genie_space_id()  # paginated title lookup
except ValueError:
    space_id = None
if space_id:
    W.genie.update_space(space_id, serialized_space=space_json, title=title, warehouse_id=wh_id)
    print(f"Updated Genie space: {space_id}")
else:
    gs = W.genie.create_space(warehouse_id=wh_id, serialized_space=space_json, title=title)
    space_id = gs.space_id
    print(f"Created Genie space: {space_id}")

# App SP needs CAN_RUN so the app's Ask page can query the space
from databricks.sdk.service.iam import AccessControlRequest, PermissionLevel
try:
    W.permissions.update("genie", space_id, access_control_list=[AccessControlRequest(
        service_principal_name=app_sp_id(), permission_level=PermissionLevel.CAN_RUN)])
    print("Granted CAN_RUN on space to app SP")
except Exception as e:
    print(f"Genie grant: {str(e)[:120]}")

# Export to genie/ for the bundle
genie_dir = APP_DIR.rsplit("/app", 1)[0] + "/genie" if APP_DIR else ""
if genie_dir:
    import os
    if not genie_dir.startswith("/Workspace"): genie_dir = "/Workspace" + genie_dir
    os.makedirs(genie_dir, exist_ok=True)
    with open(f"{genie_dir}/meridian_fpa.genie.json", "w") as f:
        f.write(space_json)
    print(f"Exported to {genie_dir}/meridian_fpa.genie.json")

# COMMAND ----------
# EVIDENCE: 3 questions via start_conversation_and_wait
print("=== PHASE 7 EVIDENCE ===")
questions = [
    "What is the FY2025 revenue?",
    "Show EBITDA margin by month for 2025",
    "Compare BUD vs FCST revenue for FY2027",
]
for q in questions:
    try:
        msg = W.genie.start_conversation_and_wait(space_id=space_id, content=q)
        print(f"\n  Q: {q}  [status={msg.status.value}]")
        for a in msg.attachments or []:  # SQL + text live in attachments
            if a.query:
                print(f"  SQL: {' '.join(a.query.query.split())[:300]}")
                r = W.genie.get_message_attachment_query_result(space_id, msg.conversation_id, msg.message_id, a.attachment_id)
                rows = r.statement_response.result.data_array if r.statement_response.result else []
                print(f"  Rows ({len(rows or [])}): {(rows or [])[:3]}")
            if a.text: print(f"  Answer: {a.text.content[:250]}")
    except Exception as e:
        print(f"\n  Q: {q}\n  Error: {e}")

# Cross-check Q1 with direct SQL
direct = spark.sql(f"SELECT SUM(revenue)/1e6 FROM {fc()}.consolidated_pnl WHERE period BETWEEN '2025-01' AND '2025-12' AND scenario_id = 'ACT'").collect()[0][0]
print(f"\n  Cross-check: Direct SQL FY2025 revenue = ${direct:.1f}M")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Decision / Why / Trade-off
# MAGIC **Decision:** Expose Lakebase tables as UC views for Genie; serialized v2 JSON with
# MAGIC sorted table identifiers, instructions, sample questions, and SQL examples.
# MAGIC **Why:** Genie rejects Lakebase catalog tables directly; UC views bridge the gap.
# MAGIC Sorting by uuid hex ensures deterministic ordering. Instructions guide Genie to use
# MAGIC consolidated_pnl for P&L and format in $M.
# MAGIC **Trade-off:** Views add a thin layer of indirection but keep Genie queries simple.
