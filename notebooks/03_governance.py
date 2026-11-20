# Databricks notebook source
# MAGIC %run ./bp_config

# COMMAND ----------
from databricks.sdk.service.catalog import EntityTagAssignment

# COMMAND ----------
# Shared column-name → comment dictionary
COL = {
    "period": "Accounting period (YYYY-MM)",
    "account_id": "4-digit GL account number",
    "cost_center_id": "Cost center code (CCnnn)",
    "scenario_id": "Scenario code (ACT, BUD, FC1, FCST, WIF)",
    "amount": "Amount in USD (positive by normal balance)",
    "is_actuals": "True if actuals, false for plan rows",
    "account_name": "Account description",
    "statement": "Financial statement (BS or PL)",
    "account_type": "Account type (asset, liability, equity, revenue, cost, contra)",
    "cost_center_name": "Cost center description",
    "year": "Calendar year",
    "month": "Calendar month (1-12)",
    "scenario_name": "Scenario description",
    "source_file": "Source CSV file path",
    "ingest_ts": "Ingestion timestamp",
}
TC = {
    "bronze_gl_actuals": "Raw GL actuals from CSV (append-only)",
    "bronze_dim_account": "Raw account dimension from CSV",
    "bronze_dim_cost_center": "Raw cost center dimension from CSV",
    "bronze_dim_time": "Raw time dimension from CSV",
    "bronze_dim_scenario": "Raw scenario dimension from CSV",
    "dim_account": "Chart of accounts (34: 15 BS + 19 PL)",
    "dim_cost_center": "Cost centers (10: producing, midstream, corporate)",
    "dim_time": "Time dimension (36 periods: 2025-01 to 2027-12)",
    "dim_scenario": "Scenario dimension (5: ACT, BUD, FC1, FCST, WIF)",
    "silver_actuals": "Validated GL actuals (3 bad rows dropped)",
    "fact_actuals": "Deduplicated actuals fact table",
}

# COMMAND ----------
# Apply table and column comments
for tbl, tc in TC.items():
    try:
        spark.sql(f"COMMENT ON TABLE {fc()}.{tbl} IS '{tc}'")
    except Exception as e:
        print(f"  Table comment {tbl}: {e}")
    for c in spark.sql(f"DESCRIBE {fc()}.{tbl}").collect():
        cn = c.col_name
        if cn in COL:
            try:
                spark.sql(f"COMMENT ON COLUMN {fc()}.{tbl}.{cn} IS '{COL[cn]}'")
            except Exception:
                pass
print(f"Comments applied to {len(TC)} tables")

# COMMAND ----------
# Governed tags: validate against tag policies before applying
tag_keys = set()
try:
    for tp in W.tag_policies.list_tag_policies():
        tag_keys.add(tp.tag_key)
    print(f"Available governed tags: {tag_keys}")
except Exception as e:
    print(f"Tag policies: {e}")

if "data_classification" in tag_keys:
    for tbl, val in [("fact_actuals","CONFIDENTIAL"),("silver_actuals","CONFIDENTIAL"),
                     ("dim_account","INTERNAL"),("dim_cost_center","INTERNAL"),
                     ("dim_time","INTERNAL"),("dim_scenario","INTERNAL")]:
        try:
            W.entity_tag_assignments.create(EntityTagAssignment(
                entity_name=f"{fc()}.{tbl}", tag_key="data_classification",
                entity_type="tables", tag_value=val))
            print(f"  data_classification={val} on {tbl}")
        except Exception:
            pass
else:
    print("  data_classification not in governed tags, skipping")

# COMMAND ----------
# App SP grants: USE CATALOG, USE SCHEMA, SELECT on schema
try:
    sp = app_sp_id()
    spark.sql(f"GRANT USE CATALOG ON CATALOG {cfg('catalog')} TO `{sp}`")
    spark.sql(f"GRANT USE SCHEMA ON SCHEMA {fc()} TO `{sp}`")
    spark.sql(f"GRANT SELECT ON SCHEMA {fc()} TO `{sp}`")
    print(f"Grants applied to app SP: {sp}")
except Exception as e:
    print(f"App SP grants (app may not exist yet): {e}")

# COMMAND ----------
# EVIDENCE: UPDATE on fact_actuals fails (materialized view)
print("=== EVIDENCE: UPDATE on fact_actuals ===")
try:
    spark.sql(f"UPDATE {fc()}.fact_actuals SET amount = 0 WHERE period = '2025-01'")
    print("FAIL: UPDATE succeeded (should have failed)")
except Exception as e:
    print(f"PASS: UPDATE failed as expected")
    print(f"  Error: {str(e)[:150]}")

# COMMAND ----------
# EVIDENCE: SHOW GRANTS on schema
print("=== EVIDENCE: SHOW GRANTS ===")
spark.sql(f"SHOW GRANTS ON SCHEMA {fc()}").show(truncate=False)

# COMMAND ----------
# EVIDENCE: Uncommented columns
print("=== EVIDENCE: Column comments ===")
uncommented = 0
for tbl in TC:
    for c in spark.sql(f"DESCRIBE {fc()}.{tbl}").collect():
        if c.col_name in COL and (not c.comment or c.comment == ""):
            uncommented += 1
            print(f"  Uncommented: {tbl}.{c.col_name}")
if uncommented == 0:
    print("  All known columns commented (PASS)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Decision / Why / Trade-off
# MAGIC **Decision:** Column comments via shared dictionary, governed tags validated
# MAGIC against tag policies, SELECT-only grants to the app SP.
# MAGIC **Why:** Column comments aid Genie and dashboard authoring. Governed tags ensure
# MAGIC compliance. SELECT-only on actuals prevents accidental writes.
# MAGIC **Trade-off:** Skipping governed keys owned by other teams (project, data_domain)
# MAGIC avoids mis-tagging but leaves some governance gaps.
