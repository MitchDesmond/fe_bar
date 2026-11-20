# Databricks notebook source
# MAGIC %pip install -q "psycopg[binary]"

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %run ./bp_config

# COMMAND ----------
from databricks.sdk.service.postgres import (
    Project, ProjectSpec, ProjectCustomTag,
    Catalog, CatalogCatalogSpec,
    SyncedTable, SyncedTableSyncedTableSpec,
    SyncedTableSyncedTableSpecSyncedTableSchedulingPolicy as SYNC_POLICY,
    Role, RoleRoleSpec, RoleIdentityType)
from databricks.sdk.service.catalog import EntityTagAssignment
import time

# COMMAND ----------
# Idempotent: create Lakebase project + UC catalog
proj = cfg("lakebase_project")
parent = f"projects/{proj}"
branch = f"{parent}/branches/{cfg('lakebase_branch')}"
pg_db = f"{branch}/databases/{cfg('pg_database')}"
lcat = cfg("lakebase_catalog")

try:
    _p = W.postgres.get_project(name=parent)
    if _p.delete_time:  # soft-deleted: restore before use
        W.postgres.undelete_project(name=parent).wait()
    print(f"Project ready: {proj}")
except Exception:
    W.postgres.create_project(project=Project(spec=ProjectSpec(
        display_name=proj, pg_version=17,
        custom_tags=[ProjectCustomTag(key="RemoveAfter", value=cfg("remove_after"))])),
        project_id=proj).wait()
    print(f"Created project: {proj}")

try:
    W.postgres.get_catalog(name=f"catalogs/{lcat}")
    print(f"Catalog exists: {lcat}")
except Exception:
    W.postgres.create_catalog(catalog=Catalog(spec=CatalogCatalogSpec(
        branch=branch, postgres_database=cfg("pg_database"),
        create_database_if_missing=True)),
        catalog_id=lcat).wait()
    print(f"Created catalog: {lcat}")

try:
    W.entity_tag_assignments.create(EntityTagAssignment(
        entity_name=lcat, tag_key="RemoveAfter",
        entity_type="catalogs", tag_value=cfg("remove_after_uc")))
except Exception:
    pass

# COMMAND ----------
# Postgres schema planning: 4 tables + append-only trigger
conn = pg_connect()
cur = conn.cursor()
cur.execute("""
CREATE SCHEMA IF NOT EXISTS planning;
CREATE TABLE IF NOT EXISTS planning.plan_input (
    period VARCHAR(7) NOT NULL,
    account_id VARCHAR(4) NOT NULL,
    cost_center_id VARCHAR(5) NOT NULL,
    scenario_id VARCHAR(10) NOT NULL CHECK (scenario_id <> 'ACT'),
    amount DECIMAL(18,2) NOT NULL,
    version INT NOT NULL DEFAULT 1,
    updated_by TEXT NOT NULL DEFAULT current_user,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (period, account_id, cost_center_id, scenario_id));
CREATE TABLE IF NOT EXISTS planning.plan_assumptions (
    scenario_id VARCHAR(10) NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (scenario_id, key));
CREATE TABLE IF NOT EXISTS planning.version_lock (
    scenario_id VARCHAR(10) PRIMARY KEY, locked_by TEXT, locked_at TIMESTAMPTZ);
CREATE TABLE IF NOT EXISTS planning.audit_log (
    id BIGSERIAL PRIMARY KEY, period VARCHAR(7), account_id VARCHAR(4),
    cost_center_id VARCHAR(5), scenario_id VARCHAR(10), action TEXT NOT NULL,
    old_amount DECIMAL(18,2), new_amount DECIMAL(18,2),
    changed_by TEXT NOT NULL DEFAULT current_user,
    changed_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE OR REPLACE FUNCTION planning.no_modify() RETURNS TRIGGER AS $$
BEGIN RAISE EXCEPTION 'audit_log is append-only'; END;
$$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS no_update ON planning.audit_log;
DROP TRIGGER IF EXISTS no_delete ON planning.audit_log;
CREATE TRIGGER no_update BEFORE UPDATE ON planning.audit_log
    FOR EACH ROW EXECUTE FUNCTION planning.no_modify();
CREATE TRIGGER no_delete BEFORE DELETE ON planning.audit_log
    FOR EACH ROW EXECUTE FUNCTION planning.no_modify();
""")
conn.commit()
print("Postgres schema planning ready (4 tables + append-only trigger)")

# COMMAND ----------
# Postgres role for app SP + grants
try:
    sp = app_sp_id()
    try:
        W.postgres.create_role(parent=branch,
            role=Role(spec=RoleRoleSpec(postgres_role=sp,
                identity_type=RoleIdentityType.SERVICE_PRINCIPAL)),
            role_id="app-sp").wait()
        print(f"Created role for SP: {sp}")
    except Exception as e:
        print(f"Role create skipped: {str(e)[:120]}")
    cur.execute(f'GRANT USAGE ON SCHEMA planning TO "{sp}"')
    cur.execute(f'GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA planning TO "{sp}"')
    cur.execute(f'GRANT USAGE ON ALL SEQUENCES IN SCHEMA planning TO "{sp}"')  # audit_log id
    conn.commit()
    print(f"Grants applied to SP: {sp}")
except Exception as e:
    conn.rollback()
    print(f"App SP grants failed: {e}")

# COMMAND ----------
# SNAPSHOT synced tables (fact_actuals + 4 dims → schema gold)
for tbl, pk in [("fact_actuals",["period","account_id","cost_center_id","scenario_id"]),
                ("dim_account",["account_id"]), ("dim_cost_center",["cost_center_id"]),
                ("dim_time",["period"]), ("dim_scenario",["scenario_id"])]:
    try:
        W.postgres.create_synced_table(
            synced_table=SyncedTable(spec=SyncedTableSyncedTableSpec(
                source_table_full_name=f"{fc()}.{tbl}",
                primary_key_columns=pk, branch=branch,
                postgres_database=cfg("pg_database"),
                create_database_objects_if_missing=True,
                scheduling_policy=SYNC_POLICY.SNAPSHOT)),
            synced_table_id=f"{lcat}.gold.{tbl}").wait()
        print(f"  Synced: {tbl}")
    except Exception as e:
        print(f"  {tbl} (may exist): {str(e)[:80]}")

# COMMAND ----------
# UC view v_combined (ACT actuals UNION ALL plan_input)
spark.sql(f"""
CREATE OR REPLACE VIEW {fc()}.v_combined AS
SELECT period, account_id, cost_center_id, scenario_id, amount, is_actuals
FROM {fc()}.fact_actuals WHERE scenario_id = 'ACT'
UNION ALL
SELECT period, account_id, cost_center_id, scenario_id, amount, false AS is_actuals
FROM {lcat}.planning.plan_input
""")
print("Created v_combined view")

# COMMAND ----------
# EVIDENCE: upsert + audit in one txn, read via UC, latency, counts
cur.execute("""
BEGIN;
INSERT INTO planning.plan_input (period, account_id, cost_center_id, scenario_id, amount, version, updated_by)
VALUES ('2027-01','4000','CC200','BUD',65000000,1,current_user)
ON CONFLICT (period, account_id, cost_center_id, scenario_id)
DO UPDATE SET amount=EXCLUDED.amount, version=plan_input.version+1, updated_at=now();
INSERT INTO planning.audit_log (period, account_id, cost_center_id, scenario_id, action, new_amount)
VALUES ('2027-01','4000','CC200','BUD','UPSERT',65000000);
COMMIT;
""")
conn.commit()
print("=== EVIDENCE: Upsert + audit in one txn ===")
try:
    uc_count = spark.sql(f"SELECT COUNT(*) FROM {lcat}.planning.plan_input").collect()[0][0]
    print(f"  Plan rows via UC: {uc_count}")
except Exception as e:
    print(f"  UC read (sync may be pending): {e}")
t0 = time.time()
cur.execute("SELECT COUNT(*) FROM planning.plan_input")
pg_count = cur.fetchone()[0]
print(f"  Postgres count: {pg_count}, latency: {time.time()-t0:.3f}s")
cur.execute("SELECT tgname FROM pg_trigger WHERE tgrelid='planning.audit_log'::regclass AND NOT tgisinternal")
print(f"  Triggers on audit_log: {[r[0] for r in cur.fetchall()]} (expect no_update, no_delete)")
uc_fact = spark.sql(f"SELECT COUNT(*) FROM {fc()}.fact_actuals").collect()[0][0]
try:
    lb_fact = spark.sql(f"SELECT COUNT(*) FROM {lcat}.gold.fact_actuals").collect()[0][0]
    print(f"  UC fact: {uc_fact} | Lakebase fact: {lb_fact} (expect equal)")
except Exception as e:
    print(f"  Lakebase sync (pending): {e}")
conn.close()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Decision / Why / Trade-off
# MAGIC **Decision:** Lakebase as OLTP planning store with SNAPSHOT synced gold tables.
# MAGIC **Why:** Postgres enables interactive cell editing with ACID; synced tables bring
# MAGIC UC data into Postgres for joins; v_combined unifies actuals+plans.
# MAGIC **Trade-off:** SNAPSHOT sync has latency vs CONTINUOUS; acceptable for planning
# MAGIC where data changes are user-driven, not high-frequency.
