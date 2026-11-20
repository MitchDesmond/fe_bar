# Meridian Energy FP&A on Databricks

A lakehouse-native FP&A app for a fictional oil and gas producer (Meridian Energy, Inc.). Planners view actuals, edit budget and forecast cells in a Databricks App, change drivers and see a three-statement model update. Genie answers finance questions. Everything deploys from one bundle.

```
CSV in UC volume -> Lakeflow pipeline (bronze/silver/gold) -> fact_actuals + 4 dims
App edits -> Lakebase Postgres (plan_input + append-only audit_log) -> UC view v_combined
Forecast engine (OLS + drivers) -> UC model @champion -> scenarios BUD/FC1/FCST/WIF
consolidated_pnl / bs / cf -> Dash app (8 pages) + Genie space
```

## Run order
1. `databricks bundle deploy --target stage_a` (warehouse, pipeline, job, app)
2. `databricks bundle run chevron_fpa_build` (notebooks 01-08 in order)
3. Optional: `databricks bundle deploy --target stage_b --var model_version=N` (serving endpoint)

Every notebook ends with an evidence cell and a Decision / Why / Trade-off note.

## Evidence from an executed run
Job `chevron_fpa_build`, run 599582910141603, deployed from this repo: all 8 tasks SUCCESS (t05 re-run after a model signature fix). Output copied from each notebook's evidence cell.

**01 Setup**
```
GL rows: 1221 (expect 1221)
FY2025 Revenue: $947.6M (expect ~$945M)
EBITDA: $465.0M, Margin: 49.1% (expect ~49%)
BS max imbalance: $0.0000 (expect <$1)
```
**02 Pipeline** (all 11 row-count checks PASS; expectations read from the event log)
```
Bronze: 1221 | Silver: 1218 | Fact: 1218
Dims: 34/10/36/5 (account/cc/time/scenario)
|actuals_only      |1221|0|
|amount_not_null   |1220|1|
|cost_center_format|1221|0|
|known_account     |1220|1|
|valid_period      |1220|1|
```
**03 Governance**
```
PASS: UPDATE failed as expected
  [EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE] 'UPDATE' expects a table but fact_actuals is a view
App SP on chevron_fpa.finance: SELECT, USE SCHEMA
All known columns commented (PASS)
```
**04 Lakebase**
```
Synced: fact_actuals, dim_account, dim_cost_center, dim_time, dim_scenario
Plan rows via UC: 2610 | Postgres count: 2610, latency: 0.004s
Triggers on audit_log: ['no_update', 'no_delete']
UC fact: 1218 | Lakebase fact: 1218 (expect equal)
```
**05 Forecasting** (3-month holdout backtest)
```
Fitted 58 (account, cost_center) series on 1218 rows
OLS revenue MAPE: 2.9% | Naive revenue MAPE: 4.6% (OLS must be lower: True)
Model: chevron_fpa.finance.energy_forecast v4, alias=champion
Local prediction revenue 2027-01: $83.2M
```
**06 Scenarios** (Lakebase upsert + audit, one transaction per scenario)
```
FC1: 174 | FCST: 870 | BUD: 696 | WIF: 870 cells upserted
consolidated_pnl (66), consolidated_bs (675), consolidated_cf (45)
BUD FY2027 revenue: $939.7M | FCST: $991.4M | WIF: $1288.8M
BS max imbalance across all scenarios: $0.0000 (target <$0.02)
```
**07 Genie** (start_conversation_and_wait; SQL and rows from the attachments)
```
Q: What is the FY2025 revenue?  [COMPLETED]
Rows (1): [['947.57280497']]  Answer: FY2025 revenue in ACT is $947.6M
Q: Show EBITDA margin by month for 2025  [COMPLETED]
Rows (12): [['2025-01','0.4977'], ['2025-02','0.4928'], ['2025-03','0.4901']] ...
Answer: ranging from 48.3% to 49.8%
Q: Compare BUD vs FCST revenue for FY2027  [COMPLETED]
Rows (2): [['BUD','939.65...'], ['FCST','991.36...']]
Answer: FCST is higher by $51.7M (+5.5%)
Cross-check: Direct SQL FY2025 revenue = $947.6M
```
**08 App** (edit goes through the app's own `plan_store.save_cells`)
```
App: chevron-fpa-planner, app status: RUNNING, compute: ACTIVE
save_cells changed 1 cell(s): BUD 2027-06 4000/CC200 = $75,000,845.46
UC v_combined reads $75,000,845.46 (match: True)
Latest audit row: APP_EDIT $75,000,845.46
```

## Not shown in this run
* Page rendering: the HTTP check in 08 got 302 (redirect to OAuth login) on all 8 pages, because apps do not accept a notebook token. Pages were not verified over HTTP.
* The stage_b serving endpoint was not deployed, so the endpoint vs local comparison is pending.

## Key decisions (details in each notebook)
* Gold dims are declared before silver so silver can LEFT JOIN dim_account and known_account fires instead of rows silently disappearing.
* Plan edits go to Lakebase for transactional cell writes plus an append-only audit log; UC views expose them to Genie, which rejects Lakebase catalog tables.
* SNAPSHOT sync over CONTINUOUS: actuals change once per close.
* Transparent OLS with a driver overlay so finance can trace every number.
* Cash is the balance-sheet plug, which ties the three statements together.
* No ids in code: warehouse, app service principal and Genie space are looked up by name.
