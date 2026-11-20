# Meridian Energy FP&A on Databricks

Lakehouse-native FP&A for a fictional oil and gas producer. Planners edit plan cells in a Databricks App, change drivers, and see P&L, balance sheet and cash flow re-tie. One bundle deploys it all.

```
CSV in UC volume -> Lakeflow pipeline (bronze/silver/gold) -> fact_actuals + 4 dims
App edits -> Lakebase Postgres (plan_input + append-only audit_log) -> UC view v_combined
Forecast engine (OLS + drivers) -> UC model @champion -> scenarios BUD/FC1/FCST/WIF
consolidated_pnl / bs / cf -> Dash app (8 pages) + Genie space
```

## Deploy and run
1. `databricks bundle deploy --target stage_a` (warehouse, pipeline, job, app)
2. `databricks bundle run chevron_fpa_build` (notebooks 01-08, idempotent)
3. Optional: `bundle deploy --target stage_b --var model_version=N` (serving endpoint, config in databricks.yml)

## Evidence
Run 599582910141603 (2026-11-09, from this repo): all 8 tasks SUCCESS in 12.5 min; committed run log with verbatim output in evidence/. Headlines: FY2025 revenue $947.6M (Genie matches direct SQL); 3 bad rows each dropped by its rule; OLS MAPE 2.9% vs 4.6% naive; BS imbalance $0.0000; app edit audited. Not verified: page rendering over HTTP; stage_b endpoint.

## Metric owners
CFO owns EBITDA margin (49.1% FY2025) and cash guidance (statements tie to $0.00). VP FP&A owns forecast accuracy and cycle time (12.5 min full re-forecast). Controller owns audit completeness (append-only, trigger-enforced).

## Key decisions (full notes in each notebook)
Dims before silver so the known_account rule fires; Lakebase for transactional cell writes, UC views for Genie; SNAPSHOT sync (actuals change once per close); transparent OLS with driver overlay; cash as the BS plug; no ids in code.

Customer deck: customer_deck.md. Limits: 22 files, under 9k each, under 90k total; no ids or personal paths.
