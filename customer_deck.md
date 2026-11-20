# Meridian Energy FP&A: customer deck

## Slide 1: The outcome
A price assumption move used to mean a multi-week spreadsheet re-cycle (illustrative baseline: ~120 analyst-hours per quarter, 3 planners rebuilding 40+ linked workbooks). On this platform the same full re-forecast is a 12.5-minute job run (measured, run 599582910141603) and a driver change re-prices the plan in seconds (measured: 0.004s). Forecast error: 2.9% MAPE vs 4.6% naive.

## Slide 2: Two conversations, one platform
**CFO (funding executive), owns EBITDA margin (FY2025 49.1%) and cash guidance:** P&L, BS and CF tie to $0.00 every run; the +30% price what-if (WIF) re-prices FY2027 revenue from $939.7M (BUD) to $1,288.8M in one audited run.

**VP FP&A (functional leader), owns forecast accuracy and cycle time:** planners edit cells with versioning and period locks, move driver sliders and ask Genie questions; one governed copy replaces reconciling 40 workbooks.

**Controller, owns audit completeness:** every plan write lands in a trigger-enforced append-only audit log.

## Slide 3: Before / after
Before: actuals as emailed CSVs, budget in linked workbooks, one broken link breaks the model. After: a governed pipeline with data-quality rules (3 injected bad rows each dropped by its rule), plans in Lakebase behind an append-only audit log, Genie answers from the same tables the app edits.

## Slide 4: Proof it runs
Every number comes from committed run evidence: evidence/run_599582910141603.md (8/8 SUCCESS, Genie vs direct SQL, BS/CF ties, audited edit).

## Slide 5: Demo script (15 min, Chevron finance audience)
1. Overview: FY2025 revenue $947.6M, EBITDA margin 49.1% (2 min)
2. Plan grid: edit a budget cell, Save, watch the audit row appear (3 min)
3. Forecast: move the oil price slider, watch FY2027 re-price (BUD vs FCST vs WIF) (4 min)
4. Ask: "Compare BUD vs FCST revenue for FY2027" (3 min)
5. Governance: SELECT-only grants, the failed UPDATE on actuals, append-only triggers (3 min)

## Honest limits and next steps
Not verified: page rendering over HTTP; stage_b endpoint not deployed. Level-up ideas: stress-scenario data skew (well outage, price crash), drift-triggered retraining, row-level security by basin.
