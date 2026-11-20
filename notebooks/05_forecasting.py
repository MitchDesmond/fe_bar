# Databricks notebook source
# MAGIC %run ./bp_config

# COMMAND ----------
import_app()
import numpy as np, pandas as pd, json, mlflow
from forecast_engine import fit_baseline, predict, forecast_scenario, pnl_summary, coefs_to_json, coefs_from_json, REV
import logging; logging.getLogger("pyspark.sql.connect.logging").setLevel(logging.ERROR)

# COMMAND ----------
# Load actuals, fit baseline, define grain
fact = spark.sql(f"SELECT period, account_id, cost_center_id, CAST(amount AS DOUBLE) AS amount FROM {fc()}.fact_actuals").toPandas()
coefs = fit_baseline(fact)
grain = list(coefs.keys())
print(f"Fitted {len(coefs)} (account, cost_center) series on {len(fact)} rows")

# COMMAND ----------
# Backtest: hold out last 3 months, compare OLS vs seasonal naive
holdout = ["2026-07","2026-08","2026-09"]
train = fact[~fact.period.isin(holdout)]
bt_coefs = fit_baseline(train)
# OLS predictions
ols_preds = []
for aid, cc in grain:
    p = predict(bt_coefs, aid, cc, holdout)
    p["account_id"] = aid; p["cost_center_id"] = cc
    ols_preds.append(p)
ols_df = pd.concat(ols_preds, ignore_index=True)
# Seasonal naive: same month last year
naive_preds = []
for p in holdout:
    yr, mo = int(p[:4]), int(p[5:7])
    prev = f"{yr-1}-{mo:02d}"
    for _, r in train[train.period == prev].iterrows():
        naive_preds.append({"period": p, "account_id": r.account_id,
            "cost_center_id": r.cost_center_id, "amount": r.amount})
naive_df = pd.DataFrame(naive_preds)
# MAPE on revenue
def rev_mape(pred, actual, periods):
    errs = []
    for p in periods:
        pr = pred[pred.period==p].merge(actual[actual.period==p], on=["account_id","cost_center_id"], suffixes=("_p","_a"))
        pr = pr[pr.account_id.isin(REV)]
        pr["ape"] = (pr.amount_p - pr.amount_a).abs() / pr.amount_a.abs().clip(lower=1)
        errs.extend(pr.ape.tolist())
    return np.mean(errs) * 100
ols_mape = rev_mape(ols_df, fact, holdout)
naive_mape = rev_mape(naive_df, fact, holdout)
print(f"OLS revenue MAPE: {ols_mape:.1f}% (target <4.4%)")
print(f"Naive revenue MAPE: {naive_mape:.1f}% (target >OLS)")

# Try ai_forecast (preview, may be disabled)
try:
    from databricks import ai_forecast
    print("ai_forecast available")
except Exception:
    print("ai_forecast not available (preview disabled)")

# COMMAND ----------
# Log pyfunc model with coefficients as artifacts, engine as code_paths
class ForecastModel(mlflow.pyfunc.PythonModel):
    def load_context(self, context):  # MLflow passes these by keyword: names must match
        import sys, json
        sys.path.insert(0, context.artifacts["app_dir"])
        from forecast_engine import coefs_from_json, forecast_scenario
        with open(context.artifacts["coefs.json"]) as f:
            self.coefs = coefs_from_json(json.load(f))
        self._fs = forecast_scenario
    def predict(self, context, model_input):
        row = model_input.iloc[0].to_dict()
        periods = [f"{y}-{m:02d}" for y in range(int(row["period_start"][:4]), int(row["period_end"][:4])+1)
                   for m in range(1, 13) if f"{y}-{m:02d}" >= row["period_start"] and f"{y}-{m:02d}" <= row["period_end"]]
        drv = {k: float(v) for k, v in row.items() if k not in ("period_start","period_end","scenario")}
        grain = list(self.coefs.keys())
        return self._fs(self.coefs, drv, periods, grain)

import tempfile, os
with tempfile.TemporaryDirectory() as tmp:
    coef_path = os.path.join(tmp, "coefs.json")
    with open(coef_path, "w") as f: json.dump(coefs_to_json(coefs), f)
    mlflow.set_experiment(f"/Users/{W.current_user.me().user_name}/chevron_fpa_forecast")
    # UC registration requires a signature: one row = scenario + period range + drivers
    ex_drv = {"production_growth":1.0,"oil_price":1.0,"gas_price":1.0,"headcount":1.0,
              "merit":1.0,"opex_change":1.0,"loe_ratio":0.145,"gt_ratio":0.048,"tax_rate":0.21}
    ex = pd.DataFrame([{"scenario":"FCST","period_start":"2027-01","period_end":"2027-03", **ex_drv}])
    sig = mlflow.models.infer_signature(ex, forecast_scenario(coefs, ex_drv, ["2027-01","2027-02","2027-03"], grain))
    with mlflow.start_run() as run:
        mlflow.log_artifact(coef_path, artifact_path="model")
        mlflow.pyfunc.log_model(
            "forecast_model",
            python_model=ForecastModel(), signature=sig, input_example=ex,
            artifacts={"coefs.json": coef_path, "app_dir": APP_DIR},
            code_paths=[f"{APP_DIR}/forecast_engine.py"],
            pip_requirements=["numpy","pandas"])
        model_uri = f"runs:/{run.info.run_id}/forecast_model"
        mv = mlflow.register_model(model_uri, cfg("model_name"))
        mlflow.MlflowClient().set_registered_model_alias(cfg("model_name"), "champion", mv.version)
        print(f"Registered model {cfg('model_name')} v{mv.version}, alias=champion")

# COMMAND ----------
# Overwrite forecast_model table for the app
fcst_periods = [f"{y}-{m:02d}" for y in (2026,2027) for m in range(1,13) if f"{y}-{m:02d}" >= "2026-10"]
drivers = {"production_growth":1.0,"oil_price":1.0,"gas_price":1.0,"headcount":1.0,
           "merit":1.0,"opex_change":1.0,"loe_ratio":0.145,"gt_ratio":0.048,"tax_rate":0.21}
fcst_df = forecast_scenario(coefs, drivers, fcst_periods, grain)
fcst_df["scenario_id"] = "FCST"
spark.createDataFrame(fcst_df[["period","account_id","cost_center_id","scenario_id","amount","lower","upper"]]).write \
    .mode("overwrite").saveAsTable(f"{fc()}.forecast_model")
print(f"Wrote {len(fcst_df)} forecast rows to {fc()}.forecast_model")

# COMMAND ----------
# EVIDENCE: MAPE comparison + local prediction (endpoint comparison needs Stage B)
print("=== PHASE 5 EVIDENCE ===")
print(f"OLS revenue MAPE: {ols_mape:.1f}% | Naive revenue MAPE: {naive_mape:.1f}% (OLS must be lower: {ols_mape < naive_mape})")
print(f"Model: {cfg('model_name')} v{mv.version}, alias=champion")
local_pred = forecast_scenario(coefs, drivers, ["2027-01"], grain)
local_rev = local_pred[local_pred.account_id.isin(REV)].amount.sum()
print(f"Local prediction revenue 2027-01: ${local_rev/1e6:.1f}M")
print("Endpoint test: deploy Stage B to enable")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Decision / Why / Trade-off
# MAGIC **Decision:** OLS on log|amount| with trend+seasonality, driver overlay for LOE/G&T/tax.
# MAGIC **Why:** OLS is transparent and fast; drivers let planners adjust assumptions without
# MAGIC refitting. Log transform handles skew; sign preservation handles interest income.
# MAGIC **Trade-off:** OLS misses nonlinearities vs ML, but is more interpretable for finance.
