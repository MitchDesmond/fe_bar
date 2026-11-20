# Databricks notebook source
# MAGIC %run ./bp_config

# COMMAND ----------
import os, pandas as pd, numpy as np

# COMMAND ----------
# Create catalog, schema, volume with RemoveAfter tag
from databricks.sdk.service.catalog import EntityTagAssignment
spark.sql(f"CREATE CATALOG IF NOT EXISTS {cfg('catalog')}")
try:
    W.entity_tag_assignments.create(EntityTagAssignment(
        entity_name=cfg("catalog"), tag_key="RemoveAfter",
        entity_type="catalogs", tag_value=cfg("remove_after_uc")))
except Exception:
    pass  # tag may already be assigned
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {fc()}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {fc()}.{cfg('volume')}")
print(f"Catalog {cfg('catalog')}, schema {fc()}, volume {cfg('volume')} ready")

# COMMAND ----------
# Chart of accounts (34: 15 BS + 19 PL)
ACCOUNTS = [
    ("1000","Cash","BS","asset"),("1100","AR from Purchasers","BS","asset"),
    ("1200","O&G Properties","BS","asset"),("1300","Accumulated DD&A","BS","contra"),
    ("1400","Unproved Leasehold","BS","asset"),("1500","Prepaid and Other","BS","asset"),
    ("1600","AP","BS","liability"),("1700","Rev & Roy Payable","BS","liability"),
    ("1800","Accrued Liabilities","BS","liability"),("2000","Current Debt","BS","liability"),
    ("2100","Long-Term Debt","BS","liability"),("2200","ARO","BS","liability"),
    ("2300","Deferred Tax","BS","liability"),("3000","Common Stock","BS","equity"),
    ("3100","Retained Earnings","BS","equity"),
    ("4000","Crude Revenue","PL","revenue"),("4100","Gas Revenue","PL","revenue"),
    ("4200","NGL Revenue","PL","revenue"),("5000","LOE","PL","cost"),
    ("5100","Field Labor","PL","cost"),("5200","Gas Plant Labor","PL","cost"),
    ("6000","Repairs & Maint","PL","cost"),("6100","Materials & Supplies","PL","cost"),
    ("6200","Insurance & Benefits","PL","cost"),("6300","Professional Fees","PL","cost"),
    ("6400","General & Admin","PL","cost"),("6500","Gathering & Transport","PL","cost"),
    ("6600","DD&A","PL","cost"),("6700","ARO Accretion","PL","cost"),
    ("6800","Exploration Expense","PL","cost"),("6900","Severance & Ad Valorem","PL","cost"),
    ("7000","Interest Expense","PL","cost"),("7100","Interest Income","PL","revenue"),
    ("8000","Income Tax Expense","PL","cost")]
COST_CENTERS = [
    ("CC100","Field & Technical"),("CC200","Permian"),
    ("CC210","Gulf of Mexico"),("CC220","Appalachia"),
    ("CC300","Midstream"),("CC400","Corporate"),
    ("CC410","Corporate IT"),("CC500","Field & Tech 2"),
    ("CC600","Midstream 2"),("CC700","Corporate Finance")]
SCENARIOS = [
    ("ACT","Actuals"),("BUD","Budget FY2027"),("FC1","Forecast Q4 2026"),
    ("FCST","Rolling Forecast"),("WIF","What-If High Price")]
PERIODS = [f"{y}-{m:02d}" for y in (2025,2026,2027) for m in range(1,13)]
N_ACT = 21  # 2025-01 .. 2026-09

# COMMAND ----------
# Generate GL actuals (P&L only; 58 rows/period × 21 + 3 bad = 1221)
np.random.seed(42)
BASE = {"4000":55.0,"4100":12.5,"4200":10.5,"5000":8.0,"5100":3.75,
  "5200":2.5,"6000":2.9,"6100":1.67,"6200":2.08,"6300":1.67,
  "6400":4.0,"6500":4.5,"6600":13.0,"6700":0.5,"6800":2.5,
  "6900":6.0,"7000":3.0,"7100":-0.25,"8000":5.0}

def cc_w(a):
    """Cost-center distribution weights per account (sum to 1.0)."""
    if a in ("4000","4100","4200","5000","6600","6900"): return {"CC200":0.5,"CC210":0.3,"CC220":0.2}
    if a in ("5100","6000"): return {"CC100":0.3,"CC500":0.3,"CC200":0.2,"CC210":0.2}
    if a in ("5200","6500"): return {"CC300":0.5,"CC600":0.5}
    if a in ("6100","6800"): return {"CC200":0.3,"CC210":0.3,"CC220":0.2,"CC100":0.2}
    if a == "6200": return {"CC400":0.5,"CC200":0.5}
    return {"CC400":0.4,"CC410":0.3,"CC700":0.3}

rows = []
for i, p in enumerate(PERIODS[:N_ACT]):
    yr, mo = int(p[:4]), int(p[5:7])
    tr = 1 + 0.003 * i          # 0.3 %/mo trend
    se = 1 + 0.03 * np.sin(2*np.pi*(mo-1)/12)  # ±3 % seasonality
    for aid, _, st, _ in ACCOUNTS:
        if st != "PL": continue
        b = BASE.get(aid, 0) * 1e6 * tr
        if aid not in ("6600","6700","7000","8000"): b *= se
        for cc, w in cc_w(aid).items():
            rows.append({"period": p, "account_id": aid, "cost_center_id": cc,
                         "scenario_id": "ACT",
                         "amount": round(b * w * (1 + np.random.normal(0, 0.02)), 2),
                         "is_actuals": True})
# 3 deliberately bad rows
rows += [{"period":"2026-13","account_id":"4000","cost_center_id":"CC200","scenario_id":"ACT","amount":1000,"is_actuals":True},
         {"period":"2026-06","account_id":"4100","cost_center_id":"CC210","scenario_id":"ACT","amount":None,"is_actuals":True},
         {"period":"2026-07","account_id":"9999","cost_center_id":"CC400","scenario_id":"ACT","amount":5000,"is_actuals":True}]
gl = pd.DataFrame(rows)
print(f"GL rows: {len(gl)} (expect 1221)")

# COMMAND ----------
# Write 5 CSVs to volume subdirectories
vp = vpath()
files = {
    "dim_account": pd.DataFrame(ACCOUNTS, columns=["account_id","account_name","statement","account_type"]),
    "dim_cost_center": pd.DataFrame(COST_CENTERS, columns=["cost_center_id","cost_center_name"]),
    "dim_time": pd.DataFrame([{"period":p,"year":int(p[:4]),"month":int(p[5:7]),"is_actuals":i<N_ACT} for i,p in enumerate(PERIODS)]),
    "dim_scenario": pd.DataFrame(SCENARIOS, columns=["scenario_id","scenario_name"]),
    "gl_actuals": gl,
}
for ent, df in files.items():
    d = f"{vp}/{ent}"
    os.makedirs(d, exist_ok=True)
    df.to_csv(f"{d}/{ent}.csv", index=False)
    print(f"Wrote {ent}: {len(df)} rows")

# COMMAND ----------
# Evidence: FY2025 revenue ~$945M, EBITDA margin ~49%, BS imbalance <$1
RA = ["4000","4100","4200","7100"]
EC = ["5000","5100","5200","6000","6100","6200","6300","6400","6500","6800","6900"]
d25 = gl[(gl.period>="2025-01")&(gl.period<="2025-12")&(gl.scenario_id=="ACT")]
rev = d25[d25.account_id.isin(RA)].amount.sum()
ebitda = rev - d25[d25.account_id.isin(EC)].amount.sum()
print(f"FY2025 Revenue: ${rev/1e6:.1f}M (expect ~$945M)")
print(f"EBITDA: ${ebitda/1e6:.1f}M, Margin: {ebitda/rev*100:.1f}% (expect ~49%)")

# BS roll-forward: cash is the plug
bs = {"1000":200e6,"1100":0,"1200":3e9,"1300":-1.2e9,"1400":2e8,"1500":5e7,
      "1600":0,"1700":0,"1800":1e8,"2000":5e7,"2100":8e8,"2200":1.5e8,
      "2300":1e8,"3000":5e8,"3100":5.5e8}
max_imb = 0
for p in PERIODS[:N_ACT]:
    pl = gl[(gl.period==p)&(gl.scenario_id=="ACT")]
    ni = pl[pl.account_id.isin(RA)].amount.sum() - pl[~pl.account_id.isin(RA)].amount.sum()
    bs["3100"] += ni
    bs["1300"] -= pl[pl.account_id=="6600"].amount.sum()  # contra-asset grows
    bs["2200"] += pl[pl.account_id=="6700"].amount.sum()  # ARO grows
    og = pl[pl.account_id.isin(["4000","4100","4200"])].amount.sum()
    ox = pl[pl.account_id.isin(EC)].amount.sum()
    bs["1100"] = og * 1.5   # AR ~45-day collection
    bs["1600"] = ox * 1.0   # AP ~30-day payment
    bs["1700"] = og * 0.05  # royalty payable
    le = bs["1600"]+bs["1700"]+bs["1800"]+bs["2000"]+bs["2100"]+bs["2200"]+bs["2300"]+bs["3000"]+bs["3100"]
    bs["1000"] = le - (bs["1100"]+bs["1200"]+bs["1400"]+bs["1500"]+bs["1300"])
    imb = abs((bs["1000"]+bs["1100"]+bs["1200"]+bs["1400"]+bs["1500"]+bs["1300"]) - le)
    max_imb = max(max_imb, imb)
print(f"BS max imbalance: ${max_imb:.4f} (expect <$1)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Decision / Why / Trade-off
# MAGIC **Decision:** Generate synthetic GL P&L data with trend+seasonality; build BS as roll-forward with cash as plug.
# MAGIC **Why:** Realistic upstream O&G economics (revenue ~$945M, EBITDA ~49%) for a convincing Chevron demo.
# MAGIC **Trade-off:** Synthetic data means numbers are illustrative, not benchmarked against real operators.
