"""forecast_engine.py - pure numpy/pandas forecasting for Meridian Energy."""
import numpy as np, pandas as pd, json

REV = ["4000","4100","4200","7100"]
EBITDA_COST = ["5000","5100","5200","6000","6100","6200","6300","6400","6500","6800","6900"]
DDA = ["6600"]; ARO_ACC = ["6700"]; INT = ["7000"]; TAX = ["8000"]

def fit_baseline(df, min_months=15):
    coefs = {}
    for (aid, cc), g in df.groupby(["account_id","cost_center_id"]):
        g = g.sort_values("period")
        n = len(g)
        t = np.arange(n, dtype=float)
        months = g["period"].str[5:7].astype(int).values
        if n >= min_months:
            D = np.zeros((n, 11))
            for i in range(11): D[:, i] = (months == i+1).astype(float)
            X = np.column_stack([np.ones(n), t, D])
        else:
            X = np.column_stack([np.ones(n), t])
        amt = g["amount"].astype(float).values  # DECIMAL -> float
        sign = 1.0 if np.median(amt) >= 0 else -1.0
        y = np.log(np.abs(amt).clip(min=1))
        beta = np.linalg.lstsq(X, y, rcond=None)[0]
        resid = y - X @ beta
        coefs[(aid, cc)] = {"beta": beta.tolist(), "sign": sign,
                            "n": n, "std": float(np.std(resid)) if n > 2 else 0.05}
    return coefs

def predict(coefs, aid, cc, periods):
    key = (aid, cc)
    if key not in coefs:
        return pd.DataFrame({"period": periods, "amount": 0, "lower": 0, "upper": 0})
    c = coefs[key]
    beta, sign, n, std = np.array(c["beta"]), c["sign"], c["n"], c["std"]
    h = len(periods)
    t = np.arange(n, n + h, dtype=float)
    months = [int(p[5:7]) for p in periods]
    if len(beta) > 2:
        D = np.zeros((h, 11))
        for i in range(11): D[:, i] = [1 if m == i+1 else 0 for m in months]
        X = np.column_stack([np.ones(h), t, D])
    else:
        X = np.column_stack([np.ones(h), t])
    y_hat = X @ beta
    z = 1.645
    spread = z * std * np.sqrt(1 + np.arange(1, h+1) / max(n, 1))
    return pd.DataFrame({"period": periods,
        "amount": np.exp(y_hat) * sign,
        "lower": np.exp(y_hat - spread) * sign,
        "upper": np.exp(y_hat + spread) * sign})

def apply_drivers(pred_df, aid, drivers):
    a = pred_df["amount"].copy()
    for i, p in enumerate(pred_df["period"]):
        yr = int(p[:4])
        if aid in ("4000","4100","4200"): a.iloc[i] *= drivers.get("production_growth", 1.0)
        if aid == "4000" and yr >= 2027: a.iloc[i] *= drivers.get("oil_price", 1.0)
        if aid == "4100": a.iloc[i] *= drivers.get("gas_price", 1.0)
        if aid in ("5100","5200"): a.iloc[i] *= drivers.get("headcount", 1.0) * drivers.get("merit", 1.0)
        if aid in ("6000","6100","6200","6300","6400"): a.iloc[i] *= drivers.get("opex_change", 1.0)
    pred_df["amount"] = a
    return pred_df

def forecast_scenario(coefs, drivers, periods, grain):
    rows = []
    for aid, cc in grain:
        p = predict(coefs, aid, cc, periods)
        p = apply_drivers(p, aid, drivers)
        p["account_id"] = aid; p["cost_center_id"] = cc
        rows.append(p)
    df = pd.concat(rows, ignore_index=True)
    for p in periods:
        crude = df[(df.period==p)&(df.account_id=="4000")].amount.sum()
        rev = df[(df.period==p)&(df.account_id.isin(REV))].amount.sum()
        if "loe_ratio" in drivers:
            mask = (df.period==p)&(df.account_id=="5000")
            tgt = crude * drivers["loe_ratio"]; s = df.loc[mask, "amount"].abs().sum()
            if s > 0: df.loc[mask, "amount"] = tgt * df.loc[mask, "amount"] / s
        if "gt_ratio" in drivers:
            mask = (df.period==p)&(df.account_id=="6500")
            tgt = rev * drivers["gt_ratio"]; s = df.loc[mask, "amount"].abs().sum()
            if s > 0: df.loc[mask, "amount"] = tgt * df.loc[mask, "amount"] / s
        if "tax_rate" in drivers:
            ec = df[(df.period==p)&(df.account_id.isin(EBITDA_COST))].amount.sum()
            dda = df[(df.period==p)&(df.account_id.isin(DDA))].amount.sum()
            aro = df[(df.period==p)&(df.account_id.isin(ARO_ACC))].amount.sum()
            intr = df[(df.period==p)&(df.account_id.isin(INT))].amount.sum()
            pretax = rev - ec - dda - aro - intr
            mask = (df.period==p)&(df.account_id=="8000")
            tgt = max(0, pretax * drivers["tax_rate"]); s = df.loc[mask, "amount"].abs().sum()
            if s > 0: df.loc[mask, "amount"] = tgt * df.loc[mask, "amount"] / s
    return df[["period","account_id","cost_center_id","amount","lower","upper"]]

def pnl_summary(df):
    g = df.groupby(["period","account_id"]).amount.sum().reset_index()
    rev = g[g.account_id.isin(REV)].groupby("period").amount.sum()
    ec = g[g.account_id.isin(EBITDA_COST)].groupby("period").amount.sum()
    dda = g[g.account_id.isin(DDA)].groupby("period").amount.sum()
    aro = g[g.account_id.isin(ARO_ACC)].groupby("period").amount.sum()
    intr = g[g.account_id.isin(INT)].groupby("period").amount.sum()
    tax = g[g.account_id.isin(TAX)].groupby("period").amount.sum()
    ebitda = rev - ec; op_inc = ebitda - dda - aro; pretax = op_inc - intr; ni = pretax - tax
    return pd.DataFrame({"revenue": rev, "ebitda": ebitda, "operating_income": op_inc,
        "pretax_income": pretax, "net_income": ni}).reset_index()

def coefs_to_json(coefs):
    return {f"{k[0]}|{k[1]}": v for k, v in coefs.items()}

def coefs_from_json(d):
    out = {}
    for k, v in d.items():
        aid, cc = k.split("|"); out[(aid, cc)] = v
    return out
