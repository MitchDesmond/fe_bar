"""bs_rollforward.py - balance sheet roll-forward with cash as plug."""
import pandas as pd, numpy as np
from forecast_engine import REV, EBITDA_COST, DDA, ARO_ACC, INT, TAX

def roll_forward_bs(pnl_df, start_bs, periods,
                    ar_days=45, ap_days=30, royalty_rate=0.05):
    """Roll BS forward from start_bs using P&L from pnl_df.
    start_bs: dict of account_id -> balance at last ACT period.
    Returns DataFrame with period, account_id, amount.
    """
    bs = dict(start_bs)
    rows = []
    for p in periods:
        pl = pnl_df[pnl_df.period == p]
        if pl.empty: continue
        ni = pl.iloc[0].get("net_income", 0)
        rev = pl.iloc[0].get("revenue", 0)
        ebitda_c = pl.iloc[0].get("ebitda", 0) - rev  # ebitda costs
        dda = pl.iloc[0].get("operating_income", 0) - pl.iloc[0].get("ebitda", 0) + ebitda_c
        # Actually get individual items from pnl_df
        ni = float(ni) if pd.notna(ni) else 0
        rev = float(rev) if pd.notna(rev) else 0
        # Retained earnings
        bs["3100"] = bs.get("3100", 0) + ni
        # Accumulated DD&A (contra-asset grows)
        dda_amt = pl.iloc[0].get("dda", 0) if "dda" in pl.columns else 0
        aro_amt = pl.iloc[0].get("aro", 0) if "aro" in pl.columns else 0
        bs["1300"] = bs.get("1300", 0) - dda_amt
        bs["2200"] = bs.get("2200", 0) + aro_amt
        # AR = revenue * days/365
        bs["1100"] = rev * ar_days / 365
        # AP = ebitda costs * days/365
        bs["1600"] = abs(ebitda_c) * ap_days / 365
        # Royalty payable = O&G revenue * rate
        og_rev = rev * 0.9  # approx O&G vs total
        bs["1700"] = og_rev * royalty_rate
        # Cash = plug
        assets = bs.get("1100",0) + bs.get("1200",0) + bs.get("1400",0) + bs.get("1500",0) + bs.get("1300",0)
        le = bs.get("1600",0) + bs.get("1700",0) + bs.get("1800",0) + bs.get("2000",0) + bs.get("2100",0) + bs.get("2200",0) + bs.get("2300",0) + bs.get("3000",0) + bs.get("3100",0)
        bs["1000"] = le - assets
        for aid in ["1000","1100","1200","1300","1400","1500","1600","1700","1800","2000","2100","2200","2300","3000","3100"]:
            rows.append({"period": p, "account_id": aid, "amount": bs.get(aid, 0)})
    return pd.DataFrame(rows)

def cash_flow_summary(pnl_df, bs_df, periods):
    """Simple indirect cash flow: NI + DD&A + ARO accretion - AR change - AP change."""
    rows = []
    prev_cash = None
    for p in periods:
        pl = pnl_df[pnl_df.period == p]
        bs_p = bs_df[bs_df.period == p]
        if pl.empty or bs_p.empty: continue
        ni = float(pl.iloc[0].get("net_income", 0) or 0)
        dda = float(pl.iloc[0].get("dda", 0) or 0) if "dda" in pl.columns else 0
        aro = float(pl.iloc[0].get("aro", 0) or 0) if "aro" in pl.columns else 0
        ar = float(bs_p[bs_p.account_id=="1100"].amount.sum() or 0)
        ap = float(bs_p[bs_p.account_id=="1600"].amount.sum() or 0)
        cfo = ni + dda + aro - ar + ap
        cash = float(bs_p[bs_p.account_id=="1000"].amount.sum() or 0)
        rows.append({"period": p, "cfo": cfo, "ending_cash": cash})
    return pd.DataFrame(rows)
