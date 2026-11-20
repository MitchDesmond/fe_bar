"""plan_store.py - Lakebase access for the app: pooled OAuth connection + audited cell writes."""
import os, time
import pandas as pd, psycopg

class PgPool:
    """One connection, re-created before the ~1h OAuth token expires."""
    def __init__(self, w): self.w = w; self._c = None; self._t = 0
    def get(self):
        if self._c is None or time.time() - self._t > 3000:
            if self._c: self._c.close()
            proj = os.environ.get("LAKEBASE_PROJECT", "chevron-fpa")
            br = os.environ.get("LAKEBASE_BRANCH", "production")
            db = os.environ.get("PG_DATABASE", "planning_db")
            e = next(iter(self.w.postgres.list_endpoints(parent=f"projects/{proj}/branches/{br}")))
            tok = self.w.postgres.generate_database_credential(endpoint=e.name).token
            self._c = psycopg.connect(host=e.status.hosts.host, port=5432, dbname=db,
                user=self.w.config.client_id, password=tok, sslmode="require", autocommit=True)
            self._t = time.time()
        return self._c

def pg_query(pool, q, params=None):
    cur = pool.get().cursor(); cur.execute(q, params or ())
    cols = [d[0] for d in cur.description] if cur.description else []
    df = pd.DataFrame(cur.fetchall(), columns=cols) if cols else pd.DataFrame()
    cur.close(); return df

# Only rows whose amount actually changed are updated (and therefore audited).
UPSERT = """INSERT INTO planning.plan_input
  (period, account_id, cost_center_id, scenario_id, amount, version, updated_by)
  VALUES (%s,%s,%s,%s,%s,1,%s) ON CONFLICT (period, account_id, cost_center_id, scenario_id)
  DO UPDATE SET amount=EXCLUDED.amount, version=plan_input.version+1,
  updated_by=EXCLUDED.updated_by, updated_at=now()
  WHERE plan_input.amount IS DISTINCT FROM EXCLUDED.amount"""
AUDIT = """INSERT INTO planning.audit_log
  (period, account_id, cost_center_id, scenario_id, action, new_amount)
  VALUES (%s,%s,%s,%s,'APP_EDIT',%s)"""

def save_cells(pool, sid, rows, user):
    """Upsert edited cells and one audit row per changed cell, in a single transaction."""
    n, conn = 0, pool.get()
    with conn.transaction():
        cur = conn.cursor()
        for r in rows:
            k = (r["period"], r["account_id"], r["cost_center_id"], sid)
            amt = round(float(r["amount"]), 2)
            cur.execute(UPSERT, (*k, amt, user))
            if cur.rowcount:
                cur.execute(AUDIT, (*k, amt)); n += 1
    return n
