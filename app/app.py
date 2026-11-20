import os, time
import dash, dash_bootstrap_components as dbc
from dash import html, dcc, Input, Output, State, dash_table, callback
import pandas as pd, plotly.express as px
import theme
from databricks.sdk import WorkspaceClient
from databricks import sql as dbsql
from plan_store import PgPool, pg_query, save_cells

CAT = os.environ.get("CATALOG", "chevron_fpa")
SCH = os.environ.get("SCHEMA", "finance")
FC = f"{CAT}.{SCH}"
WH_NAME = os.environ.get("WAREHOUSE_NAME", "")
GENIE_TITLE = os.environ.get("GENIE_SPACE_TITLE", "")

w = WorkspaceClient()
WH_ID = os.environ.get("WAREHOUSE_ID", ""); GENIE_ID = ""
if not WH_ID and WH_NAME:
    WH_ID = next((x.id for x in w.warehouses.list() if x.name == WH_NAME), "")
WH_PATH = f"/sql/1.0/warehouses/{WH_ID}" if WH_ID else None
try:
    tk = None
    while GENIE_TITLE and not GENIE_ID:
        r = w.genie.list_spaces(page_token=tk)
        GENIE_ID = next((s.space_id for s in r.spaces or [] if s.title == GENIE_TITLE), "")
        tk = r.next_page_token
        if not tk: break
except Exception: pass

SC = theme.SCENARIO_COLORS
SH = dict(backgroundColor=theme.NAVY, color="white", fontWeight=600)

def sql_query(q):
    if not WH_PATH: raise RuntimeError("no SQL warehouse")
    conn = dbsql.connect(server_hostname=w.config.host.replace("https://", ""),
        http_path=WH_PATH, credentials_provider=lambda: w.config.authenticate)
    cur = conn.cursor(); cur.execute(q)
    cols = [d[0] for d in cur.description] if cur.description else []
    df = pd.DataFrame(cur.fetchall(), columns=cols) if cols else pd.DataFrame()
    cur.close(); conn.close(); return df

pool = PgPool(w)

NAV = dbc.Nav([dbc.NavLink(n, href=h, active="exact") for n, h in
    [("Overview","/"),("P&L","/pl"),("Balance Sheet","/bs"),("Cash Flow","/cf"),
     ("Plan Grid","/plan"),("Forecast","/forecast"),("Ask","/ask"),("Audit","/audit")]],
    vertical=True, pills=True)

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP], suppress_callback_exceptions=True)
app.index_string = app.index_string.replace("</head>", f"<style>{theme.CSS}</style></head>")
server = app.server

def sidebar():
    return dbc.Col(dbc.Row([html.H4("Meridian Energy", className="text-white mb-3"), NAV]),
        className="sidebar col-md-2", style={"minHeight": "100vh", "padding": "20px"})

def page_overview():
    try:
        df = sql_query(f"SELECT scenario_id, period, revenue, ebitda FROM {FC}.consolidated_pnl ORDER BY period")
        if df.empty: return "No data"
        fig = px.bar(df, x="period", y="revenue", color="scenario_id", color_discrete_map=SC, title="Revenue")
        return html.Div([html.H2("Overview"), dcc.Graph(figure=fig)])
    except Exception as e: return html.Div(f"Error: {e}")

def page_pl():
    try:
        df = sql_query(f"SELECT * FROM {FC}.consolidated_pnl ORDER BY scenario_id, period")
        if df.empty: return "No data"
        fig = px.bar(df, x="period", y="ebitda", color="scenario_id", color_discrete_map=SC, title="EBITDA")
        tbl = dash_table.DataTable(data=df.to_dict("records"), page_size=15, style_header=SH)
        return html.Div([html.H2("P&L"), dcc.Graph(figure=fig), tbl])
    except Exception as e: return html.Div(f"Error: {e}")

def page_bs():
    try:
        df = sql_query(f"SELECT * FROM {FC}.consolidated_bs ORDER BY scenario_id, period, account_id")
        if df.empty: return "No data"
        tbl = dash_table.DataTable(data=df.to_dict("records"), page_size=20, style_header=SH)
        return html.Div([html.H2("Balance Sheet"), tbl])
    except Exception as e: return html.Div(f"Error: {e}")

def page_cf():
    try:
        df = sql_query(f"SELECT * FROM {FC}.consolidated_cf ORDER BY scenario_id, period")
        if df.empty: return "No data"
        fig = px.line(df, x="period", y="ending_cash", color="scenario_id", color_discrete_map=SC, title="Ending Cash")
        return html.Div([html.H2("Cash Flow"), dcc.Graph(figure=fig)])
    except Exception as e: return html.Div(f"Error: {e}")

def page_plan():
    return html.Div([html.H2("Plan Grid"),
        dcc.Dropdown(id="plan-scenario", options=[{"label":s,"value":s} for s in ["BUD","FCST","FC1","WIF"]], value="BUD"),
        html.Div(id="plan-table"),
        html.Button("Save", id="plan-save", n_clicks=0, className="btn btn-primary mt-2"),
        html.Div(id="plan-status")])

def page_forecast():
    return html.Div([html.H2("Forecast Drivers"),
        dcc.Slider(id="drv-oil", min=0.5, max=2.0, step=0.05, value=1.0), html.Label("Oil Price"),
        dcc.Slider(id="drv-gas", min=0.5, max=2.0, step=0.05, value=1.0), html.Label("Gas Price"),
        dcc.Slider(id="drv-prod", min=0.8, max=1.3, step=0.01, value=1.0), html.Label("Production"),
        dcc.Graph(id="forecast-preview"),
        html.Button("Publish", id="forecast-publish", n_clicks=0, className="btn btn-primary mt-2"),
        html.Div(id="forecast-status")])

def page_ask():
    return html.Div([html.H2("Ask Genie"),
        dcc.Textarea(id="genie-q", value="What is the FY2025 revenue?", style={"width":"100%","height":60}),
        html.Button("Ask", id="genie-ask", n_clicks=0, className="btn btn-primary mt-2"),
        html.Div(id="genie-answer", className="mt-3")])

def page_audit():
    return html.Div([html.H2("Audit Log"), html.Div(id="audit-table")])

PAGES = {"/": page_overview, "/pl": page_pl, "/bs": page_bs, "/cf": page_cf,
         "/plan": page_plan, "/forecast": page_forecast, "/ask": page_ask, "/audit": page_audit}

app.layout = dbc.Container([dcc.Location(id="url"), dbc.Row([sidebar(), dbc.Col(id="content", className="col-md-10", style={"padding":"20px"})])], fluid=True)

@callback(Output("content","children"), Input("url","pathname"))
def render_page(path):
    return PAGES.get(path, page_overview)()

@callback(Output("plan-table","children"), Input("plan-scenario","value"))
def show_plan(sid):
    try:
        df = pg_query(pool, "SELECT period, account_id, cost_center_id, amount FROM planning.plan_input WHERE scenario_id = %s ORDER BY period LIMIT 50", (sid,))
        return dash_table.DataTable(data=df.to_dict("records"), page_size=15, id="plan-grid",
            editable=True, style_header=SH)
    except Exception as e: return str(e)

@callback(Output("plan-status","children"), Input("plan-save","n_clicks"), State("plan-scenario","value"), State("plan-grid","data"))
def save_plan(n, sid, rows):
    if not n or not rows: return ""
    try: return f"Saved {save_cells(pool, sid, rows, w.config.client_id)} changed cell(s) to Lakebase with audit"
    except Exception as e: return f"Error: {e}"

@callback(Output("forecast-preview","figure"), Input("drv-oil","value"), Input("drv-gas","value"), Input("drv-prod","value"))
def forecast_preview(oil, gas, prod):
    try:
        q = f"SELECT period, SUM(amount) AS revenue FROM {FC}.forecast_model WHERE account_id IN ('4000','4100','4200','7100') GROUP BY period ORDER BY period"
        df = sql_query(q)
        if df.empty: return {}
        df["revenue"] = df["revenue"] * oil * gas * prod
        return px.line(df, x="period", y="revenue", title="Forecast Revenue Preview").update_layout(template="plotly_white+chevron")
    except Exception: return {}

@callback(Output("genie-answer","children"), Input("genie-ask","n_clicks"), State("genie-q","value"))
def ask_genie(n, q):
    if not n or not GENIE_ID: return "No Genie space"
    try:
        msg = w.genie.start_conversation_and_wait(space_id=GENIE_ID, content=q)
        return dcc.Markdown("\n\n".join(a.text.content for a in msg.attachments or [] if a.text) or "No answer")
    except Exception as e: return f"Error: {e}"

@callback(Output("audit-table","children"), Input("url","pathname"))
def show_audit(path):
    if path != "/audit": return ""
    try:
        df = sql_query(f"SELECT * FROM {FC}.v_plan_audit ORDER BY changed_at DESC LIMIT 50")
        if df.empty: df = pg_query(pool, "SELECT * FROM planning.audit_log ORDER BY changed_at DESC LIMIT 50")
        return dash_table.DataTable(data=df.to_dict("records"), page_size=15, style_header=SH)
    except Exception as e: return str(e)
