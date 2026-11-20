import plotly.graph_objects as go
import plotly.io as pio

BLUE, NAVY, SKY, RED = "#0066B2", "#0B2D71", "#009DD9", "#E21836"
GREY_DARK, GREY_LIGHT, BG, WHITE, GREEN = "#6B6D6F", "#DBDCDD", "#F4F6F9", "#FFFFFF", "#3A913F"
SERIES = [BLUE, NAVY, SKY, GREY_DARK, RED, "#5B8DB8", "#8FB8DE"]
SCENARIO_COLORS = {"ACT": GREY_DARK, "BUD": BLUE, "FCST": NAVY, "FC1": SKY, "WIF": RED}
FONT = "'Segoe UI', 'Helvetica Neue', Arial, sans-serif"

pio.templates["chevron"] = go.layout.Template(layout=go.Layout(
    colorway=SERIES, font=dict(family=FONT, color=NAVY, size=13),
    paper_bgcolor=WHITE, plot_bgcolor=WHITE,
    xaxis=dict(gridcolor=GREY_LIGHT, linecolor=GREY_LIGHT, zeroline=False),
    yaxis=dict(gridcolor=GREY_LIGHT, linecolor=GREY_LIGHT, zeroline=False),
    legend=dict(orientation="h", y=-0.2), margin=dict(t=40, b=30, l=40, r=20),
    hoverlabel=dict(bgcolor=NAVY, font_color=WHITE)))
pio.templates.default = "plotly_white+chevron"

CSS = f"""
:root {{ --bs-primary: {BLUE}; --bs-primary-rgb: 0,102,178; --bs-danger: {RED};
        --bs-info: {SKY}; --bs-link-color: {BLUE}; --bs-body-bg: {BG}; --bs-body-color: #1d2733; }}
body {{ font-family: {FONT}; background: {BG}; }}
.btn-primary {{ background: {BLUE}; border-color: {BLUE}; }}
.btn-primary:hover {{ background: {NAVY}; border-color: {NAVY}; }}
.sidebar {{ background: {NAVY}; color: {WHITE}; }}
.sidebar .nav-link {{ color: #C9D6E8; }}
.sidebar .nav-link.active {{ background: {BLUE}; color: {WHITE}; border-left: 4px solid {SKY}; }}
.card {{ border: 1px solid {GREY_LIGHT}; border-top: 3px solid {BLUE}; }}
.kpi-negative {{ color: {RED}; }} .kpi-positive {{ color: {GREEN}; }}
"""
