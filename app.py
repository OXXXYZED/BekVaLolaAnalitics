import os
import base64
from pathlib import Path
from datetime import datetime, timedelta, date

import altair as alt
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from typing import Optional, List
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
    FilterExpression,
    Filter,
    FilterExpressionList,
    OrderBy,
)

# ----------------------------
# Page
# ----------------------------
st.set_page_config(
    page_title="Bek va Lola • Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

alt.data_transformers.disable_max_rows()

load_dotenv()

# GA4 / Firebase Analytics config
PROPERTY_ID = os.getenv("GA4_PROPERTY_ID", "")
RELEASE_DATE = date(2025, 12, 27)
TODAY = date.today()
RELEASE_DATE_STR = RELEASE_DATE.strftime("%Y-%m-%d")
TODAY_STR = TODAY.strftime("%Y-%m-%d")

# Handle credentials path (Cloud Run / Firebase Hosting blocks env keys starting with GOOGLE_)
# Allowed alternatives:
# - GA4_CREDENTIALS_PATH: absolute or relative path to JSON key
# - GOOGLE_APPLICATION_CREDENTIALS: standard env (works locally)
cred_env = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
alt_cred = os.getenv("GA4_CREDENTIALS_PATH")
if not cred_env and alt_cred:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = alt_cred
    cred_env = alt_cred

if cred_env and not os.path.isabs(cred_env):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(Path(__file__).parent / cred_env)

# ----------------------------
# Theme
# ----------------------------
COLORS = {
    "bg": "#F6F8FC",
    "card": "#FFFFFF",
    "border": "rgba(15,23,42,0.14)",
    "text": "#0F172A",
    "muted": "#64748B",

    "accent": "#2563EB",
    "android": "#16A34A",
    "ios": "#2563EB",
    "other": "#94A3B8",

    "new_users": "#F59E0B",   # soft orange
    "sessions": "#2563EB",    # blue
    "minigame": "#EF4444",    # red
    "purple": "#7C3AED",

    "neon": "rgba(37,99,235,0.16)",
    "neon2": "rgba(124,58,237,0.12)",
}

# Load local logo as base64
def get_logo_base64():
    for name in ["Beklola.png", "beklola.png"]:
        logo_path = Path(__file__).parent / "images" / name
        if logo_path.exists():
            try:
                with open(logo_path, "rb") as f:
                    return base64.b64encode(f.read()).decode()
            except Exception:
                continue
    return ""

LOGO_BASE64 = get_logo_base64() 


# ----------------------------
# Altair clean light theme (transparent background; Streamlit card shows bg)
# ----------------------------
@alt.theme.register("clean_light", enable=True)
def _clean_light_theme():
    return {
        "config": {
            "background": "transparent",
            "view": {"stroke": "transparent"},
            "axis": {
                "labelColor": COLORS["muted"],
                "titleColor": COLORS["muted"],
                "gridColor": "rgba(15,23,42,0.06)",
                "domainColor": "rgba(15,23,42,0.18)",
                "tickColor": "rgba(15,23,42,0.18)",
                "labelFontSize": 11,
                "titleFontSize": 11,
            },
            "legend": {"labelColor": COLORS["muted"], "titleColor": COLORS["muted"]},
            "title": {"color": COLORS["text"]},
        }
    }

alt.themes.register("clean_light", _clean_light_theme)
alt.theme.enable("clean_light")

# ----------------------------
# GA4 helpers
# ----------------------------
@st.cache_resource(show_spinner=False)
def ga4_client() -> BetaAnalyticsDataClient:
    return BetaAnalyticsDataClient()


def run_report(
    dimensions: List[str],
    metrics: List[str],
    start: str,
    end: str,
    limit: Optional[int] = None,
    dimension_filter: Optional[FilterExpression] = None,
    order_by: Optional[List[OrderBy]] = None,
):
    request = RunReportRequest(
        property=f"properties/{PROPERTY_ID}",
        dimensions=[Dimension(name=d) for d in dimensions],
        metrics=[Metric(name=m) for m in metrics],
        date_ranges=[DateRange(start_date=start, end_date=end)],
        limit=limit,
        dimension_filter=dimension_filter,
        order_bys=order_by or [],
    )
    resp = ga4_client().run_report(request)
    rows = []
    for r in resp.rows:
        row = {}
        for i, d in enumerate(dimensions):
            row[d] = r.dimension_values[i].value
        for j, m in enumerate(metrics):
            row[m] = float(r.metric_values[j].value)
        rows.append(row)
    return pd.DataFrame(rows)


# ----------------------------
# CSS theme
# ----------------------------
st.markdown(
    f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

/* Force light color scheme globally - override system dark mode */
:root,
:root[data-theme="dark"],
:root[data-theme="light"] {{
  color-scheme: light only !important;
  --primary-color: #2563EB !important;
}}

html, body, [class*="css"] {{
  font-family: "Inter", system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif !important;
  color: {COLORS["text"]} !important;
  font-weight: 400 !important;
  color-scheme: light !important;
}}

/* Text selection highlight */
*::selection {{
  background: rgba(37,99,235,0.20) !important;
  color: #0F172A !important;
}}
*::-moz-selection {{
  background: rgba(37,99,235,0.20) !important;
  color: #0F172A !important;
}}

/* Override dark mode preference */
@media (prefers-color-scheme: dark) {{
  :root {{
    color-scheme: light only !important;
  }}

  html, body, .stApp, [class*="css"] {{
    background: {COLORS["bg"]} !important;
    color: {COLORS["text"]} !important;
  }}

  /* Force light on all BaseWeb portals/popovers in dark mode */
  [data-baseweb="popover"],
  [data-baseweb="popover"] *,
  [data-baseweb="menu"],
  [data-baseweb="menu"] *,
  [data-baseweb="calendar"],
  [data-baseweb="calendar"] *,
  [data-baseweb="layer"],
  [data-baseweb="layer"] *,
  div[data-floating-ui-portal],
  div[data-floating-ui-portal] * {{
    color-scheme: light !important;
    background-color: #FFFFFF !important;
    color: #0F172A !important;
  }}

  /* Reset specific elements that should be transparent */
  [data-baseweb="calendar"] td,
  [data-baseweb="calendar"] button {{
    background-color: transparent !important;
  }}

  [data-baseweb="calendar"] button[aria-selected="true"] {{
    background-color: #2563EB !important;
    color: #FFFFFF !important;
  }}
}}

.stApp {{
  background: {COLORS["bg"]} !important;
  color: {COLORS["text"]} !important;
}}

.block-container {{
  max-width: 1320px;
  padding: 0.7rem 1.6rem 2rem 1.6rem;
}}

#MainMenu, footer, header, [data-testid="stToolbar"], [data-testid="stDecoration"],
[data-testid="stStatusWidget"], [data-testid="stHeader"], .viewerBadge_container__r5tak,
.stDeployButton, #stDecoration, .reportview-container .main footer,
a[href*="streamlit.io"], footer a, [class*="_profileContainer"],
[class*="stAppDeployButton"], [class*="_profilePreview"], [class*="gzau3"],
iframe[title*="streamlit"], div[class*="profile"] {{
  display: none !important; visibility: hidden !important; height: 0 !important;
  width: 0 !important; overflow: hidden !important;
}}

[data-testid="stHeader"] {{
  background: transparent !important;
  border-bottom: 0 !important;
  box-shadow: none !important;
}}

.muted {{ color: {COLORS["muted"]} !important; }}

/* Hide sidebar */
[data-testid="stSidebar"], [data-testid="stSidebarCollapsedControl"] {{
  display: none !important;
}}

/* ===== GLOBAL PORTAL/LAYER OVERRIDE FOR LIGHT THEME ===== */
/* BaseWeb uses layers for popovers, modals, etc. */
[data-baseweb="layer"],
[data-baseweb="layer"] > div,
body > div[data-baseweb="layer"],
body > div > [data-baseweb="popover"],
body > div > [data-baseweb="menu"] {{
  color-scheme: light !important;
}}

/* Floating UI portal (used by newer Streamlit versions) */
div[data-floating-ui-portal],
div[data-floating-ui-portal] > div {{
  color-scheme: light !important;
}}

/* ===============================
   FIX 1: SELECT DROPDOWNS - Force light theme
   =============================== */

/* Force light color scheme on all select components */
[data-baseweb="select"],
[data-baseweb="select"] *,
[data-baseweb="popover"],
[data-baseweb="popover"] *,
[data-baseweb="menu"],
[data-baseweb="menu"] * {{
  color-scheme: light !important;
}}

[data-baseweb="select"] > div {{
  background-color: #FFFFFF !important;
  border: 1px solid rgba(15,23,42,0.18) !important;
  border-radius: 12px !important;
  box-shadow: none !important;
}}

/* Fix select placeholder and value text */
[data-baseweb="select"] [data-baseweb="select"] > div > div {{
  color: #0F172A !important;
}}

[data-baseweb="select"] input {{
  color: #0F172A !important;
  -webkit-text-fill-color: #0F172A !important;
  background: transparent !important;
}}

/* Selected value text */
[data-baseweb="select"] > div > div {{
  color: #0F172A !important;
}}

/* All text inside select */
.stSelectbox label,
.stSelectbox [data-baseweb="select"] *:not(svg) {{
  color: #0F172A !important;
}}

/* Fix dropdown arrow */
[data-baseweb="select"] svg {{
  color: #0F172A !important;
  fill: #0F172A !important;
}}

/* Focus states */
[data-baseweb="select"] > div:focus-within {{
  border-color: rgba(37,99,235,0.45) !important;
  box-shadow: 0 0 0 3px rgba(37,99,235,0.14) !important;
}}

/* ===== DROPDOWN MENU POPOVER - AGGRESSIVE LIGHT THEME ===== */
[data-baseweb="popover"],
[data-baseweb="popover"] > div,
[data-baseweb="popover"] > div > div,
[data-baseweb="menu"],
[data-baseweb="menu"] > div {{
  background: #FFFFFF !important;
  background-color: #FFFFFF !important;
  border: 1px solid rgba(15,23,42,0.14) !important;
  border-radius: 12px !important;
  box-shadow: 0 4px 16px rgba(0,0,0,0.12) !important;
}}

/* BaseWeb menu list */
[data-baseweb="menu"] ul,
[data-baseweb="popover"] ul {{
  background: #FFFFFF !important;
  background-color: #FFFFFF !important;
}}

ul[role="listbox"],
[data-baseweb="menu"] ul[role="listbox"] {{
  background: #FFFFFF !important;
  background-color: #FFFFFF !important;
  border: none !important;
  border-radius: 12px !important;
}}

ul[role="listbox"] li,
[data-baseweb="menu"] li,
[data-baseweb="menu"] ul li {{
  color: #0F172A !important;
  background: #FFFFFF !important;
  background-color: #FFFFFF !important;
}}

ul[role="listbox"] li:hover,
[data-baseweb="menu"] li:hover {{
  background: rgba(37,99,235,0.08) !important;
  background-color: rgba(37,99,235,0.08) !important;
}}

/* ✅ Fix: selected / highlighted option background (remove black strip) */
ul[role="listbox"] li[aria-selected="true"],
div[role="option"][aria-selected="true"],
[data-baseweb="menu"] li[aria-selected="true"] {{
  background: rgba(37,99,235,0.10) !important;
  background-color: rgba(37,99,235,0.10) !important;
  color: #0F172A !important;
}}
ul[role="listbox"] li[aria-selected="true"] *,
div[role="option"][aria-selected="true"] *,
[data-baseweb="menu"] li[aria-selected="true"] * {{
  color: #0F172A !important;
}}

/* highlighted item while moving with mouse/keyboard */
ul[role="listbox"] li[data-highlighted="true"],
div[role="option"][data-highlighted="true"],
[data-baseweb="menu"] li[data-highlighted="true"],
ul[role="listbox"] li:focus,
[data-baseweb="menu"] li:focus {{
  background: rgba(37,99,235,0.08) !important;
  background-color: rgba(37,99,235,0.08) !important;
  color: #0F172A !important;
}}


/* ===============================
   FIX 2: DATEPICKER - Force light theme
   =============================== */

/* Force light color scheme on datepicker */
[data-baseweb="datepicker"],
[data-baseweb="datepicker"] *,
[data-baseweb="calendar"],
[data-baseweb="calendar"] *,
.stDateInput,
.stDateInput * {{
  color-scheme: light !important;
}}

/* Date input field */
[data-baseweb="datepicker"] > div,
.stDateInput > div > div {{
  background-color: #FFFFFF !important;
  border: 1px solid rgba(15,23,42,0.18) !important;
  border-radius: 12px !important;
  box-shadow: none !important;
}}

/* Date input text */
[data-baseweb="datepicker"] input,
.stDateInput input {{
  color: #0F172A !important;
  -webkit-text-fill-color: #0F172A !important;
  background: transparent !important;
  font-weight: 500 !important;
}}

/* Calendar icon */
[data-baseweb="datepicker"] svg,
.stDateInput svg {{
  color: #0F172A !important;
  fill: #0F172A !important;
}}

/* Focus state */
[data-baseweb="datepicker"] > div:focus-within,
.stDateInput > div > div:focus-within {{
  border-color: rgba(37,99,235,0.45) !important;
  box-shadow: 0 0 0 3px rgba(37,99,235,0.14) !important;
}}

/* ========= CALENDAR POPUP - NUCLEAR LIGHT THEME ========= */

/* RESET: Force ALL elements in calendar to have transparent/white background */
[data-baseweb="calendar"] *:not([aria-selected="true"]) {{
  background: transparent !important;
  background-color: transparent !important;
}}

/* Calendar container - white background */
[data-baseweb="calendar"] {{
  background: #FFFFFF !important;
  background-color: #FFFFFF !important;
  color: #0F172A !important;
  border: 1px solid rgba(15,23,42,0.14) !important;
  border-radius: 14px !important;
  box-shadow: 0 8px 24px rgba(0,0,0,0.12) !important;
}}

[data-baseweb="calendar"] > div {{
  background: #FFFFFF !important;
  background-color: #FFFFFF !important;
}}

/* All text in calendar - dark */
[data-baseweb="calendar"] * {{
  color: #0F172A !important;
}}

/* Month/Year dropdowns */
[data-baseweb="calendar"] [data-baseweb="select"] > div {{
  background: #F8FAFC !important;
  background-color: #F8FAFC !important;
}}

/* Navigation arrows */
[data-baseweb="calendar"] button svg,
[data-baseweb="calendar"] svg {{
  color: #0F172A !important;
  fill: #0F172A !important;
}}

/* Weekday labels - gray */
[data-baseweb="calendar"] th,
[data-baseweb="calendar"] [role="columnheader"] {{
  color: #64748B !important;
}}

/* Hover on buttons */
[data-baseweb="calendar"] button:hover {{
  background: rgba(37,99,235,0.08) !important;
  background-color: rgba(37,99,235,0.08) !important;
}}

/* ===== SELECTED DATE - BLUE ===== */
[data-baseweb="calendar"] [aria-selected="true"],
[data-baseweb="calendar"] [aria-selected="true"] > *,
[data-baseweb="calendar"] button[aria-selected="true"],
[data-baseweb="calendar"] td[aria-selected="true"],
[data-baseweb="calendar"] td[aria-selected="true"] button {{
  background: #2563EB !important;
  background-color: #2563EB !important;
  color: #FFFFFF !important;
  border-radius: 50% !important;
}}

/* Text inside selected - white */
[data-baseweb="calendar"] [aria-selected="true"] *,
[data-baseweb="calendar"] button[aria-selected="true"] *,
[data-baseweb="calendar"] td[aria-selected="true"] * {{
  color: #FFFFFF !important;
}}

/* Today's date - blue border */
[data-baseweb="calendar"] [aria-current="date"]:not([aria-selected="true"]) {{
  border: 2px solid #2563EB !important;
  border-radius: 50% !important;
}}

/* Disabled/outside month days */
[data-baseweb="calendar"] button:disabled {{
  color: #CBD5E1 !important;
  opacity: 0.4 !important;
}}

/* Range highlight */
[data-baseweb="calendar"] [data-highlighted="true"]:not([aria-selected="true"]) {{
  background: rgba(37,99,235,0.1) !important;
  background-color: rgba(37,99,235,0.1) !important;
}}

/* Pseudo-elements reset */
[data-baseweb="calendar"] *::before,
[data-baseweb="calendar"] *::after {{
  background: transparent !important;
  background-color: transparent !important;
}}

/* Range between dates */
[data-baseweb="calendar"] td[data-in-range="true"]::before,
[data-baseweb="calendar"] [data-in-range="true"] {{
  background: rgba(37,99,235,0.1) !important;
  background-color: rgba(37,99,235,0.1) !important;
}}

/* Quick select dropdown at bottom */
[data-baseweb="calendar"] + div,
[data-baseweb="calendar"] ~ div {{
  background: #FFFFFF !important;
  background-color: #FFFFFF !important;
}}

[data-baseweb="calendar"] + div [data-baseweb="select"] > div,
[data-baseweb="calendar"] ~ div [data-baseweb="select"] > div {{
  background: #FFFFFF !important;
  background-color: #FFFFFF !important;
  border: 1px solid rgba(15,23,42,0.18) !important;
}}

/* ===============================
   FIX 3: CHARTS - Prevent overflow
   =============================== */

/* Cards / charts container */
.card {{
  background: {COLORS["card"]} !important;
  border: 1px solid {COLORS["border"]} !important;
  border-radius: 18px;
  box-shadow: 0 10px 24px rgba(15,23,42,0.06);
  overflow: hidden !important;
}}

.card:hover,
[data-testid="stVegaLiteChart"]:hover {{
  box-shadow:
    0 0 0 1px rgba(37,99,235,0.10),
    0 0 16px {COLORS["neon"]},
    0 0 22px {COLORS["neon2"]};
    transform: none !important;
  transition: all 160ms ease;
}}

/* Chart container - prevent overflow */
[data-testid="stVegaLiteChart"] {{
  background: {COLORS["card"]} !important;
  border: 1px solid {COLORS["border"]} !important;
  border-radius: 18px;
  padding: 16px !important;
  box-shadow: 0 6px 18px rgba(15,23,42,0.05);
  overflow: hidden !important;
}}

[data-testid="stVegaLiteChart"] > div {{
  background: transparent !important;
  overflow: hidden !important;
}}

/* Ensure charts don't overflow */
[data-testid="stVegaLiteChart"] canvas,
[data-testid="stVegaLiteChart"] svg {{
  max-width: 100% !important;
  height: auto !important;
}}

/* ---------- Header centered ---------- */
.header {{
  display:flex;
  justify-content:center;
  align-items:center;
  margin: 6px 0 24px 0;
}}

.header-left {{
  display: flex;
  align-items: center;
  gap: 14px;
}}

.header img {{
  height: 60px;
  width: auto;
}}

.h-title {{
  font-family: 'Poppins', 'Inter', sans-serif !important;
  font-size: 2.2rem;
  font-weight: 700;
  letter-spacing: -0.01em;
}}

/*Last update timestamp */
.last-update {{
  font-size: 0.85rem;
  color: #64748B;
  font-weight: 500;
  text-align: right;
  padding: 8px 12px;
  background: rgba(255, 255, 255, 0.6);
  border-radius: 10px;
  border: 1px solid rgba(15,23,42,0.08);
  position: absolute;
  right: 0;
  top: 0;
}}

.last-update-time {{
  font-weight: 650;
  color: {COLORS["text"]};
}}

/* ---------- KPI alignment placing correctly---------- */
.kpi-grid {{
  display: grid;
  grid-template-columns: repeat(5, 1fr);
  gap: 16px;
  margin-bottom: 20px;
}}
.kpi {{
  padding: 18px;
  display: flex;
  flex-direction: column;
  justify-content: space-between;
  min-height: 120px;
}}
.kpi-head {{
  display:flex;
  align-items:center;
  gap:10px;
  margin-bottom: 12px;
}}
.kpi-ico {{
  width: 40px;
  height: 40px;
  border-radius: 12px;
  display:flex;
  align-items:center;
  justify-content:center;
  font-size: 20px;
  background: rgba(37,99,235,0.10);
  color: #2563EB;
  flex-shrink: 0;
}}
.kpi-ico.purple {{ background: rgba(124,58,237,0.10); color:#7C3AED; }}
.kpi-ico.orange {{ background: rgba(245,158,11,0.12); color:#F59E0B; }}
.kpi-ico.green {{ background: rgba(22,163,74,0.10); color:#16A34A; }}
.kpi-ico.blue {{ background: rgba(59,130,246,0.12); color:#3B82F6; }}

.kpi-label {{
  font-size: 0.95rem;
  color: {COLORS["muted"]};
  font-weight: 500;
  line-height: 1.3;
}}
.kpi-value {{
  font-size: 2.2rem;
  font-weight: 700;
  letter-spacing: -0.02em;
  margin-top: auto;
}}

/* ---------- Section header row (title left, filters right) ---------- */
.sec-row {{
  display:flex;
  align-items:flex-end;
  justify-content:space-between;
  gap: 12px;
  margin-top: 18px;
  margin-bottom: 8px;
}}
.sec-title {{
  font-size: 1.12rem;
  font-weight: 650;
  letter-spacing: -0.01em;
  margin: 0;
}}
.sec-sub {{
  color: {COLORS["muted"]};
  font-weight: 400;
  font-size: 0.95rem;
  margin: 4px 0 0 0;
}}

/* ---------- Legend card ---------- */
.legend-card {{
  padding: none !important;
  border: none !important;
  box-shadow: none !important;
}}
.stat-row {{
  display:flex;
  align-items:flex-start;
  justify-content:space-between;
  gap: 12px;
  padding: 10px 8px;
  border-bottom: 1px solid rgba(15,23,42,0.08);
}}
.stat-row:last-child {{
  border-bottom: none;
  padding-bottom: 8px;
}}
.dot {{
  width: 10px;
  height: 10px;
  border-radius: 999px;
  margin-right: 10px;
  margin-top: 5px;
  flex: 0 0 auto;
  box-shadow: 0 0 0 3px rgba(15,23,42,0.04);
}}
.stat-left {{
  display:flex;
  align-items:flex-start;
  gap: 10px;
  line-height: 1.15;
}}
.stat-label {{
  font-weight: 600;
}}
.stat-sub {{
  color: {COLORS["muted"]};
  font-weight: 400;
  font-size: 0.9rem;
  margin-top: 3px;
}}
.stat-right {{
  font-weight: 600;
  text-align:right;
  white-space: nowrap;
}}

/* ---------- Retention metrics visibility ---------- */
[data-testid="stMetricValue"] {{
  color: {COLORS["text"]} !important;
  font-weight: 650 !important;
}}
[data-testid="stMetricLabel"] {{
  color: {COLORS["muted"]} !important;
  font-weight: 450 !important;
}}

/* ---------- Top games list ---------- */
.rank-card {{
  padding: 10px 12px;
}}
.rank-row {{
  display:grid;
  grid-template-columns: 52px 1fr 120px;
  gap: 10px;
  align-items:center;
  padding: 10px 0;
  border-bottom: 1px solid rgba(15,23,42,0.08);
}}
.rank-row:last-child {{
  border-bottom: none;
}}
.rank-badge {{
  font-size: 1.2rem;
  font-weight: 650;
}}
.rank-name {{
  font-weight: 500;
}}
.rank-val {{
  text-align:right;
  font-weight: 600;
}}

@media (max-width: 980px) {{
  .kpi-grid {{ grid-template-columns: 1fr; }}
  .rank-row {{ grid-template-columns: 52px 1fr 100px; }}
}}

/* Transparent glass background for metrics */
[data-testid="stMetric"] {{
  background: rgba(255, 255, 255, 0.5) !important;
  backdrop-filter: blur(10px) !important;
  -webkit-backdrop-filter: blur(10px) !important;
  border: 1px solid rgba(15, 23, 42, 0.08) !important;
  border-radius: 16px !important;
  padding: 16px !important;
  box-shadow: 0 4px 12px rgba(15, 23, 42, 0.04) !important;
}}

[data-testid="stMetric"]:hover {{
  background: rgba(255, 255, 255, 0.65) !important;
  box-shadow: 0 6px 16px rgba(15, 23, 42, 0.08) !important;
  transition: all 0.2s ease !important;
}}

/* Smooth animations for charts and cards */
[data-testid="stVegaLiteChart"],
.card,
[data-testid="stMetric"] {{
  animation: fadeInUp 0.6s ease-out;
  transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1) !important;
}}

@keyframes fadeInUp {{
  from {{
    opacity: 0;
    transform: translateY(20px);
  }}
  to {{
    opacity: 1;
    transform: translateY(0);
  }}
}}


</style>
""",
    unsafe_allow_html=True,
)



# ----------------------------
# GA4 property check
# ----------------------------
if not PROPERTY_ID:
    st.error("GA4_PROPERTY_ID topilmadi. .env fayliga qo'shing yoki qo'lda kiriting.")
    st.stop()

# ----------------------------
# Mini-games map
# ----------------------------
MINIGAME_NAMES = {
    "AstroBek": "Astrobek",
    "Badantarbiya": "Badantarbiya",
    "HiddeAndSikLolaRoom": "Bekinmachoq",
    "Market": "Bozor",
    "Shapes": "Shakllar",
    "NumbersShape": "Raqamlar",
    "Words": "So'zlar",
    "MapMatchGame": "Xarita",
    "FindHiddenLetters": "Yashirin harflar",
    "RocketGame": "Raketa",
    "TacingLetter": "Harflar yozish",
    "Baroqvoy": "Baroqvoy",
    "Ballons": "Sharlar",
    "HygieneTeath": "Tish tozalash",
    "HygieneHand": "Qo'l yuvish",
    "BasketBall": "Basketbol",
    "FootBall": "Futbol",
}

def get_minigame_name(name):
    if name is None:
        return "Noma'lum"
    return MINIGAME_NAMES.get(name, name)

# ----------------------------
# GA4 / Firebase derived meta
# ----------------------------
def latest_event_date() -> str:
    try:
        df = run_report(
            ["date"],
            ["eventCount"],
            RELEASE_DATE.strftime("%Y-%m-%d"),
            TODAY.strftime("%Y-%m-%d"),
            order_by=[OrderBy(dimension=OrderBy.DimensionOrderBy(dimension_name="date"), desc=True)],
            limit=1,
        )
        if df.empty:
            return "N/A"
        return pd.to_datetime(df["date"][0]).strftime("%d.%m.%Y")
    except Exception:
        return "N/A"


def latest_app_version() -> str:
    try:
        df = run_report(
            ["appVersion"],
            ["activeUsers"],
            (TODAY - timedelta(days=30)).strftime("%Y-%m-%d"),
            TODAY.strftime("%Y-%m-%d"),
            order_by=[OrderBy(metric=OrderBy.MetricOrderBy(metric_name="activeUsers"), desc=True)],
            limit=1,
        )
        if df.empty:
            return "N/A"
        return str(df["appVersion"][0])
    except Exception:
        return "N/A"


last_update_date = latest_event_date()
last_update_version = latest_app_version()



st.markdown(f'''
<div class="header">
    {f'<img src="data:image/png;base64,{LOGO_BASE64}" style="height:60px;width:auto;" />' if LOGO_BASE64 else '<div class="h-title">Bek va Lola Analytics</div>'}
</div>
''', unsafe_allow_html=True)


# ----------------------------
# KPI (4 cards)
# ----------------------------
# Defaults for initial view
default_start = datetime.now() - timedelta(days=30)
default_end = datetime.now()

# DAU - Daily Active Users (yesterday, as today may be incomplete)
try:
    total_df = run_report([], ["activeUsers"], RELEASE_DATE_STR, TODAY_STR)
    kpi_total_users = int(total_df["activeUsers"][0]) if not total_df.empty else None
except Exception:
    kpi_total_users = None

try:
    yesterday = TODAY - timedelta(days=1)
    y_str = yesterday.strftime("%Y-%m-%d")
    dau_df = run_report(["date"], ["activeUsers"], y_str, y_str)
    kpi_dau = int(dau_df["activeUsers"].sum()) if not dau_df.empty else None
except Exception:
    kpi_dau = None

try:
    mau_start = (TODAY - timedelta(days=30)).strftime("%Y-%m-%d")
    mau_df = run_report([], ["activeUsers"], mau_start, TODAY_STR)
    kpi_mau = int(mau_df["activeUsers"][0]) if not mau_df.empty else None
except Exception:
    kpi_mau = None

try:
    sess_start = (TODAY - timedelta(days=7)).strftime("%Y-%m-%d")
    sess_df = run_report([], ["sessions"], sess_start, TODAY_STR)
    kpi_sessions = int(sess_df["sessions"][0]) if not sess_df.empty else None
except Exception:
    kpi_sessions = None

st.markdown(
    f"""
<div class="kpi-grid">
  <div class="kpi card">
    <div class="kpi-head">
      <div class="kpi-ico green">👥</div>
        <div style="font-size: 1.1rem; font-weight: 600; color: #444;">
            Umumiy foydalanuvchilar
        </div>
    </div>
    <div style="display: flex; flex-direction: column; gap: 0.1rem;">
        <div style="font-size: 2.2rem; font-weight: 650; color: #1a1a1a; line-height: 1.2;">
            {f"{kpi_total_users:,}" if kpi_total_users is not None else "N/A"}
          </div>
    </div>
</div>

<div class="kpi card">
    <div class="kpi-head">
      <div class="kpi-ico blue">📊</div>
        <div style="font-size: 1.1rem; font-weight: 600; color: #444;">
            Kunlik faol foydalanuvchilar
        </div>
    </div>
    <div style="display: flex; flex-direction: column; gap: 0.1rem;">
        <div style="font-size: 2.2rem; font-weight: 650; color: #1a1a1a; line-height: 1.2;">
            {f"{kpi_dau:,}" if kpi_dau is not None else "N/A"}
          </div>
    </div>
</div>

<div class="kpi card">
    <div class="kpi-head">
      <div class="kpi-ico purple">📅</div>
        <div style="font-size: 1.1rem; font-weight: 600; color: #444;">
            Oylik faol foydalanuvchilar
        </div>
    </div>
    <div style="display: flex; flex-direction: column; gap: 0.1rem;">
        <div style="font-size: 2.2rem; font-weight: 650; color: #1a1a1a; line-height: 1.2;">
            {f"{kpi_mau:,}" if kpi_mau is not None else "N/A"}
          </div>
    </div>
</div>

<div class="kpi card">
    <div class="kpi-head">
      <div class="kpi-ico">📈</div>
        <div style="font-size: 1.1rem; font-weight: 600; color: #444;">
            O'yin seanslari soni
        </div>
    </div>
    <div style="display: flex; flex-direction: column; gap: 0.1rem;">
        <div style="font-size: 2.2rem; font-weight: 650; color: #1a1a1a; line-height: 1.2;">
            {f"{kpi_sessions:,}" if kpi_sessions is not None else "N/A"}
          </div>
    </div>
</div>

  <div class="kpi card">
    <div class="kpi-head">
      <div class="kpi-ico orange">🔄</div>
        <div style="font-size: 1.1rem; font-weight: 600; color: #444;">
            So'ngi yangilanish
        </div>
    </div>
    <div style="display: flex; flex-direction: column; gap: 0.25rem;">
        <div style="font-size: 1.75rem; font-weight: 600; color: #1a1a1a; line-height: 1.2;">
            {last_update_date}
        </div>
        <div style="font-size: 1.15rem; font-weight: 500; color: #666;">
            Versiya • {last_update_version}
        </div>
    </div>
</div>
""",
    unsafe_allow_html=True,
)


# ----------------------------
# 1) Platform donut + legend
# ----------------------------
st.markdown(
    """
<div class="sec-row">
  <div>
    <div class="sec-title">📱 Platformalar</div>
    <div class="sec-sub">Foydalanuvchilar taqsimoti</div>
  </div>
  <div></div>
</div>
""",
    unsafe_allow_html=True,
)

try:
    platform_df = run_report(
        ["operatingSystem"],
        ["activeUsers"],
        RELEASE_DATE_STR,
        TODAY_STR,
        order_by=[OrderBy(metric=OrderBy.MetricOrderBy(metric_name="activeUsers"), desc=True)],
    )

    if not platform_df.empty:
        platform_df = platform_df.rename(columns={"operatingSystem": "PLATFORM", "activeUsers": "USERS"})
        platform_df["PLATFORM"] = platform_df["PLATFORM"].replace({"ANDROID": "Android", "IOS": "iOS"})
        platform_df["PLATFORM"] = platform_df["PLATFORM"].apply(lambda x: x if x in ["Android", "iOS"] else "Boshqalar")
        total = int(platform_df["USERS"].sum())
        platform_df["PERCENT"] = (platform_df["USERS"] / total * 100).round(1)

        CHART_H = 300
        c_chart, c_nums = st.columns([1.25, 0.85], gap="large", vertical_alignment="center")

        with c_chart:
            donut = (
                alt.Chart(platform_df)
                .mark_arc(innerRadius=118, outerRadius=150, opacity=0.92)
                .encode(
                    theta=alt.Theta(field="USERS", type="quantitative"),
                    color=alt.Color(
                        field="PLATFORM",
                        type="nominal",
                        scale=alt.Scale(
                            domain=["Android", "iOS", "Boshqalar"],
                            range=[COLORS["android"], COLORS["ios"], COLORS["other"]],
                        ),
                        legend=None,
                    ),
                    tooltip=[
                        alt.Tooltip("PLATFORM:N", title="Platforma"),
                        alt.Tooltip("USERS:Q", title="Foydalanuvchilar", format=","),
                        alt.Tooltip("PERCENT:Q", title="Ulush", format=".1f"),
                    ],
                )
                .properties(height=CHART_H, padding={"top": 18, "left": 8, "right": 8, "bottom": 18})
            )
            st.altair_chart(donut, use_container_width=True)

        with c_nums:
            # Build legend HTML as single block
            legend_html = f'''
<div class="stat-row">
  <div>
    <div class="stat-left"><span class="dot" style="background:{COLORS["accent"]};"></span>
      <span class="stat-label">Jami</span>
    </div>
  </div>
  <div class="stat-right">{total:,}</div>
</div>'''

            for _, r in platform_df.iterrows():
                p = r["PLATFORM"]
                u = int(r["USERS"])
                pr = float(r["PERCENT"])
                color = COLORS["android"] if p == "Android" else COLORS["ios"] if p == "iOS" else COLORS["other"]

                legend_html += f'''
<div class="stat-row">
  <div>
    <div class="stat-left"><span class="dot" style="background:{color};"></span>
      <span class="stat-label">{p}</span>
    </div>
    <div class="stat-sub">{pr:.1f}%</div>
  </div>
  <div class="stat-right">{u:,}</div>
</div>'''

            st.markdown(f'<div class="legend-card card" style="background: #FFFFFF; border: 1px solid rgba(15,23,42,0.14); border-radius: 18px; padding: 16px; box-shadow: 0 10px 24px rgba(15,23,42,0.06);">{legend_html}</div>', unsafe_allow_html=True)

    else:
        st.info("Ma'lumotlar mavjud emas")
except Exception as e:
    st.error(f"Platformalar xatolik: {e}")

# ----------------------------
# Client versions donut + legend
# ----------------------------
st.markdown(
    """
<div class="sec-row">
  <div>
    <div class="sec-title">🧩 Versiyalar</div>
    <div class="sec-sub">O'yin versiyasi bo'yicha foydalanuvchilar taqsimoti</div>
  </div>
  <div></div>
</div>
""",
    unsafe_allow_html=True,
)

try:
    versions_df = run_report(
        ["appVersion"],
        ["activeUsers"],
        (TODAY - timedelta(days=60)).strftime("%Y-%m-%d"),
        TODAY_STR,
        order_by=[OrderBy(metric=OrderBy.MetricOrderBy(metric_name="activeUsers"), desc=True)],
        limit=12,
    )

    if not versions_df.empty:
        versions_df = versions_df.rename(columns={"appVersion": "CLIENT_VERSION", "activeUsers": "USERS"})
        versions_df = versions_df[versions_df["CLIENT_VERSION"].notna()]
        total_v = int(versions_df["USERS"].sum())
        versions_df["PERCENT"] = (versions_df["USERS"] / total_v * 100).round(1)

        palette = [
            "#2563EB", "#7C3AED", "#16A34A", "#F59E0B",
            "#EF4444", "#06B6D4", "#F97316", "#0EA5E9",
            "#A855F7", "#22C55E", "#EAB308", "#FB7185",
        ]
        domain = versions_df["CLIENT_VERSION"].tolist()
        color_map = {v: palette[i % len(palette)] for i, v in enumerate(domain)}
        scale = alt.Scale(domain=domain, range=[color_map[v] for v in domain])

        CHART_H = 300
        c_chart, c_nums = st.columns([1.25, 0.85], gap="large", vertical_alignment="center")

        with c_chart:
            pie = (
                alt.Chart(versions_df)
                .mark_arc(innerRadius=118, outerRadius=150, opacity=0.92)
                .encode(
                    theta=alt.Theta(field="USERS", type="quantitative"),
                    color=alt.Color("CLIENT_VERSION:N", scale=scale, legend=None),
                    tooltip=[
                        alt.Tooltip("CLIENT_VERSION:N", title="Versiya"),
                        alt.Tooltip("USERS:Q", title="Foydalanuvchilar", format=","),
                        alt.Tooltip("PERCENT:Q", title="Ulush", format=".1f"),
                    ],
                )
                .properties(height=CHART_H, padding={"top": 18, "left": 8, "right": 8, "bottom": 18})
            )
            st.altair_chart(pie, use_container_width=True)

        with c_nums:
            legend_html = f'''
<div class="stat-row">
  <div>
    <div class="stat-left"><span class="dot" style="background:{COLORS["accent"]};"></span>
      <span class="stat-label">Jami</span>
    </div>
  </div>
  <div class="stat-right">{total_v:,}</div>
</div>'''

            for _, r in versions_df.iterrows():
                v = r["CLIENT_VERSION"]
                u = int(r["USERS"])
                pr = float(r["PERCENT"])
                dot_color = color_map.get(v, COLORS["other"])

                legend_html += f'''
<div class="stat-row">
  <div>
    <div class="stat-left"><span class="dot" style="background:{dot_color};"></span>
      <span class="stat-label">{v}</span>
    </div>
    <div class="stat-sub">{pr:.1f}%</div>
  </div>
  <div class="stat-right">{u:,}</div>
</div>'''

            st.markdown(
                f'<div class="legend-card card" style="background: #FFFFFF; border: 1px solid rgba(15,23,42,0.14); border-radius: 18px; padding: 16px; box-shadow: 0 10px 24px rgba(15,23,42,0.06);">{legend_html}</div>',
                unsafe_allow_html=True,
            )

    else:
        st.info("Ma'lumotlar mavjud emas")

except Exception as e:
    st.error(f"Versiyalar xatolik: {e}")




# ----------------------------
# 2) New users
# ----------------------------
left, right = st.columns([1.35, 1], gap="large", vertical_alignment="bottom")
with left:
    st.markdown('''<div class="sec-title">👥 Yangi foydalanuvchilar</div><div class="sec-sub">Tanlangan davr bo'yicha o'sish dinamikasi</div>''', unsafe_allow_html=True)
with right:
    f1, f2 = st.columns([0.9, 1.1], gap="small")
    with f1:
        period_type = st.selectbox("Kesim", ["Kunlik", "Haftalik", "Oylik"], key="new_users_period")
    with f2:
        date_range = st.date_input(
            "Sana oralig'i",
            value=(datetime(2025, 12, 27).date(), datetime.now().date()),
            key="new_users_date",
        )

if len(date_range) == 2:
    start_date, end_date = date_range
    
    # Har doim 27-dekabrdan boshlanadi
    min_date = datetime(2025, 12, 27).date()
    if start_date < min_date:
        start_date = min_date
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    try:
        daily_df = run_report(["date"], ["newUsers"], start_str, end_str)

        if not daily_df.empty:
            daily_df["SANA"] = pd.to_datetime(daily_df["date"])
            daily_df["YANGI_USERS"] = daily_df["newUsers"].astype(int)

            if period_type == "Kunlik":
                new_users_df = daily_df[["SANA", "YANGI_USERS"]].copy()
            elif period_type == "Haftalik":
                daily_df["SANA"] = daily_df["SANA"].dt.to_period("W").apply(lambda r: r.start_time)
                new_users_df = daily_df.groupby("SANA", as_index=False)["YANGI_USERS"].sum()
            else:
                daily_df["SANA"] = daily_df["SANA"].dt.to_period("M").dt.to_timestamp()
                new_users_df = daily_df.groupby("SANA", as_index=False)["YANGI_USERS"].sum()

            new_users_df["SANA_STR"] = new_users_df["SANA"].dt.strftime("%Y-%m-%d")

            m1, m2, m3 = st.columns(3)
            m1.metric("Jami", f"{int(new_users_df['YANGI_USERS'].sum()):,}")
            m2.metric("Eng yuqori", f"{int(new_users_df['YANGI_USERS'].max()):,}")
            m3.metric("O'rtacha", f"{int(round(new_users_df['YANGI_USERS'].mean(), 0)):,}")
            
            chart = (
                alt.Chart(new_users_df)
                .mark_bar(color=COLORS["new_users"], cornerRadiusTopLeft=6, cornerRadiusTopRight=6, opacity=0.92)
                .encode(
                    x=alt.X("SANA_STR:O", title="", axis=alt.Axis(labelAngle=-30, labelFontWeight=600), sort=None),
                    y=alt.Y("YANGI_USERS:Q", title="", axis=alt.Axis(labelFontWeight=600)),
                    tooltip=[
                        alt.Tooltip("SANA_STR:O", title="Sana"),
                        alt.Tooltip("YANGI_USERS:Q", title="Yangi", format=","),
                    ],
                )
                .properties(height=320, padding={"top": 18, "left": 8, "right": 8, "bottom": 8})
            )
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("Tanlangan davr uchun ma'lumotlar mavjud emas")
    except Exception as e:
        st.error(f"Yangi foydalanuvchilar xatolik: {e}")

# ----------------------------
# 3) Sessions
# ----------------------------
left, right = st.columns([1.35, 1], gap="large", vertical_alignment="bottom")
with left:
    st.markdown('''<div class="sec-title">📈 O'yin senaslari soni</div><div class="sec-sub">Faollik ko'rinishi</div>''', unsafe_allow_html=True)
with right:
    s1, s2 = st.columns([0.9, 1.1], gap="small")
    with s1:
        session_view = st.selectbox("Ko'rinish", ["Kunlik", "Soatlik"], key="session_view")
    with s2:
        if session_view == "Soatlik":
            session_date = st.date_input("Sana", value=datetime.now().date(), key="session_date")
            session_period = None
        else:
            session_period = st.selectbox(
                "Davr",
                ["Hammasi", "So'nggi 7 kun", "So'nggi 14 kun", "So'nggi 30 kun"],
                key="session_period",
            )
            session_date = None

try:
    if session_view == "Soatlik":
        date_str = session_date.strftime("%Y-%m-%d")
        sessions_df = run_report(
            ["hour"],
            ["sessions", "activeUsers"],
            date_str,
            date_str,
            order_by=[OrderBy(dimension=OrderBy.DimensionOrderBy(dimension_name="hour"))],
        )

        if not sessions_df.empty:
            sessions_df["SOAT"] = pd.to_numeric(sessions_df["hour"], errors="coerce").fillna(0).astype(int)
            sessions_df["HODISALAR"] = pd.to_numeric(sessions_df["sessions"], errors="coerce").fillna(0).astype(int)
            sessions_df["FOYDALANUVCHILAR"] = pd.to_numeric(sessions_df["activeUsers"], errors="coerce").fillna(0).astype(int)
            sessions_df["SOAT_LABEL"] = sessions_df["SOAT"].apply(lambda x: f"{x:02d}:00")

            m1, m2 = st.columns(2)
            m1.metric("Seanslar", f"{int(sessions_df['HODISALAR'].sum()):,}")
            m2.metric("Faol foydalanuvchilar", f"{int(sessions_df['FOYDALANUVCHILAR'].sum()):,}")

            chart = (
                alt.Chart(sessions_df)
                .mark_bar(color=COLORS["sessions"], cornerRadiusTopLeft=6, cornerRadiusTopRight=6, opacity=0.92)
                .encode(
                    x=alt.X("SOAT_LABEL:N", title="", sort=None, axis=alt.Axis(labelAngle=0, labelFontWeight=600)),
                    y=alt.Y("HODISALAR:Q", title="", axis=alt.Axis(labelFontWeight=600)),
                    tooltip=[
                        alt.Tooltip("SOAT_LABEL:N", title="Soat"),
                        alt.Tooltip("HODISALAR:Q", title="Seanslar", format=","),
                        alt.Tooltip("FOYDALANUVCHILAR:Q", title="Foydalanuvchilar", format=","),
                    ],
                )
                .properties(height=320, padding={"top": 18, "left": 8, "right": 8, "bottom": 8})
            )
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("Tanlangan sana uchun ma'lumotlar mavjud emas")
    else:
        # Kunlik ko'rinish uchun
        if session_period == "Hammasi":
            start_date = datetime(2025, 12, 27)
            end_date = datetime.now()  # Hozirgi vaqt
        else:
            days_map = {"So'nggi 7 kun": 7, "So'nggi 14 kun": 14, "So'nggi 30 kun": 30}
            days = days_map[session_period]
            end_date = datetime.now()  # Hozirgi vaqt
            start_date = end_date - timedelta(days=days)

        sessions_df = run_report(
            ["date"],
            ["sessions", "averageSessionDuration"],
            start_date.strftime("%Y-%m-%d"),
            end_date.strftime("%Y-%m-%d"),
            order_by=[OrderBy(dimension=OrderBy.DimensionOrderBy(dimension_name="date"))],
        )

        if not sessions_df.empty:
            sessions_df["SESSIYALAR"] = pd.to_numeric(sessions_df["sessions"], errors="coerce").fillna(0).astype(int)
            # averageSessionDuration seconds -> minutes
            sessions_df["ORTACHA_DAVOMIYLIK"] = pd.to_numeric(sessions_df.get("averageSessionDuration", 0), errors="coerce").fillna(0) / 60
            m1, m2, m3 = st.columns(3)
            m1.metric("Jami", f"{int(sessions_df['SESSIYALAR'].sum()):,}")
            m2.metric("O'rtacha kunlik", f"{int(sessions_df['SESSIYALAR'].mean()):,}")
            m3.metric("O'rtacha o'yin davomiyligi", f"{round(float(sessions_df['ORTACHA_DAVOMIYLIK'].mean()), 1)} daq")

            sessions_df["SANA"] = pd.to_datetime(sessions_df["date"])
            sessions_df["SANA_STR"] = sessions_df["SANA"].dt.strftime("%Y-%m-%d")

            chart = (
                alt.Chart(sessions_df)
                .mark_bar(color=COLORS["sessions"], cornerRadiusTopLeft=6, cornerRadiusTopRight=6, opacity=0.92)
                .encode(
                    x=alt.X("SANA_STR:O", title="", axis=alt.Axis(labelAngle=-30, labelFontWeight=600), sort=None),
                    y=alt.Y("SESSIYALAR:Q", title="", axis=alt.Axis(labelFontWeight=600)),
                    tooltip=[
                        alt.Tooltip("SANA_STR:O", title="Sana"),
                        alt.Tooltip("SESSIYALAR:Q", title="Sessiyalar", format=","),
                        alt.Tooltip("ORTACHA_DAVOMIYLIK:Q", title="Daqiqa", format=".1f"),
                    ],
                )
                .properties(height=320, padding={"top": 18, "left": 8, "right": 8, "bottom": 8})
            )
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("Ma'lumotlar mavjud emas")
except Exception as e:
    st.error(f"Sessiyalar xatolik: {e}")


# ----------------------------
# 4) DAU Trend
# ----------------------------
left, right = st.columns([1.35, 1], gap="large", vertical_alignment="bottom")
with left:
    st.markdown('<div class="sec-title">📊 Kunlik faol foydalanuvchilar (DAU)</div><div class="sec-sub">Har kungi unikal foydalanuvchilar soni</div>', unsafe_allow_html=True)
with right:
    dau_period = st.selectbox(
        "Davr",
        ["So'nggi 7 kun", "So'nggi 14 kun", "So'nggi 30 kun", "So'nggi 90 kun"],
        key="dau_period",
    )

try:
    dau_days_map = {"So'nggi 7 kun": 7, "So'nggi 14 kun": 14, "So'nggi 30 kun": 30, "So'nggi 90 kun": 90}
    dau_days = dau_days_map[dau_period]

    # Set end date to yesterday instead of today
    dau_end = datetime.now() - timedelta(days=1)

    # Set custom start date for 90 days option
    if dau_period == "So'nggi 90 kun":
        dau_start = datetime(2025, 12, 27)
    else:
        dau_start = dau_end - timedelta(days=dau_days)

    dau_trend_df = run_report(
        ["date"],
        ["activeUsers"],
        dau_start.strftime("%Y-%m-%d"),
        dau_end.strftime("%Y-%m-%d"),
        order_by=[OrderBy(dimension=OrderBy.DimensionOrderBy(dimension_name="date"))],
    )
    if not dau_trend_df.empty:
        dau_trend_df["DAU"] = pd.to_numeric(dau_trend_df["activeUsers"], errors="coerce").fillna(0).astype(int)
        dau_trend_df["SANA"] = pd.to_datetime(dau_trend_df["date"])
        dau_trend_df["SANA_STR"] = dau_trend_df["SANA"].dt.strftime("%Y-%m-%d")

        m1, m2, m3 = st.columns(3)
        m1.metric("O'rtacha DAU", f"{int(dau_trend_df['DAU'].mean()):,}")
        m2.metric("Eng yuqori", f"{int(dau_trend_df['DAU'].max()):,}")
        m3.metric("Eng past", f"{int(dau_trend_df['DAU'].min()):,}")

        # Davrga qarab tickCount ni sozlash
        tick_count = 5 if dau_period == "So'nggi 90 kun" else 7

        # Area chart for DAU: Bold labels, no grid
        dau_area = (
            alt.Chart(dau_trend_df)
            .mark_area(
                color=COLORS["sessions"],
                opacity=0.2,
                line=False
            )
            .encode(
                x=alt.X("SANA:T", title="", axis=alt.Axis(format="%Y-%m-%d", labelAngle=-30, tickCount=tick_count, labelFontWeight=600)),
                y=alt.Y("DAU:Q", title="", axis=alt.Axis(labelFontWeight=600)),
            )
        )

        dau_line = (
            alt.Chart(dau_trend_df)
            .mark_line(color=COLORS["sessions"], strokeWidth=2.6, opacity=0.9)
            .encode(
                x=alt.X("SANA:T", title="", axis=alt.Axis(format="%Y-%m-%d", labelAngle=-30, tickCount=tick_count, labelFontWeight=600)),
                y=alt.Y("DAU:Q", title="", axis=alt.Axis(labelFontWeight=600)),
                tooltip=[
                    alt.Tooltip("SANA:T", title="Sana", format="%Y-%m-%d"),
                    alt.Tooltip("DAU:Q", title="DAU", format=","),
                ],
            )
        )

        dau_points = (
            alt.Chart(dau_trend_df)
            .mark_circle(size=60, color=COLORS["sessions"], opacity=0.85)
            .encode(x="SANA:T", y="DAU:Q")
        )

        st.altair_chart((dau_area + dau_line + dau_points).properties(height=320, padding={"top": 18, "left": 8, "right": 8, "bottom": 8}), use_container_width=True)
    else:
        st.info("Ma'lumotlar mavjud emas")
except Exception as e:
    st.error(f"DAU trend xatolik: {e}")

# ----------------------------
# 5) MAU Trend
# ----------------------------
left, right = st.columns([1.35, 1], gap="large", vertical_alignment="bottom")
with left:
    st.markdown('<div class="sec-title">📅 Oylik faol foydalanuvchilar (MAU)</div><div class="sec-sub">Har oylik unikal foydalanuvchilar soni</div>', unsafe_allow_html=True)
with right:
    mau_period = st.selectbox(
        "Davr",
        ["So'nggi 6 oy", "So'nggi 12 oy"],
        key="mau_period",
    )

try:
    mau_months_map = {"So'nggi 6 oy": 6, "So'nggi 12 oy": 12}
    mau_months = mau_months_map[mau_period]
    mau_end = datetime.now()
    mau_start = mau_end - timedelta(days=mau_months * 30)

    # Release bolgan vaqtdan boshlab analiz qilsin
    min_date = datetime(2025, 12, 27)
    if mau_start < min_date:
        mau_start = min_date

    mau_trend_df = run_report(
        ["yearMonth"],
        ["activeUsers"],
        mau_start.strftime("%Y-%m-%d"),
        mau_end.strftime("%Y-%m-%d"),
        order_by=[OrderBy(dimension=OrderBy.DimensionOrderBy(dimension_name="yearMonth"))],
    )

    if not mau_trend_df.empty:
        mau_trend_df["OY"] = pd.to_datetime(mau_trend_df["yearMonth"].astype(int).astype(str) + "01", format="%Y%m%d")
        mau_trend_df["MAU"] = pd.to_numeric(mau_trend_df["activeUsers"], errors="coerce").fillna(0).astype(int)

        MONTHS_UZ = {
            1: "Yanvar", 2: "Fevral", 3: "Mart", 4: "Aprel",
            5: "May", 6: "Iyun", 7: "Iyul", 8: "Avgust",
            9: "Sentabr", 10: "Oktabr", 11: "Noyabr", 12: "Dekabr"
        }

        mau_trend_df["OY_LABEL"] = mau_trend_df["OY"].apply(
            lambda d: f"{d.year}-{MONTHS_UZ[d.month]}"
        )

        m1, m2, m3 = st.columns(3)
        m1.metric("O'rtacha MAU", f"{int(mau_trend_df['MAU'].mean()):,}")
        m2.metric("Eng yuqori", f"{int(mau_trend_df['MAU'].max()):,}")
        m3.metric("Oxirgi oy", f"{int(mau_trend_df['MAU'].iloc[-1]):,}")

        mau_chart = (
            alt.Chart(mau_trend_df)
            .mark_bar(color=COLORS["purple"], cornerRadiusTopLeft=6, cornerRadiusTopRight=6, opacity=0.92)
            .encode(
               x=alt.X("OY_LABEL:O", title="", axis=alt.Axis(labelAngle=-30, labelFontWeight=600), sort=None),
               y=alt.Y("MAU:Q", title="", axis=alt.Axis(labelFontWeight=600)),
               tooltip=[
                    alt.Tooltip("OY_LABEL:O", title="Yil-Oy"),
                    alt.Tooltip("MAU:Q", title="MAU", format=","),
                ],
            )
            .properties(height=320, padding={"top": 18, "left": 8, "right": 8, "bottom": 8})
        )
        st.altair_chart(mau_chart, use_container_width=True)
    else:
        st.info("Ma'lumotlar mavjud emas")
except Exception as e:
    st.error(f"MAU trend xatolik: {e}")

# Removed mini-game trend, top 5, and retention sections per request
