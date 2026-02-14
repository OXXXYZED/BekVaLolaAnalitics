import streamlit as st
import pandas as pd
import altair as alt
from datetime import datetime, timedelta
from decimal import Decimal
import snowflake.connector
import base64
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

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

    "new_users": "#F59E0B",
    "sessions": "#2563EB",
    "minigame": "#EF4444",
    "purple": "#7C3AED",

    "neon": "rgba(37,99,235,0.16)",
    "neon2": "rgba(124,58,237,0.12)",
}

def get_logo_base64():
    logo_path = Path(__file__).parent / "images" / "Beklola.png"
    with open(logo_path, "rb") as f:
        return base64.b64encode(f.read()).decode()

LOGO_BASE64 = get_logo_base64()


# ----------------------------
# Altair clean light theme
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
# CSS theme
# ----------------------------
st.markdown(
    f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

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

@media (prefers-color-scheme: dark) {{
  :root {{ color-scheme: light only !important; }}
  html, body, .stApp, [class*="css"] {{
    background: {COLORS["bg"]} !important;
    color: {COLORS["text"]} !important;
  }}
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
  [data-baseweb="calendar"] td,
  [data-baseweb="calendar"] button {{ background-color: transparent !important; }}
  [data-baseweb="calendar"] button[aria-selected="true"] {{
    background-color: #2563EB !important;
    color: #FFFFFF !important;
  }}
}}

.stApp {{ background: {COLORS["bg"]} !important; color: {COLORS["text"]} !important; }}
.block-container {{ max-width: 1320px; padding: 0.7rem 1.6rem 2rem 1.6rem; }}

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
[data-testid="stSidebar"], [data-testid="stSidebarCollapsedControl"] {{ display: none !important; }}

[data-baseweb="layer"], [data-baseweb="layer"] > div,
body > div[data-baseweb="layer"], body > div > [data-baseweb="popover"],
body > div > [data-baseweb="menu"] {{ color-scheme: light !important; }}
div[data-floating-ui-portal], div[data-floating-ui-portal] > div {{ color-scheme: light !important; }}

[data-baseweb="select"], [data-baseweb="select"] *,
[data-baseweb="popover"], [data-baseweb="popover"] *,
[data-baseweb="menu"], [data-baseweb="menu"] * {{ color-scheme: light !important; }}

[data-baseweb="select"] > div {{
  background-color: #FFFFFF !important;
  border: 1px solid rgba(15,23,42,0.18) !important;
  border-radius: 12px !important;
  box-shadow: none !important;
}}

[data-baseweb="select"] input {{
  color: #0F172A !important;
  -webkit-text-fill-color: #0F172A !important;
  background: transparent !important;
}}

[data-baseweb="select"] > div > div {{ color: #0F172A !important; }}
.stSelectbox label, .stSelectbox [data-baseweb="select"] *:not(svg) {{ color: #0F172A !important; }}
[data-baseweb="select"] svg {{ color: #0F172A !important; fill: #0F172A !important; }}
[data-baseweb="select"] > div:focus-within {{
  border-color: rgba(37,99,235,0.45) !important;
  box-shadow: 0 0 0 3px rgba(37,99,235,0.14) !important;
}}

[data-baseweb="popover"], [data-baseweb="popover"] > div,
[data-baseweb="popover"] > div > div,
[data-baseweb="menu"], [data-baseweb="menu"] > div {{
  background: #FFFFFF !important;
  background-color: #FFFFFF !important;
  border: 1px solid rgba(15,23,42,0.14) !important;
  border-radius: 12px !important;
  box-shadow: 0 4px 16px rgba(0,0,0,0.12) !important;
}}

[data-baseweb="menu"] ul, [data-baseweb="popover"] ul {{ background: #FFFFFF !important; background-color: #FFFFFF !important; }}
ul[role="listbox"], [data-baseweb="menu"] ul[role="listbox"] {{
  background: #FFFFFF !important;
  background-color: #FFFFFF !important;
  border: none !important;
  border-radius: 12px !important;
}}
ul[role="listbox"] li, [data-baseweb="menu"] li, [data-baseweb="menu"] ul li {{
  color: #0F172A !important;
  background: #FFFFFF !important;
  background-color: #FFFFFF !important;
}}
ul[role="listbox"] li:hover, [data-baseweb="menu"] li:hover {{
  background: rgba(37,99,235,0.08) !important;
  background-color: rgba(37,99,235,0.08) !important;
}}
ul[role="listbox"] li[aria-selected="true"],
div[role="option"][aria-selected="true"],
[data-baseweb="menu"] li[aria-selected="true"] {{
  background: rgba(37,99,235,0.10) !important;
  background-color: rgba(37,99,235,0.10) !important;
  color: #0F172A !important;
}}
ul[role="listbox"] li[aria-selected="true"] *,
div[role="option"][aria-selected="true"] *,
[data-baseweb="menu"] li[aria-selected="true"] * {{ color: #0F172A !important; }}
ul[role="listbox"] li[data-highlighted="true"],
div[role="option"][data-highlighted="true"],
[data-baseweb="menu"] li[data-highlighted="true"],
ul[role="listbox"] li:focus, [data-baseweb="menu"] li:focus {{
  background: rgba(37,99,235,0.08) !important;
  background-color: rgba(37,99,235,0.08) !important;
  color: #0F172A !important;
}}

[data-baseweb="datepicker"], [data-baseweb="datepicker"] *,
[data-baseweb="calendar"], [data-baseweb="calendar"] *,
.stDateInput, .stDateInput * {{ color-scheme: light !important; }}

[data-baseweb="datepicker"] > div, .stDateInput > div > div {{
  background-color: #FFFFFF !important;
  border: 1px solid rgba(15,23,42,0.18) !important;
  border-radius: 12px !important;
  box-shadow: none !important;
}}
[data-baseweb="datepicker"] input, .stDateInput input {{
  color: #0F172A !important;
  -webkit-text-fill-color: #0F172A !important;
  background: transparent !important;
  font-weight: 500 !important;
}}
[data-baseweb="datepicker"] svg, .stDateInput svg {{ color: #0F172A !important; fill: #0F172A !important; }}
[data-baseweb="datepicker"] > div:focus-within, .stDateInput > div > div:focus-within {{
  border-color: rgba(37,99,235,0.45) !important;
  box-shadow: 0 0 0 3px rgba(37,99,235,0.14) !important;
}}

[data-baseweb="calendar"] *:not([aria-selected="true"]) {{
  background: transparent !important;
  background-color: transparent !important;
}}
[data-baseweb="calendar"] {{
  background: #FFFFFF !important;
  background-color: #FFFFFF !important;
  color: #0F172A !important;
  border: 1px solid rgba(15,23,42,0.14) !important;
  border-radius: 14px !important;
  box-shadow: 0 8px 24px rgba(0,0,0,0.12) !important;
}}
[data-baseweb="calendar"] > div {{ background: #FFFFFF !important; background-color: #FFFFFF !important; }}
[data-baseweb="calendar"] * {{ color: #0F172A !important; }}
[data-baseweb="calendar"] [data-baseweb="select"] > div {{
  background: #F8FAFC !important;
  background-color: #F8FAFC !important;
}}
[data-baseweb="calendar"] button svg, [data-baseweb="calendar"] svg {{
  color: #0F172A !important;
  fill: #0F172A !important;
}}
[data-baseweb="calendar"] th, [data-baseweb="calendar"] [role="columnheader"] {{ color: #64748B !important; }}
[data-baseweb="calendar"] button:hover {{
  background: rgba(37,99,235,0.08) !important;
  background-color: rgba(37,99,235,0.08) !important;
}}
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
[data-baseweb="calendar"] [aria-selected="true"] *,
[data-baseweb="calendar"] button[aria-selected="true"] *,
[data-baseweb="calendar"] td[aria-selected="true"] * {{ color: #FFFFFF !important; }}
[data-baseweb="calendar"] [aria-current="date"]:not([aria-selected="true"]) {{
  border: 2px solid #2563EB !important;
  border-radius: 50% !important;
}}
[data-baseweb="calendar"] button:disabled {{ color: #CBD5E1 !important; opacity: 0.4 !important; }}
[data-baseweb="calendar"] [data-highlighted="true"]:not([aria-selected="true"]) {{
  background: rgba(37,99,235,0.1) !important;
  background-color: rgba(37,99,235,0.1) !important;
}}
[data-baseweb="calendar"] *::before, [data-baseweb="calendar"] *::after {{
  background: transparent !important;
  background-color: transparent !important;
}}
[data-baseweb="calendar"] td[data-in-range="true"]::before,
[data-baseweb="calendar"] [data-in-range="true"] {{
  background: rgba(37,99,235,0.1) !important;
  background-color: rgba(37,99,235,0.1) !important;
}}
[data-baseweb="calendar"] + div, [data-baseweb="calendar"] ~ div {{
  background: #FFFFFF !important;
  background-color: #FFFFFF !important;
}}
[data-baseweb="calendar"] + div [data-baseweb="select"] > div,
[data-baseweb="calendar"] ~ div [data-baseweb="select"] > div {{
  background: #FFFFFF !important;
  background-color: #FFFFFF !important;
  border: 1px solid rgba(15,23,42,0.18) !important;
}}

.card {{
  background: {COLORS["card"]} !important;
  border: 1px solid {COLORS["border"]} !important;
  border-radius: 18px;
  box-shadow: 0 10px 24px rgba(15,23,42,0.06);
  overflow: hidden !important;
}}

.card:hover, [data-testid="stVegaLiteChart"]:hover {{
  box-shadow:
    0 0 0 1px rgba(37,99,235,0.10),
    0 0 16px {COLORS["neon"]},
    0 0 22px {COLORS["neon2"]};
  transform: none !important;
  transition: all 160ms ease;
}}

[data-testid="stVegaLiteChart"] {{
  background: {COLORS["card"]} !important;
  border: 1px solid {COLORS["border"]} !important;
  border-radius: 18px;
  padding: 16px !important;
  box-shadow: 0 6px 18px rgba(15,23,42,0.05);
  overflow: hidden !important;
}}

[data-testid="stVegaLiteChart"] > div {{ background: transparent !important; overflow: hidden !important; }}
[data-testid="stVegaLiteChart"] canvas, [data-testid="stVegaLiteChart"] svg {{
  max-width: 100% !important;
  height: auto !important;
}}

.header {{
  display:flex;
  justify-content:center;
  align-items:center;
  margin: 6px 0 24px 0;
}}
.header img {{ height: 60px; width: auto; }}

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
  width: 40px; height: 40px; border-radius: 12px;
  display:flex; align-items:center; justify-content:center;
  font-size: 20px;
  background: rgba(37,99,235,0.10); color: #2563EB;
  flex-shrink: 0;
}}
.kpi-ico.purple {{ background: rgba(124,58,237,0.10); color:#7C3AED; }}
.kpi-ico.orange {{ background: rgba(245,158,11,0.12); color:#F59E0B; }}
.kpi-ico.green {{ background: rgba(22,163,74,0.10); color:#16A34A; }}
.kpi-ico.blue {{ background: rgba(59,130,246,0.12); color:#3B82F6; }}

.kpi-label {{ font-size: 0.95rem; color: {COLORS["muted"]}; font-weight: 500; line-height: 1.3; }}
.kpi-value {{ font-size: 2.2rem; font-weight: 700; letter-spacing: -0.02em; margin-top: auto; }}

.sec-row {{
  display:flex; align-items:flex-end; justify-content:space-between;
  gap: 12px; margin-top: 18px; margin-bottom: 8px;
}}
.sec-title {{ font-size: 1.12rem; font-weight: 650; letter-spacing: -0.01em; margin: 0; }}
.sec-sub {{ color: {COLORS["muted"]}; font-weight: 400; font-size: 0.95rem; margin: 4px 0 0 0; }}

.legend-card {{ padding: none !important; border: none !important; box-shadow: none !important; }}
.stat-row {{
  display:flex; align-items:flex-start; justify-content:space-between;
  gap: 12px; padding: 10px 8px;
  border-bottom: 1px solid rgba(15,23,42,0.08);
}}
.stat-row:last-child {{ border-bottom: none; padding-bottom: 8px; }}
.dot {{
  width: 10px; height: 10px; border-radius: 999px;
  margin-right: 10px; margin-top: 5px; flex: 0 0 auto;
  box-shadow: 0 0 0 3px rgba(15,23,42,0.04);
}}
.stat-left {{ display:flex; align-items:flex-start; gap: 10px; line-height: 1.15; }}
.stat-label {{ font-weight: 600; }}
.stat-sub {{ color: {COLORS["muted"]}; font-weight: 400; font-size: 0.9rem; margin-top: 3px; }}
.stat-right {{ font-weight: 600; text-align:right; white-space: nowrap; }}

[data-testid="stMetricValue"] {{ color: {COLORS["text"]} !important; font-weight: 650 !important; }}
[data-testid="stMetricLabel"] {{ color: {COLORS["muted"]} !important; font-weight: 450 !important; }}

.rank-card {{ padding: 10px 12px; }}
.rank-row {{
  display:grid; grid-template-columns: 52px 1fr 120px;
  gap: 10px; align-items:center;
  padding: 10px 0; border-bottom: 1px solid rgba(15,23,42,0.08);
}}
.rank-row:last-child {{ border-bottom: none; }}
.rank-badge {{ font-size: 1.2rem; font-weight: 650; }}
.rank-name {{ font-weight: 500; }}
.rank-val {{ text-align:right; font-weight: 600; }}

@media (max-width: 980px) {{
  .kpi-grid {{ grid-template-columns: 1fr; }}
  .rank-row {{ grid-template-columns: 52px 1fr 100px; }}
}}

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

[data-testid="stVegaLiteChart"], .card, [data-testid="stMetric"] {{
  animation: fadeInUp 0.4s ease-out;
  transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1) !important;
}}

@keyframes fadeInUp {{
  from {{ opacity: 0; transform: translateY(20px); }}
  to {{ opacity: 1; transform: translateY(0); }}
}}
</style>
""",
    unsafe_allow_html=True,
)


# ----------------------------
# Secrets
# ----------------------------
if "snowflake" not in st.secrets:
    st.error("Snowflake credentials topilmadi. Iltimos, secrets ni sozlang.")
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
# Snowflake connection — longer TTL, no unnecessary reconnects
# ----------------------------
@st.cache_resource(ttl=3600)  # 1 hour — tokens last that long
def get_connection():
    return snowflake.connector.connect(
        user=st.secrets["snowflake"]["user"],
        password=st.secrets["snowflake"]["password"],
        account=st.secrets["snowflake"]["account"],
        warehouse=st.secrets["snowflake"]["warehouse"],
        database=st.secrets["snowflake"]["database"],
        schema=st.secrets["snowflake"]["schema"],
        # ✅ PERF: Keep warehouse alive between queries, avoid cold-start delay
        session_parameters={
            "AUTOCOMMIT": True,
            "QUERY_TAG": "streamlit_dashboard",
        },
    )

def _execute_query(query: str) -> pd.DataFrame:
    """Execute query and return DataFrame."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(query)
    columns = [desc[0] for desc in cur.description]
    data = cur.fetchall()
    df = pd.DataFrame(data, columns=columns)
    for col in df.columns:
        if df[col].dtype == object:
            if df[col].apply(lambda x: isinstance(x, Decimal)).any():
                df[col] = df[col].apply(lambda x: float(x) if isinstance(x, Decimal) else x)
            try:
                numeric_col = pd.to_numeric(df[col], errors="coerce")
                if not numeric_col.isna().all():
                    df[col] = numeric_col
            except Exception:
                pass
    return df

# ✅ PERF: Longer cache (30 min) — KPI data doesn't change that fast
@st.cache_data(ttl=1800, show_spinner=False)
def run_query(query: str) -> pd.DataFrame:
    try:
        return _execute_query(query)
    except snowflake.connector.errors.ProgrammingError as e:
        if "390114" in str(e) or "Authentication token has expired" in str(e):
            get_connection.clear()
            return _execute_query(query)
        raise


GAME_ID = 181330318
DB = "UNITY_ANALYTICS_GCP_US_CENTRAL1_UNITY_ANALYTICS_PDA.SHARES"


# ----------------------------
# ✅ PERF: Single query for ALL KPIs — replaces 5 separate round-trips
# ----------------------------
@st.cache_data(ttl=1800, show_spinner=False)
def load_all_kpis() -> dict:
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    mau_start = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    mau_end = datetime.now().strftime("%Y-%m-%d")
    week_start = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

    query = f"""
    SELECT
        COUNT(DISTINCT USER_ID)                                                          AS TOTAL_USERS,
        COUNT(DISTINCT CASE WHEN EVENT_DATE = '{yesterday}' THEN USER_ID END)           AS DAU,
        COUNT(DISTINCT CASE WHEN EVENT_DATE BETWEEN '{mau_start}' AND '{mau_end}'
                            THEN USER_ID END)                                            AS MAU,
        COUNT(DISTINCT CASE WHEN EVENT_DATE BETWEEN '{week_start}' AND '{mau_end}'
                            THEN SESSION_ID END)                                         AS SESSIONS_7D
    FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY
    WHERE GAME_ID = {GAME_ID}
    """
    try:
        df = run_query(query)
        row = df.iloc[0]
        return {
            "total_users": int(row["TOTAL_USERS"] or 0),
            "dau":         int(row["DAU"] or 0),
            "mau":         int(row["MAU"] or 0),
            "sessions":    int(row["SESSIONS_7D"] or 0),
        }
    except Exception:
        return {"total_users": None, "dau": None, "mau": None, "sessions": None}


@st.cache_data(ttl=1800, show_spinner=False)
def load_last_version() -> str:
    try:
        df = run_query(f"""
            SELECT COALESCE(CLIENT_VERSION, 'Noma''lum') AS LAST_UPDATE_VERSION
            FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY
            WHERE GAME_ID = {GAME_ID}
              AND CLIENT_VERSION IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (ORDER BY EVENT_DATE DESC) = 1
        """)
        return df["LAST_UPDATE_VERSION"][0] if not df.empty else "N/A"
    except Exception:
        return "N/A"


# ✅ PERF: Fetch KPI + version in parallel threads
@st.cache_data(ttl=1800, show_spinner=False)
def load_kpis_and_version() -> tuple:
    with ThreadPoolExecutor(max_workers=2) as pool:
        f_kpis    = pool.submit(load_all_kpis)
        f_version = pool.submit(load_last_version)
        kpis    = f_kpis.result()
        version = f_version.result()
    return kpis, version


# ✅ PERF: Parallel fetch for platform + versions donuts
@st.cache_data(ttl=1800, show_spinner=False)
def load_platform_and_versions() -> tuple:
    def _platform():
        return run_query(f"""
            SELECT
                PLATFORM_GROUP AS PLATFORM,
                SUM(USERS) AS USERS
            FROM (
                SELECT
                    CASE
                        WHEN PLATFORM = 'ANDROID' THEN 'Android'
                        WHEN PLATFORM = 'IOS' THEN 'iOS'
                        ELSE 'Boshqalar'
                    END AS PLATFORM_GROUP,
                    COUNT(DISTINCT USER_ID) AS USERS
                FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY
                WHERE GAME_ID = {GAME_ID}
                GROUP BY PLATFORM
            )
            GROUP BY PLATFORM_GROUP
            ORDER BY USERS DESC
        """)

    def _versions():
        return run_query(f"""
            WITH version_data AS (
                SELECT
                    COALESCE(CLIENT_VERSION, 'UNKNOWN') AS CLIENT_VERSION,
                    COUNT(DISTINCT USER_ID) AS USERS,
                    TRY_TO_NUMBER(SPLIT_PART(CLIENT_VERSION, '.', 1)) AS major,
                    TRY_TO_NUMBER(SPLIT_PART(CLIENT_VERSION, '.', 2)) AS minor,
                    TRY_TO_NUMBER(SPLIT_PART(CLIENT_VERSION, '.', 3)) AS patch
                FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY
                WHERE GAME_ID = {GAME_ID}
                  AND EVENT_DATE >= '2025-12-27'
                  AND COALESCE(CLIENT_VERSION, 'UNKNOWN') != 'UNKNOWN'
                GROUP BY CLIENT_VERSION
            ),
            latest_version AS (
                SELECT CLIENT_VERSION
                FROM version_data
                ORDER BY major DESC, minor DESC, patch DESC
                LIMIT 1
            )
            SELECT
                CASE
                    WHEN v.CLIENT_VERSION = l.CLIENT_VERSION THEN v.CLIENT_VERSION
                    ELSE '1.0.0'
                END AS CLIENT_VERSION,
                SUM(v.USERS) AS USERS
            FROM version_data v
            CROSS JOIN latest_version l
            GROUP BY
                CASE
                    WHEN v.CLIENT_VERSION = l.CLIENT_VERSION THEN v.CLIENT_VERSION
                    ELSE '1.0.0'
                END
            ORDER BY
                CASE WHEN CLIENT_VERSION = '1.0.0' THEN 0 ELSE 1 END,
                CLIENT_VERSION ASC
        """)

    with ThreadPoolExecutor(max_workers=2) as pool:
        f_plat = pool.submit(_platform)
        f_ver  = pool.submit(_versions)
        return f_plat.result(), f_ver.result()


# ----------------------------
# Header
# ----------------------------
st.markdown(f'''
<div class="header">
    <img src="data:image/png;base64,{LOGO_BASE64}" style="height:60px;width:auto;" />
</div>
''', unsafe_allow_html=True)


# ----------------------------
# KPI Cards — single DB round-trip
# ----------------------------
kpis, last_update_version = load_kpis_and_version()
last_update_date = "15.01.2026"

total_users = kpis["total_users"]
dau         = kpis["dau"]
mau         = kpis["mau"]
sessions    = kpis["sessions"]

st.markdown(
    f"""
<div class="kpi-grid">
  <div class="kpi card">
    <div class="kpi-head">
      <div class="kpi-ico green">👥</div>
      <div style="font-size: 1.1rem; font-weight: 600; color: #444;">Umumiy foydalanuvchilar</div>
    </div>
    <div style="font-size: 2.2rem; font-weight: 650; color: #1a1a1a; line-height: 1.2;">
      {f"{total_users:,}" if total_users is not None else "N/A"}
    </div>
  </div>

  <div class="kpi card">
    <div class="kpi-head">
      <div class="kpi-ico blue">📊</div>
      <div style="font-size: 1.1rem; font-weight: 600; color: #444;">Kunlik faol foydalanuvchilar</div>
    </div>
    <div style="font-size: 2.2rem; font-weight: 650; color: #1a1a1a; line-height: 1.2;">
      {f"{dau:,}" if dau is not None else "N/A"}
    </div>
  </div>

  <div class="kpi card">
    <div class="kpi-head">
      <div class="kpi-ico purple">📅</div>
      <div style="font-size: 1.1rem; font-weight: 600; color: #444;">Oylik faol foydalanuvchilar</div>
    </div>
    <div style="font-size: 2.2rem; font-weight: 650; color: #1a1a1a; line-height: 1.2;">
      {f"{mau:,}" if mau is not None else "N/A"}
    </div>
  </div>

  <div class="kpi card">
    <div class="kpi-head">
      <div class="kpi-ico">📈</div>
      <div style="font-size: 1.1rem; font-weight: 600; color: #444;">O'yin seanslari soni</div>
    </div>
    <div style="font-size: 2.2rem; font-weight: 650; color: #1a1a1a; line-height: 1.2;">
      {f"{sessions:,}" if sessions is not None else "N/A"}
    </div>
  </div>

  <div class="kpi card">
    <div class="kpi-head">
      <div class="kpi-ico orange">🔄</div>
      <div style="font-size: 1.1rem; font-weight: 600; color: #444;">So'ngi yangilanish</div>
    </div>
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
# Platform + Versions — fetched in parallel
# ----------------------------
platform_df, versions_raw_df = load_platform_and_versions()

# Platform donut
st.markdown("""
<div class="sec-row">
  <div>
    <div class="sec-title">📱 Platformalar</div>
    <div class="sec-sub">Foydalanuvchilar taqsimoti</div>
  </div>
</div>
""", unsafe_allow_html=True)

if not platform_df.empty:
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
                    field="PLATFORM", type="nominal",
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
        st.altair_chart(donut, width="stretch")
    with c_nums:
        legend_html = f'''
<div class="stat-row">
  <div><div class="stat-left"><span class="dot" style="background:{COLORS["accent"]};"></span>
    <span class="stat-label">Jami</span></div></div>
  <div class="stat-right">{total:,}</div>
</div>'''
        for _, r in platform_df.iterrows():
            p = r["PLATFORM"]
            u = int(r["USERS"])
            pr = float(r["PERCENT"])
            color = COLORS["android"] if p == "Android" else COLORS["ios"] if p == "iOS" else COLORS["other"]
            legend_html += f'''
<div class="stat-row">
  <div><div class="stat-left"><span class="dot" style="background:{color};"></span>
    <span class="stat-label">{p}</span></div>
    <div class="stat-sub">{pr:.1f}%</div></div>
  <div class="stat-right">{u:,}</div>
</div>'''
        st.markdown(f'<div class="legend-card card" style="background:#FFFFFF;border:1px solid rgba(15,23,42,0.14);border-radius:18px;padding:16px;box-shadow:0 10px 24px rgba(15,23,42,0.06);">{legend_html}</div>', unsafe_allow_html=True)
else:
    st.info("Ma'lumotlar mavjud emas")


# Versions donut
st.markdown("""
<div class="sec-row">
  <div>
    <div class="sec-title">🧩 Versiyalar</div>
    <div class="sec-sub">O'yin versiyasi bo'yicha foydalanuvchilar taqsimoti</div>
  </div>
</div>
""", unsafe_allow_html=True)

versions_df = versions_raw_df
if not versions_df.empty:
    known_rows = versions_df[versions_df["CLIENT_VERSION"] != "UNKNOWN"]
    if not known_rows.empty:
        first_version = known_rows.iloc[0:1].copy()
        last_version  = known_rows.iloc[-1:].copy()
        versions_df = pd.concat([last_version, first_version], ignore_index=True)
        total_v = int(versions_df["USERS"].sum())
        versions_df["PERCENT"] = (versions_df["USERS"] / total_v * 100).round(1)

        palette = ["#2563EB","#7C3AED","#16A34A","#F59E0B","#EF4444","#06B6D4",
                   "#F97316","#0EA5E9","#A855F7","#22C55E","#EAB308","#FB7185"]
        domain = versions_df["CLIENT_VERSION"].tolist()
        color_map = {v: palette[i % len(palette)] for i, v in enumerate(domain)}
        scale = alt.Scale(domain=domain, range=[color_map[v] for v in domain])

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
                .properties(height=300, padding={"top": 18, "left": 8, "right": 8, "bottom": 18})
            )
            st.altair_chart(pie, width="stretch")
        with c_nums:
            legend_html = f'''
<div class="stat-row">
  <div><div class="stat-left"><span class="dot" style="background:{COLORS["accent"]};"></span>
    <span class="stat-label">Jami</span></div></div>
  <div class="stat-right">{total_v:,}</div>
</div>'''
            for _, r in versions_df.iterrows():
                v = r["CLIENT_VERSION"]
                u = int(r["USERS"])
                pr = float(r["PERCENT"])
                dot_color = color_map.get(v, COLORS["other"])
                legend_html += f'''
<div class="stat-row">
  <div><div class="stat-left"><span class="dot" style="background:{dot_color};"></span>
    <span class="stat-label">{v}</span></div>
    <div class="stat-sub">{pr:.1f}%</div></div>
  <div class="stat-right">{u:,}</div>
</div>'''
            st.markdown(f'<div class="legend-card card" style="background:#FFFFFF;border:1px solid rgba(15,23,42,0.14);border-radius:18px;padding:16px;box-shadow:0 10px 24px rgba(15,23,42,0.06);">{legend_html}</div>', unsafe_allow_html=True)
    else:
        st.info("Ma'lumotlar mavjud emas")
else:
    st.info("Ma'lumotlar mavjud emas")


# ----------------------------
# New users
# ----------------------------
left, right = st.columns([1.35, 1], gap="large", vertical_alignment="bottom")
with left:
    st.markdown('<div class="sec-title">👥 Yangi foydalanuvchilar</div><div class="sec-sub">Tanlangan davr bo\'yicha o\'sish dinamikasi</div>', unsafe_allow_html=True)
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
    min_date = datetime(2025, 12, 27).date()
    if start_date < min_date:
        start_date = min_date
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = (end_date + timedelta(days=1)).strftime("%Y-%m-%d")

    try:
        trunc = {"Kunlik": "PLAYER_START_DATE", "Haftalik": "DATE_TRUNC('week', PLAYER_START_DATE)", "Oylik": "DATE_TRUNC('month', PLAYER_START_DATE)"}[period_type]
        new_users_df = run_query(f"""
            SELECT {trunc} as SANA, COUNT(DISTINCT USER_ID) as YANGI_USERS
            FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY
            WHERE GAME_ID = {GAME_ID}
              AND PLAYER_START_DATE >= '{start_str}' AND PLAYER_START_DATE < '{end_str}'
            GROUP BY {trunc}
            ORDER BY SANA
        """)

        if not new_users_df.empty:
            new_users_df["SANA"] = pd.to_datetime(new_users_df["SANA"])
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
            st.altair_chart(chart, width="stretch")
        else:
            st.info("Tanlangan davr uchun ma'lumotlar mavjud emas")
    except Exception as e:
        st.error(f"Yangi foydalanuvchilar xatolik: {e}")


# ----------------------------
# Sessions
# ----------------------------
left, right = st.columns([1.35, 1], gap="large", vertical_alignment="bottom")
with left:
    st.markdown('<div class="sec-title">📈 O\'yin seanslari soni</div><div class="sec-sub">Faollik ko\'rinishi</div>', unsafe_allow_html=True)
with right:
    s1, s2 = st.columns([0.9, 1.1], gap="small")
    with s1:
        session_view = st.selectbox("Ko'rinish", ["Kunlik", "Soatlik"], key="session_view")
    with s2:
        if session_view == "Soatlik":
            session_date = st.date_input("Sana", value=datetime.now().date(), key="session_date")
            session_period = None
        else:
            session_period = st.selectbox("Davr", ["Hammasi", "So'nggi 7 kun", "So'nggi 14 kun", "So'nggi 30 kun"], key="session_period")
            session_date = None

try:
    if session_view == "Soatlik":
        date_str = session_date.strftime("%Y-%m-%d")
        sessions_df = run_query(f"""
            SELECT
                HOUR(DATEADD(hour, 5, EVENT_TIMESTAMP)) as SOAT,
                COUNT(*) as HODISALAR,
                COUNT(DISTINCT USER_ID) as FOYDALANUVCHILAR
            FROM {DB}.ACCOUNT_EVENTS
            WHERE GAME_ID = {GAME_ID}
              AND DATE(EVENT_TIMESTAMP) = '{date_str}'
            GROUP BY HOUR(DATEADD(hour, 5, EVENT_TIMESTAMP))
            ORDER BY SOAT
        """)
        if not sessions_df.empty:
            sessions_df["SOAT"] = pd.to_numeric(sessions_df["SOAT"], errors="coerce").fillna(0).astype(int)
            sessions_df["HODISALAR"] = pd.to_numeric(sessions_df["HODISALAR"], errors="coerce").fillna(0).astype(int)
            sessions_df["FOYDALANUVCHILAR"] = pd.to_numeric(sessions_df["FOYDALANUVCHILAR"], errors="coerce").fillna(0).astype(int)
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
            st.altair_chart(chart, width="stretch")
        else:
            st.info("Tanlangan sana uchun ma'lumotlar mavjud emas")
    else:
        if session_period == "Hammasi":
            s_start = datetime(2025, 12, 27)
            s_end = datetime.now()
        else:
            days_map = {"So'nggi 7 kun": 7, "So'nggi 14 kun": 14, "So'nggi 30 kun": 30}
            s_end = datetime.now()
            s_start = s_end - timedelta(days=days_map[session_period])

        sessions_df = run_query(f"""
            SELECT
                EVENT_DATE as SANA,
                COUNT(DISTINCT SESSION_ID) as SESSIYALAR,
                ROUND(AVG(TOTAL_TIME_MS) / 60000, 1) as ORTACHA_DAVOMIYLIK
            FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY
            WHERE GAME_ID = {GAME_ID}
              AND EVENT_DATE BETWEEN '{s_start.strftime("%Y-%m-%d")}' AND '{s_end.strftime("%Y-%m-%d")}'
            GROUP BY EVENT_DATE
            ORDER BY EVENT_DATE
        """)
        if not sessions_df.empty:
            m1, m2, m3 = st.columns(3)
            m1.metric("Jami", f"{int(sessions_df['SESSIYALAR'].sum()):,}")
            m2.metric("O'rtacha kunlik", f"{int(sessions_df['SESSIYALAR'].mean()):,}")
            m3.metric("O'rtacha o'yin davomiyligi", f"{round(float(sessions_df['ORTACHA_DAVOMIYLIK'].mean()), 1)} daq")
            sessions_df["SANA"] = pd.to_datetime(sessions_df["SANA"])
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
            st.altair_chart(chart, width="stretch")
        else:
            st.info("Ma'lumotlar mavjud emas")
except Exception as e:
    st.error(f"Sessiyalar xatolik: {e}")


# ----------------------------
# DAU Trend
# ----------------------------
left, right = st.columns([1.35, 1], gap="large", vertical_alignment="bottom")
with left:
    st.markdown('<div class="sec-title">📊 Kunlik faol foydalanuvchilar (DAU)</div><div class="sec-sub">Har kungi unikal foydalanuvchilar soni</div>', unsafe_allow_html=True)
with right:
    dau_period = st.selectbox("Davr", ["So'nggi 7 kun", "So'nggi 14 kun", "So'nggi 30 kun", "So'nggi 90 kun"], key="dau_period")

try:
    dau_days_map = {"So'nggi 7 kun": 7, "So'nggi 14 kun": 14, "So'nggi 30 kun": 30, "So'nggi 90 kun": 90}
    dau_end = datetime.now() - timedelta(days=1)
    dau_start = datetime(2025, 12, 27) if dau_period == "So'nggi 90 kun" else dau_end - timedelta(days=dau_days_map[dau_period])

    dau_trend_df = run_query(f"""
        SELECT EVENT_DATE as SANA, COUNT(DISTINCT USER_ID) as DAU
        FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY
        WHERE GAME_ID = {GAME_ID}
          AND EVENT_DATE BETWEEN '{dau_start.strftime("%Y-%m-%d")}' AND '{dau_end.strftime("%Y-%m-%d")}'
        GROUP BY EVENT_DATE
        ORDER BY EVENT_DATE
    """)
    if not dau_trend_df.empty:
        dau_trend_df["SANA"] = pd.to_datetime(dau_trend_df["SANA"])
        dau_trend_df["SANA_STR"] = dau_trend_df["SANA"].dt.strftime("%Y-%m-%d")
        m1, m2, m3 = st.columns(3)
        m1.metric("O'rtacha DAU", f"{int(dau_trend_df['DAU'].mean()):,}")
        m2.metric("Eng yuqori", f"{int(dau_trend_df['DAU'].max()):,}")
        m3.metric("Eng past", f"{int(dau_trend_df['DAU'].min()):,}")
        tick_count = 5 if dau_period == "So'nggi 90 kun" else 7
        dau_area = (alt.Chart(dau_trend_df)
            .mark_area(color=COLORS["sessions"], opacity=0.2, line=False)
            .encode(
                x=alt.X("SANA:T", title="", axis=alt.Axis(format="%Y-%m-%d", labelAngle=-30, tickCount=tick_count, labelFontWeight=600)),
                y=alt.Y("DAU:Q", title="", axis=alt.Axis(labelFontWeight=600)),
            ))
        dau_line = (alt.Chart(dau_trend_df)
            .mark_line(color=COLORS["sessions"], strokeWidth=2.6, opacity=0.9)
            .encode(
                x=alt.X("SANA:T", title="", axis=alt.Axis(format="%Y-%m-%d", labelAngle=-30, tickCount=tick_count, labelFontWeight=600)),
                y=alt.Y("DAU:Q", title="", axis=alt.Axis(labelFontWeight=600)),
                tooltip=[alt.Tooltip("SANA:T", title="Sana", format="%Y-%m-%d"), alt.Tooltip("DAU:Q", title="DAU", format=",")],
            ))
        dau_pts = (alt.Chart(dau_trend_df).mark_circle(size=60, color=COLORS["sessions"], opacity=0.85).encode(x="SANA:T", y="DAU:Q"))
        st.altair_chart((dau_area + dau_line + dau_pts).properties(height=320, padding={"top": 18, "left": 8, "right": 8, "bottom": 8}), use_container_width=True)
    else:
        st.info("Ma'lumotlar mavjud emas")
except Exception as e:
    st.error(f"DAU trend xatolik: {e}")


# ----------------------------
# MAU Trend
# ----------------------------
left, right = st.columns([1.35, 1], gap="large", vertical_alignment="bottom")
with left:
    st.markdown('<div class="sec-title">📅 Oylik faol foydalanuvchilar (MAU)</div><div class="sec-sub">Har oylik unikal foydalanuvchilar soni</div>', unsafe_allow_html=True)
with right:
    mau_period = st.selectbox("Davr", ["So'nggi 6 oy", "So'nggi 12 oy"], key="mau_period")

try:
    mau_months_map = {"So'nggi 6 oy": 6, "So'nggi 12 oy": 12}
    mau_end = datetime.now()
    mau_start = max(mau_end - timedelta(days=mau_months_map[mau_period] * 30), datetime(2025, 12, 27))

    mau_trend_df = run_query(f"""
        SELECT DATE_TRUNC('month', EVENT_DATE) as OY, COUNT(DISTINCT USER_ID) as MAU
        FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY
        WHERE GAME_ID = {GAME_ID}
          AND EVENT_DATE BETWEEN '{mau_start.strftime("%Y-%m-%d")}' AND '{mau_end.strftime("%Y-%m-%d")}'
        GROUP BY DATE_TRUNC('month', EVENT_DATE)
        ORDER BY OY
    """)
    if not mau_trend_df.empty:
        mau_trend_df["OY"] = pd.to_datetime(mau_trend_df["OY"])
        MONTHS_UZ = {1:"Yanvar",2:"Fevral",3:"Mart",4:"Aprel",5:"May",6:"Iyun",
                     7:"Iyul",8:"Avgust",9:"Sentabr",10:"Oktabr",11:"Noyabr",12:"Dekabr"}
        mau_trend_df["OY_LABEL"] = mau_trend_df["OY"].apply(lambda d: f"{d.year}-{MONTHS_UZ[d.month]}")
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
                tooltip=[alt.Tooltip("OY_LABEL:O", title="Yil-Oy"), alt.Tooltip("MAU:Q", title="MAU", format=",")],
            )
            .properties(height=320, padding={"top": 18, "left": 8, "right": 8, "bottom": 8})
        )
        st.altair_chart(mau_chart, use_container_width=True)
    else:
        st.info("Ma'lumotlar mavjud emas")
except Exception as e:
    st.error(f"MAU trend xatolik: {e}")


# ----------------------------
# Mini games trends + Top 5 — fetched in parallel
# ----------------------------
@st.cache_data(ttl=1800, show_spinner=False)
def load_minigame_data():
    def _mg_list():
        return run_query(f"""
            SELECT DISTINCT EVENT_JSON:MiniGameName::STRING as MINI_GAME
            FROM {DB}.ACCOUNT_EVENTS
            WHERE GAME_ID = {GAME_ID} AND EVENT_NAME = 'playedMiniGameStatus'
              AND EVENT_JSON:MiniGameName::STRING IS NOT NULL
        """)
    def _top5():
        return run_query(f"""
            SELECT EVENT_JSON:MiniGameName::STRING as MINI_GAME, COUNT(*) as OYINLAR
            FROM {DB}.ACCOUNT_EVENTS
            WHERE GAME_ID = {GAME_ID} AND EVENT_NAME = 'playedMiniGameStatus'
              AND EVENT_JSON:MiniGameName::STRING IS NOT NULL
            GROUP BY EVENT_JSON:MiniGameName::STRING
            ORDER BY OYINLAR DESC
            LIMIT 5
        """)
    with ThreadPoolExecutor(max_workers=2) as pool:
        f_list = pool.submit(_mg_list)
        f_top  = pool.submit(_top5)
        return f_list.result(), f_top.result()

mg_list_df, top_games = load_minigame_data()

# Mini games trend
left, right = st.columns([1.35, 1], gap="large", vertical_alignment="bottom")
with left:
    st.markdown('<div class="sec-title">🎮 Mini o\'yinlar trendi</div><div class="sec-sub">Tanlangan mini-o\'yin va davr bo\'yicha o\'yinga kirishlar soni</div>', unsafe_allow_html=True)
with right:
    m1, m2 = st.columns([1.2, 1], gap="small")
    with m1:
        st.markdown('<div style="font-size: 14px; font-weight: 500; margin-bottom: 4px;">Davr</div>', unsafe_allow_html=True)
        mg_date_range = st.date_input(
            "Sana oralig'i",
            value=(datetime.now().date() - timedelta(days=30), datetime.now().date()),
            key="mg_date",
            label_visibility="collapsed",
        )
    with m2:
        st.markdown('<div style="font-size: 14px; font-weight: 500; margin-bottom: 4px;">Mini o\'yin</div>', unsafe_allow_html=True)
        mg_options = ["Barchasi"] + [get_minigame_name(mg) for mg in mg_list_df["MINI_GAME"].tolist() if mg]
        mg_original = {get_minigame_name(mg): mg for mg in mg_list_df["MINI_GAME"].tolist() if mg}
        selected_mg = st.selectbox("Mini o'yin", mg_options, key="mg_filter", label_visibility="collapsed")

if len(mg_date_range) == 2:
    mg_start, mg_end = mg_date_range
    min_date = datetime(2025, 12, 27).date()
    if mg_start < min_date:
        mg_start = min_date
    mg_start_str = mg_start.strftime("%Y-%m-%d")
    mg_end_str = (mg_end + timedelta(days=1)).strftime("%Y-%m-%d")

    try:
        if selected_mg == "Barchasi":
            mg_stats = run_query(f"""
                SELECT DATE(EVENT_TIMESTAMP) as SANA, COUNT(*) as OYINLAR
                FROM {DB}.ACCOUNT_EVENTS
                WHERE GAME_ID = {GAME_ID} AND EVENT_NAME = 'playedMiniGameStatus'
                  AND EVENT_TIMESTAMP >= '{mg_start_str}' AND EVENT_TIMESTAMP < '{mg_end_str}'
                GROUP BY DATE(EVENT_TIMESTAMP)
                ORDER BY SANA
            """)
        else:
            original_name = mg_original.get(selected_mg, selected_mg)
            mg_stats = run_query(f"""
                SELECT DATE(EVENT_TIMESTAMP) as SANA, COUNT(*) as OYINLAR
                FROM {DB}.ACCOUNT_EVENTS
                WHERE GAME_ID = {GAME_ID} AND EVENT_NAME = 'playedMiniGameStatus'
                  AND EVENT_JSON:MiniGameName::STRING = '{original_name}'
                  AND EVENT_TIMESTAMP >= '{mg_start_str}' AND EVENT_TIMESTAMP < '{mg_end_str}'
                GROUP BY DATE(EVENT_TIMESTAMP)
                ORDER BY SANA
            """)

        if not mg_stats.empty:
            mg_stats["SANA"] = pd.to_datetime(mg_stats["SANA"])
            area = (alt.Chart(mg_stats).mark_area(color=COLORS["minigame"], opacity=0.2, line=False)
                .encode(x=alt.X("SANA:T", title="", axis=alt.Axis(format="%Y-%m-%d", labelAngle=-30, tickCount=5, labelFontWeight=600)),
                        y=alt.Y("OYINLAR:Q", title="", axis=alt.Axis(labelFontWeight=600))))
            line = (alt.Chart(mg_stats).mark_line(color=COLORS["minigame"], strokeWidth=2.6, opacity=0.9)
                .encode(x=alt.X("SANA:T", title="", axis=alt.Axis(format="%Y-%m-%d", labelAngle=-30, tickCount=10, labelFontWeight=600)),
                        y=alt.Y("OYINLAR:Q", title="", axis=alt.Axis(labelFontWeight=600)),
                        tooltip=[alt.Tooltip("SANA:T", title="Sana", format="%Y-%m-%d"), alt.Tooltip("OYINLAR:Q", title="O'yinlar", format=",")]))
            pts = (alt.Chart(mg_stats).mark_circle(size=60, color=COLORS["minigame"], opacity=0.85).encode(x="SANA:T", y="OYINLAR:Q"))
            st.altair_chart((area + line + pts).properties(height=320, padding={"top": 18, "left": 8, "right": 8, "bottom": 8}), width="stretch")
        else:
            st.info("Tanlangan davr uchun ma'lumotlar mavjud emas")
    except Exception as e:
        st.error(f"Mini oyinlar trendi xatolik: {e}")


# Top 5 mini-games
st.markdown("""
<div class="sec-row">
  <div>
    <div class="sec-title">🏆 TOP 5 mini o'yin</div>
    <div class="sec-sub">Eng ko'p o'ynalganlar</div>
  </div>
</div>
""", unsafe_allow_html=True)

if not top_games.empty:
    top_games["NOMI"] = top_games["MINI_GAME"].apply(get_minigame_name)
    medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"]
    rows_html = ""
    for i, row in top_games.reset_index(drop=True).iterrows():
        medal = medals[i] if i < len(medals) else f"#{i+1}"
        rows_html += f'''
<div class="rank-row">
  <div class="rank-badge">{medal}</div>
  <div class="rank-name">{row["NOMI"]}</div>
  <div class="rank-val">{int(row["OYINLAR"]):,}</div>
</div>'''
    st.markdown(f'<div class="rank-card card" style="margin-bottom: 16px;">{rows_html}</div>', unsafe_allow_html=True)
    chart = (
        alt.Chart(top_games)
        .mark_bar(color=COLORS["purple"], cornerRadiusTopRight=8, cornerRadiusBottomRight=8, size=34, opacity=0.92)
        .encode(
            x=alt.X("OYINLAR:Q", title="", axis=alt.Axis(labelFontWeight=600)),
            y=alt.Y("NOMI:N", title="", sort="-x", axis=alt.Axis(labelFontWeight=600)),
            tooltip=[alt.Tooltip("NOMI:N", title="O'yin"), alt.Tooltip("OYINLAR:Q", title="O'ynalishlar", format=",")],
        )
        .properties(height=290, padding={"top": 18, "left": 8, "right": 8, "bottom": 8})
    )
    st.altair_chart(chart, width="stretch")
else:
    st.info("Ma'lumotlar mavjud emas")


# ----------------------------
# Retention — 3 parallel queries
# ----------------------------
st.markdown("""
<div class="sec-row">
  <div>
    <div class="sec-title">🔄 Saqlanib qolish darajasi</div>
    <div class="sec-sub">Ma'lum kundan keyin ilovaga qaytgan foydalanuvchilar foizi</div>
  </div>
</div>
""", unsafe_allow_html=True)

@st.cache_data(ttl=3600, show_spinner=False)  # retention changes slowly — cache 1h
def load_retention():
    def _ret(day: int):
        try:
            df = run_query(f"""
                WITH first_day AS (
                    SELECT USER_ID, MIN(EVENT_DATE) as first_date
                    FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY
                    WHERE GAME_ID = {GAME_ID}
                    GROUP BY USER_ID
                ),
                returned AS (
                    SELECT f.USER_ID
                    FROM first_day f
                    JOIN {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY s
                      ON f.USER_ID = s.USER_ID
                     AND s.EVENT_DATE = DATEADD(day, {day}, f.first_date)
                     AND s.GAME_ID = {GAME_ID}
                )
                SELECT ROUND(COUNT(DISTINCT r.USER_ID) * 100.0 / NULLIF(COUNT(DISTINCT f.USER_ID), 0), 1) as RET
                FROM first_day f
                LEFT JOIN returned r ON f.USER_ID = r.USER_ID
            """)
            return float(df['RET'][0] or 0.0)
        except Exception:
            return None

    with ThreadPoolExecutor(max_workers=3) as pool:
        f1  = pool.submit(_ret, 1)
        f7  = pool.submit(_ret, 7)
        f30 = pool.submit(_ret, 30)
        return f1.result(), f7.result(), f30.result()

ret1, ret7, ret30 = load_retention()
c1, c2, c3 = st.columns(3)
c1.metric("1-kun",  f"{ret1}%"  if ret1  is not None else "N/A")
c2.metric("7-kun",  f"{ret7}%"  if ret7  is not None else "N/A")
c3.metric("30-kun", f"{ret30}%" if ret30 is not None else "N/A")
