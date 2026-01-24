import streamlit as st
import pandas as pd
import altair as alt
from datetime import datetime, timedelta
from decimal import Decimal
import snowflake.connector
import base64
from pathlib import Path

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

    "new_users": "#F59E0B",   # soft orange
    "sessions": "#2563EB",    # blue
    "minigame": "#EF4444",    # red
    "purple": "#7C3AED",

    "neon": "rgba(37,99,235,0.16)",
    "neon2": "rgba(124,58,237,0.12)",
}

# Load local logo as base64
def get_logo_base64():
    logo_path = Path(__file__).parent / "images" / "Beklola.png"
    with open(logo_path, "rb") as f:
        return base64.b64encode(f.read()).decode()

LOGO_BASE64 = get_logo_base64() 


# ----------------------------
# Altair clean light theme (transparent background; Streamlit card shows bg)
# ----------------------------
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
alt.themes.enable("clean_light")


# ----------------------------
# CSS (Mutolaa-like clean light + FIX all issues)
# ----------------------------
st.markdown(
    f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

html, body, [class*="css"] {{
  font-family: "Inter", system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif !important;
  color: {COLORS["text"]} !important;
  font-weight: 400 !important;
}}

.stApp {{
  background: {COLORS["bg"]} !important;
  color: {COLORS["text"]} !important;
}}

.block-container {{
  max-width: 1320px;
  padding: 0.7rem 1.6rem 2rem 1.6rem;
}}

#MainMenu, footer {{ visibility: hidden; }}

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

/* ===============================
   FIX 1: SELECT DROPDOWNS - Make text visible
   =============================== */
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

/* Dropdown menu */
ul[role="listbox"] {{
  background: #FFFFFF !important;
  border: 1px solid rgba(15,23,42,0.14) !important;
  border-radius: 12px !important;
}}
ul[role="listbox"] li {{
  color: #0F172A !important;
}}
ul[role="listbox"] li:hover {{
  background: rgba(37,99,235,0.08) !important;
}}

/* ✅ Fix: selected / highlighted option background (remove black strip) */
ul[role="listbox"] li[aria-selected="true"],
div[role="option"][aria-selected="true"]{{
  background: rgba(37,99,235,0.10) !important;
  color: #0F172A !important;
}}
ul[role="listbox"] li[aria-selected="true"] *,
div[role="option"][aria-selected="true"] *{{
  color: #0F172A !important;
}}

/* highlighted item while moving with mouse/keyboard */
ul[role="listbox"] li[data-highlighted="true"],
div[role="option"][data-highlighted="true"]{{
  background: rgba(37,99,235,0.08) !important;
  color: #0F172A !important;
}}


/* ===============================
   FIX 2: DATEPICKER - Calendar stays dark with white numbers
   =============================== */

/* Date input field */
[data-baseweb="datepicker"] > div,
.stDateInput > div > div {{
  background-color: #FFFFFF !important;
  border: 1px solid rgba(15,23,42,0.18) !important;
  border-radius: 12px !important;
  box-shadow: none !important;
}}

/* Date input text - BLACK when selected */
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

/* ========= CALENDAR POPUP - DARK BACKGROUND, WHITE TEXT ========= */

/* Calendar popup container - DARK */
[data-baseweb="calendar"],
div[role="dialog"],
div[aria-label="Calendar"] {{
  background: #1E293B !important;
  color: #FFFFFF !important;
  border: 1px solid rgba(255,255,255,0.1) !important;
  border-radius: 14px !important;
  box-shadow: 0 12px 28px rgba(0,0,0,0.3) !important;
}}

/* Calendar header - DARK */
[data-baseweb="calendar"] header {{
  background: #1E293B !important;
  color: #FFFFFF !important;
}}

/* Month/Year dropdowns in header - DARK */
[data-baseweb="calendar"] [data-baseweb="select"] > div {{
  background: #334155 !important;
  border-color: rgba(255,255,255,0.1) !important;
}}

[data-baseweb="calendar"] [data-baseweb="select"] * {{
  color: #FFFFFF !important;
}}

/* Navigation arrows - WHITE */
[data-baseweb="calendar"] button[aria-label*="previous"],
[data-baseweb="calendar"] button[aria-label*="next"] {{
  color: #FFFFFF !important;
}}

[data-baseweb="calendar"] button svg {{
  color: #FFFFFF !important;
  fill: #FFFFFF !important;
}}

/* Weekday labels - WHITE */
[data-baseweb="calendar"] [role="row"] span,
[data-baseweb="calendar"] th {{
  color: #94A3B8 !important;
  font-weight: 500 !important;
}}

/* Day numbers - WHITE */
[data-baseweb="calendar"] td button,
[data-baseweb="calendar"] [role="button"] {{
  background: transparent !important;
  color: #FFFFFF !important;
  font-weight: 500 !important;
}}

/* Hover state - lighter background */
[data-baseweb="calendar"] td button:hover,
[data-baseweb="calendar"] [role="button"]:hover {{
  background: rgba(255,255,255,0.1) !important;
  color: #FFFFFF !important;
}}

/* Selected day - BLUE with WHITE text */
[data-baseweb="calendar"] td button[aria-selected="true"],
[data-baseweb="calendar"] [role="button"][aria-selected="true"] {{
  background: #2563EB !important;
  color: #FFFFFF !important;
  border-radius: 999px !important;
}}

/* Today's date - outlined */
[data-baseweb="calendar"] td button[aria-label*="today"],
[data-baseweb="calendar"] [role="button"][aria-current="date"] {{
  border: 2px solid #3B82F6 !important;
  color: #FFFFFF !important;
}}

/* Disabled days - gray */
[data-baseweb="calendar"] td button:disabled,
[data-baseweb="calendar"] [role="button"]:disabled {{
  color: #475569 !important;
  opacity: 0.5 !important;
}}

/* Date range display - BLACK */
.stDateInput [data-testid="stMarkdownContainer"] {{
  color: #0F172A !important;
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
  gap: 14px;
  margin: 6px 0 24px 0;
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

/* ---------- KPI ---------- */
.kpi-grid {{
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 12px;
  margin-bottom: 14px;
}}
.kpi {{
  padding: 14px;
}}
.kpi-head {{
  display:flex;
  align-items:center;
  gap:10px;
  margin-bottom: 8px;
}}
.kpi-ico {{
  width: 36px;
  height: 36px;
  border-radius: 12px;
  display:flex;
  align-items:center;
  justify-content:center;
  font-size: 18px;
  background: rgba(37,99,235,0.10);
  color: #2563EB;
}}
.kpi-ico.purple {{ background: rgba(124,58,237,0.10); color:#7C3AED; }}
.kpi-ico.orange {{ background: rgba(245,158,11,0.12); color:#F59E0B; }}
.kpi-ico.green {{ background: rgba(22,163,74,0.10); color:#16A34A; }}

.kpi-label {{
  font-size: 0.95rem;
  color: {COLORS["muted"]};
  font-weight: 500;
}}
.kpi-value {{
  font-size: 2.0rem;
  font-weight: 650;
  letter-spacing: -0.02em;
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
    "HiddeAndSikLolaRoom": "Berkinmachoq",
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
# Snowflake (logic unchanged)
# ----------------------------
@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        user=st.secrets["snowflake"]["user"],
        password=st.secrets["snowflake"]["password"],
        account=st.secrets["snowflake"]["account"],
        warehouse=st.secrets["snowflake"]["warehouse"],
        database=st.secrets["snowflake"]["database"],
        schema=st.secrets["snowflake"]["schema"],
    )

def run_query(query: str) -> pd.DataFrame:
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

GAME_ID = 181330318
DB = "UNITY_ANALYTICS_GCP_US_CENTRAL1_UNITY_ANALYTICS_PDA.SHARES"


# ----------------------------
# Header (centered logo + title)
# ----------------------------
# ----------------------------
# Header (centered logo + title)
# ----------------------------
st.markdown(f'''
<div class="header">
    <img src="data:image/png;base64,{LOGO_BASE64}" style="height:60px;width:auto;" />
</div>
''', unsafe_allow_html=True)


# ----------------------------
# KPI (3 cards)
# ----------------------------
try:
    total_users = run_query(f"""
        SELECT COUNT(DISTINCT USER_ID) as TOTAL
        FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY
        WHERE GAME_ID = {GAME_ID}
    """)
    kpi_total_users = int(total_users["TOTAL"][0])
except Exception:
    kpi_total_users = None

# Defaults for initial view
default_start = datetime.now() - timedelta(days=30)
default_end = datetime.now()

try:
    new_users_total_df = run_query(f"""
        SELECT COUNT(DISTINCT USER_ID) as TOTAL_NEW
        FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY
        WHERE GAME_ID = {GAME_ID}
        AND PLAYER_START_DATE BETWEEN '{default_start.strftime("%Y-%m-%d")}' AND '{default_end.strftime("%Y-%m-%d")}'
    """)
    kpi_new_users = int(new_users_total_df["TOTAL_NEW"][0])
except Exception:
    kpi_new_users = None

try:
    end_dt = datetime.now()
    start_dt = end_dt - timedelta(days=7)
    sess_kpi_df = run_query(f"""
        SELECT COUNT(DISTINCT SESSION_ID) as TOTAL_SESS
        FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY
        WHERE GAME_ID = {GAME_ID}
        AND EVENT_DATE BETWEEN '{start_dt.strftime("%Y-%m-%d")}' AND '{end_dt.strftime("%Y-%m-%d")}'
    """)
    kpi_sessions = int(sess_kpi_df["TOTAL_SESS"][0])
except Exception:
    kpi_sessions = None

st.markdown(
    f"""
<div class="kpi-grid">
  <div class="kpi card">
    <div class="kpi-head">
      <div class="kpi-ico green">👥</div>
      <div class="kpi-label">Foydalanuvchilar</div>
    </div>
    <div class="kpi-value">{f"{kpi_total_users:,}" if kpi_total_users is not None else "N/A"}</div>
  </div>

  <div class="kpi card">
    <div class="kpi-head">
      <div class="kpi-ico orange">✨</div>
      <div class="kpi-label">Yangi foydalanuvchilar</div>
    </div>
    <div class="kpi-value">{f"{kpi_new_users:,}" if kpi_new_users is not None else "N/A"}</div>
  </div>

  <div class="kpi card">
    <div class="kpi-head">
      <div class="kpi-ico">📈</div>
      <div class="kpi-label">Sessiyalar</div>
    </div>
    <div class="kpi-value">{f"{kpi_sessions:,}" if kpi_sessions is not None else "N/A"}</div>
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
    platform_df = run_query(f"""
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
                .properties(height=CHART_H, padding={"top": 6, "left": 8, "right": 8, "bottom": 8})
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
except Exception:
    st.info("Ma'lumotlar mavjud emas")


# ----------------------------
# 2) New users
# ----------------------------
left, right = st.columns([1.35, 1], gap="large", vertical_alignment="bottom")
with left:
    st.markdown('<div class="sec-title">👥 Yangi foydalanuvchilar</div><div class="sec-sub">Tanlangan davr boyicha trend</div>', unsafe_allow_html=True)
with right:
    f1, f2 = st.columns([0.9, 1.1], gap="small")
    with f1:
        period_type = st.selectbox("Kesim", ["Kunlik", "Haftalik", "Oylik"], key="new_users_period")
    with f2:
        date_range = st.date_input(
            "Sana oralig'i",
            value=(datetime.now() - timedelta(days=30), datetime.now()),
            key="new_users_date",
        )

if len(date_range) == 2:
    start_date, end_date = date_range
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    try:
        if period_type == "Kunlik":
            new_users_df = run_query(f"""
                SELECT
                    PLAYER_START_DATE as SANA,
                    COUNT(DISTINCT USER_ID) as YANGI_USERS
                FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY
                WHERE GAME_ID = {GAME_ID}
                AND PLAYER_START_DATE BETWEEN '{start_str}' AND '{end_str}'
                GROUP BY PLAYER_START_DATE
                ORDER BY PLAYER_START_DATE
            """)
        elif period_type == "Haftalik":
            new_users_df = run_query(f"""
                SELECT
                    DATE_TRUNC('week', PLAYER_START_DATE) as SANA,
                    COUNT(DISTINCT USER_ID) as YANGI_USERS
                FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY
                WHERE GAME_ID = {GAME_ID}
                AND PLAYER_START_DATE BETWEEN '{start_str}' AND '{end_str}'
                GROUP BY DATE_TRUNC('week', PLAYER_START_DATE)
                ORDER BY SANA
            """)
        else:
            new_users_df = run_query(f"""
                SELECT
                    DATE_TRUNC('month', PLAYER_START_DATE) as SANA,
                    COUNT(DISTINCT USER_ID) as YANGI_USERS
                FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY
                WHERE GAME_ID = {GAME_ID}
                AND PLAYER_START_DATE BETWEEN '{start_str}' AND '{end_str}'
                GROUP BY DATE_TRUNC('month', PLAYER_START_DATE)
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
                    x=alt.X("SANA_STR:O", title="", axis=alt.Axis(labelAngle=-30), sort=None),
                    y=alt.Y("YANGI_USERS:Q", title=""),
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
    except Exception:
        st.info("Ma'lumotlarni yuklashda xatolik")


# ----------------------------
# 3) Sessions
# ----------------------------
left, right = st.columns([1.35, 1], gap="large", vertical_alignment="bottom")
with left:
    st.markdown('<div class="sec-title">📈 Sessiyalar</div><div class="sec-sub">Faollik korinishi</div>', unsafe_allow_html=True)
with right:
    s1, s2 = st.columns([0.9, 1.1], gap="small")
    with s1:
        session_view = st.selectbox("Ko'rinish", ["Kunlik", "Soatlik"], key="session_view")
    with s2:
        if session_view == "Soatlik":
            session_date = st.date_input("Sana", value=datetime.now(), key="session_date")
            session_period = None
        else:
            session_period = st.selectbox(
                "Davr",
                ["So'nggi 7 kun", "So'nggi 14 kun", "So'nggi 30 kun"],
                key="session_period",
            )
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
            m1.metric("Hodisalar", f"{int(sessions_df['HODISALAR'].sum()):,}")
            m2.metric("Faol foydalanuvchilar", f"{int(sessions_df['FOYDALANUVCHILAR'].sum()):,}")

            chart = (
                alt.Chart(sessions_df)
                .mark_bar(color=COLORS["sessions"], cornerRadiusTopLeft=6, cornerRadiusTopRight=6, opacity=0.92)
                .encode(
                    x=alt.X("SOAT_LABEL:N", title="", sort=None, axis=alt.Axis(labelAngle=0)),
                    y=alt.Y("HODISALAR:Q", title=""),
                    tooltip=[
                        alt.Tooltip("SOAT_LABEL:N", title="Soat"),
                        alt.Tooltip("HODISALAR:Q", title="Hodisalar", format=","),
                        alt.Tooltip("FOYDALANUVCHILAR:Q", title="Foydalanuvchilar", format=","),
                    ],
                )
                .properties(height=320, padding={"top": 18, "left": 8, "right": 8, "bottom": 8})
            )
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("Tanlangan sana uchun ma'lumotlar mavjud emas")
    else:
        days_map = {"So'nggi 7 kun": 7, "So'nggi 14 kun": 14, "So'nggi 30 kun": 30}
        days = days_map[session_period]
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)

        sessions_df = run_query(f"""
            SELECT
                EVENT_DATE as SANA,
                COUNT(DISTINCT SESSION_ID) as SESSIYALAR,
                ROUND(AVG(TOTAL_TIME_MS) / 60000, 1) as ORTACHA_DAVOMIYLIK
            FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY
            WHERE GAME_ID = {GAME_ID}
            AND EVENT_DATE BETWEEN '{start_date.strftime("%Y-%m-%d")}' AND '{end_date.strftime("%Y-%m-%d")}'
            GROUP BY EVENT_DATE
            ORDER BY EVENT_DATE
        """)

        if not sessions_df.empty:
            m1, m2, m3 = st.columns(3)
            m1.metric("Jami", f"{int(sessions_df['SESSIYALAR'].sum()):,}")
            m2.metric("O'rtacha kunlik", f"{int(sessions_df['SESSIYALAR'].mean()):,}")
            m3.metric("O'rtacha vaqt (daq)", f"{round(float(sessions_df['ORTACHA_DAVOMIYLIK'].mean()), 1)}")

            sessions_df["SANA"] = pd.to_datetime(sessions_df["SANA"])
            sessions_df["SANA_STR"] = sessions_df["SANA"].dt.strftime("%Y-%m-%d")

            chart = (
                alt.Chart(sessions_df)
                .mark_bar(color=COLORS["sessions"], cornerRadiusTopLeft=6, cornerRadiusTopRight=6, opacity=0.92)
                .encode(
                    x=alt.X("SANA_STR:O", title="", axis=alt.Axis(labelAngle=-30), sort=None),
                    y=alt.Y("SESSIYALAR:Q", title=""),
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
except Exception:
    st.info("Ma'lumotlarni yuklashda xatolik")


# ----------------------------
# 4) Mini-game trend
# ----------------------------
left, right = st.columns([1.35, 1], gap="large", vertical_alignment="bottom")
with left:
    st.markdown('<div class="sec-title">🎮 Mini oyinlar trendi</div><div class="sec-sub">Tanlangan davr boyicha</div>', unsafe_allow_html=True)
with right:
    m1, m2 = st.columns([1.2, 1], gap="small")
    with m1:
        mg_date_range = st.date_input(
            "Sana oralig'i",
            value=(datetime.now() - timedelta(days=30), datetime.now()),
            key="mg_date",
        )
    with m2:
        try:
            mg_list = run_query(f"""
                SELECT DISTINCT EVENT_JSON:MiniGameName::STRING as MINI_GAME
                FROM {DB}.ACCOUNT_EVENTS
                WHERE GAME_ID = {GAME_ID} AND EVENT_NAME = 'playedMiniGameStatus'
                AND EVENT_JSON:MiniGameName::STRING IS NOT NULL
            """)
            mg_options = ["Barchasi"] + [get_minigame_name(mg) for mg in mg_list["MINI_GAME"].tolist() if mg]
            mg_original = {get_minigame_name(mg): mg for mg in mg_list["MINI_GAME"].tolist() if mg}
            selected_mg = st.selectbox("Mini o'yin", mg_options, key="mg_filter")
        except Exception:
            selected_mg = "Barchasi"
            mg_original = {}

if len(mg_date_range) == 2:
    mg_start, mg_end = mg_date_range
    mg_start_str = mg_start.strftime("%Y-%m-%d")
    mg_end_str = mg_end.strftime("%Y-%m-%d")

    try:
        if selected_mg == "Barchasi":
            mg_stats = run_query(f"""
                SELECT
                    DATE(EVENT_TIMESTAMP) as SANA,
                    COUNT(*) as OYINLAR
                FROM {DB}.ACCOUNT_EVENTS
                WHERE GAME_ID = {GAME_ID}
                AND EVENT_NAME = 'playedMiniGameStatus'
                AND EVENT_TIMESTAMP BETWEEN '{mg_start_str}' AND '{mg_end_str}'
                GROUP BY DATE(EVENT_TIMESTAMP)
                ORDER BY SANA
            """)
        else:
            original_name = mg_original.get(selected_mg, selected_mg)
            mg_stats = run_query(f"""
                SELECT
                    DATE(EVENT_TIMESTAMP) as SANA,
                    COUNT(*) as OYINLAR
                FROM {DB}.ACCOUNT_EVENTS
                WHERE GAME_ID = {GAME_ID}
                AND EVENT_NAME = 'playedMiniGameStatus'
                AND EVENT_JSON:MiniGameName::STRING = '{original_name}'
                AND EVENT_TIMESTAMP BETWEEN '{mg_start_str}' AND '{mg_end_str}'
                GROUP BY DATE(EVENT_TIMESTAMP)
                ORDER BY SANA
            """)

        if not mg_stats.empty:
            mg_stats["SANA"] = pd.to_datetime(mg_stats["SANA"])

            # Shaded area
            area = (
                alt.Chart(mg_stats)
                .mark_area(
                    color=COLORS["minigame"],
                    opacity=0.2,
                    line=False
                )
                .encode(
                    x=alt.X("SANA:T", title="", axis=alt.Axis(format="%Y-%m-%d", labelAngle=-30, tickCount=10)),
                    y=alt.Y("OYINLAR:Q", title=""),
                )
            )
            
            # Line
            line = (
                alt.Chart(mg_stats)
                .mark_line(color=COLORS["minigame"], strokeWidth=2.6, opacity=0.9)
                .encode(
                    x=alt.X("SANA:T", title="", axis=alt.Axis(format="%Y-%m-%d", labelAngle=-30, tickCount=10)),
                    y=alt.Y("OYINLAR:Q", title=""),
                    tooltip=[
                        alt.Tooltip("SANA:T", title="Sana", format="%Y-%m-%d"),
                        alt.Tooltip("OYINLAR:Q", title="O'yinlar", format=","),
                    ],
                )
            )
            
            # Points
            points = (
                alt.Chart(mg_stats)
                .mark_circle(size=60, color=COLORS["minigame"], opacity=0.85)
                .encode(x="SANA:T", y="OYINLAR:Q")
            )

            st.altair_chart((area + line + points).properties(height=320, padding={"top": 18, "left": 8, "right": 8, "bottom": 8}), use_container_width=True)
        else:
            st.info("Tanlangan davr uchun ma'lumotlar mavjud emas")
    except Exception:
        st.info("Ma'lumotlarni yuklashda xatolik")


# ----------------------------
# 5) Top 5 mini-games
# ----------------------------
st.markdown(
    """
<div class="sec-row">
  <div>
    <div class="sec-title">🏆 TOP 5 mini o'yin</div>
    <div class="sec-sub">Eng ko'p o'ynalganlar</div>
  </div>
  <div></div>
</div>
""",
    unsafe_allow_html=True,
)

try:
    top_games = run_query(f"""
        SELECT
            EVENT_JSON:MiniGameName::STRING as MINI_GAME,
            COUNT(*) as OYINLAR
        FROM {DB}.ACCOUNT_EVENTS
        WHERE GAME_ID = {GAME_ID} AND EVENT_NAME = 'playedMiniGameStatus'
        AND EVENT_JSON:MiniGameName::STRING IS NOT NULL
        GROUP BY EVENT_JSON:MiniGameName::STRING
        ORDER BY OYINLAR DESC
        LIMIT 5
    """)

    if not top_games.empty:
        top_games["NOMI"] = top_games["MINI_GAME"].apply(get_minigame_name)
        medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"]

        # Build all rows as single HTML block
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
                x=alt.X("OYINLAR:Q", title=""),
                y=alt.Y("NOMI:N", title="", sort="-x"),
                tooltip=[
                    alt.Tooltip("NOMI:N", title="O'yin"),
                    alt.Tooltip("OYINLAR:Q", title="O'ynalishlar", format=","),
                ],
            )
            .properties(height=290, padding={"top": 18, "left": 8, "right": 8, "bottom": 8})
        )
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("Ma'lumotlar mavjud emas")
except Exception:
    st.info("Ma'lumotlarni yuklashda xatolik")


# ----------------------------
# 6) Retention
# ----------------------------
st.markdown(
    """
<div class="sec-row">
  <div>
    <div class="sec-title">🔄 Saqlanib qolish darajasi</div>
    <div class="sec-sub">Ma'lum kundan keyin ilovaga qaytgan foydalanuvchilar foizi</div>
  </div>
  <div></div>
</div>
""",
    unsafe_allow_html=True,
)

c1, c2, c3 = st.columns(3)

try:
    d1 = run_query(f"""
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
             AND s.EVENT_DATE = DATEADD(day, 1, f.first_date)
             AND s.GAME_ID = {GAME_ID}
        )
        SELECT ROUND(COUNT(DISTINCT r.USER_ID) * 100.0 / NULLIF(COUNT(DISTINCT f.USER_ID), 0), 1) as RET
        FROM first_day f
        LEFT JOIN returned r ON f.USER_ID = r.USER_ID
    """)
    c1.metric("1-kun", f"{float(d1['RET'][0] or 0.0)}%")
except Exception:
    c1.metric("1-kun", "N/A")

try:
    d7 = run_query(f"""
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
             AND s.EVENT_DATE = DATEADD(day, 7, f.first_date)
             AND s.GAME_ID = {GAME_ID}
        )
        SELECT ROUND(COUNT(DISTINCT r.USER_ID) * 100.0 / NULLIF(COUNT(DISTINCT f.USER_ID), 0), 1) as RET
        FROM first_day f
        LEFT JOIN returned r ON f.USER_ID = r.USER_ID
    """)
    c2.metric("7-kun", f"{float(d7['RET'][0] or 0.0)}%")
except Exception:
    c2.metric("7-kun", "N/A")

try:
    d30 = run_query(f"""
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
             AND s.EVENT_DATE = DATEADD(day, 30, f.first_date)
             AND s.GAME_ID = {GAME_ID}
        )
        SELECT ROUND(COUNT(DISTINCT r.USER_ID) * 100.0 / NULLIF(COUNT(DISTINCT f.USER_ID), 0), 1) as RET
        FROM first_day f
        LEFT JOIN returned r ON f.USER_ID = r.USER_ID
    """)
    c3.metric("30-kun", f"{float(d30['RET'][0] or 0.0)}%")
except Exception:
    c3.metric("30-kun", "N/A")
