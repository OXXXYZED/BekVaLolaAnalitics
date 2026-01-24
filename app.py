import streamlit as st
import pandas as pd
import altair as alt
from datetime import datetime, timedelta
from decimal import Decimal
import snowflake.connector

# Page setup
st.set_page_config(
    page_title="Bek va Lola • Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Theme colors
COLORS = {
    "bg": "#070A12",
    "border": "#2A3354",
    "text": "#F3F7FF",
    "muted": "#B6C2DD",
    "accent": "#2EF2C2",
    "new_users": "#B56BFF",
    "sessions": "#4AA8FF",
    "minigame": "#FF5A7A",
    "android": "#35E27B",
    "ios": "#4AA8FF",
    "other": "#A9B3C9",
    "purple": "#B56BFF",
}

alt.data_transformers.disable_max_rows()

# Secrets check
if "snowflake" not in st.secrets:
    st.error("Snowflake credentials topilmadi. Iltimos, secrets ni sozlang.")
    st.stop()

# Mini-game names (display)
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

# Snowflake connection (cached)
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

    # Decimal -> float + numeric coercion
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

# Altair transparent theme for dark UI
def _transparent_theme():
    return {
        "config": {
            "background": "transparent",
            "view": {"stroke": "transparent"},
            "axis": {
                "labelColor": COLORS["muted"],
                "titleColor": COLORS["muted"],
                "gridColor": "rgba(255, 255, 255, 0.08)",
                "domainColor": "rgba(255, 255, 255, 0.14)",
                "tickColor": "rgba(255, 255, 255, 0.14)",
                "labelFontSize": 11,
                "titleFontSize": 11,
            },
            "legend": {"labelColor": COLORS["muted"], "titleColor": COLORS["muted"]},
            "title": {"color": COLORS["text"]},
        }
    }

alt.themes.register("transparent_pro", _transparent_theme)
alt.themes.enable("transparent_pro")

# UI styles
st.markdown(
    f"""
<style>
  .stApp {{
    background:
      radial-gradient(1200px 700px at 15% 0%, rgba(46,242,194,0.22), transparent 60%),
      radial-gradient(1000px 650px at 85% 10%, rgba(181,107,255,0.18), transparent 58%),
      {COLORS["bg"]};
    color: {COLORS["text"]};
  }}
  .block-container {{
    max-width: 1320px;
    padding-top: 1.0rem;
    padding-bottom: 2.0rem;
    padding-left: 1.6rem;
    padding-right: 1.6rem;
  }}

  #MainMenu {{ visibility: hidden; }}
  footer {{ visibility: hidden; }}

  [data-testid="stHeader"] {{
    background: transparent !important;
    border-bottom: 0 !important;
    box-shadow: none !important;
  }}

  .muted {{ color: {COLORS["muted"]}; }}

  [data-testid="stSidebarCollapsedControl"] {{
    top: 14px !important;
    left: 14px !important;
    z-index: 9999 !important;
    background: rgba(18, 35, 58, 0.92) !important;
    border: 1px solid rgba(255,255,255,0.14) !important;
    border-radius: 14px !important;
    padding: 6px 8px !important;
    box-shadow: 0 10px 24px rgba(0,0,0,0.25) !important;
  }}
  [data-testid="stSidebarCollapseButton"] {{
    background: rgba(18, 35, 58, 0.92) !important;
    border: 1px solid rgba(255,255,255,0.14) !important;
    border-radius: 12px !important;
    padding: 4px 6px !important;
  }}

  [data-testid="stVegaLiteChart"] {{
    background: transparent !important;
    border: 1px solid rgba(255,255,255,0.10);
    border-radius: 16px;
    padding: 10px;
  }}
  [data-testid="stVegaLiteChart"] > div {{ background: transparent !important; }}

  .hero {{
    background: linear-gradient(180deg, rgba(18,35,58,0.78), rgba(10,16,28,0.55));
    border: 1px solid rgba(255,255,255,0.12);
    border-radius: 18px;
    padding: 14px 16px;
    box-shadow: 0 12px 28px rgba(0,0,0,0.22);
    margin-bottom: 12px;
  }}
  .hero-title {{
    font-size: 1.65rem;
    font-weight: 950;
    letter-spacing: -0.02em;
  }}

  .kpi-grid {{
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 12px;
    margin-bottom: 12px;
  }}
  .kpi {{
    background: linear-gradient(180deg, rgba(18,35,58,0.60), rgba(10,16,28,0.36));
    border: 1px solid rgba(255,255,255,0.12);
    border-radius: 16px;
    padding: 14px 14px;
    box-shadow: 0 10px 22px rgba(0,0,0,0.14);
    backdrop-filter: blur(6px);
  }}
  .kpi-label {{
    font-size: 0.92rem;
    color: {COLORS["muted"]};
    font-weight: 850;
    margin-bottom: 8px;
  }}
  .kpi-value {{
    font-size: 2.05rem;
    font-weight: 950;
    letter-spacing: -0.02em;
    line-height: 1.05;
    color: {COLORS["text"]};
  }}

  .panel {{
    background: linear-gradient(180deg, rgba(18,35,58,0.46), rgba(10,16,28,0.28));
    border: 1px solid rgba(255,255,255,0.12);
    border-radius: 18px;
    padding: 8px 12px 12px 12px;
    backdrop-filter: blur(6px);
    box-shadow: 0 10px 22px rgba(0,0,0,0.12);
    margin-bottom: 12px;
  }}
  .panel-title {{
    font-size: 1.05rem;
    font-weight: 950;
    letter-spacing: -0.01em;
    margin: 2px 0 2px 0;
  }}
  .panel-sub {{
    color: {COLORS["muted"]};
    font-weight: 750;
    font-size: 0.92rem;
    margin: 0 0 6px 0;
  }}

  [data-testid="stSidebar"] {{
    background: linear-gradient(180deg, rgba(18,35,58,0.96), rgba(10,16,28,0.90)) !important;
    border-right: 1px solid rgba(255,255,255,0.12) !important;
  }}
  [data-testid="stSidebar"] * {{ color: {COLORS["text"]} !important; }}
  [data-testid="stSidebar"] .stDateInput label,
  [data-testid="stSidebar"] .stSelectbox label {{
    color: {COLORS["muted"]} !important;
    font-weight: 850 !important;
  }}

  /* Legend container matches chart card look */
  .legend-card {{
    background: rgba(18, 35, 58, 0.55) !important;
    border: 1px solid rgba(255,255,255,0.14);
    border-radius: 16px;
    padding: 10px;
    box-shadow: 0 10px 22px rgba(0,0,0,0.14);
  }}

  /* Extra top padding prevents the top pill-looking row from feeling glued */
  .stat-card {{
    background: linear-gradient(180deg, rgba(18,35,58,0.36), rgba(10,16,28,0.22));
    border: 1px solid rgba(255,255,255,0.10);
    border-radius: 16px;
    padding: 18px 12px 12px 12px;
    backdrop-filter: blur(6px);
  }}
  .stat-row {{
    display:flex;
    align-items:flex-start;
    justify-content:space-between;
    gap: 12px;
    padding: 10px 0;
    border-bottom: 1px solid rgba(255,255,255,0.08);
  }}
  .stat-row:last-child {{ border-bottom: none; padding-bottom: 2px; }}
  .dot {{
    width: 10px;
    height: 10px;
    border-radius: 999px;
    margin-right: 10px;
    margin-top: 5px;
    flex: 0 0 auto;
    box-shadow: 0 0 0 3px rgba(255,255,255,0.04);
  }}
  .stat-left {{
    display:flex;
    align-items:flex-start;
    gap: 10px;
    font-weight: 900;
    line-height: 1.15;
  }}
  .stat-label {{
    font-weight: 950;
    letter-spacing: -0.01em;
  }}
  .stat-sub {{
    color: {COLORS["muted"]};
    font-weight: 750;
    font-size: 0.9rem;
    margin-top: 3px;
  }}
  .stat-right {{
    font-weight: 950;
    color: {COLORS["text"]};
    text-align:right;
    white-space: nowrap;
    letter-spacing: -0.01em;
  }}

  .rank-card {{
    background: linear-gradient(180deg, rgba(18,35,58,0.36), rgba(10,16,28,0.22));
    border: 1px solid rgba(255,255,255,0.10);
    border-radius: 16px;
    padding: 10px 12px;
    backdrop-filter: blur(6px);
  }}
  .rank-row {{
    display:grid;
    grid-template-columns: 52px 1fr 120px;
    gap: 10px;
    align-items:center;
    padding: 10px 0;
    border-bottom: 1px solid rgba(255,255,255,0.08);
  }}
  .rank-row:last-child {{ border-bottom: none; }}
  .rank-badge {{ font-size: 1.2rem; font-weight: 900; }}
  .rank-name {{ font-weight: 900; }}
  .rank-val {{ text-align:right; font-weight: 950; }}

  @media (max-width: 980px) {{
    .kpi-grid {{ grid-template-columns: 1fr; }}
    .rank-row {{ grid-template-columns: 52px 1fr 100px; }}
  }}
</style>
""",
    unsafe_allow_html=True,
)

# Filters (sidebar)
with st.sidebar:
    st.markdown("## ⚙️ Filtrlar")
    st.markdown('<div class="muted">Davr va ko‘rinishlarni tanlang.</div>', unsafe_allow_html=True)
    st.divider()

    period_type = st.selectbox(
        "👥 Yangi foydalanuvchilar (kesim)",
        ["Kunlik", "Haftalik", "Oylik"],
        key="new_users_period",
    )
    date_range = st.date_input(
        "👥 Yangi foydalanuvchilar (sana oralig‘i)",
        value=(datetime.now() - timedelta(days=30), datetime.now()),
        key="new_users_date",
    )

    st.divider()

    session_view = st.selectbox("📈 Sessiyalar (ko‘rinish)", ["Kunlik", "Soatlik"], key="session_view")
    if session_view == "Soatlik":
        session_date = st.date_input("📈 Soatlik (sana)", value=datetime.now(), key="session_date")
        session_period = None
    else:
        session_period = st.selectbox(
            "📈 Kunlik (davr)",
            ["So'nggi 7 kun", "So'nggi 14 kun", "So'nggi 30 kun"],
            key="session_period",
        )
        session_date = None

    st.divider()

    mg_date_range = st.date_input(
        "🎮 Mini o‘yinlar (sana oralig‘i)",
        value=(datetime.now() - timedelta(days=30), datetime.now()),
        key="mg_date",
    )

    try:
        mg_list = run_query(f"""
            SELECT DISTINCT EVENT_JSON:MiniGameName::STRING as MINI_GAME
            FROM {DB}.ACCOUNT_EVENTS
            WHERE GAME_ID = {GAME_ID} AND EVENT_NAME = 'playedMiniGameStatus'
            AND EVENT_JSON:MiniGameName::STRING IS NOT NULL
        """)
        mg_options = ["Barchasi"] + [get_minigame_name(mg) for mg in mg_list["MINI_GAME"].tolist() if mg]
        mg_original = {get_minigame_name(mg): mg for mg in mg_list["MINI_GAME"].tolist() if mg}
        selected_mg = st.selectbox("🎮 Mini o‘yin", mg_options, key="mg_filter")
    except Exception:
        selected_mg = "Barchasi"
        mg_original = {}

    st.caption("Tanlangan filtrlar barcha grafiklarni yangilaydi.")

# Header
st.markdown(
    """
<div class="hero">
  <div class="hero-title">📊 Bek va Lola — Analitikasi</div>
  <div class="muted" style="margin-top:4px;">Bitta sahifa • aniq vizuallar • tez filtrlash</div>
</div>
""",
    unsafe_allow_html=True,
)

# KPI 1: total users
try:
    total_users = run_query(f"""
        SELECT COUNT(DISTINCT USER_ID) as TOTAL
        FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY
        WHERE GAME_ID = {GAME_ID}
    """)
    kpi_total_users = int(total_users["TOTAL"][0])
except Exception:
    kpi_total_users = None

# KPI 2: new users in selected range
kpi_new_users = None
if len(date_range) == 2:
    start_date, end_date = date_range
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    try:
        new_users_total_df = run_query(f"""
            SELECT COUNT(DISTINCT USER_ID) as TOTAL_NEW
            FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY
            WHERE GAME_ID = {GAME_ID}
            AND PLAYER_START_DATE BETWEEN '{start_str}' AND '{end_str}'
        """)
        kpi_new_users = int(new_users_total_df["TOTAL_NEW"][0])
    except Exception:
        kpi_new_users = None

# KPI 3: sessions in selected view
kpi_sessions = None
try:
    if session_view == "Soatlik":
        date_str = session_date.strftime("%Y-%m-%d")
        sess_kpi_df = run_query(f"""
            SELECT COUNT(DISTINCT SESSION_ID) as TOTAL_SESS
            FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY
            WHERE GAME_ID = {GAME_ID}
            AND EVENT_DATE = '{date_str}'
        """)
        kpi_sessions = int(sess_kpi_df["TOTAL_SESS"][0])
    else:
        days_map = {"So'nggi 7 kun": 7, "So'nggi 14 kun": 14, "So'nggi 30 kun": 30}
        days = days_map[session_period]
        end_dt = datetime.now()
        start_dt = end_dt - timedelta(days=days)
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
  <div class="kpi">
    <div class="kpi-label">👥 Foydalanuvchilar</div>
    <div class="kpi-value">{f"{kpi_total_users:,}" if kpi_total_users is not None else "N/A"}</div>
  </div>
  <div class="kpi">
    <div class="kpi-label">✨ Yangi foydalanuvchilar (tanlangan davr)</div>
    <div class="kpi-value">{f"{kpi_new_users:,}" if kpi_new_users is not None else "N/A"}</div>
  </div>
  <div class="kpi">
    <div class="kpi-label">📈 Sessiyalar (tanlangan ko‘rinish)</div>
    <div class="kpi-value">{f"{kpi_sessions:,}" if kpi_sessions is not None else "N/A"}</div>
  </div>
</div>
""",
    unsafe_allow_html=True,
)

# Platform donut + legend (legend gets its own card like the chart)
st.markdown(
    '<div class="panel"><div class="panel-title">📱 Platformalar</div>'
    '<div class="panel-sub">Foydalanuvchilar taqsimoti</div>',
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
                .mark_arc(innerRadius=118, outerRadius=150)
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
                # Lower top padding moves donut up visually
                .properties(height=CHART_H, padding={"top": 6, "left": 8, "right": 8, "bottom": 8})
            )
            st.altair_chart(donut, use_container_width=True)

        with c_nums:
            st.markdown('<div class="legend-card">', unsafe_allow_html=True)

            st.markdown(
                f"""
<div class="stat-row" style="padding-top:2px;">
  <div>
    <div class="stat-left"><span class="dot" style="background:{COLORS["accent"]};"></span>
      <span class="stat-label">Jami</span>
    </div>
  </div>
  <div class="stat-right">{total:,}</div>
</div>
""",
                unsafe_allow_html=True,
            )

            for _, r in platform_df.iterrows():
                p = r["PLATFORM"]
                u = int(r["USERS"])
                pr = float(r["PERCENT"])
                color = COLORS["android"] if p == "Android" else COLORS["ios"] if p == "iOS" else COLORS["other"]

                st.markdown(
                    f"""
<div class="stat-row">
  <div>
    <div class="stat-left"><span class="dot" style="background:{color};"></span>
      <span class="stat-label">{p}</span>
    </div>
    <div class="stat-sub">{pr:.1f}%</div>
  </div>
  <div class="stat-right">{u:,}</div>
</div>
""",
                    unsafe_allow_html=True,
                )

            st.markdown("</div></div>", unsafe_allow_html=True)

    else:
        st.info("Ma'lumotlar mavjud emas")
except Exception:
    st.info("Ma'lumotlar mavjud emas")
st.markdown("</div>", unsafe_allow_html=True)

# New users
st.markdown(
    '<div class="panel"><div class="panel-title">👥 Yangi foydalanuvchilar</div>'
    '<div class="panel-sub">Tanlangan davr bo‘yicha trend</div>',
    unsafe_allow_html=True,
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
            m3.metric("O‘rtacha", f"{int(round(new_users_df['YANGI_USERS'].mean(), 0)):,}")

            chart = (
                alt.Chart(new_users_df)
                .mark_bar(color=COLORS["new_users"], cornerRadiusTopLeft=6, cornerRadiusTopRight=6)
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
st.markdown("</div>", unsafe_allow_html=True)

# Sessions
st.markdown(
    '<div class="panel"><div class="panel-title">📈 Sessiyalar</div>'
    '<div class="panel-sub">Faollik ko‘rinishi</div>',
    unsafe_allow_html=True,
)
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
                .mark_bar(color=COLORS["sessions"], cornerRadiusTopLeft=6, cornerRadiusTopRight=6)
                .encode(
                    x=alt.X("SOAT_LABEL:N", title="", sort=None, axis=alt.Axis(labelAngle=0)),
                    y=alt.Y("HODISALAR:Q", title=""),
                    tooltip=[
                        alt.Tooltip("SOAT_LABEL:N", title="Soat"),
                        alt.Tooltip("HODISALAR:Q", title="Hodisalar", format=","),
                        alt.Tooltip("FOYDALANUVCHILAR:Q", title="Foydalanuvchilar", format=","),
                    ],
                )
                .properties(height=320)
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
            m2.metric("O‘rtacha kunlik", f"{int(sessions_df['SESSIYALAR'].mean()):,}")
            m3.metric("O‘rtacha vaqt (daq)", f"{round(float(sessions_df['ORTACHA_DAVOMIYLIK'].mean()), 1)}")

            sessions_df["SANA"] = pd.to_datetime(sessions_df["SANA"])
            sessions_df["SANA_STR"] = sessions_df["SANA"].dt.strftime("%Y-%m-%d")

            chart = (
                alt.Chart(sessions_df)
                .mark_bar(color=COLORS["sessions"], cornerRadiusTopLeft=6, cornerRadiusTopRight=6)
                .encode(
                    x=alt.X("SANA_STR:O", title="", axis=alt.Axis(labelAngle=-30), sort=None),
                    y=alt.Y("SESSIYALAR:Q", title=""),
                    tooltip=[
                        alt.Tooltip("SANA_STR:O", title="Sana"),
                        alt.Tooltip("SESSIYALAR:Q", title="Sessiyalar", format=","),
                        alt.Tooltip("ORTACHA_DAVOMIYLIK:Q", title="Daqiqa", format=".1f"),
                    ],
                )
                .properties(height=320)
            )
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("Ma'lumotlar mavjud emas")
except Exception:
    st.info("Ma'lumotlarni yuklashda xatolik")
st.markdown("</div>", unsafe_allow_html=True)

# Mini-game trend
st.markdown(
    '<div class="panel"><div class="panel-title">🎮 Mini o‘yinlar trendi</div>'
    '<div class="panel-sub">Tanlangan davr bo‘yicha</div>',
    unsafe_allow_html=True,
)
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

            line = (
                alt.Chart(mg_stats)
                .mark_line(color=COLORS["minigame"], strokeWidth=2.8)
                .encode(
                    x=alt.X("SANA:T", title="", axis=alt.Axis(format="%Y-%m-%d", labelAngle=-30, tickCount=10)),
                    y=alt.Y("OYINLAR:Q", title=""),
                    tooltip=[
                        alt.Tooltip("SANA:T", title="Sana", format="%Y-%m-%d"),
                        alt.Tooltip("OYINLAR:Q", title="O‘yinlar", format=","),
                    ],
                )
            )
            points = (
                alt.Chart(mg_stats)
                .mark_circle(size=70, color=COLORS["minigame"], opacity=0.9)
                .encode(x="SANA:T", y="OYINLAR:Q")
            )
            st.altair_chart((line + points).properties(height=320), use_container_width=True)
        else:
            st.info("Tanlangan davr uchun ma'lumotlar mavjud emas")
    except Exception:
        st.info("Ma'lumotlarni yuklashda xatolik")
st.markdown("</div>", unsafe_allow_html=True)

# Top 5 mini-games
st.markdown(
    '<div class="panel"><div class="panel-title">🏆 TOP 5 mini o‘yin</div>'
    '<div class="panel-sub">Tanlangan davr bo‘yicha</div>',
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

        st.markdown('<div class="rank-card">', unsafe_allow_html=True)
        for i, row in top_games.reset_index(drop=True).iterrows():
            medal = medals[i] if i < len(medals) else f"#{i+1}"
            st.markdown(
                f"""
<div class="rank-row">
  <div class="rank-badge">{medal}</div>
  <div class="rank-name">{row["NOMI"]}</div>
  <div class="rank-val">{int(row["OYINLAR"]):,}</div>
</div>
""",
                unsafe_allow_html=True,
            )
        st.markdown("</div>", unsafe_allow_html=True)

        chart = (
            alt.Chart(top_games)
            .mark_bar(color=COLORS["purple"], cornerRadiusTopRight=8, cornerRadiusBottomRight=8, size=34)
            .encode(
                x=alt.X("OYINLAR:Q", title=""),
                y=alt.Y("NOMI:N", title="", sort="-x"),
                tooltip=[
                    alt.Tooltip("NOMI:N", title="O‘yin"),
                    alt.Tooltip("OYINLAR:Q", title="O‘ynalishlar", format=","),
                ],
            )
            .properties(height=290)
        )
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("Ma'lumotlar mavjud emas")
except Exception:
    st.info("Ma'lumotlarni yuklashda xatolik")
st.markdown("</div>", unsafe_allow_html=True)

# Retention (Uzbek)
st.markdown(
    '<div class="panel"><div class="panel-title">🔄 Saqlanib qolish darajasi</div>'
    '<div class="panel-sub">Ma’lum kundan keyin ilovaga qaytgan foydalanuvchilar foizi</div>',
    unsafe_allow_html=True,
)

c1, c2, c3 = st.columns(3)

try:
    d1 = run_query(f"""
        WITH first_day AS (
            SELECT USER_ID, MIN(EVENT_DATE) as first_date
            FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY WHERE GAME_ID = {GAME_ID} GROUP BY USER_ID
        ),
        returned AS (
            SELECT f.USER_ID FROM first_day f
            JOIN {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY s ON f.USER_ID = s.USER_ID
            AND s.EVENT_DATE = DATEADD(day, 1, f.first_date) AND s.GAME_ID = {GAME_ID}
        )
        SELECT ROUND(COUNT(DISTINCT r.USER_ID) * 100.0 / NULLIF(COUNT(DISTINCT f.USER_ID), 0), 1) as RET
        FROM first_day f LEFT JOIN returned r ON f.USER_ID = r.USER_ID
    """)
    c1.metric("1-kun", f"{float(d1['RET'][0] or 0.0)}%")
except Exception:
    c1.metric("1-kun", "N/A")

try:
    d7 = run_query(f"""
        WITH first_day AS (
            SELECT USER_ID, MIN(EVENT_DATE) as first_date
            FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY WHERE GAME_ID = {GAME_ID} GROUP BY USER_ID
        ),
        returned AS (
            SELECT f.USER_ID FROM first_day f
            JOIN {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY s ON f.USER_ID = s.USER_ID
            AND s.EVENT_DATE = DATEADD(day, 7, f.first_date) AND s.GAME_ID = {GAME_ID}
        )
        SELECT ROUND(COUNT(DISTINCT r.USER_ID) * 100.0 / NULLIF(COUNT(DISTINCT f.USER_ID), 0), 1) as RET
        FROM first_day f LEFT JOIN returned r ON f.USER_ID = r.USER_ID
    """)
    c2.metric("7-kun", f"{float(d7['RET'][0] or 0.0)}%")
except Exception:
    c2.metric("7-kun", "N/A")

try:
    d30 = run_query(f"""
        WITH first_day AS (
            SELECT USER_ID, MIN(EVENT_DATE) as first_date
            FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY WHERE GAME_ID = {GAME_ID} GROUP BY USER_ID
        ),
        returned AS (
            SELECT f.USER_ID FROM first_day f
            JOIN {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY s ON f.USER_ID = s.USER_ID
            AND s.EVENT_DATE = DATEADD(day, 30, f.first_date) AND s.GAME_ID = {GAME_ID}
        )
        SELECT ROUND(COUNT(DISTINCT r.USER_ID) * 100.0 / NULLIF(COUNT(DISTINCT f.USER_ID), 0), 1) as RET
        FROM first_day f LEFT JOIN returned r ON f.USER_ID = r.USER_ID
    """)
    c3.metric("30-kun", f"{float(d30['RET'][0] or 0.0)}%")
except Exception:
    c3.metric("30-kun", "N/A")

st.markdown("</div>", unsafe_allow_html=True)
st.caption("📊 Unity Analytics • Ma’lumotlar kechikish bilan yangilanishi mumkin.")
