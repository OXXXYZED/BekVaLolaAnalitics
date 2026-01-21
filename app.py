import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import snowflake.connector

# Настройки страницы
st.set_page_config(page_title="Bek va Lola Analytics", layout="wide")

# Подключение к Snowflake
@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        user=st.secrets["snowflake"]["user"],
        password=st.secrets["snowflake"]["password"],
        account=st.secrets["snowflake"]["account"],
        warehouse=st.secrets["snowflake"]["warehouse"],
        database=st.secrets["snowflake"]["database"],
        schema=st.secrets["snowflake"]["schema"]
    )

def run_query(query):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(query)
    columns = [desc[0] for desc in cur.description]
    data = cur.fetchall()
    return pd.DataFrame(data, columns=columns)

# Заголовок
st.title("🎮 Bek va Lola Analytics")

# GAME_ID для Bek va Lola
GAME_ID = 181330318
DB = "UNITY_ANALYTICS_GCP_US_CENTRAL1_UNITY_ANALYTICS_PDA.SHARES"

# ============ FILTERS ============
st.sidebar.header("🔧 Filters")

# Date Range
st.sidebar.subheader("📅 Date Range")
date_option = st.sidebar.selectbox(
    "Select period",
    ["Last 7 days", "Last 14 days", "Last 30 days", "Last 90 days", "Custom"]
)

if date_option == "Custom":
    start_date = st.sidebar.date_input("Start date", datetime.now() - timedelta(days=30))
    end_date = st.sidebar.date_input("End date", datetime.now())
else:
    days_map = {"Last 7 days": 7, "Last 14 days": 14, "Last 30 days": 30, "Last 90 days": 90}
    days = days_map[date_option]
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)

start_str = start_date.strftime('%Y-%m-%d')
end_str = end_date.strftime('%Y-%m-%d')

# Platform Filter
st.sidebar.subheader("📱 Platform")
platform_filter = st.sidebar.multiselect(
    "Select platforms",
    ["ANDROID", "IOS"],
    default=["ANDROID", "IOS"]
)
platform_str = "','".join(platform_filter)

# Version Filter
st.sidebar.subheader("📦 App Version")
try:
    versions_df = run_query(f"""
        SELECT DISTINCT CLIENT_VERSION
        FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY
        WHERE GAME_ID = {GAME_ID}
        ORDER BY CLIENT_VERSION DESC
    """)
    versions_list = versions_df['CLIENT_VERSION'].tolist()
    version_filter = st.sidebar.multiselect("Select versions", versions_list, default=versions_list)
    version_str = "','".join(version_filter)
except:
    version_filter = []
    version_str = ""

# Base WHERE clause
WHERE = f"""
WHERE GAME_ID = {GAME_ID}
AND EVENT_DATE BETWEEN '{start_str}' AND '{end_str}'
AND PLATFORM IN ('{platform_str}')
AND CLIENT_VERSION IN ('{version_str}')
"""

WHERE_EVENTS = f"""
WHERE GAME_ID = {GAME_ID}
AND EVENT_TIMESTAMP BETWEEN '{start_str}' AND '{end_str}'
"""

st.markdown("---")

# ============ KEY METRICS ============
st.subheader("📈 Key Metrics")

col1, col2, col3, col4 = st.columns(4)

# All Users
try:
    all_users = run_query(f"""
        SELECT COUNT(DISTINCT USER_ID) as TOTAL
        FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY {WHERE}
    """)
    col1.metric("👥 All Users", f"{all_users['TOTAL'][0]:,}")
except:
    col1.metric("👥 All Users", "N/A")

# DAU (average)
try:
    dau_avg = run_query(f"""
        SELECT ROUND(AVG(daily_users), 0) as AVG_DAU
        FROM (
            SELECT EVENT_DATE, COUNT(DISTINCT USER_ID) as daily_users
            FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY {WHERE}
            GROUP BY EVENT_DATE
        )
    """)
    col2.metric("📊 Avg DAU", f"{int(dau_avg['AVG_DAU'][0]):,}")
except:
    col2.metric("📊 Avg DAU", "N/A")

# WAU
try:
    wau = run_query(f"""
        SELECT COUNT(DISTINCT USER_ID) as WAU
        FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY {WHERE}
        AND EVENT_DATE >= DATEADD(day, -7, '{end_str}')
    """)
    col3.metric("📅 WAU", f"{wau['WAU'][0]:,}")
except:
    col3.metric("📅 WAU", "N/A")

# MAU
try:
    mau = run_query(f"""
        SELECT COUNT(DISTINCT USER_ID) as MAU
        FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY {WHERE}
    """)
    col4.metric("📆 MAU", f"{mau['MAU'][0]:,}")
except:
    col4.metric("📆 MAU", "N/A")

# Second row metrics
col5, col6, col7, col8 = st.columns(4)

# New Users
try:
    new_users = run_query(f"""
        SELECT COUNT(DISTINCT USER_ID) as NEW_USERS
        FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY {WHERE}
        AND PLAYER_START_DATE BETWEEN '{start_str}' AND '{end_str}'
    """)
    col5.metric("🆕 New Users", f"{new_users['NEW_USERS'][0]:,}")
except:
    col5.metric("🆕 New Users", "N/A")

# Sessions per DAU
try:
    spd = run_query(f"""
        SELECT ROUND(COUNT(DISTINCT SESSION_ID) * 1.0 / NULLIF(COUNT(DISTINCT USER_ID), 0), 2) as SPD
        FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY {WHERE}
    """)
    col6.metric("🔄 Sessions/User", f"{spd['SPD'][0]}")
except:
    col6.metric("🔄 Sessions/User", "N/A")

# Avg Session Length
try:
    session_len = run_query(f"""
        SELECT ROUND(AVG(TOTAL_TIME_MS) / 60000, 2) as AVG_MIN
        FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY {WHERE}
        AND TOTAL_TIME_MS > 0
    """)
    col7.metric("⏱️ Avg Session", f"{session_len['AVG_MIN'][0]} min")
except:
    col7.metric("⏱️ Avg Session", "N/A")

# Total Sessions
try:
    total_sessions = run_query(f"""
        SELECT COUNT(DISTINCT SESSION_ID) as SESSIONS
        FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY {WHERE}
    """)
    col8.metric("🎮 Total Sessions", f"{total_sessions['SESSIONS'][0]:,}")
except:
    col8.metric("🎮 Total Sessions", "N/A")

# Third row
col9, col10, col11, col12 = st.columns(4)

# Events per User
try:
    epu = run_query(f"""
        SELECT ROUND(COUNT(*) * 1.0 / NULLIF(COUNT(DISTINCT USER_ID), 0), 1) as EPU
        FROM {DB}.ACCOUNT_EVENTS {WHERE_EVENTS}
    """)
    col9.metric("🎯 Events/User", f"{epu['EPU'][0]}")
except:
    col9.metric("🎯 Events/User", "N/A")

# Total Events
try:
    total_events = run_query(f"""
        SELECT COUNT(*) as EVENTS
        FROM {DB}.ACCOUNT_EVENTS {WHERE_EVENTS}
    """)
    col10.metric("📊 Total Events", f"{total_events['EVENTS'][0]:,}")
except:
    col10.metric("📊 Total Events", "N/A")

# Stickiness
try:
    if mau['MAU'][0] > 0 and dau_avg['AVG_DAU'][0]:
        stickiness = round(dau_avg['AVG_DAU'][0] / mau['MAU'][0] * 100, 1)
        col11.metric("📌 Stickiness", f"{stickiness}%")
    else:
        col11.metric("📌 Stickiness", "N/A")
except:
    col11.metric("📌 Stickiness", "N/A")

# Events per Session
try:
    eps = run_query(f"""
        SELECT ROUND(AVG(NUMBER_OF_EVENTS), 1) as EPS
        FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY {WHERE}
    """)
    col12.metric("🔢 Events/Session", f"{eps['EPS'][0]}")
except:
    col12.metric("🔢 Events/Session", "N/A")

st.markdown("---")

# ============ RETENTION ============
st.subheader("🔄 Retention Analysis")
col_ret1, col_ret2, col_ret3, col_ret4 = st.columns(4)

# Day 1
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
            WHERE f.first_date BETWEEN '{start_str}' AND DATEADD(day, -1, '{end_str}')
        )
        SELECT ROUND(COUNT(DISTINCT r.USER_ID) * 100.0 / NULLIF(COUNT(DISTINCT f.USER_ID), 0), 1) as RET
        FROM first_day f LEFT JOIN returned r ON f.USER_ID = r.USER_ID
        WHERE f.first_date BETWEEN '{start_str}' AND DATEADD(day, -1, '{end_str}')
    """)
    col_ret1.metric("📅 Day 1", f"{d1['RET'][0]}%")
except:
    col_ret1.metric("📅 Day 1", "N/A")

# Day 7
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
            WHERE f.first_date BETWEEN '{start_str}' AND DATEADD(day, -7, '{end_str}')
        )
        SELECT ROUND(COUNT(DISTINCT r.USER_ID) * 100.0 / NULLIF(COUNT(DISTINCT f.USER_ID), 0), 1) as RET
        FROM first_day f LEFT JOIN returned r ON f.USER_ID = r.USER_ID
        WHERE f.first_date BETWEEN '{start_str}' AND DATEADD(day, -7, '{end_str}')
    """)
    col_ret2.metric("📅 Day 7", f"{d7['RET'][0]}%")
except:
    col_ret2.metric("📅 Day 7", "N/A")

# Day 14
try:
    d14 = run_query(f"""
        WITH first_day AS (
            SELECT USER_ID, MIN(EVENT_DATE) as first_date
            FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY WHERE GAME_ID = {GAME_ID} GROUP BY USER_ID
        ),
        returned AS (
            SELECT f.USER_ID FROM first_day f
            JOIN {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY s ON f.USER_ID = s.USER_ID
            AND s.EVENT_DATE = DATEADD(day, 14, f.first_date) AND s.GAME_ID = {GAME_ID}
            WHERE f.first_date BETWEEN '{start_str}' AND DATEADD(day, -14, '{end_str}')
        )
        SELECT ROUND(COUNT(DISTINCT r.USER_ID) * 100.0 / NULLIF(COUNT(DISTINCT f.USER_ID), 0), 1) as RET
        FROM first_day f LEFT JOIN returned r ON f.USER_ID = r.USER_ID
        WHERE f.first_date BETWEEN '{start_str}' AND DATEADD(day, -14, '{end_str}')
    """)
    col_ret3.metric("📅 Day 14", f"{d14['RET'][0]}%")
except:
    col_ret3.metric("📅 Day 14", "N/A")

# Day 30
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
            WHERE f.first_date BETWEEN '{start_str}' AND DATEADD(day, -30, '{end_str}')
        )
        SELECT ROUND(COUNT(DISTINCT r.USER_ID) * 100.0 / NULLIF(COUNT(DISTINCT f.USER_ID), 0), 1) as RET
        FROM first_day f LEFT JOIN returned r ON f.USER_ID = r.USER_ID
        WHERE f.first_date BETWEEN '{start_str}' AND DATEADD(day, -30, '{end_str}')
    """)
    col_ret4.metric("📅 Day 30", f"{d30['RET'][0]}%")
except:
    col_ret4.metric("📅 Day 30", "N/A")

st.markdown("---")

# ============ DAU CHART ============
st.subheader("📊 Daily Active Users")
try:
    dau_df = run_query(f"""
        SELECT EVENT_DATE, COUNT(DISTINCT USER_ID) as USERS
        FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY {WHERE}
        GROUP BY EVENT_DATE ORDER BY EVENT_DATE
    """)
    if not dau_df.empty:
        st.line_chart(dau_df.set_index('EVENT_DATE')['USERS'])
except:
    pass

# ============ NEW USERS CHART ============
st.subheader("👥 Daily New Users")
try:
    new_df = run_query(f"""
        SELECT PLAYER_START_DATE as DATE, COUNT(DISTINCT USER_ID) as NEW_USERS
        FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY {WHERE}
        AND PLAYER_START_DATE BETWEEN '{start_str}' AND '{end_str}'
        GROUP BY PLAYER_START_DATE ORDER BY PLAYER_START_DATE
    """)
    if not new_df.empty:
        st.line_chart(new_df.set_index('DATE')['NEW_USERS'])
except:
    pass

# ============ SESSION LENGTH ============
st.subheader("⏱️ Avg Session Length (minutes)")
try:
    sess_df = run_query(f"""
        SELECT EVENT_DATE, ROUND(AVG(TOTAL_TIME_MS) / 60000, 2) as AVG_MIN
        FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY {WHERE}
        AND TOTAL_TIME_MS > 0
        GROUP BY EVENT_DATE ORDER BY EVENT_DATE
    """)
    if not sess_df.empty:
        st.line_chart(sess_df.set_index('EVENT_DATE')['AVG_MIN'])
except:
    pass

st.markdown("---")

# ============ MINI-GAMES ============
st.subheader("🎮 Mini-Games Statistics")
try:
    mg_df = run_query(f"""
        SELECT
            EVENT_JSON:MiniGameName::STRING as MINI_GAME,
            COUNT(*) as PLAYS,
            ROUND(AVG(EVENT_JSON:duration::FLOAT), 2) as AVG_DURATION_SEC,
            ROUND(SUM(CASE WHEN EVENT_JSON:isComplated::INT = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as COMPLETION_RATE
        FROM {DB}.ACCOUNT_EVENTS
        WHERE GAME_ID = {GAME_ID} AND EVENT_NAME = 'playedMiniGameStatus'
        AND EVENT_TIMESTAMP BETWEEN '{start_str}' AND '{end_str}'
        GROUP BY EVENT_JSON:MiniGameName::STRING ORDER BY PLAYS DESC
    """)
    if not mg_df.empty:
        st.dataframe(mg_df, use_container_width=True)
        col_mg1, col_mg2 = st.columns(2)
        with col_mg1:
            st.bar_chart(mg_df.set_index('MINI_GAME')['PLAYS'])
        with col_mg2:
            st.bar_chart(mg_df.set_index('MINI_GAME')['COMPLETION_RATE'])
except:
    st.info("Нет данных по мини-играм")

st.markdown("---")

# ============ LOBBY ACTIONS ============
st.subheader("🏠 Lobby Actions")
try:
    lobby_df = run_query(f"""
        SELECT
            EVENT_JSON:lobbyActionName::STRING as ACTION,
            COUNT(*) as COUNT,
            ROUND(SUM(CASE WHEN EVENT_JSON:isComplated::INT = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as COMPLETION_RATE
        FROM {DB}.ACCOUNT_EVENTS
        WHERE GAME_ID = {GAME_ID} AND EVENT_NAME = 'lobbyActionInExit'
        AND EVENT_TIMESTAMP BETWEEN '{start_str}' AND '{end_str}'
        GROUP BY EVENT_JSON:lobbyActionName::STRING ORDER BY COUNT DESC
    """)
    if not lobby_df.empty:
        st.dataframe(lobby_df, use_container_width=True)
        st.bar_chart(lobby_df.set_index('ACTION')['COUNT'])
except:
    st.info("Нет данных")

st.markdown("---")

# ============ BREAKDOWNS ============
col_l, col_m, col_r = st.columns(3)

with col_l:
    st.subheader("📱 By Platform")
    try:
        p_df = run_query(f"""
            SELECT PLATFORM, COUNT(DISTINCT USER_ID) as USERS
            FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY {WHERE}
            GROUP BY PLATFORM
        """)
        if not p_df.empty:
            st.bar_chart(p_df.set_index('PLATFORM')['USERS'])
    except:
        pass

with col_m:
    st.subheader("🌍 By Country")
    try:
        c_df = run_query(f"""
            SELECT USER_COUNTRY, COUNT(DISTINCT USER_ID) as USERS
            FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY {WHERE}
            GROUP BY USER_COUNTRY ORDER BY USERS DESC LIMIT 10
        """)
        if not c_df.empty:
            st.bar_chart(c_df.set_index('USER_COUNTRY')['USERS'])
    except:
        pass

with col_r:
    st.subheader("📦 By Version")
    try:
        v_df = run_query(f"""
            SELECT CLIENT_VERSION, COUNT(DISTINCT USER_ID) as USERS
            FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY {WHERE}
            GROUP BY CLIENT_VERSION ORDER BY USERS DESC
        """)
        if not v_df.empty:
            st.bar_chart(v_df.set_index('CLIENT_VERSION')['USERS'])
    except:
        pass

st.markdown("---")

# ============ HOURLY ============
st.subheader("🕐 Activity by Hour")
try:
    h_df = run_query(f"""
        SELECT HOUR(EVENT_TIMESTAMP) as HOUR, COUNT(*) as EVENTS
        FROM {DB}.ACCOUNT_EVENTS {WHERE_EVENTS}
        GROUP BY HOUR(EVENT_TIMESTAMP) ORDER BY HOUR
    """)
    if not h_df.empty:
        st.bar_chart(h_df.set_index('HOUR')['EVENTS'])
except:
    pass

# ============ DAY OF WEEK ============
st.subheader("📅 Activity by Day of Week")
try:
    dow_df = run_query(f"""
        SELECT
            CASE DAYOFWEEK(EVENT_DATE)
                WHEN 0 THEN '1_Sun' WHEN 1 THEN '2_Mon' WHEN 2 THEN '3_Tue'
                WHEN 3 THEN '4_Wed' WHEN 4 THEN '5_Thu' WHEN 5 THEN '6_Fri' WHEN 6 THEN '7_Sat'
            END as DAY_NAME,
            COUNT(DISTINCT USER_ID) as USERS
        FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY {WHERE}
        GROUP BY DAYOFWEEK(EVENT_DATE) ORDER BY DAY_NAME
    """)
    if not dow_df.empty:
        st.bar_chart(dow_df.set_index('DAY_NAME')['USERS'])
except:
    pass

st.markdown("---")

# ============ TOP EVENTS ============
st.subheader("🎯 Top Events")
try:
    e_df = run_query(f"""
        SELECT EVENT_NAME, COUNT(*) as COUNT
        FROM {DB}.ACCOUNT_EVENTS {WHERE_EVENTS}
        GROUP BY EVENT_NAME ORDER BY COUNT DESC LIMIT 15
    """)
    if not e_df.empty:
        st.dataframe(e_df, use_container_width=True)
except:
    pass

st.markdown("---")
st.caption("Data source: Unity Analytics via Snowflake")
