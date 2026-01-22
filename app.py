import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import snowflake.connector
from io import BytesIO

# Настройки страницы
st.set_page_config(
    page_title="Bek va Lola Analytics",
    page_icon="🎮",
    layout="wide"
)

# Dictionary for friendly action names
ACTION_NAMES = {
    # Sessions
    'sessionStart': '🚀 Game Start',
    'sessionEnd': '🔚 Game End',
    'appStart': '📱 App Launch',
    'appQuit': '📴 App Close',

    # Mini-games
    'playedMiniGameStatus': '🎮 Mini-game Played',
    'miniGameStarted': '▶️ Mini-game Started',
    'miniGameCompleted': '✅ Mini-game Completed',
    'miniGameFailed': '❌ Mini-game Failed',

    # Lobby & Navigation
    'lobbyActionInExit': '🏠 Lobby Action',
    'lobbyEnter': '🚪 Lobby Enter',
    'lobbyExit': '🚶 Lobby Exit',
    'sceneLoaded': '🎬 Scene Loaded',

    # Progress & Achievements
    'levelUp': '⬆️ Level Up',
    'achievementUnlocked': '🏆 Achievement Unlocked',
    'rewardClaimed': '🎁 Reward Claimed',
    'questCompleted': '📋 Quest Completed',

    # Purchases & Monetization
    'purchase': '💰 Purchase',
    'iapPurchase': '💳 In-App Purchase',
    'adWatched': '📺 Ad Watched',
    'adSkipped': '⏭️ Ad Skipped',

    # Social
    'shareClicked': '📤 Share Clicked',
    'inviteSent': '✉️ Invite Sent',

    # Settings
    'settingsChanged': '⚙️ Settings Changed',
    'languageChanged': '🌐 Language Changed',
    'soundToggled': '🔊 Sound Toggled',

    # Tutorial
    'tutorialStarted': '📖 Tutorial Started',
    'tutorialCompleted': '🎓 Tutorial Completed',
    'tutorialSkipped': '⏩ Tutorial Skipped',

    # Notifications
    'NotificationPermissionGranted': '🔔 Notification Permission',
}

def get_friendly_name(event_name):
    """Returns friendly action name"""
    return ACTION_NAMES.get(event_name, f'🎯 {event_name}')

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
    df = pd.DataFrame(data, columns=columns)
    # Convert numeric columns from Decimal/string to float
    for col in df.columns:
        if df[col].dtype == object:
            try:
                df[col] = pd.to_numeric(df[col], errors='ignore')
            except:
                pass
    return df

# Header
st.title("🎮 Bek va Lola Analytics")
st.caption("Game analytics powered by Unity Analytics")

# GAME_ID для Bek va Lola
GAME_ID = 181330318
DB = "UNITY_ANALYTICS_GCP_US_CENTRAL1_UNITY_ANALYTICS_PDA.SHARES"

# ============ SIDEBAR FILTERS ============
st.sidebar.header("🔧 Filters")

# Date Range
st.sidebar.subheader("📅 Date Range")
date_option = st.sidebar.selectbox(
    "Select period",
    ["Last 7 days", "Last 14 days", "Last 30 days", "Last 90 days", "Custom"],
    index=2
)

if date_option == "Custom":
    start_date = st.sidebar.date_input("Start date", datetime.now() - timedelta(days=30))
    end_date = st.sidebar.date_input("End date", datetime.now())
else:
    days_map = {
        "Last 7 days": 7,
        "Last 14 days": 14,
        "Last 30 days": 30,
        "Last 90 days": 90
    }
    days = days_map[date_option]
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)

start_str = start_date.strftime('%Y-%m-%d')
end_str = end_date.strftime('%Y-%m-%d')

st.sidebar.info(f"📆 {start_str} → {end_str}")

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

# Country Filter
st.sidebar.subheader("🌍 Country")
try:
    countries_df = run_query(f"""
        SELECT DISTINCT USER_COUNTRY
        FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY
        WHERE GAME_ID = {GAME_ID} AND USER_COUNTRY IS NOT NULL
        ORDER BY USER_COUNTRY
    """)
    countries_list = countries_df['USER_COUNTRY'].tolist()
    country_filter = st.sidebar.multiselect("Select countries", countries_list, default=countries_list)
    country_str = "','".join(country_filter)
except:
    country_filter = []
    country_str = ""

# Base WHERE clause
WHERE = f"""
WHERE GAME_ID = {GAME_ID}
AND EVENT_DATE BETWEEN '{start_str}' AND '{end_str}'
AND PLATFORM IN ('{platform_str}')
AND CLIENT_VERSION IN ('{version_str}')
AND USER_COUNTRY IN ('{country_str}')
"""

WHERE_EVENTS = f"""
WHERE GAME_ID = {GAME_ID}
AND EVENT_TIMESTAMP BETWEEN '{start_str}' AND '{end_str}'
"""

# Previous period for delta calculations
days_diff = (end_date - start_date).days if hasattr(end_date, 'days') else (datetime.combine(end_date, datetime.min.time()) - datetime.combine(start_date, datetime.min.time())).days
prev_end = start_date - timedelta(days=1)
prev_start = prev_end - timedelta(days=days_diff)
prev_start_str = prev_start.strftime('%Y-%m-%d') if hasattr(prev_start, 'strftime') else prev_start.strftime('%Y-%m-%d')
prev_end_str = prev_end.strftime('%Y-%m-%d') if hasattr(prev_end, 'strftime') else prev_end.strftime('%Y-%m-%d')

WHERE_PREV = f"""
WHERE GAME_ID = {GAME_ID}
AND EVENT_DATE BETWEEN '{prev_start_str}' AND '{prev_end_str}'
AND PLATFORM IN ('{platform_str}')
AND CLIENT_VERSION IN ('{version_str}')
AND USER_COUNTRY IN ('{country_str}')
"""

def calc_delta(current, previous):
    """Calculate percentage change between periods"""
    if previous and previous > 0:
        delta = ((current - previous) / previous) * 100
        return f"{delta:+.1f}%"
    return None

def export_to_csv(df, filename):
    """Convert dataframe to CSV for download"""
    return df.to_csv(index=False).encode('utf-8')

# ============ TABS ============
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📈 Overview",
    "🔄 Retention",
    "🎮 Mini-Games",
    "📊 Segments",
    "🎯 Player Actions"
])

# ============ TAB 1: OVERVIEW ============
with tab1:
    st.subheader("📈 Key Metrics")
    st.caption("Main player activity indicators for the selected period. Green/red shows change vs previous period.")

    with st.spinner("Loading metrics..."):
        col1, col2, col3, col4 = st.columns(4)

        # All Users with delta
        try:
            all_users = run_query(f"""
                SELECT COUNT(DISTINCT USER_ID) as TOTAL
                FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY {WHERE}
            """)
            all_users_prev = run_query(f"""
                SELECT COUNT(DISTINCT USER_ID) as TOTAL
                FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY {WHERE_PREV}
            """)
            current = int(all_users['TOTAL'][0])
            prev = int(all_users_prev['TOTAL'][0]) if not all_users_prev.empty else 0
            col1.metric("👥 Total Players", f"{current:,}", delta=calc_delta(current, prev), help="Unique players in period")
        except:
            col1.metric("👥 Total Players", "N/A")

        # DAU (average) with delta
        try:
            dau_avg = run_query(f"""
                SELECT ROUND(AVG(daily_users), 0) as AVG_DAU
                FROM (
                    SELECT EVENT_DATE, COUNT(DISTINCT USER_ID) as daily_users
                    FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY {WHERE}
                    GROUP BY EVENT_DATE
                )
            """)
            dau_avg_prev = run_query(f"""
                SELECT ROUND(AVG(daily_users), 0) as AVG_DAU
                FROM (
                    SELECT EVENT_DATE, COUNT(DISTINCT USER_ID) as daily_users
                    FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY {WHERE_PREV}
                    GROUP BY EVENT_DATE
                )
            """)
            current = int(dau_avg['AVG_DAU'][0])
            prev = int(dau_avg_prev['AVG_DAU'][0]) if not dau_avg_prev.empty and dau_avg_prev['AVG_DAU'][0] else 0
            col2.metric("📊 Avg DAU", f"{current:,}", delta=calc_delta(current, prev), help="Average daily active users")
        except:
            col2.metric("📊 Avg DAU", "N/A")

        # WAU
        try:
            wau = run_query(f"""
                SELECT COUNT(DISTINCT USER_ID) as WAU
                FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY {WHERE}
                AND EVENT_DATE >= DATEADD(day, -7, '{end_str}')
            """)
            col3.metric("📅 WAU", f"{wau['WAU'][0]:,}", help="Weekly active users")
        except:
            col3.metric("📅 WAU", "N/A")

        # MAU
        try:
            mau = run_query(f"""
                SELECT COUNT(DISTINCT USER_ID) as MAU
                FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY {WHERE}
            """)
            col4.metric("📆 MAU", f"{mau['MAU'][0]:,}", help="Monthly active users")
        except:
            col4.metric("📆 MAU", "N/A")

        # Second row metrics
        col5, col6, col7, col8 = st.columns(4)

        # New Users with delta
        try:
            new_users = run_query(f"""
                SELECT COUNT(DISTINCT USER_ID) as NEW_USERS
                FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY {WHERE}
                AND PLAYER_START_DATE BETWEEN '{start_str}' AND '{end_str}'
            """)
            new_users_prev = run_query(f"""
                SELECT COUNT(DISTINCT USER_ID) as NEW_USERS
                FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY {WHERE_PREV}
                AND PLAYER_START_DATE BETWEEN '{prev_start_str}' AND '{prev_end_str}'
            """)
            current = int(new_users['NEW_USERS'][0])
            prev = int(new_users_prev['NEW_USERS'][0]) if not new_users_prev.empty else 0
            col5.metric("🆕 New Players", f"{current:,}", delta=calc_delta(current, prev), help="Players who started in this period")
        except:
            col5.metric("🆕 New Players", "N/A")

        # Sessions per DAU
        try:
            spd = run_query(f"""
                SELECT ROUND(COUNT(DISTINCT SESSION_ID) * 1.0 / NULLIF(COUNT(DISTINCT USER_ID), 0), 2) as SPD
                FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY {WHERE}
            """)
            col6.metric("🔄 Sessions/Player", f"{spd['SPD'][0]}", help="Average sessions per player")
        except:
            col6.metric("🔄 Sessions/Player", "N/A")

        # Avg Session Length with delta
        try:
            session_len = run_query(f"""
                SELECT ROUND(AVG(TOTAL_TIME_MS) / 60000, 2) as AVG_MIN
                FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY {WHERE}
                AND TOTAL_TIME_MS > 0
            """)
            session_len_prev = run_query(f"""
                SELECT ROUND(AVG(TOTAL_TIME_MS) / 60000, 2) as AVG_MIN
                FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY {WHERE_PREV}
                AND TOTAL_TIME_MS > 0
            """)
            current = float(session_len['AVG_MIN'][0])
            prev = float(session_len_prev['AVG_MIN'][0]) if not session_len_prev.empty and session_len_prev['AVG_MIN'][0] else 0
            col7.metric("⏱️ Avg Session", f"{current} min", delta=calc_delta(current, prev), help="Average session duration")
        except:
            col7.metric("⏱️ Avg Session", "N/A")

        # Total Sessions with delta
        try:
            total_sessions = run_query(f"""
                SELECT COUNT(DISTINCT SESSION_ID) as SESSIONS
                FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY {WHERE}
            """)
            total_sessions_prev = run_query(f"""
                SELECT COUNT(DISTINCT SESSION_ID) as SESSIONS
                FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY {WHERE_PREV}
            """)
            current = int(total_sessions['SESSIONS'][0])
            prev = int(total_sessions_prev['SESSIONS'][0]) if not total_sessions_prev.empty else 0
            col8.metric("🎮 Total Sessions", f"{current:,}", delta=calc_delta(current, prev), help="Total game sessions")
        except:
            col8.metric("🎮 Total Sessions", "N/A")

        # Third row
        col9, col10, col11, col12 = st.columns(4)

        # Actions per User
        try:
            epu = run_query(f"""
                SELECT ROUND(COUNT(*) * 1.0 / NULLIF(COUNT(DISTINCT USER_ID), 0), 1) as EPU
                FROM {DB}.ACCOUNT_EVENTS {WHERE_EVENTS}
            """)
            col9.metric("🎯 Actions/Player", f"{epu['EPU'][0]}", help="Average actions per player")
        except:
            col9.metric("🎯 Actions/Player", "N/A")

        # Total Actions
        try:
            total_events = run_query(f"""
                SELECT COUNT(*) as EVENTS
                FROM {DB}.ACCOUNT_EVENTS {WHERE_EVENTS}
            """)
            col10.metric("📊 Total Actions", f"{total_events['EVENTS'][0]:,}", help="Total in-game actions")
        except:
            col10.metric("📊 Total Actions", "N/A")

        # Stickiness
        try:
            if mau['MAU'][0] > 0 and dau_avg['AVG_DAU'][0]:
                stickiness = round(dau_avg['AVG_DAU'][0] / mau['MAU'][0] * 100, 1)
                col11.metric("📌 Stickiness", f"{stickiness}%", help="DAU/MAU - how often players return")
            else:
                col11.metric("📌 Stickiness", "N/A")
        except:
            col11.metric("📌 Stickiness", "N/A")

        # Actions per Session
        try:
            eps = run_query(f"""
                SELECT ROUND(AVG(NUMBER_OF_EVENTS), 1) as EPS
                FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY {WHERE}
            """)
        col12.metric("🔢 Actions/Session", f"{eps['EPS'][0]}", help="Average actions per session")
    except:
        col12.metric("🔢 Actions/Session", "N/A")

    st.markdown("---")

    # DAU Chart
    st.subheader("📊 Daily Active Users (DAU)")
    st.caption("Unique players who played each day")
    with st.spinner("Loading DAU chart..."):
        try:
            dau_df = run_query(f"""
                SELECT EVENT_DATE, COUNT(DISTINCT USER_ID) as USERS
                FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY {WHERE}
                GROUP BY EVENT_DATE ORDER BY EVENT_DATE
            """)
            if not dau_df.empty:
                st.line_chart(dau_df.set_index('EVENT_DATE')['USERS'])
                st.download_button(
                    "📥 Export DAU to CSV",
                    export_to_csv(dau_df, "dau"),
                    "dau_data.csv",
                    "text/csv",
                    key="download_dau"
                )
        except:
            st.info("No data available")

    # New Users Chart
    st.subheader("👥 Daily New Players")
    st.caption("Players who launched the game for the first time")
    with st.spinner("Loading new players chart..."):
        try:
            new_df = run_query(f"""
                SELECT PLAYER_START_DATE as DATE, COUNT(DISTINCT USER_ID) as NEW_USERS
                FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY {WHERE}
                AND PLAYER_START_DATE BETWEEN '{start_str}' AND '{end_str}'
                GROUP BY PLAYER_START_DATE ORDER BY PLAYER_START_DATE
            """)
            if not new_df.empty:
                st.line_chart(new_df.set_index('DATE')['NEW_USERS'])
                st.download_button(
                    "📥 Export New Players to CSV",
                    export_to_csv(new_df, "new_players"),
                    "new_players_data.csv",
                    "text/csv",
                    key="download_new_players"
                )
        except:
            st.info("No data available")

    # Session Length
    st.subheader("⏱️ Average Session Length (minutes)")
    st.caption("How long players spend in the game per session")
    with st.spinner("Loading session chart..."):
        try:
            sess_df = run_query(f"""
                SELECT EVENT_DATE, ROUND(AVG(TOTAL_TIME_MS) / 60000, 2) as AVG_MIN
                FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY {WHERE}
                AND TOTAL_TIME_MS > 0
                GROUP BY EVENT_DATE ORDER BY EVENT_DATE
            """)
            if not sess_df.empty:
                st.line_chart(sess_df.set_index('EVENT_DATE')['AVG_MIN'])
                st.download_button(
                    "📥 Export Session Length to CSV",
                    export_to_csv(sess_df, "sessions"),
                    "session_length_data.csv",
                    "text/csv",
                    key="download_sessions"
                )
        except:
            st.info("No data available")

# ============ TAB 2: RETENTION ============
with tab2:
    st.subheader("🔄 Player Retention Analysis")
    st.caption("What percentage of players return to the game N days after first launch")

    with st.spinner("Loading retention metrics..."):
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
        ret_val = float(d1['RET'][0]) if d1['RET'][0] is not None else 0
        col_ret1.metric("📅 Day 1", f"{ret_val}%", help="Returned next day")
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
        ret_val = float(d7['RET'][0]) if d7['RET'][0] is not None else 0
        col_ret2.metric("📅 Day 7", f"{ret_val}%", help="Returned after a week")
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
        ret_val = float(d14['RET'][0]) if d14['RET'][0] is not None else 0
        col_ret3.metric("📅 Day 14", f"{ret_val}%", help="Returned after 2 weeks")
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
        ret_val = float(d30['RET'][0]) if d30['RET'][0] is not None else 0
        col_ret4.metric("📅 Day 30", f"{ret_val}%", help="Returned after a month")
    except:
        col_ret4.metric("📅 Day 30", "N/A")

    st.markdown("---")

    # Retention Curve
    st.subheader("📉 Retention Curve")
    st.caption("Shows how the percentage of active players changes each day after first launch. Day 0 = 100% (all new players)")

    with st.spinner("Loading retention curve..."):
        try:
            retention_df = run_query(f"""
                WITH first_day AS (
                    SELECT USER_ID, MIN(EVENT_DATE) as first_date
                    FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY
                    WHERE GAME_ID = {GAME_ID}
                    GROUP BY USER_ID
                ),
                cohort_size AS (
                    SELECT COUNT(DISTINCT USER_ID) as total_users
                    FROM first_day
                    WHERE first_date BETWEEN '{start_str}' AND DATEADD(day, -30, '{end_str}')
                ),
                user_activity AS (
                    SELECT
                        f.USER_ID,
                        DATEDIFF(day, f.first_date, s.EVENT_DATE) as days_since_start
                    FROM first_day f
                    JOIN {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY s
                        ON f.USER_ID = s.USER_ID AND s.GAME_ID = {GAME_ID}
                    WHERE f.first_date BETWEEN '{start_str}' AND DATEADD(day, -30, '{end_str}')
                )
                SELECT
                    days_since_start as DAY,
                    ROUND(COUNT(DISTINCT USER_ID) * 100.0 / (SELECT total_users FROM cohort_size), 1) as RETENTION
                FROM user_activity
                WHERE days_since_start BETWEEN 0 AND 30
                GROUP BY days_since_start
                ORDER BY days_since_start
            """)
            if not retention_df.empty and len(retention_df) > 1:
                # Ensure numeric types for chart
                retention_df['DAY'] = pd.to_numeric(retention_df['DAY'], errors='coerce')
                retention_df['RETENTION'] = pd.to_numeric(retention_df['RETENTION'], errors='coerce')
                retention_df = retention_df.dropna()
                st.line_chart(retention_df.set_index('DAY')['RETENTION'])

                col_exp, col_dl = st.columns([3, 1])
                with col_exp:
                    with st.expander("📊 Retention Curve Details"):
                        st.dataframe(
                            retention_df.rename(columns={'DAY': 'Day', 'RETENTION': 'Retention %'}),
                            use_container_width=True,
                            hide_index=True
                        )
                with col_dl:
                    st.download_button(
                        "📥 Export Retention to CSV",
                        export_to_csv(retention_df, "retention"),
                        "retention_data.csv",
                        "text/csv",
                        key="download_retention"
                    )
            else:
                st.info("Not enough data to build retention curve. Try selecting a wider date range.")
        except Exception as e:
            st.info("Not enough data to build retention curve")

    st.markdown("---")

    # Retention tips
    with st.expander("💡 How to Read Retention Metrics"):
        st.markdown("""
        **Retention** shows what percentage of players returned to the game N days after their first launch.

        - **Day 1 (D1)**: Good benchmark: 40%+. Critical for casual games.
        - **Day 7 (D7)**: Good benchmark: 15-20%+. Shows long-term interest.
        - **Day 30 (D30)**: Good benchmark: 5-10%+. Shows loyalty.

        **Retention Curve** helps you see:
        - Where main player churn happens
        - When the curve stabilizes (core audience)
        - Onboarding effectiveness (first 3 days)
        """)

# ============ TAB 3: MINI-GAMES ============
with tab3:
    st.subheader("🎮 Mini-Games Statistics")
    st.caption("Analysis of mini-game plays by players")

    with st.spinner("Loading mini-games data..."):
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
                mg_display = mg_df.rename(columns={
                    'MINI_GAME': 'Mini-Game',
                    'PLAYS': 'Plays',
                    'AVG_DURATION_SEC': 'Avg Duration (sec)',
                    'COMPLETION_RATE': 'Completion %'
                })
                st.dataframe(mg_display, use_container_width=True, hide_index=True)
                st.download_button(
                    "📥 Export Mini-Games to CSV",
                    export_to_csv(mg_df, "minigames"),
                    "minigames_data.csv",
                    "text/csv",
                    key="download_minigames"
                )

                col_mg1, col_mg2 = st.columns(2)
                with col_mg1:
                    st.subheader("🎯 Mini-Game Popularity")
                    st.caption("How many times each mini-game was played")
                    st.bar_chart(mg_df.set_index('MINI_GAME')['PLAYS'])
                with col_mg2:
                    st.subheader("✅ Completion Rate")
                    st.caption("Percentage of players who completed the mini-game")
                    st.bar_chart(mg_df.set_index('MINI_GAME')['COMPLETION_RATE'])
            else:
                st.info("No mini-game data for selected period")
        except:
            st.info("No mini-game data available")

    st.markdown("---")

    # Lobby Actions
    st.subheader("🏠 Lobby Actions")
    st.caption("What players do in the main menu")
    with st.spinner("Loading lobby actions..."):
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
                lobby_display = lobby_df.rename(columns={
                    'ACTION': 'Action',
                    'COUNT': 'Count',
                    'COMPLETION_RATE': 'Completion %'
                })
                st.dataframe(lobby_display, use_container_width=True, hide_index=True)
                st.download_button(
                    "📥 Export Lobby Actions to CSV",
                    export_to_csv(lobby_df, "lobby"),
                    "lobby_actions_data.csv",
                    "text/csv",
                    key="download_lobby"
                )
                st.bar_chart(lobby_df.set_index('ACTION')['COUNT'])
            else:
                st.info("No lobby action data available")
        except:
            st.info("No lobby action data available")

# ============ TAB 4: BREAKDOWNS ============
with tab4:
    st.subheader("📊 Player Segments")
    st.caption("Audience breakdown by various parameters")

    col_l, col_r = st.columns(2)

    with col_l:
        st.subheader("📱 By Platform")
        st.caption("Player distribution between Android and iOS")
        try:
            p_df = run_query(f"""
                SELECT PLATFORM, COUNT(DISTINCT USER_ID) as PLAYERS
                FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY {WHERE}
                GROUP BY PLATFORM
            """)
            if not p_df.empty:
                st.bar_chart(p_df.set_index('PLATFORM')['PLAYERS'])
        except:
            st.info("No data available")

    with col_r:
        st.subheader("📦 By App Version")
        st.caption("Which versions players are using")
        try:
            v_df = run_query(f"""
                SELECT CLIENT_VERSION as VERSION, COUNT(DISTINCT USER_ID) as PLAYERS
                FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY {WHERE}
                GROUP BY CLIENT_VERSION ORDER BY PLAYERS DESC
            """)
            if not v_df.empty:
                st.bar_chart(v_df.set_index('VERSION')['PLAYERS'])
        except:
            st.info("No data available")

    st.markdown("---")

    st.subheader("🌍 By Country (Top 10)")
    st.caption("Geographic distribution of players")
    try:
        c_df = run_query(f"""
            SELECT USER_COUNTRY as COUNTRY, COUNT(DISTINCT USER_ID) as PLAYERS
            FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY {WHERE}
            GROUP BY USER_COUNTRY ORDER BY PLAYERS DESC LIMIT 10
        """)
        if not c_df.empty:
            st.bar_chart(c_df.set_index('COUNTRY')['PLAYERS'])
    except:
        st.info("No data available")

    st.markdown("---")

    col_h, col_d = st.columns(2)

    with col_h:
        st.subheader("🕐 Activity by Hour")
        st.caption("When players are most active during the day (UTC +5)")
        try:
            h_df = run_query(f"""
    SELECT
        HOUR(DATEADD(hour, 5, EVENT_TIMESTAMP)) as HOUR,
        COUNT(*) as ACTIONS
    FROM {DB}.ACCOUNT_EVENTS {WHERE_EVENTS}
    GROUP BY HOUR(DATEADD(hour, 5, EVENT_TIMESTAMP))
    ORDER BY HOUR
"""
)
            if not h_df.empty:
                st.bar_chart(h_df.set_index('HOUR')['ACTIONS'])
        except:
            st.info("No data available")

    with col_d:
        st.subheader("📅 Activity by Day of Week")
        st.caption("Which days players play more")
        try:
            dow_df = run_query(f"""
                SELECT
                    CASE DAYOFWEEK(EVENT_DATE)
                        WHEN 0 THEN 'Sun' WHEN 1 THEN 'Mon' WHEN 2 THEN 'Tue'
                        WHEN 3 THEN 'Wed' WHEN 4 THEN 'Thu' WHEN 5 THEN 'Fri' WHEN 6 THEN 'Sat'
                    END as DAY,
                    DAYOFWEEK(EVENT_DATE) as day_num,
                    COUNT(DISTINCT USER_ID) as PLAYERS
                FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY {WHERE}
                GROUP BY DAYOFWEEK(EVENT_DATE) ORDER BY day_num
            """)
            if not dow_df.empty:
                st.bar_chart(dow_df.set_index('DAY')['PLAYERS'])
        except:
            st.info("No data available")

# ============ TAB 5: PLAYER ACTIONS ============
with tab5:
    st.subheader("🎯 In-Game Player Actions")
    st.caption("All player interactions: launches, completions, purchases, and more")

    try:
        e_df = run_query(f"""
            SELECT EVENT_NAME, COUNT(*) as COUNT
            FROM {DB}.ACCOUNT_EVENTS {WHERE_EVENTS}
            GROUP BY EVENT_NAME ORDER BY COUNT DESC LIMIT 20
        """)
        if not e_df.empty:
            e_df['Action'] = e_df['EVENT_NAME'].apply(get_friendly_name)
            e_df['Total'] = e_df['COUNT']

            st.dataframe(
                e_df[['Action', 'EVENT_NAME', 'Total']].rename(columns={'EVENT_NAME': 'Event Code'}),
                use_container_width=True,
                hide_index=True
            )

            st.subheader("📊 Top Player Actions")
            st.bar_chart(e_df.set_index('Action')['Total'])
    except:
        st.info("No action data available")

    st.markdown("---")

    st.subheader("🔍 Action Deep Dive")
    st.caption("Select an action to view daily trends")

    try:
        event_names = run_query(f"""
            SELECT DISTINCT EVENT_NAME
            FROM {DB}.ACCOUNT_EVENTS {WHERE_EVENTS}
            ORDER BY EVENT_NAME
        """)
        if not event_names.empty:
            event_options = {get_friendly_name(name): name for name in event_names['EVENT_NAME'].tolist()}
            selected_friendly = st.selectbox("Select action", list(event_options.keys()))
            selected_event = event_options[selected_friendly]

            if selected_event:
                event_detail = run_query(f"""
                    SELECT
                        DATE(EVENT_TIMESTAMP) as DATE,
                        COUNT(*) as COUNT,
                        COUNT(DISTINCT USER_ID) as UNIQUE_USERS
                    FROM {DB}.ACCOUNT_EVENTS
                    WHERE GAME_ID = {GAME_ID}
                    AND EVENT_NAME = '{selected_event}'
                    AND EVENT_TIMESTAMP BETWEEN '{start_str}' AND '{end_str}'
                    GROUP BY DATE(EVENT_TIMESTAMP)
                    ORDER BY DATE
                """)
                if not event_detail.empty:
                    col_e1, col_e2 = st.columns(2)
                    with col_e1:
                        st.metric("📊 Total Count", f"{event_detail['COUNT'].sum():,}", help="How many times this action occurred")
                    with col_e2:
                        st.metric("👥 Unique Players", f"{event_detail['UNIQUE_USERS'].sum():,}", help="How many different players performed this action")

                    st.subheader(f"📈 Trend: {selected_friendly}")
                    st.line_chart(event_detail.set_index('DATE')['COUNT'])
    except:
        st.info("No data available")

    st.markdown("---")

    # Notification Permission Analysis
    st.subheader("🔔 Notification Permission")
    st.caption("How players respond to notification permission request")

    try:
        notif_df = run_query(f"""
            SELECT
                EVENT_JSON:permissionGranted::INT as GRANTED,
                COUNT(*) as COUNT,
                COUNT(DISTINCT USER_ID) as UNIQUE_USERS
            FROM {DB}.ACCOUNT_EVENTS
            WHERE GAME_ID = {GAME_ID}
            AND EVENT_NAME = 'NotificationPermissionGranted'
            AND EVENT_TIMESTAMP BETWEEN '{start_str}' AND '{end_str}'
            GROUP BY EVENT_JSON:permissionGranted::INT
        """)

        if not notif_df.empty:
            col_n1, col_n2, col_n3 = st.columns(3)

            # Total requests
            total_count = int(notif_df['COUNT'].sum())
            total_users = int(notif_df['UNIQUE_USERS'].sum())

            # Granted (1)
            granted_row = notif_df[notif_df['GRANTED'] == 1]
            granted_count = int(granted_row['COUNT'].iloc[0]) if not granted_row.empty else 0
            granted_users = int(granted_row['UNIQUE_USERS'].iloc[0]) if not granted_row.empty else 0

            # Denied (0)
            denied_row = notif_df[notif_df['GRANTED'] == 0]
            denied_count = int(denied_row['COUNT'].iloc[0]) if not denied_row.empty else 0
            denied_users = int(denied_row['UNIQUE_USERS'].iloc[0]) if not denied_row.empty else 0

            # Calculate rate
            grant_rate = round(granted_count * 100 / total_count, 1) if total_count > 0 else 0

            with col_n1:
                st.metric("📊 Total Requests", f"{total_count:,}", help="Total notification permission requests")
                st.metric("👥 Unique Players", f"{total_users:,}")

            with col_n2:
                st.metric("✅ Granted", f"{granted_count:,}", help="Players who allowed notifications")
                st.metric("👥 Unique Players", f"{granted_users:,}")

            with col_n3:
                st.metric("❌ Denied", f"{denied_count:,}", help="Players who denied notifications")
                st.metric("👥 Unique Players", f"{denied_users:,}")

            st.markdown(f"**Grant Rate: {grant_rate}%** of players allowed notifications")

            # Daily trend
            notif_trend = run_query(f"""
                SELECT
                    DATE(EVENT_TIMESTAMP) as DATE,
                    SUM(CASE WHEN EVENT_JSON:permissionGranted::INT = 1 THEN 1 ELSE 0 END) as GRANTED,
                    SUM(CASE WHEN EVENT_JSON:permissionGranted::INT = 0 THEN 1 ELSE 0 END) as DENIED
                FROM {DB}.ACCOUNT_EVENTS
                WHERE GAME_ID = {GAME_ID}
                AND EVENT_NAME = 'NotificationPermissionGranted'
                AND EVENT_TIMESTAMP BETWEEN '{start_str}' AND '{end_str}'
                GROUP BY DATE(EVENT_TIMESTAMP)
                ORDER BY DATE
            """)

            if not notif_trend.empty:
                st.subheader("📈 Daily Notification Permission Trend")
                st.line_chart(notif_trend.set_index('DATE')[['GRANTED', 'DENIED']])
        else:
            st.info("No notification permission data for selected period")
    except:
        st.info("No notification permission data available")

    # Action reference
    with st.expander("📖 Action Reference"):
        st.markdown("""
        **What player actions mean:**

        | Action | Description |
        |--------|-------------|
        | 🚀 Game Start | Player started a new game session |
        | 🎮 Mini-game Played | Player played one of the mini-games |
        | 🏠 Lobby Action | Player interacted with main menu elements |
        | ⬆️ Level Up | Player reached a new level |
        | 🏆 Achievement Unlocked | Player unlocked an achievement |
        | 💰 Purchase | Player made a purchase |
        | 📺 Ad Watched | Player watched an ad |
        """)

st.markdown("---")
st.caption("📊 Data: Unity Analytics via Snowflake | 🎮 Bek va Lola")
