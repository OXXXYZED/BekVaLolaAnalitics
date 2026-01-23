import streamlit as st
import pandas as pd
import altair as alt
from datetime import datetime, timedelta
from decimal import Decimal
import snowflake.connector

# Sahifa sozlamalari
st.set_page_config(
    page_title="Bek va Lola - Statistika",
    page_icon="📊",
    layout="wide"
)

# Ranglar
COLORS = {
    'primary': '#FF8C42',      # Orange for new users
    'secondary': '#4A90D9',    # Blue for sessions
    'success': '#7CB342',      # Green for comparison
    'danger': '#E57373',       # Red for line charts
    'light': '#F5F5F5',
    'dark': '#333333'
}

# Check secrets exist
if "snowflake" not in st.secrets:
    st.error("Snowflake credentials topilmadi. Iltimos, secrets ni sozlang.")
    st.stop()

# Mini o'yinlar nomlari
MINIGAME_NAMES = {
    'AstroBek': "Astrobek",
    'Badantarbiya': "Badantarbiya",
    'HiddeAndSikLolaRoom': "Berkinmachoq",
    'Market': "Bozor",
    'Shapes': "Shakllar",
    'NumbersShape': "Raqamlar",
    'Words': "So'zlar",
    'MapMatchGame': "Xarita",
    'FindHiddenLetters': "Yashirin harflar",
    'RocketGame': "Raketa",
    'TacingLetter': "Harflar yozish",
    'Baroqvoy': "Baroqvoy",
    'Ballons': "Sharlar",
    'HygieneTeath': "Tish tozalash",
    'HygieneHand': "Qo'l yuvish",
    'BasketBall': "Basketbol",
    'FootBall': "Futbol",
}

def get_minigame_name(name):
    """Mini o'yin nomini qaytaradi"""
    if name is None:
        return "Noma'lum"
    if name in MINIGAME_NAMES:
        return MINIGAME_NAMES[name]
    return name

# Snowflake ulanish
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

    # Decimal to float conversion
    for col in df.columns:
        if df[col].dtype == object:
            if df[col].apply(lambda x: isinstance(x, Decimal)).any():
                df[col] = df[col].apply(lambda x: float(x) if isinstance(x, Decimal) else x)
            try:
                numeric_col = pd.to_numeric(df[col], errors='coerce')
                if not numeric_col.isna().all():
                    df[col] = numeric_col
            except:
                pass
    return df

# GAME_ID va Database
GAME_ID = 181330318
DB = "UNITY_ANALYTICS_GCP_US_CENTRAL1_UNITY_ANALYTICS_PDA.SHARES"

# CSS for wider charts
st.markdown("""
<style>
    .block-container {
        padding-left: 2rem;
        padding-right: 2rem;
        max-width: 100%;
    }
    [data-testid="stMetricValue"] {
        font-size: 1.8rem;
    }
</style>
""", unsafe_allow_html=True)

# ============ SARLAVHA ============
st.title("📊 Bek va Lola - Statistika")

# ============ UMUMIY KO'RSATKICHLAR ============
st.markdown("---")

col1, col2, col3 = st.columns(3)

# Jami foydalanuvchilar
try:
    total_users = run_query(f"""
        SELECT COUNT(DISTINCT USER_ID) as TOTAL
        FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY
        WHERE GAME_ID = {GAME_ID}
    """)
    col1.metric(
        "👥 Foydalanuvchilar",
        f"{total_users['TOTAL'][0]:,}",
        help="Jami yuklab olishlar soni"
    )
except:
    col1.metric("👥 Foydalanuvchilar", "N/A")

# Mini o'yinlar soni
col2.metric(
    "🎮 Mini o'yinlar soni",
    f"{len(MINIGAME_NAMES)}",
    help="Ilovadagi mini o'yinlar soni"
)

# So'nggi yangilanish
try:
    last_update = run_query(f"""
        SELECT MAX(EVENT_DATE) as LAST_DATE
        FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY
        WHERE GAME_ID = {GAME_ID}
    """)
    last_date = last_update['LAST_DATE'][0]
    if last_date:
        col3.metric(
            "📅 So'nggi yangilanish",
            "15.01.2026",
            help="Oxirgi ma'lumotlar sanasi"
        )
    else:
        col3.metric("📅 So'nggi yangilanish", "N/A")
except:
    col3.metric("📅 So'nggi yangilanish", "N/A")

# ============ PLATFORMALAR BO'YICHA TAQSIMOT ============
st.markdown("---")
st.subheader("📱 Platformalar bo'yicha taqsimot")

try:
    platform_df = run_query(f"""
        SELECT
            CASE
                WHEN PLATFORM = 'ANDROID' THEN 'Android'
                WHEN PLATFORM = 'IOS' THEN 'iOS'
                ELSE 'Boshqalar'
            END AS PLATFORM,
            COUNT(DISTINCT USER_ID) AS USERS
        FROM {DB}.ACCOUNT_FACT_USER_SESSIONS_DAY
        WHERE GAME_ID = {GAME_ID}
        GROUP BY PLATFORM
        ORDER BY USERS DESC
    """)

    if not platform_df.empty:
        total = platform_df['USERS'].sum()
        platform_df['PERCENT'] = round(platform_df['USERS'] / total * 100, 1)

        col_pie, col_legend = st.columns([2, 1])

        with col_pie:
            pie_chart = alt.Chart(platform_df).mark_arc(innerRadius=50).encode(
                theta=alt.Theta(field="USERS", type="quantitative"),
                color=alt.Color(
                    field="PLATFORM",
                    type="nominal",
                    scale=alt.Scale(
                        domain=['Android', 'iOS', 'Boshqalar'],
                        range=[COLORS['success'], COLORS['secondary'], '#9E9E9E']
                    ),
                    legend=None
                ),
                tooltip=[
                    alt.Tooltip('PLATFORM:N', title='Platforma'),
                    alt.Tooltip('USERS:Q', title="Foydalanuvchilar", format=','),
                    alt.Tooltip('PERCENT:Q', title='Foiz', format='.1f')
                ]
            ).properties(height=300)

            st.altair_chart(pie_chart, use_container_width=True)

        with col_legend:
            st.markdown("### ")
            for _, row in platform_df.iterrows():
                platform = row['PLATFORM']
                users = row['USERS']
                percent = row['PERCENT']

                if platform == 'Android':
                    color = COLORS['success']
                elif platform == 'iOS':
                    color = COLORS['secondary']
                else:
                    color = '#9E9E9E'

                st.markdown(f"""
                <div style="display: flex; align-items: center; margin-bottom: 15px;">
                    <div style="width: 20px; height: 20px; background-color: {color};
                        border-radius: 4px; margin-right: 10px;"></div>
                    <div>
                        <strong>{platform}</strong><br>
                        <span style="color: #666;">{users:,} ({percent}%)</span>
                    </div>
                </div>
                """, unsafe_allow_html=True)

except Exception as e:
    st.info("Ma'lumotlar mavjud emas")


# ============ YANGI FOYDALANUVCHILAR ============
st.markdown("---")
st.subheader("👥 Yangi foydalanuvchilar")

# Filtrlar
col_filter1, col_filter2, col_filter3 = st.columns([1, 2, 4])

with col_filter1:
    period_type = st.selectbox(
        "Davr",
        ["Kunlik", "Haftalik", "Oylik"],
        key="new_users_period"
    )

with col_filter2:
    date_range = st.date_input(
        "Sana oralig'i",
        value=(datetime.now() - timedelta(days=30), datetime.now()),
        key="new_users_date"
    )

if len(date_range) == 2:
    start_date, end_date = date_range
    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')

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
        else:  # Oylik
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
            # Use ordinal scale for evenly spaced bars
            new_users_df['SANA'] = pd.to_datetime(new_users_df['SANA'])
            new_users_df['SANA_STR'] = new_users_df['SANA'].dt.strftime('%Y-%m-%d')

            chart = alt.Chart(new_users_df).mark_bar(
                color=COLORS['primary'],
                cornerRadiusTopLeft=3,
                cornerRadiusTopRight=3
            ).encode(
                x=alt.X('SANA_STR:O', title='Sana', axis=alt.Axis(labelAngle=-45), sort=None),
                y=alt.Y('YANGI_USERS:Q', title='Yangi foydalanuvchilar'),
                tooltip=[
                    alt.Tooltip('SANA_STR:O', title='Sana'),
                    alt.Tooltip('YANGI_USERS:Q', title='Yangi foydalanuvchilar', format=',')
                ]
            ).properties(height=400)
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("Tanlangan davr uchun ma'lumotlar mavjud emas")
    except Exception as e:
        st.info("Ma'lumotlarni yuklashda xatolik")

# ============ FOYDALANISH SESSIYALARI ============
st.markdown("---")
st.subheader("📈 Foydalanish sessiyalari")

# Filtrlar
col_sess1, col_sess2, col_sess3 = st.columns([1, 1, 5])

with col_sess1:
    session_view = st.selectbox(
        "Ko'rinish",
        ["Kunlik", "Soatlik"],
        key="session_view"
    )

with col_sess2:
    if session_view == "Soatlik":
        session_date = st.date_input(
            "Sana",
            value=datetime.now(),
            key="session_date"
        )
    else:
        session_period = st.selectbox(
            "Davr",
            ["So'nggi 7 kun", "So'nggi 14 kun", "So'nggi 30 kun"],
            key="session_period"
        )

try:
    if session_view == "Soatlik":
        date_str = session_date.strftime('%Y-%m-%d')
        # Use ACCOUNT_EVENTS table - show events count by hour
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
            # Convert to proper types
            sessions_df['SOAT'] = pd.to_numeric(sessions_df['SOAT'], errors='coerce').fillna(0).astype(int)
            sessions_df['HODISALAR'] = pd.to_numeric(sessions_df['HODISALAR'], errors='coerce').fillna(0).astype(int)
            sessions_df['FOYDALANUVCHILAR'] = pd.to_numeric(sessions_df['FOYDALANUVCHILAR'], errors='coerce').fillna(0).astype(int)

            # Soatlarni formatlash
            sessions_df['SOAT_LABEL'] = sessions_df['SOAT'].apply(lambda x: f"{x:02d}:00")

            # Simple bar chart - just show events by hour
            chart = alt.Chart(sessions_df).mark_bar(
                cornerRadiusTopLeft=3,
                cornerRadiusTopRight=3,
                color=COLORS['secondary']
            ).encode(
                x=alt.X('SOAT_LABEL:N', title='Soat', sort=None, axis=alt.Axis(labelAngle=0)),
                y=alt.Y('HODISALAR:Q', title='Hodisalar soni'),
                tooltip=[
                    alt.Tooltip('SOAT_LABEL:N', title='Soat'),
                    alt.Tooltip('HODISALAR:Q', title='Hodisalar', format=','),
                    alt.Tooltip('FOYDALANUVCHILAR:Q', title='Foydalanuvchilar', format=',')
                ]
            ).properties(height=400)
            st.altair_chart(chart, use_container_width=True)

            # Show metrics
            col_h1, col_h2 = st.columns(2)
            col_h1.metric("Jami hodisalar", f"{sessions_df['HODISALAR'].sum():,}")
            col_h2.metric("Faol foydalanuvchilar", f"{sessions_df['FOYDALANUVCHILAR'].sum():,}")
        else:
            st.info("Tanlangan sana uchun ma'lumotlar mavjud emas")
    else:
        # Kunlik ko'rinish
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
            AND EVENT_DATE BETWEEN '{start_date.strftime('%Y-%m-%d')}' AND '{end_date.strftime('%Y-%m-%d')}'
            GROUP BY EVENT_DATE
            ORDER BY EVENT_DATE
        """)

        if not sessions_df.empty:
            # Metrics row
            avg_sessions = int(sessions_df['SESSIYALAR'].mean())
            avg_duration = round(sessions_df['ORTACHA_DAVOMIYLIK'].mean(), 1)
            total_sessions = int(sessions_df['SESSIYALAR'].sum())

            col_m1, col_m2, col_m3 = st.columns(3)
            col_m1.metric("Jami sessiyalar", f"{total_sessions:,}")
            col_m2.metric("O'rtacha kunlik", f"{avg_sessions:,}")
            col_m3.metric("O'rtacha davomiylik", f"{avg_duration} daqiqa")

            # Full width chart - use ordinal scale for evenly spaced bars
            sessions_df['SANA'] = pd.to_datetime(sessions_df['SANA'])
            sessions_df['SANA_STR'] = sessions_df['SANA'].dt.strftime('%Y-%m-%d')

            chart = alt.Chart(sessions_df).mark_bar(
                color=COLORS['secondary'],
                cornerRadiusTopLeft=3,
                cornerRadiusTopRight=3
            ).encode(
                x=alt.X('SANA_STR:O', title='Sana', axis=alt.Axis(labelAngle=-45), sort=None),
                y=alt.Y('SESSIYALAR:Q', title='Sessiyalar soni'),
                tooltip=[
                    alt.Tooltip('SANA_STR:O', title='Sana'),
                    alt.Tooltip('SESSIYALAR:Q', title='Sessiyalar', format=',')
                ]
            ).properties(height=400)
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("Ma'lumotlar mavjud emas")
except Exception as e:
    st.info("Ma'lumotlarni yuklashda xatolik")

# ============ MINI O'YINLAR STATISTIKASI ============
st.markdown("---")
st.subheader("🎮 Mini o'yinlar statistikasi")

# Filtrlar
col_mg1, col_mg2, col_mg3 = st.columns([1, 2, 4])

with col_mg1:
    try:
        mg_list = run_query(f"""
            SELECT DISTINCT EVENT_JSON:MiniGameName::STRING as MINI_GAME
            FROM {DB}.ACCOUNT_EVENTS
            WHERE GAME_ID = {GAME_ID} AND EVENT_NAME = 'playedMiniGameStatus'
            AND EVENT_JSON:MiniGameName::STRING IS NOT NULL
        """)
        mg_options = ["Barchasi"] + [get_minigame_name(mg) for mg in mg_list['MINI_GAME'].tolist() if mg]
        mg_original = {get_minigame_name(mg): mg for mg in mg_list['MINI_GAME'].tolist() if mg}
        selected_mg = st.selectbox("Mini o'yin", mg_options, key="mg_filter")
    except:
        selected_mg = "Barchasi"
        mg_original = {}

with col_mg2:
    mg_date_range = st.date_input(
        "Sana oralig'i",
        value=(datetime.now() - timedelta(days=30), datetime.now()),
        key="mg_date"
    )

if len(mg_date_range) == 2:
    mg_start, mg_end = mg_date_range
    mg_start_str = mg_start.strftime('%Y-%m-%d')
    mg_end_str = mg_end.strftime('%Y-%m-%d')

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
            num_points = len(mg_stats)
            chart = alt.Chart(mg_stats).mark_line(
                color=COLORS['danger'],
                strokeWidth=3,
                point=alt.OverlayMarkDef(color=COLORS['danger'], size=80, filled=True)
            ).encode(
                x=alt.X('SANA:T', title='Sana', axis=alt.Axis(
                    format='%Y-%m-%d',
                    labelAngle=-45,
                    tickCount=min(num_points, 15)
                )),
                y=alt.Y('OYINLAR:Q', title="O'yinlar soni"),
                tooltip=[
                    alt.Tooltip('SANA:T', title='Sana', format='%Y-%m-%d'),
                    alt.Tooltip('OYINLAR:Q', title="O'yinlar", format=',')
                ]
            ).properties(height=400)
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("Tanlangan davr uchun ma'lumotlar mavjud emas")
    except:
        st.info("Ma'lumotlarni yuklashda xatolik")

# ============ RETENTION RATE ============
st.markdown("---")
st.subheader("🔄 Retention rate")

col_r1, col_r2, col_r3 = st.columns(3)

# D1 Retention
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
    ret_val = float(d1['RET'][0]) if d1['RET'][0] is not None else 0
    col_r1.metric("📅 D1 retention", f"{ret_val}%", help="Keyingi kuni qaytganlar")
except:
    col_r1.metric("📅 D1 retention", "N/A")

# D7 Retention
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
    ret_val = float(d7['RET'][0]) if d7['RET'][0] is not None else 0
    col_r2.metric("📅 D7 retention", f"{ret_val}%", help="7 kundan keyin qaytganlar")
except:
    col_r2.metric("📅 D7 retention", "N/A")

# D30 Retention
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
    ret_val = float(d30['RET'][0]) if d30['RET'][0] is not None else 0
    col_r3.metric("📅 D30 retention", f"{ret_val}%", help="30 kundan keyin qaytganlar")
except:
    col_r3.metric("📅 D30 retention", "N/A")

# ============ ENG MASHHUR MINI O'YINLAR ============
st.markdown("---")
st.subheader("🏆 Eng mashhur mini o'yinlar")

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
        top_games['NOMI'] = top_games['MINI_GAME'].apply(get_minigame_name)

        # Raqamli ro'yxat
        for i, row in top_games.iterrows():
            col_rank, col_name, col_plays = st.columns([1, 4, 2])
            rank = i + 1

            # Medal uchun ranglar
            if rank == 1:
                medal = "🥇"
            elif rank == 2:
                medal = "🥈"
            elif rank == 3:
                medal = "🥉"
            else:
                medal = f"#{rank}"

            with col_rank:
                st.markdown(f"### {medal}")
            with col_name:
                st.markdown(f"### {row['NOMI']}")
            with col_plays:
                st.markdown(f"### {row['OYINLAR']:,}")

        st.markdown("")

        # Gorizontal bar chart
        chart = alt.Chart(top_games).mark_bar(
            color=COLORS['primary'],
            cornerRadiusTopRight=5,
            cornerRadiusBottomRight=5,
            size=35
        ).encode(
            x=alt.X('OYINLAR:Q', title="O'ynalishlar soni"),
            y=alt.Y('NOMI:N', title=None, sort='-x'),
            tooltip=[
                alt.Tooltip('NOMI:N', title="O'yin"),
                alt.Tooltip('OYINLAR:Q', title="O'ynalishlar", format=',')
            ]
        ).properties(height=280)
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("Ma'lumotlar mavjud emas")
except:
    st.info("Ma'lumotlarni yuklashda xatolik")

# ============ FOOTER ============
st.markdown("---")
st.caption("📊 Ma'lumotlar: Unity Analytics | 🎮 Bek va Lola")
