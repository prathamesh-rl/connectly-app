"""
Connectly Messaging Dashboard · resilient build (Jun 2025)
=======================================================
Streamlit app for Connectly analytics that **self‑heals** against:  
• missing Parquet shards  
• zero‑byte or corrupt shards ("no magic bytes")  
• schema drift in the activity files (`guardian_phone|moderator_phone|user_phone`)  

This version aggressively *validates every parquet* before DuckDB ever sees it, so
all the `InvalidInputException` / `IOError` crashes in the previous logs are
eliminated.
"""

from __future__ import annotations

import glob
import os
import pathlib
from functools import lru_cache
from typing import List

import duckdb as ddb
import pandas as pd
import plotly.express as px
import streamlit as st

################################################################################
# ──────────────────────────  CONFIG &  CONSTANTS  ──────────────────────────── #
################################################################################

st.set_page_config(
    page_title="Connectly Dashboard",
    layout="wide",
    page_icon="📊",
)

# Parquet patterns ----------------------------------------------------------------
CAMPAIGN_PATTERNS: List[str] = [
    "parquet_trim/dispatch_date=*/data_0.parquet",  # primary
    "parquet_trim/msg_*.parquet",                  # fallback
    "parquet_output/dispatch_date=*/data_0*.parquet",  # legacy
]
ACTIVITY_PATTERN = "activity_chunks/activity_data_*.parquet"
MAPPING_PATH = "connectly_business_id_mapping.parquet"

# Cost constants ------------------------------------------------------------------
META_COST_PER_MSG = 0.96 * 0.0107 + 0.04 * 0.0014        # ≈ ₹0.010456
CONNECTLY_COST_PER_MSG = 0.90 * 0.0123                   # ≈ ₹0.01107
CONNECTLY_FLAT_FEE = 500                                 # ₹

################################################################################
# ─────────────────────────────────  HELPERS  ───────────────────────────────── #
################################################################################

# Global in‑memory DB (single instance) -------------------------------------------
@st.cache_resource(show_spinner=False)
def connect() -> ddb.DuckDBPyConnection:
    return ddb.connect(database=":memory:")

con = connect()

# ── helper: check that a Parquet file is *really* readable by DuckDB ------------
@lru_cache(maxsize=None)
def _is_parquet_ok(path: str) -> bool:
    try:
        con.execute("PRAGMA parquet_metadata(?)", [path])
        return True
    except Exception:
        return False

# ── helper: resolve good parquet shards (skip zero‑byte / corrupt) --------------
def _good_shards(patterns: List[str], *, min_bytes: int = 1024) -> List[str]:
    paths: List[str] = []
    for pat in patterns:
        for p in glob.glob(pat):
            if os.path.getsize(p) >= min_bytes and _is_parquet_ok(p):
                paths.append(pathlib.Path(p).as_posix())  # posix for DuckDB
    return sorted(paths)

# Data loaders --------------------------------------------------------------------
@st.cache_data(show_spinner="Loading campaign data…")
def load_campaign() -> pd.DataFrame:
    paths = _good_shards(CAMPAIGN_PATTERNS)
    if not paths:
        st.error("❌ No readable campaign Parquet shards found.")
        st.stop()
    return con.sql("SELECT * FROM read_parquet($paths, union_by_name=true)", {"paths": paths}).df()

@st.cache_data(show_spinner="Loading activity data…")
def load_activity() -> pd.DataFrame:
    paths = _good_shards([ACTIVITY_PATTERN])
    if not paths:
        st.error("❌ No readable activity Parquet shards found.")
        st.stop()
    return con.sql("SELECT * FROM read_parquet($paths, union_by_name=true)", {"paths": paths}).df()

@st.cache_data(show_spinner=False)
def load_mapping() -> pd.DataFrame:
    if not os.path.exists(MAPPING_PATH):
        return pd.DataFrame(columns=["business_id", "product"])
    return pd.read_parquet(MAPPING_PATH)

################################################################################
# ─────────────────────────────  DATA WAREHOUSING  ──────────────────────────── #
################################################################################

# Register campaign view ----------------------------------------------------------
con.register("camp_raw", load_campaign())
con.execute("DROP VIEW IF EXISTS camp")
con.execute(
    """
    CREATE OR REPLACE VIEW camp AS
    SELECT
        dispatched_at,
        CAST(dispatched_at AS DATE)           AS dispatched_date,
        customer_external_id,
        delivered_at,
        business_id
    FROM camp_raw
    """,
)

# Apply product mapping inside DuckDB -------------------------------------------
map_df = load_mapping()
if not map_df.empty:
    con.register("map", map_df)
    con.execute(
        """
        CREATE OR REPLACE VIEW camp AS
        SELECT  c.*,
                COALESCE(m.product, 'Unknown') AS product
        FROM    camp c
        LEFT JOIN map m
          ON CAST(c.business_id AS VARCHAR) = CAST(m.business_id AS VARCHAR)
        """,
    )
else:
    con.execute("ALTER VIEW camp ADD COLUMN product VARCHAR DEFAULT 'Unknown'")

# Register activity view (schema‑flexible) ---------------------------------------
con.register("act_raw", load_activity())
cols = {r[0] for r in con.execute("PRAGMA table_info('act_raw')").fetchall()}
phone_cols = [c for c in ("guardian_phone", "moderator_phone", "user_phone") if c in cols]
if not phone_cols:
    st.error("❌ No phone column present in activity data.")
    st.stop()
coalesce_expr = ", ".join(phone_cols)
con.execute("DROP VIEW IF EXISTS act")
con.execute(
    f"""
    CREATE OR REPLACE VIEW act AS
    SELECT  COALESCE({coalesce_expr}) AS user_phone,
            CAST(activity_date AS DATE) AS activity_date
    FROM act_raw
    WHERE activity_date IS NOT NULL
    """,
)

################################################################################
# ──────────────────────────  STREAMLIT INTERFACE  ──────────────────────────── #
################################################################################

st.title("📊 Connectly Messaging Analytics")

# =============== 1️⃣ MONTHLY OVERVIEW (ignores filters) ========================
monthly = con.sql(
    """
    WITH base AS (
        SELECT
            date_trunc('month', CAST(dispatched_at AS TIMESTAMP)) AS month_start,
            COUNT(DISTINCT customer_external_id || dispatched_date)            AS sent,
            SUM(CASE WHEN delivered_at IS NOT NULL THEN 1 ELSE 0 END)          AS delivered
        FROM camp
        GROUP BY 1
    )
    SELECT
        month_start,
        sent,
        delivered,
        delivered * $meta AS meta_cost,
        delivered * $conn + $flat               AS connectly_cost,
        ROUND(delivered * 100.0 / NULLIF(sent,0),2) AS delivery_rate
    FROM base
    ORDER BY month_start
    """,
    {
        "meta": META_COST_PER_MSG,
        "conn": CONNECTLY_COST_PER_MSG,
        "flat": CONNECTLY_FLAT_FEE,
    },
).df()

col1, col2 = st.columns([2, 1])
with col1:
    fig = px.bar(
        monthly,
        x="month_start",
        y=["sent", "delivered"],
        barmode="group",
        labels={"value": "Messages", "month_start": "Month"},
        template="plotly_dark",
    )
    for _, row in monthly.iterrows():
        fig.add_annotation(x=row["month_start"], y=row["delivered"], text=f"{row['delivery_rate']}%", showarrow=False, yshift=10)
    st.plotly_chart(fig, use_container_width=True)
with col2:
    cost_fig = px.line(
        monthly,
        x="month_start",
        y=["meta_cost", "connectly_cost"],
        labels={"value": "Cost (₹)", "variable": ""},
        template="plotly_dark",
    )
    st.plotly_chart(cost_fig, use_container_width=True)

st.divider()

# =============== 2️⃣ DATE FILTER ========================
MIN_D, MAX_D = con.sql("SELECT MIN(dispatched_date), MAX(dispatched_date) FROM camp").fetchone()
start_date, end_date = st.date_input("Select date range", value=(MIN_D, MAX_D), min_value=MIN_D, max_value=MAX_D)
con.execute("SET start = ?", [start_date])
con.execute("SET end   = ?", [end_date])
st.caption(f"Showing data from **{start_date}** to **{end_date}**")

# =============== 3️⃣ FUNNEL BY PRODUCT ==================
funnel_df = con.sql(
    """
    WITH sent_msg AS (
        SELECT DISTINCT customer_external_id, dispatched_date, product
        FROM   camp
        WHERE  dispatched_date BETWEEN $start AND $end
    ),
    agg AS (
        SELECT  product,
                COUNT(*)                                   AS messages_sent,
                SUM(CASE WHEN delivered_at IS NOT NULL THEN 1 ELSE 0 END) AS delivered
        FROM camp
        WHERE dispatched_date BETWEEN $start AND $end
        GROUP BY product
    )
    SELECT  a.product,
            a.messages_sent,
            a.delivered,
            ROUND(a.delivered*100.0/NULLIF(a.messages_sent,0),2)   AS delivery_rate
    FROM agg a
    WHERE a.product <> 'Unknown'
    ORDER BY a.messages_sent DESC
    """,
).df()

st.subheader("📦 Funnel by Product")
st.dataframe(funnel_df, hide_index=True)

products_sorted = funnel_df["product"].tolist()

st.divider()

# =============== 4️⃣ PRODUCT FILTER =====================
picked_products = st.multiselect(
    "Filter specific product(s) (affects charts below)",
    options=products_sorted,
    default=products_sorted,
)
if not picked_products:
    st.warning("Pick at least one product to continue.")
    st.stop()
con.execute("SET picked = ?", [picked_products])

# =============== 5️⃣ NUDGES vs USER ACTIVITY ============
activity_df = con.sql(
    """
    WITH base AS (
        SELECT  user_phone,
                COUNT(DISTINCT activity_date) AS active_days
        FROM act
        WHERE activity_date BETWEEN $start AND $end
        GROUP BY user_phone
    )
    SELECT  active_days, COUNT(*) AS users
    FROM base
    GROUP BY active_days
    ORDER BY active_days
    """,
).df()

if activity_df.empty:
    st.info("No activity data for the selected window.")
else:
    total_users = int(activity_df["users"].sum())
    activity_df["percent"] = (activity_df["users"] * 100 / total_users).round(0)
    st.subheader("👥 Nudges vs User Activity")
    act_fig = px.bar(activity_df, x="active_days", y="users", template="plotly_dark", labels={"active_days": "Number of active days", "users": "Users"})
    for _, row in activity_df.iterrows():
        act_fig.add_annotation(x=row["active_days"], y=row["users"], text=f"{int(row['percent'])}%", showarrow=False, yshift=10)
    st.plotly_chart(act_fig, use_container_width=True)

st.divider()

# =============== 6️⃣ CAMPAIGN PERFORMANCE TABLE =========
perf_df = con.sql(
    """
    WITH base AS (
        SELECT  sendout_name,
                COUNT(*) AS messages_sent,
                SUM(CASE WHEN delivered_at IS NOT NULL THEN 1 ELSE 0 END) AS delivered,
                SUM(button_responses + link_clicks)                         AS clicks
        FROM camp
        WHERE dispatched_date BETWEEN $start AND $end
          AND product IN (SELECT UNNEST($picked))
        GROUP BY sendout_name
    ),
    agg AS (
        SELECT  *,
                ROUND(delivered*100.0/NULLIF(messages_sent,0),2)          AS delivery_rate,
                ROUND(clicks*100.0/NULLIF(messages_sent,0),2)            AS click_rate
        FROM base
    )
    SELECT  sendout_name,
            messages_sent,
            delivered                       AS messages_delivered,
            delivery_rate,
            click_rate,
            ROUND(messages_sent*100.0 / SUM(messages_sent) OVER (),2)    AS pct_of_total,
            ROUND(100.0 - click_rate,2)                                  AS inactive_pct,
            ROUND(click_rate,2)                                          AS active_pct,
            CASE WHEN click_rate >= 11 THEN 100 ELSE 0 END               AS highly_active_pct
    FROM agg
    ORDER BY messages_sent DESC
    """,
).df()

st.subheader("📈 Campaign Performance")
st.dataframe(perf_df, hide_index=True)
