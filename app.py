"""
Connectly Messaging Dashboard · Stable build (Jun‑2025)
-------------------------------------------------------
This Streamlit app shows interactive analytics on campaign messaging and
user activity for our internal product **Connectly**.

Key improvements vs previous draft
• Robust parquet loading  – skips tiny/corrupt shards
• All campaign & activity shards scanned (no single‑file shortcut)
• Fixes pandas AttributeError by using bracket notation for the *product*
  column
• Caches DuckDB result frames for snappy page loads
• Keeps memory footprint low by aggregating inside DuckDB only
• Clean dark‑theme plots and fully horizontal % labels
"""

import streamlit as st, duckdb, pandas as pd, matplotlib.pyplot as plt
import matplotlib.style as style
import datetime, glob, os, textwrap

# ──────────────────────────────────────────────────────────────────────────────
# THEME
# ----------------------------------------------------------------------------
style.use("dark_background")
BG, TXT = "#0e1117", "#d3d3d3"
plt.rcParams["text.color"] = TXT
st.set_page_config(page_title="Connectly Dashboard", layout="wide")
st.title("📊 Connectly Messaging Dashboard")

# ──────────────────────────────────────────────────────────────────────────────
# FILE DISCOVERY HELPERS
# ----------------------------------------------------------------------------
PAT_CAM = [
    "parquet_trim/dispatch_date=*/data_0.parquet",
    "parquet_trim/msg_*.parquet",
    "parquet_output/dispatch_date=*/data_0*.parquet",
]
PAT_ACT = [
    "activity_trim/act_*.parquet",
    "activity_chunks/activity_data_*.parquet",
]
MAP_FILE = "connectly_business_id_mapping.parquet"


def valid_files(patterns, *, min_bytes: int = 1024):
    """Return files that exist and are ≥ *min_bytes* (skip empty/corrupt)."""
    out: list[str] = []
    for pat in patterns:
        for f in glob.glob(pat):
            try:
                if os.path.getsize(f) >= min_bytes:
                    out.append(f)
            except FileNotFoundError:
                continue
    return out


CAM_FILES = valid_files(PAT_CAM)
ACT_FILES = valid_files(PAT_ACT)
if not CAM_FILES:
    st.error("❌ No valid campaign parquet files found.")
    st.stop()

# ──────────────────────────────────────────────────────────────────────────────
# DUCKDB CONNECTION & VIEWS
# ----------------------------------------------------------------------------
con = duckdb.connect()


def register_parquet_view(view: str, files: list[str]):
    """Create/replace a temp view using DuckDB read_parquet(list)."""
    quoted = ", ".join(repr(f) for f in files)
    con.execute(
        f"CREATE OR REPLACE TEMP VIEW {view} AS "
        f"SELECT * FROM read_parquet([{quoted}], union_by_name=true);"
    )


register_parquet_view("camp", CAM_FILES)
if ACT_FILES:
    register_parquet_view("act_raw", ACT_FILES)
else:
    con.execute(
        "CREATE OR REPLACE TEMP VIEW act_raw AS "
        "SELECT NULL::VARCHAR AS user, NULL::DATE AS activity_date LIMIT 0;"
    )

# Normalise activity schema (guardian|moderator|user phone)
con.execute(
    """
    CREATE OR REPLACE TEMP VIEW act AS
    SELECT COALESCE(guardian_phone, moderator_phone, user_phone) AS user,
           CAST(activity_date AS DATE)                           AS activity_date
    FROM act_raw;
    """
)

# ──────────────────────────────────────────────────────────────────────────────
# UTILS
# ----------------------------------------------------------------------------
@st.cache_data(show_spinner=False)
def run_df(sql: str) -> pd.DataFrame:  # cached query helper
    return con.sql(textwrap.dedent(sql)).df()

# discover date bounds once
MIN_D = con.sql("SELECT MIN(dispatched_at)::DATE FROM camp").fetchone()[0]
MAX_D = con.sql("SELECT MAX(dispatched_at)::DATE FROM camp").fetchone()[0]

# ──────────────────────────────────────────────────────────────────────────────
# 1. MONTHLY MESSAGING & COST OVERVIEW (ignores filters)
# ----------------------------------------------------------------------------
monthly = run_df(
    """
    SELECT date_trunc('month', CAST(dispatched_at AS TIMESTAMP))::DATE AS m,
           COUNT(DISTINCT customer_external_id||'_'||CAST(dispatched_at::DATE AS VARCHAR)) AS sent,
           COUNT(DISTINCT CASE WHEN delivered IS NOT NULL
                                THEN customer_external_id||'_'||CAST(dispatched_at::DATE AS VARCHAR) END) AS delivered
    FROM camp
    GROUP BY 1 ORDER BY 1;
    """
)
monthly["label"] = pd.to_datetime(monthly.m).dt.strftime("%b %y")
monthly["rate"] = (monthly.delivered / monthly.sent * 100).round(2)

# cost calc (delivered‑only)
cost = monthly[["m", "delivered"]].copy()
d = cost.delivered
cost["label"] = monthly.label
cost["meta"] = (d * 0.96 * 0.0107 + d * 0.04 * 0.0014).round(2)
cost["connectly"] = (d * 0.9 * 0.0123 + 500).round(2)

st.subheader("📈 Monthly Messaging & Cost Overview")
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4), facecolor=BG)
# sent vs delivered bars
x = range(len(monthly))
bar_w = 0.35
ax1.bar([i - bar_w / 2 for i in x], monthly.sent, bar_w, color="#00b4d8", label="Sent")
ax1.bar([i + bar_w / 2 for i in x], monthly.delivered, bar_w, color="#ffb703", label="Delivered")
for i, r in monthly.iterrows():
    ax1.text(i - bar_w / 2, r.sent, f"{r.sent/1e6:.1f}M", ha="center", va="bottom", fontsize=8)
    ax1.text(i + bar_w / 2, r.delivered, f"{r.rate:.0f}%", ha="center", va="bottom", fontsize=8)
ax1.set_xticks(x)
ax1.set_xticklabels(monthly.label, rotation=45)
ax1.set_title("Sent vs Delivered")
ax1.legend()
# cost lines
x2 = range(len(cost))
ax2.plot(x2, cost.meta, marker="o", label="Meta Cost")
ax2.plot(x2, cost.connectly, marker="o", label="Connectly Cost")
for i, r in cost.iterrows():
    ax2.text(i, r.meta, f"₹{r.meta:,.0f}", ha="center", va="bottom", fontsize=8)
    ax2.text(i, r.connectly, f"₹{r.connectly:,.0f}", ha="center", va="bottom", fontsize=8)
ax2.set_xticks(x2)
ax2.set_xticklabels(cost.label, rotation=45)
ax2.set_title("Monthly Cost")
ax2.legend()

st.pyplot(fig)

# ──────────────────────────────────────────────────────────────────────────────
# 2. DATE FILTERS
# ----------------------------------------------------------------------------
c1, c2 = st.columns(2)
sd = c1.date_input("Start date", MAX_D - datetime.timedelta(days=30), min_value=MIN_D, max_value=MAX_D)
ed = c2.date_input("End date", MAX_D, min_value=MIN_D, max_value=MAX_D)

# ──────────────────────────────────────────────────────────────────────────────
# 3. FUNNEL BY PRODUCT (all products, respects date filters)
# ----------------------------------------------------------------------------
funnel = run_df(
    f"""
    WITH base AS (
      SELECT COALESCE(mp.product,'Unknown') AS product,
             customer_external_id||'_'||CAST(dispatched_at::DATE AS VARCHAR) AS msg_id,
             delivered
      FROM camp m
      LEFT JOIN read_parquet('{MAP_FILE}') mp
        ON CAST(m.business_id AS VARCHAR)=CAST(mp.business_id AS VARCHAR)
      WHERE dispatched_at::DATE BETWEEN DATE '{sd}' AND DATE '{ed}'
    )
    SELECT product,
           COUNT(DISTINCT msg_id) AS messages_sent,
           COUNT(*) FILTER (WHERE delivered IS NOT NULL) AS messages_delivered
    FROM base
    WHERE product <> 'Unknown'
    GROUP BY 1;
    """
)
# add total row
funnel_tot = funnel[["messages_sent", "messages_delivered"]].sum(numeric_only=True)
funnel_tot["product"] = "Total"
funnel = pd.concat([funnel, pd.DataFrame([funnel_tot])], ignore_index=True)
funnel["delivery_rate (%)"] = (funnel.messages_delivered / funnel.messages_sent * 100).round(2)

st.subheader("📦 Funnel by Product (all)")
st.dataframe(funnel, use_container_width=True)

# ──────────────────────────────────────────────────────────────────────────────
# 4. PRODUCT FILTER (for charts below)
# ----------------------------------------------------------------------------
prod_df = run_df(
    f"""
    SELECT DISTINCT mp.product
    FROM camp m
    LEFT JOIN read_parquet('{MAP_FILE}') mp
      ON CAST(m.business_id AS VARCHAR)=CAST(mp.business_id AS VARCHAR)
    WHERE dispatched_at::DATE BETWEEN DATE '{sd}' AND DATE '{ed}'
      AND mp.product IS NOT NULL;
    """
)
products = sorted(prod_df["product"].unique().tolist())
prod_sel = st.selectbox("Filter product for Activity & Campaign ↓", ["All"] + products)

join_mp = f"""
  LEFT JOIN read_parquet('{MAP_FILE}') mp
  ON CAST(m.business_id AS VARCHAR)=CAST(mp.business_id AS VARCHAR)
"""
prod_clause = "" if prod_sel == "All" else f"AND mp.product = '{prod_sel}'"

# ──────────────────────────────────────────────────────────────────────────────
# 5. NUDGES VS USER ACTIVITY
# ----------------------------------------------------------------------------
nudge_dist = run_df(
    f"""
    WITH nudges AS (
      SELECT customer_external_id AS user
      FROM camp m
      {join_mp}
      WHERE dispatched_at::DATE BETWEEN DATE '{sd}' AND DATE '{ed}' {prod_clause}
      GROUP BY 1
    )
    SELECT COALESCE(a.active_days,0) AS active_days, COUNT(*) AS users
    FROM nudges
    LEFT JOIN (
      SELECT user, COUNT(DISTINCT activity_date) AS active_days
      FROM act
      WHERE activity_date BETWEEN DATE '{sd}' AND DATE '{ed}'
      GROUP BY 1
    ) a USING (user)
    GROUP BY 1 ORDER BY 1;
    """
)

cnt = nudge_dist.set_index("active_days").users
pct = (cnt / cnt.sum() * 100).round(0).astype(int)

st.subheader("👥 Nudges vs User Activity (%)")
fig_h, ax_h = plt.subplots(figsize=(8, 4), facecolor=BG)
bars = ax_h.bar(cnt.index, cnt.values, color="#90e0ef")
for b, val in zip(bars, pct[cnt.index]):
    ax_h.text(
        b.get_x() + b.get_width() / 2,
        b.get_height() + 0.5,
        f"{val}%",
        ha="center",
        va="bottom",
        color=TXT,
        fontsize=8,
    )
ax_h.set_xlabel("Active days")
ax_h.set_ylabel("Users")
st.pyplot(fig_h)

# ──────────────────────────────────────────────────────────────────────────────
# 6. CAMPAIGN PERFORMANCE TABLE
# ----------------------------------------------------------------------------
camp = run_df(
    f"""
    WITH msgs AS (
      SELECT sendout_name,
             customer_external_id AS user,
             customer_external_id||'_'||CAST(dispatched_at::DATE AS VARCHAR) AS msg_id,
             delivered, button_responses, link_clicks
      FROM camp m
      {join_mp}
      WHERE dispatched_at::DATE BETWEEN DATE '{sd}' AND DATE '{ed}' {prod_clause}
    ),
    base AS (
      SELECT sendout_name,
             COUNT(DISTINCT msg_id)                                     AS messages_sent,
             COUNT(*) FILTER (WHERE delivered IS NOT NULL)              AS messages_delivered,
             COUNT(button_responses) + COUNT(link_clicks)               AS total_clicks
      FROM msgs GROUP BY 1
    ),
    segments AS (
      SELECT DISTINCT sendout_name, user FROM msgs
    ), j AS (
      SELECT s.sendout_name,
             CASE
               WHEN COALESCE(a.active_days,0)=0                THEN 'Inactive'
               WHEN COALESCE(a.active_days,0) BETWEEN 1 AND 10 THEN 'Active'
               ELSE                                               'Highly Active'
             END AS seg
      FROM segments s
      LEFT JOIN (
            SELECT user, COUNT(DISTINCT activity_date) AS active_days
            FROM act WHERE activity_date BETWEEN DATE '{sd}' AND DATE '{ed}'
            GROUP BY 1
      ) a USING (user)
    ),
    seg_pct AS (
      SELECT
          sendout_name,
          ROUND(SUM(CASE WHEN seg='Inactive'      THEN 1 ELSE 0 END)*100.0/COUNT(*),1) AS inactive_pct,
          ROUND(SUM(CASE WHEN seg='Active'        THEN 1 ELSE 0 END)*100.0/COUNT(*),1) AS active_pct,
          ROUND(SUM(CASE WHEN seg='Highly Active' THEN 1 ELSE 0 END)*100.0/COUNT(*),1) AS high_pct
      FROM j GROUP BY 1
    )
    SELECT b.*, s.*
    FROM base b LEFT JOIN seg_pct s USING (sendout_name);
    """
)

camp["delivery_rate (%)"] = (camp.messages_delivered / camp.messages_sent * 100).round(1)
camp["click_rate (%)"] = (camp.total_clicks / camp.messages_sent * 100).round(1)
camp["% of Total"] = (camp.messages_sent / camp.messages_sent.sum() * 100).round(1)

for col in [
    "delivery_rate (%)",
    "click_rate (%)",
    "% of Total",
    "inactive_pct",
    "active_pct",
    "high_pct",
]:
    camp[col] = camp[col].fillna(0).round(1).astype(str) + "%"

camp = camp.rename(
    columns={
        "inactive_pct": "Inactive %",
        "active_pct": "Active %",
        "high_pct": "Highly Active %",
    }
)[
    [
        "sendout_name",
        "messages_sent",
        "messages_delivered",
        "delivery_rate (%)",
        "click_rate (%)",
        "% of Total",
        "Inactive %",
        "Active %",
        "Highly Active %",
    ]
].sort_values("messages_sent", ascending=False)

st.subheader("🎯 Campaign Performance")
st.dataframe(camp, use_container_width=True)
