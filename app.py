# -----------------------------------------------------------------
#  Connectly Messaging Dashboard · HTTPFS build  (18 Jun 2025)
#  – loads Parquet straight from GitHub Raw over HTTPS
# -----------------------------------------------------------------
import streamlit as st, duckdb, pandas as pd, matplotlib.pyplot as plt, matplotlib.style as style
import datetime, textwrap, os

# ─── page / theme ────────────────────────────────────────────────
style.use("dark_background")
BG = "#0e1117"
st.set_page_config(page_title="Connectly Dashboard", layout="wide", page_icon="📊")
st.title("📊 Connectly Messaging Dashboard")

# ─── DuckDB connection (enable HTTPFS) ───────────────────────────
con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")      # <— key line
qdf = lambda sql: con.sql(textwrap.dedent(sql)).df()

# ─── GitHub Raw root (adjust user/repo/branch if needed) ─────────
RAW = "https://raw.githubusercontent.com/prathamesh-rl/connectly-app/main/"

# every Parquet pattern we used before, now as HTTPS paths
PAT_CAM = [
    RAW + "parquet_trim/dispatch_date=*/data_0.parquet",
    RAW + "parquet_trim/msg_*.parquet",
    RAW + "parquet_output/dispatch_date=*/data_0*.parquet",
]
PAT_ACT = [
    RAW + "activity_trim/act_*.parquet",
    RAW + "activity_chunks/activity_data_*.parquet",
]
MAP_FILE = RAW + "connectly_business_id_mapping.parquet"

# build DuckDB read_parquet() list literal (no local globbing)
lit = lambda L: "[" + ", ".join(f"'{p}'" for p in L) + "]"
scan_c = f"read_parquet({lit(PAT_CAM)}, union_by_name=true)"
scan_a = f"read_parquet({lit(PAT_ACT)}, union_by_name=true)"

# ─── optional debug toggle ───────────────────────────────────────
if st.sidebar.checkbox("🔍 show debug"):
    st.sidebar.write({"campaign_patterns": PAT_CAM[:3], "activity_patterns": PAT_ACT[:2]})

# ─── helper dates ────────────────────────────────────────────────
MIN_D, MAX_D = datetime.date(2025, 1, 1), datetime.date(2025, 6, 18)

# ════════════════════════════════════════════════════════════════
#  SECTION 1 · Monthly Messaging & Cost Overview (no filters)
# ════════════════════════════════════════════════════════════════
monthly = qdf(f"""
    SELECT DATE_TRUNC('month', CAST(dispatched_at AS TIMESTAMP))::DATE AS m,
           COUNT(DISTINCT customer_external_id||'_'||
                 CAST(dispatched_at::DATE AS VARCHAR))                 AS sent,
           COUNT(DISTINCT CASE WHEN delivered IS NOT NULL
                 THEN customer_external_id||'_'||
                      CAST(dispatched_at::DATE AS VARCHAR) END)        AS delivered
    FROM {scan_c}
    GROUP BY 1 ORDER BY 1;
""")
monthly["label"] = pd.to_datetime(monthly.m).dt.strftime("%b %y")
monthly["rate"]  = (monthly.delivered/monthly.sent*100).round(1)

cost = monthly[["m","delivered"]].copy()
d    = cost.delivered
cost["label"]     = pd.to_datetime(cost.m).dt.strftime("%b %y")
cost["meta"]      = (d*0.96*0.0107 + d*0.04*0.0014).round(0)
cost["connectly"] = (d*0.90*0.0123 + 500).round(0)

st.subheader("📈 Monthly Messaging & Cost Overview")
fig,(ax1,ax2)=plt.subplots(1,2,figsize=(12,4),facecolor=BG)
x=range(len(monthly)); w=.35
ax1.bar([i-w/2 for i in x], monthly.sent,      w, color="#00b4d8", label="Sent")
ax1.bar([i+w/2 for i in x], monthly.delivered, w, color="#ffb703", label="Delivered")
for i,r in monthly.iterrows():
    ax1.text(i-w/2, r.sent,      f"{r.sent/1e6:.1f}M", ha='center', va='bottom', fontsize=8)
    ax1.text(i+w/2, r.delivered, f"{r.rate:.0f}%",     ha='center', va='bottom', fontsize=8)
ax1.set_xticks(x); ax1.set_xticklabels(monthly.label, rotation=45); ax1.set_title("Sent vs Delivered"); ax1.legend()

x2=range(len(cost))
ax2.plot(x2, cost.meta,      marker="o", color="#00b4d8", label="Meta cost")
ax2.plot(x2, cost.connectly, marker="o", color="#ffb703", label="Connectly cost")
for i,r in cost.iterrows():
    ax2.text(i, r.meta,      f"₹{r.meta:,.0f}", ha='center', va='bottom', color="#00b4d8", fontsize=8)
    ax2.text(i, r.connectly, f"₹{r.connectly:,.0f}", ha='center', va='bottom', color="#ffb703", fontsize=8)
ax2.set_xticks(x2); ax2.set_xticklabels(cost.label, rotation=45); ax2.set_title("Monthly Cost"); ax2.legend()
st.pyplot(fig)

# ════════════════════════════════════════════════════════════════
#  SECTION 2 · Date range filter
# ════════════════════════════════════════════════════════════════
c1,c2 = st.columns(2)
sd = c1.date_input("Start date", MAX_D - datetime.timedelta(days=30),
                   min_value=MIN_D, max_value=MAX_D)
ed = c2.date_input("End date",   MAX_D,
                   min_value=MIN_D, max_value=MAX_D)

# ════════════════════════════════════════════════════════════════
#  SECTION 3 · Funnel by Product
# ════════════════════════════════════════════════════════════════
funnel = qdf(f"""
WITH msgs AS (
  SELECT COALESCE(product,'Unknown')            AS product,
         customer_external_id                   AS user,
         MIN(delivered)                         AS deliv,
         MIN(CASE WHEN button_responses>0 OR link_clicks>0 THEN 1 END) AS clicked
  FROM {scan_c}
  LEFT JOIN read_parquet('{MAP_FILE}') USING (business_id)
  WHERE dispatched_at::DATE BETWEEN '{sd}' AND '{ed}'
  GROUP BY 1,2
), agg AS (
  SELECT product,
         COUNT(DISTINCT user)                                         AS sent,
         COUNT(DISTINCT CASE WHEN deliv IS NOT NULL THEN user END)    AS delivered,
         COUNT(DISTINCT CASE WHEN clicked=1 THEN user END)            AS clicked
  FROM msgs GROUP BY 1
)
SELECT product,
       sent,
       delivered,
       ROUND(delivered*100.0/sent,1) AS delivery_rate,
       ROUND(clicked  *100.0/sent,1) AS click_rate
FROM agg WHERE product<>'Unknown' ORDER BY sent DESC;
""")
tot = funnel[["sent","delivered"]].sum().to_frame().T
tot.insert(0,"product","Total")
tot["delivery_rate"] = round(tot.delivered*100/tot.sent,1)
tot["click_rate"]    = round(funnel.clicked.sum()*100/tot.sent,1)
funnel = pd.concat([tot,funnel],ignore_index=True)

st.subheader("🪜 Funnel by Product")
st.dataframe(funnel.style.format({"delivery_rate":"{:.1f}%","click_rate":"{:.1f}%"}),
             use_container_width=True)

# ════════════════════════════════════════════════════════════════
#  SECTION 4 · Product filter
# ════════════════════════════════════════════════════════════════
prod_opts = funnel.product[funnel.product!="Total"].tolist()
sel_prod  = st.multiselect("Filter products (affects charts below)",
                           prod_opts, default=prod_opts)
prod_clause = "AND COALESCE(product,'Unknown') IN (" + ",".join("'" + p + "'" for p in sel_prod) + ")"

# ════════════════════════════════════════════════════════════════
#  SECTION 5 · Nudges vs User Activity
# ════════════════════════════════════════════════════════════════
nv = qdf(f"""
WITH d AS (
  SELECT user, COUNT(DISTINCT activity_date) AS days
  FROM read_parquet({lit(PAT_ACT)})    -- activity direct from Raw URLs
  WHERE activity_date BETWEEN '{sd}' AND '{ed}'
  GROUP BY 1
)
SELECT days, COUNT(*) AS users
FROM d GROUP BY 1 ORDER BY 1;
""")
nv["pct"] = (nv.users/nv.users.sum()*100).round(1)

st.subheader("📊 Nudges vs User Activity")
fig2, ax = plt.subplots(figsize=(6,4), facecolor=BG)
ax.bar(nv.days, nv.users, color="#90be6d")
for d,u,p in zip(nv.days, nv.users, nv.pct):
    ax.text(d, u, f"{p:.0f}%", ha='center', va='bottom', fontsize=8)
ax.set_xlabel("Active days in period"); ax.set_ylabel("Users")
st.pyplot(fig2)

# ════════════════════════════════════════════════════════════════
#  SECTION 6 · Campaign Performance Table
# ════════════════════════════════════════════════════════════════
camp = qdf(f"""
WITH msgs AS (
  SELECT sendout_name,
         customer_external_id AS user,
         button_responses + link_clicks                       AS clicks,
         delivered IS NOT NULL                                AS is_delivered
  FROM {scan_c}
  LEFT JOIN read_parquet('{MAP_FILE}') USING (business_id)
  WHERE dispatched_at::DATE BETWEEN '{sd}' AND '{ed}' {prod_clause}
)
SELECT sendout_name,
       COUNT(DISTINCT user)                                                     AS messages_sent,
       COUNT(DISTINCT CASE WHEN is_delivered THEN user END)                     AS messages_delivered,
       ROUND(100.0*SUM(clicks)/NULLIF(COUNT(DISTINCT user),0),1)                AS click_rate,
       ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER (),1)                            AS pct_of_total
FROM msgs GROUP BY 1 ORDER BY messages_sent DESC;
""")
camp["delivery_rate (%)"] = (camp.messages_delivered/camp.messages_sent*100).round(1)
camp["pct_of_total (%)"]  = camp.pct_of_total.round(1)

st.subheader("🎯 Campaign Performance")
st.dataframe(camp.drop(columns="pct_of_total").rename(columns={
    "delivery_rate (%)":"delivery_rate(%)","click_rate":"click_rate(%)"}),
    use_container_width=True)

st.caption("© 2025 Rocket Learning • internal dashboard")
