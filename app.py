# -----------------------------------------------------------------
#  Connectly Messaging Dashboard · low-memory + multi-user build
# -----------------------------------------------------------------
#  ▸ One DuckDB connection per browser session → thread-safe
#  ▸ PRAGMA memory_limit keeps RAM < ~700 MB (Cloud limit is 1 GB)
#  ▸ Heavy SQL blocks wrapped in @st.cache_data(ttl=900)
#  ▸ Column projection so DuckDB only reads used columns
# -----------------------------------------------------------------
import streamlit as st, duckdb, pandas as pd
import matplotlib.pyplot as plt, matplotlib.style as style
import datetime, glob, textwrap, pathlib

# ─── page & theme ────────────────────────────────────────────────
style.use("dark_background")
BG, TXT = "#0e1117", "#d3d3d3"
plt.rcParams.update({"text.color": TXT, "axes.edgecolor": TXT, "axes.labelcolor": TXT})
st.set_page_config(page_title="Connectly Dashboard", layout="wide", page_icon="📊")
st.title("📊 Connectly Messaging Dashboard")

# ─── repo helpers -------------------------------------------------
ROOT = pathlib.Path(__file__).resolve().parent
_abs  = lambda p: (ROOT / p).as_posix()

PAT_CAM = (
    _abs("parquet_trim/dispatch_date=*/data_0.parquet"),
    _abs("parquet_trim/msg_*.parquet"),
    _abs("parquet_output/dispatch_date=*/data_0*.parquet"),
)
PAT_ACT = (
    _abs("activity_trim/act_*.parquet"),
    _abs("activity_chunks/activity_data_*.parquet"),
)
MAP_FILE = _abs("connectly_business_id_mapping.parquet")

FILES_CAM = [p for pat in PAT_CAM for p in glob.glob(pat)]
FILES_ACT = [p for pat in PAT_ACT for p in glob.glob(pat)]
if not FILES_CAM:
    st.error("❌ No campaign Parquet shards found"); st.stop()

lit = lambda L: "[" + ", ".join(f"'{x}'" for x in L) + "]"
scan_c = f"read_parquet({lit(FILES_CAM)}, union_by_name=true)"
scan_a = f"read_parquet({lit(FILES_ACT)}, union_by_name=true)" if FILES_ACT else None

# ─── per-session DuckDB connection (thread-safe) -----------------
@st.cache_resource(show_spinner=False)
def connect_duckdb():
    con = duckdb.connect()
    con.execute("PRAGMA memory_limit='700MB'")
    return con

con = connect_duckdb()
qdf = lambda q: con.sql(textwrap.dedent(q)).df()

# ─── activity view (schema-adaptive) -----------------------------
if scan_a:
    cols = [r[0] for r in con.execute(f"PRAGMA describe({scan_a})").fetchall()]
    phones = [c for c in ("guardian_phone","moderator_phone","user_phone") if c in cols] or ["user_phone"]
    casted = ", ".join(f"CAST({c} AS VARCHAR)" for c in phones)
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW act_src AS
        SELECT COALESCE({casted}) AS user,
               CAST(activity_date AS DATE) AS activity_date
        FROM {scan_a}
    """)
else:
    con.execute("CREATE OR REPLACE TEMP VIEW act_src AS SELECT NULL AS user, NULL::DATE AS activity_date LIMIT 0;")

MIN_D, MAX_D = datetime.date(2025,1,1), datetime.date(2025,6,18)

# ════════════════════════════════════════════════════════════════
#  SECTION 1 · Monthly Messaging & Cost Overview
# ════════════════════════════════════════════════════════════════
@st.cache_data(ttl=900)
def monthly_df():
    return qdf(f"""
        SELECT DATE_TRUNC('month', CAST(dispatched_at AS TIMESTAMP))::DATE AS m,
               COUNT(DISTINCT customer_external_id||'_'||CAST(dispatched_at::DATE AS VARCHAR)) AS sent,
               COUNT(DISTINCT CASE WHEN delivered IS NOT NULL
                     THEN customer_external_id||'_'||CAST(dispatched_at::DATE AS VARCHAR) END) AS delivered
        FROM {scan_c}
        GROUP BY 1 ORDER BY 1;
    """)
monthly = monthly_df()
monthly["label"] = pd.to_datetime(monthly.m).dt.strftime("%b %y")
monthly["rate"]  = (monthly.delivered/monthly.sent*100).round(1)

cost = monthly[["m","delivered"]].copy()
d = cost.delivered
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
#  SECTION 2 · Date range filters
# ════════════════════════════════════════════════════════════════
c1,c2 = st.columns(2)
sd = c1.date_input("Start date", MAX_D - datetime.timedelta(days=30), min_value=MIN_D, max_value=MAX_D)
ed = c2.date_input("End date",   MAX_D,                              min_value=MIN_D, max_value=MAX_D)

# ════════════════════════════════════════════════════════════════
#  SECTION 3 · Funnel by Product
# ════════════════════════════════════════════════════════════════
@st.cache_data(ttl=900)
def funnel_df(sd, ed):
    return qdf(f"""
    WITH msgs AS (
      SELECT COALESCE(product,'Unknown') AS product,
             customer_external_id        AS user,
             MIN(delivered)              AS deliv,
             MIN(CASE WHEN button_responses>0 OR link_clicks>0 THEN 1 END) AS clicked
      FROM {scan_c}
      LEFT JOIN read_parquet('{MAP_FILE}') USING (business_id)
      WHERE dispatched_at::DATE BETWEEN '{sd}' AND '{ed}'
      GROUP BY 1,2
    ), agg AS (
      SELECT product,
             COUNT(DISTINCT user)                               AS sent,
             COUNT(DISTINCT CASE WHEN deliv IS NOT NULL THEN user END) AS delivered,
             COUNT(DISTINCT CASE WHEN clicked=1 THEN user END)         AS clicked
      FROM msgs GROUP BY 1
    )
    SELECT product, sent, delivered,
           ROUND(delivered*100.0/sent,1) AS delivery_rate,
           ROUND(clicked  *100.0/sent,1) AS click_rate
    FROM agg WHERE product<>'Unknown' ORDER BY sent DESC;
    """)
funnel = funnel_df(sd, ed)
tot = funnel[["sent","delivered"]].sum().to_frame().T
tot.insert(0,"product","Total")
tot["delivery_rate"] = round(tot.delivered*100/tot.sent,1)
tot["click_rate"]    = round(funnel.click_rate.mul(funnel.sent).sum()/tot.sent,1)
funnel = pd.concat([tot,funnel],ignore_index=True)

st.subheader("🪜 Funnel by Product")
st.dataframe(funnel.style.format({"delivery_rate":"{:.1f}%","click_rate":"{:.1f}%"}),
             use_container_width=True)

# ════════════════════════════════════════════════════════════════
#  SECTION 4 · Product filter
# ════════════════════════════════════════════════════════════════
prod_opts = funnel.product[funnel.product!="Total"].tolist()
sel_prod  = st.multiselect("Filter products (affects charts below)", prod_opts, default=prod_opts)
prod_clause = "AND COALESCE(product,'Unknown') IN (" + ",".join("'" + p + "'" for p in sel_prod) + ")"

# ════════════════════════════════════════════════════════════════
#  SECTION 5 · Nudges vs User Activity
# ════════════════════════════════════════════════════════════════
@st.cache_data(ttl=900)
def nudge_dist(sd, ed, clause):
    return qdf(f"""
    WITH nudges AS (
      SELECT customer_external_id AS user
      FROM {scan_c}
      LEFT JOIN read_parquet('{MAP_FILE}') USING (business_id)
      WHERE dispatched_at::DATE BETWEEN '{sd}' AND '{ed}' {clause}
      GROUP BY 1
    ), act AS (
      SELECT user, COUNT(DISTINCT activity_date) AS days
      FROM act_src WHERE activity_date BETWEEN '{sd}' AND '{ed}'
      GROUP BY 1
    )
    SELECT COALESCE(days,0) AS days, COUNT(*) AS users
    FROM nudges LEFT JOIN act USING (user)
    GROUP BY 1 ORDER BY 1;
    """)
nv = nudge_dist(sd, ed, prod_clause)
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
@st.cache_data(ttl=900)
def campaign_df(sd, ed, clause):
    return qdf(f"""
    WITH msgs AS (
      SELECT sendout_name,
             customer_external_id AS user,
             delivered IS NOT NULL                              AS is_delivered,
             button_responses+link_clicks                       AS clicks
      FROM {scan_c}
      LEFT JOIN read_parquet('{MAP_FILE}') USING (business_id)
      WHERE dispatched_at::DATE BETWEEN '{sd}' AND '{ed}' {clause}
    ), base AS (
      SELECT sendout_name,
             COUNT(DISTINCT user)                               AS messages_sent,
             COUNT(DISTINCT CASE WHEN is_delivered THEN user END) AS messages_delivered,
             SUM(clicks)                                        AS total_clicks
      FROM msgs GROUP BY 1
    ), seg AS (
      SELECT DISTINCT sendout_name, user FROM msgs
    ), bucket AS (
      SELECT s.sendout_name,
             CASE
               WHEN COALESCE(a.days,0)=0                   THEN 'Inactive'
               WHEN COALESCE(a.days,0) BETWEEN 1 AND 10    THEN 'Active'
               ELSE                                             'Highly Active'
             END AS seg
      FROM seg s
      LEFT JOIN (
        SELECT user, COUNT(DISTINCT activity_date) AS days
        FROM act_src WHERE activity_date BETWEEN '{sd}' AND '{ed}'
        GROUP BY 1
      ) a USING (user)
    ), pct AS (
      SELECT sendout_name,
             ROUND(SUM(seg='Inactive')*100.0/COUNT(*),1) AS inactive_pct,
             ROUND(SUM(seg='Active')  *100.0/COUNT(*),1) AS active_pct,
             ROUND(SUM(seg='Highly Active')*100.0/COUNT(*),1) AS high_pct
      FROM bucket GROUP BY 1
    )
    SELECT b.*, p.*
    FROM base b LEFT JOIN pct p USING (sendout_name)
    """)
camp = campaign_df(sd, ed, prod_clause)
camp["delivery_rate (%)"] = (camp.messages_delivered/camp.messages_sent*100).round(1)
camp["click_rate (%)"]    = (camp.total_clicks/camp.messages_sent*100).round(1)
camp["% of Total"]        = (camp.messages_sent/camp.messages_sent.sum()*100).round(1)
for c in ["inactive_pct","active_pct","high_pct","delivery_rate (%)","click_rate (%)","% of Total"]:
    camp[c] = camp[c].fillna(0).astype(str)+"%"
camp = camp.rename(columns={"inactive_pct":"Inactive %","active_pct":"Active %","high_pct":"Highly Active %"})\
           .sort_values("messages_sent",ascending=False)

st.subheader("🎯 Campaign Performance")
st.dataframe(camp,use_container_width=True)

# ════════════════════════════════════════════════════════════════
st.caption("© 2025 Rocket Learning • internal dashboard")
