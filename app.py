# -----------------------------------------------------------------
#  Connectly Messaging Dashboard · lean SQL build  (Jun-2025)
#  – probes every parquet w/ `LIMIT 1` to skip bad shards
# -----------------------------------------------------------------
import streamlit as st, duckdb, pandas as pd, matplotlib.pyplot as plt, matplotlib.style as style
import datetime, glob, textwrap, pathlib, os

# ─── theme & page ────────────────────────────────────────────────
style.use("dark_background")
BG, TXT = "#0e1117", "#d3d3d3"
plt.rcParams.update({"text.color": TXT, "axes.edgecolor": TXT, "axes.labelcolor": TXT})
st.set_page_config(page_title="Connectly Dashboard", layout="wide", page_icon="📊")
st.title("📊 Connectly Messaging Dashboard")

# ─── repo root & helpers ─────────────────────────────────────────
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
MIN_D, MAX_D = datetime.date(2025, 1, 1), datetime.date(2025, 6, 8)

# ─── duckdb connection & probe helper ───────────────────────────
con = duckdb.connect()
def _good_files(pattern: str) -> list[str]:
    good, bad = [], 0
    for p in glob.glob(pattern):
        try:
            con.execute("PRAGMA parquet_metadata(?)", [p])           # quick peek
            con.execute(f"SELECT 1 FROM read_parquet('{p}') LIMIT 1")# tiny read
            good.append(pathlib.Path(p).as_posix())
        except Exception:
            bad += 1
    return good, bad

def _first_good(pats):
    for pat in pats:
        g, b = _good_files(pat)
        if g:
            return g, b
    return [], 0

FILES_CAM, BAD_CAM = _first_good(PAT_CAM)
FILES_ACT, BAD_ACT = _first_good(PAT_ACT)

if not FILES_CAM:
    st.error("❌ No readable campaign Parquet files found. Check repo paths.")
    st.stop()

# optional debug sidebar
if st.sidebar.checkbox("🔍 show debug"):
    st.sidebar.write({
        "campaign_ok": len(FILES_CAM), "campaign_bad": BAD_CAM,
        "activity_ok": len(FILES_ACT), "activity_bad": BAD_ACT,
        "first_campaign": FILES_CAM[0] if FILES_CAM else None,
        "first_activity": FILES_ACT[0] if FILES_ACT else None,
    })

# ─── table scans ────────────────────────────────────────────────
lit  = lambda L: "[" + ", ".join(f"'{x}'" for x in L) + "]"
scan_c = f"read_parquet({lit(FILES_CAM)}, union_by_name=true)"
scan_a = f"read_parquet({lit(FILES_ACT)}, union_by_name=true)" if FILES_ACT else None
qdf    = lambda sql: con.sql(textwrap.dedent(sql)).df()

# ─── activity view (VARCHAR cast, dynamic columns) ──────────────
if scan_a:
    cols = [r[0] for r in con.execute(f"PRAGMA describe({scan_a})").fetchall()]
    phones = [c for c in ("guardian_phone","moderator_phone","user_phone") if c in cols] or ["user_phone"]
    coalesce = ", ".join(f"CAST({c} AS VARCHAR)" for c in phones)
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW act_src AS
        SELECT COALESCE({coalesce})        AS user,
               CAST(activity_date AS DATE) AS activity_date
        FROM {scan_a};
    """)
else:
    con.execute("CREATE OR REPLACE TEMP VIEW act_src AS SELECT NULL AS user, NULL::DATE AS activity_date LIMIT 0;")

# ════════════════════════════════════════════════════════════════
#  SECTION 1 · Monthly Messaging & Cost Overview
# ════════════════════════════════════════════════════════════════
monthly = qdf(f"""
    SELECT DATE_TRUNC('month', CAST(dispatched_at AS TIMESTAMP))::DATE AS m,
           COUNT(DISTINCT customer_external_id||'_'||CAST(dispatched_at::DATE AS VARCHAR)) AS sent,
           COUNT(DISTINCT CASE WHEN delivered IS NOT NULL
                 THEN customer_external_id||'_'||CAST(dispatched_at::DATE AS VARCHAR) END) AS delivered
    FROM {scan_c} GROUP BY 1 ORDER BY 1;
""")
monthly["label"] = pd.to_datetime(monthly.m).dt.strftime("%b %y")
monthly["rate"]  = (monthly.delivered / monthly.sent * 100).round(1)

cost = monthly[["m","delivered"]].copy()
d = cost.delivered
cost["label"]     = pd.to_datetime(cost.m).dt.strftime("%b %y")
cost["meta"]      = (d*0.96*0.0107 + d*0.04*0.0014).round(0)
cost["connectly"] = (d*0.9*0.0123 + 500).round(0)

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
#  SECTION 2 · Date filter
# ════════════════════════════════════════════════════════════════
c1,c2 = st.columns(2)
sd = c1.date_input("Start date", MAX_D - datetime.timedelta(days=30), min_value=MIN_D, max_value=MAX_D)
ed = c2.date_input("End date",   MAX_D,                                min_value=MIN_D, max_value=MAX_D)

# ════════════════════════════════════════════════════════════════
#  SECTION 3 · Funnel by Product
# ════════════════════════════════════════════════════════════════
funnel = qdf(f"""
WITH msgs AS (
  SELECT COALESCE(mp.product,'Unknown')                 AS product,
         customer_external_id                           AS user,
         MIN(delivered)                                 AS deliv,
         MIN(CASE WHEN button_responses>0 OR link_clicks>0 THEN 1 END) AS clicked
  FROM {scan_c} m
  LEFT JOIN parquet_scan('{MAP_FILE}') mp
    ON CAST(m.business_id AS VARCHAR)=CAST(mp.business_id AS VARCHAR)
  WHERE dispatched_at::DATE BETWEEN DATE '{sd}' AND DATE '{ed}'
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
total = funnel[["sent","delivered"]].sum().to_frame().T
total.insert(0,"product","Total")
total["delivery_rate"] = round(total.delivered*100/total.sent,1)
total["click_rate"]    = round(funnel.clicked.sum()*100/total.sent,1)
funnel = pd.concat([total,funnel],ignore_index=True)

st.subheader("🪜 Funnel by Product")
st.dataframe(funnel.style.format({"delivery_rate":"{:.1f}%","click_rate":"{:.1f}%"}),
             use_container_width=True)

# ════════════════════════════════════════════════════════════════
#  SECTION 4 · Product filter
# ════════════════════════════════════════════════════════════════
prod_opts = funnel.product[funnel.product!="Total"].tolist()
sel_prod  = st.multiselect("Filter products (affects charts below)",
                           prod_opts, default=prod_opts)
prod_clause = "AND mp.product IN (" + ",".join("'" + p + "'" for p in sel_prod) + ")"

# ════════════════════════════════════════════════════════════════
#  SECTION 5 · Nudges vs Activity
# ════════════════════════════════════════════════════════════════
nv = qdf(f"""
WITH d AS (
  SELECT user, COUNT(DISTINCT activity_date) AS days
  FROM act_src
  WHERE activity_date BETWEEN DATE '{sd}' AND DATE '{ed}'
  GROUP BY 1
)
SELECT days, COUNT(*) AS users
FROM d GROUP BY 1 ORDER BY 1;
""")
nv["pct"] = (nv.users / nv.users.sum() * 100).round(1)

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
  SELECT m.sendout_name,
         customer_external_id AS user,
         delivered IS NOT NULL                              AS is_delivered,
         button_responses + link_clicks                     AS clicks
  FROM {scan_c} m
  LEFT JOIN parquet_scan('{MAP_FILE}') mp
    ON CAST(m.business_id AS VARCHAR)=CAST(mp.business_id AS VARCHAR)
  WHERE dispatched_at::DATE BETWEEN DATE '{sd}' AND DATE '{ed}' {prod_clause}
)
SELECT sendout_name,
       COUNT(DISTINCT user)                               AS messages_sent,
       COUNT(DISTINCT CASE WHEN is_delivered THEN user END) AS messages_delivered,
       ROUND(SUM(clicks)*100.0/NULLIF(COUNT(DISTINCT user),0),1) AS click_rate,
       ROUND(COUNT(*)*100.0/SUM(COUNT(*)) OVER (),1)       AS pct_of_total
FROM msgs GROUP BY 1 ORDER BY messages_sent DESC;
""")
camp["delivery_rate (%)"] = (camp.messages_delivered/camp.messages_sent*100).round(1)
camp["click_rate (%)"]    = camp.click_rate
camp["% of Total"]        = camp.pct_of_total
camp = camp[["sendout_name","messages_sent","messages_delivered","delivery_rate (%)",
             "click_rate (%)","% of Total"]]

st.subheader("🎯 Campaign Performance")
st.dataframe(camp.style.format({"delivery_rate (%)":"{:.1f}%","click_rate (%)":"{:.1f}%","% of Total":"{:.1f}%"}),
             use_container_width=True)

st.caption("© 2025 Rocket Learning • internal dashboard")
