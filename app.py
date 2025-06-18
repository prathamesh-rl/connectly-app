# -----------------------------------------------------------------
#  Connectly Messaging Dashboard · lean SQL build  (Jun‑2025)
# -----------------------------------------------------------------
import streamlit as st, duckdb, pandas as pd, matplotlib.pyplot as plt, matplotlib.style as style
import datetime, glob, textwrap, pathlib, os

# ─── theme ───────────────────────────────────────────────────────
style.use("dark_background")
BG, TXT = "#0e1117", "#d3d3d3"
plt.rcParams["text.color"] = TXT
plt.rcParams["axes.edgecolor"] = TXT
plt.rcParams["axes.labelcolor"] = TXT
st.set_page_config(page_title="Connectly Dashboard", layout="wide")
st.title("📊 Connectly Messaging Dashboard")

# ─── repo root & helpers ─────────────────────────────────────────
ROOT = pathlib.Path(__file__).resolve().parent
_abs  = lambda p: (ROOT / p).as_posix()

# ─── file patterns (simple, no shard gymnastics) ─────────────────
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

# ─── duckdb connection ───────────────────────────────────────────
con = duckdb.connect()
qdf = lambda sql: con.sql(textwrap.dedent(sql)).df()

# pick first pattern that matches at least one file
_first_match = lambda pats: next((p for p in pats if glob.glob(p)), None)
P_CAM = _first_match(PAT_CAM)
P_ACT = _first_match(PAT_ACT)

if not P_CAM:
    st.error("❌ No campaign Parquet files found – check repo paths or Git‑LFS.")
    st.stop()

scan_c = f"parquet_scan('{P_CAM}',  union_by_name=true)"
scan_a = f"parquet_scan('{P_ACT}', union_by_name=true)" if P_ACT else None

# ─── activity view (handles two schemas) ─────────────────────────
if scan_a:
    try:
        con.execute(f"""
            CREATE OR REPLACE TEMP VIEW act_src AS
            SELECT COALESCE(guardian_phone, moderator_phone, user_phone) AS user,
                   CAST(activity_date AS DATE) AS activity_date
            FROM {scan_a};
        """)
    except duckdb.BinderException:
        con.execute(f"""
            CREATE OR REPLACE TEMP VIEW act_src AS
            SELECT user_phone AS user,
                   CAST(activity_date AS DATE) AS activity_date
            FROM {scan_a};
        """)
else:
    con.execute("CREATE OR REPLACE TEMP VIEW act_src AS SELECT NULL AS user, NULL::DATE AS activity_date LIMIT 0;")

# ─── monthly charts ──────────────────────────────────────────────
monthly = qdf(f"""
    SELECT DATE_TRUNC('month', CAST(dispatched_at AS TIMESTAMP))::DATE AS m,
           COUNT(DISTINCT customer_external_id||'_'||CAST(dispatched_at::DATE AS VARCHAR)) AS sent,
           COUNT(DISTINCT CASE WHEN delivered IS NOT NULL
                 THEN customer_external_id||'_'||CAST(dispatched_at::DATE AS VARCHAR) END) AS delivered
    FROM {scan_c} GROUP BY 1 ORDER BY 1;
""")
monthly["label"] = pd.to_datetime(monthly.m).dt.strftime("%b %y")
monthly["rate"]  = (monthly.delivered / monthly.sent * 100).round(2)

cost = monthly[["m","delivered"]].copy()
d = cost.delivered
cost["label"]     = pd.to_datetime(cost.m).dt.strftime("%b %y")
cost["meta"]      = (d*0.96*0.0107 + d*0.04*0.0014).round(2)
cost["connectly"] = (d*0.9*0.0123 + 500).round(2)

st.subheader("📈 Monthly Messaging & Cost Overview")
fig,(ax1,ax2)=plt.subplots(1,2,figsize=(12,4),facecolor=BG)
x,w=range(len(monthly)),.35
ax1.bar([i-w/2 for i in x],monthly.sent,w,color="#00b4d8",label="Sent")
ax1.bar([i+w/2 for i in x],monthly.delivered,w,color="#ffb703",label="Delivered")
for i,r in monthly.iterrows():
    ax1.text(i-w/2,r.sent,f"{r.sent/1e6:.1f}M",ha='center',va='bottom',fontsize=8)
    ax1.text(i+w/2,r.delivered,f"{r.rate:.0f}%",ha='center',va='bottom',fontsize=8)
ax1.set_xticks(x);ax1.set_xticklabels(monthly.label,rotation=45);ax1.set_title("Sent vs Delivered");ax1.legend()

x2=range(len(cost))
ax2.plot(x2,cost.meta,marker="o",color="#00b4d8",label="Meta cost")
ax2.plot(x2,cost.connectly,marker="o",color="#ffb703",label="Connectly cost")
for i,r in cost.iterrows():
    ax2.text(i,r.meta,f"₹{r.meta:,.0f}",ha='center',va='bottom',color="#00b4d8",fontsize=8)
    ax2.text(i,r.connectly,f"₹{r.connectly:,.0f}",ha='center',va='bottom',color="#ffb703",fontsize=8)
ax2.set_xticks(x2);ax2.set_xticklabels(cost.label,rotation=45);ax2.set_title("Monthly Cost");ax2.legend()

st.pyplot(fig)

# ─── date pickers ───────────────────────────────────────────────
c1,c2=st.columns(2)
sd=c1.date_input("Start date",MAX_D-datetime.timedelta(days=30),min_value=MIN_D,max_value=MAX_D)
ed=c2.date_input("End date",MAX_D,min_value=MIN_D,max_value=MAX_D)

# ─── funnel (all products) ──────────────────────────────────────
funnel=qdf(f"""
WITH base AS (
  SELECT COALESCE(mp.product,'Unknown') AS product,
         customer_external_id||'_'||CAST(dispatched_at::DATE AS VARCHAR) AS msg_id,
         delivered
  FROM {scan_c} m
  LEFT JOIN parquet_scan('{MAP_FILE}') mp
    ON CAST(m.business_id AS VARCHAR)=CAST(mp.business_id AS VARCHAR)
  WHERE dispatched_at::DATE BETWEEN DATE '{sd}' AND DATE '{ed}'
)
SELECT product,
       COUNT(DISTINCT msg_id) AS messages_sent,
       COUNT(*) FILTER (WHERE delivered IS NOT NULL) AS messages_delivered
FROM base WHERE product<>'Unknown' GROUP BY 1;
""")

_tot = funnel.iloc[:, 1:3].sum(); _tot["product"] = "Total"
funnel = pd.concat([funnel, pd.DataFrame([_tot])], ignore_index=True)
funnel["delivery_rate (%)"] = (funnel.messages_delivered / funnel.messages_sent * 100).round(2)

st.subheader("📦 Funnel by Product (all)")
st.dataframe(funnel, use_container_width=True)

# ─── product picker ─────────────────────────────────────────────
prod_df = qdf(f"""
    SELECT DISTINCT mp.product
    FROM {scan_c} m
    LEFT JOIN parquet_scan('{MAP_FILE}') mp
      ON CAST(m.business_id AS VARCHAR)=CAST(mp.business_id AS VARCHAR)
    WHERE dispatched_at::DATE BETWEEN DATE '{sd}' AND DATE '{ed}' AND mp.product IS NOT NULL;
""")
products = sorted(prod_df["product"].unique())
prod_sel = st.selectbox("Filter product for charts below", ["All"] + products)
prod_clause = "" if prod_sel == "All" else f"AND mp.product = '{prod_sel}'"

# ─── activity distribution ─────────────────────────────────────
activity = qdf(f"""
WITH prod_users AS (
  SELECT DISTINCT customer_external_id
  FROM {scan_c} m
  LEFT JOIN parquet_scan('{MAP_FILE}') mp
    ON CAST(m.business_id AS VARCHAR)=CAST(mp.business_id AS VARCHAR)
  WHERE dispatched_at::DATE BETWEEN DATE '{sd}' AND DATE '{ed}' {prod_clause}
),
activity AS (
  SELECT user AS customer_external_id,
         COUNT(DISTINCT activity_date) AS days_active
  FROM act_src
  WHERE activity_date BETWEEN DATE '{sd}' AND DATE '{ed}' GROUP BY 1
),
merged AS (
  SELECT pu.customer_external_id,
         COALESCE(a.days_active,0) AS days_active
  FROM prod_users pu LEFT JOIN activity a USING(customer_external_id)
)
SELECT days_active, COUNT(*) AS users
FROM merged GROUP BY 1 ORDER BY 1;
""")
activity["pct"] = (activity.users / activity.users.sum() * 100).round(1)

st.subheader("👥 Nudges vs User Activity")
fig2, ax = plt.subplots(figsize=(8, 4), facecolor=BG)
ax.bar(activity.days_active, activity.users, color="#38b000")
for x, y, p in activity.itertuples(index=False):
    ax.text(x, y, f"{p:.0f}%", ha='center', va='bottom', fontsize=8)
ax.set_xlabel("Active days in period"); ax.set_ylabel("User count")
st.pyplot(fig2)

# ─── campaign performance ──────────────────────────────────────
camp=qdf(f"""
WITH msgs AS (
  SELECT sendout_name,
         customer_external_id AS user,
         customer_external_id||'_'||CAST(dispatched_at::DATE AS VARCHAR) AS msg_id,
         delivered, COALESCE(button_responses,0) AS br, COALESCE(link_clicks,0) AS lc
  FROM {scan_c} m
  LEFT JOIN parquet_scan('{MAP_FILE}') mp
    ON CAST(m.business_id AS VARCHAR)=CAST(mp.business_id AS VARCHAR)
  WHERE dispatched_at::DATE BETWEEN DATE '{sd}' AND DATE '{ed}' {prod_clause}
),
base AS (
  SELECT sendout_name,
         COUNT(DISTINCT msg_id)                                     AS messages_sent,
         COUNT(*) FILTER (WHERE delivered IS NOT NULL)              AS messages_delivered,
         SUM(br)+SUM(lc)                                            AS total_clicks
  FROM msgs GROUP BY 1
),
segments AS (
  SELECT DISTINCT sendout_name, user FROM msgs
), j AS (
  SELECT s.sendout_name,
         CASE WHEN COALESCE(a.active_days,0)=0                THEN 'Inactive'
              WHEN COALESCE(a.active_days,0) BETWEEN 1 AND 10 THEN 'Active'
              ELSE                                               'Highly Active' END AS seg
  FROM segments s
  LEFT JOIN (
        SELECT user, COUNT(DISTINCT activity_date) AS active_days
        FROM act_src WHERE activity_date BETWEEN DATE '{sd}' AND DATE '{ed}'
        GROUP BY 1) a USING(user)
)
SELECT b.sendout_name,
       b.messages_sent,
       b.messages_delivered,
       ROUND(b.messages_delivered::DOUBLE/b.messages_sent*100,2) AS "delivery_rate (%)",
       ROUND(b.total_clicks::DOUBLE/b.messages_sent*100,2)        AS "click_rate (%)",
       ROUND(b.messages_sent::DOUBLE/SUM(b.messages_sent) OVER()*100,2) AS "% of Total",
       ROUND(100*SUM(CASE WHEN seg='Inactive'      THEN 1 END) OVER(PARTITION BY b.sendout_name)/NULLIF(b.messages_sent,0),1) AS "Inactive %",
       ROUND(100*SUM(CASE WHEN seg='Active'        THEN 1 END) OVER(PARTITION BY b.sendout_name)/NULLIF(b.messages_sent,0),1) AS "Active %",
       ROUND(100*SUM(CASE WHEN seg='Highly Active' THEN 1 END) OVER(PARTITION BY b.sendout_name)/NULLIF(b.messages_sent,0),1) AS "Highly Active %"
FROM base b
JOIN j USING(sendout_name)
GROUP BY 1,2,3,4,5,6
ORDER BY messages_sent DESC;
""")

for col in ["delivery_rate (%)","click_rate (%)","% of Total","Inactive %","Active %","Highly Active %"]:
    camp[col]=camp[col].fillna(0).round(1).astype(str)+"%"

camp=camp[[
    "sendout_name","messages_sent","messages_delivered",
    "delivery_rate (%)","click_rate (%)","% of Total",
    "Inactive %","Active %","Highly Active %"
]]

st.subheader("🎯 Campaign Performance")
st.dataframe(camp,use_container_width=True)
