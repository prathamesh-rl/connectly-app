# -----------------------------------------------------------------
#  Connectly Messaging Dashboard · lean SQL build  (Jun-2025)
# -----------------------------------------------------------------
import streamlit as st, duckdb, pandas as pd, matplotlib.pyplot as plt, matplotlib.style as style
import datetime, glob, textwrap

# ─── theme ───────────────────────────────────────────────────────
style.use("dark_background")
BG, TXT = "#0e1117", "#d3d3d3"
plt.rcParams["text.color"] = TXT
st.set_page_config(page_title="Connectly Dashboard", layout="wide")
st.title("📊 Connectly Messaging Dashboard")

# ─── file patterns ──────────────────────────────────────────────
PAT_CAM = (
    "parquet_trim/dispatch_date=*/data_0.parquet",
    "parquet_trim/msg_*.parquet",
    "parquet_output/dispatch_date=*/data_0*.parquet",
)
PAT_ACT = (
    "activity_trim/act_*.parquet",
    "activity_chunks/activity_data_*.parquet",
)
MAP_FILE = "connectly_business_id_mapping.parquet"

def first_match(pats):
    for p in pats:
        if glob.glob(p):
            return p
    return None

P_CAM = first_match(PAT_CAM)
P_ACT = first_match(PAT_ACT)
if P_CAM is None:
    st.error("❌ No campaign Parquet files found"); st.stop()

# ─── dates ───────────────────────────────────────────────────────
MIN_D, MAX_D = datetime.date(2025, 1, 1), datetime.date(2025, 6, 8)

# ─── DuckDB helpers ─────────────────────────────────────────────
con    = duckdb.connect()
scan_c = f"parquet_scan('{P_CAM}',  union_by_name=true)"
scan_a = f"parquet_scan('{P_ACT}', union_by_name=true)" if P_ACT else None
qdf    = lambda sql: con.sql(textwrap.dedent(sql)).df()

# activity view (handles two schemas)
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

# ─── monthly charts (unchanged) ─────────────────────────────────
monthly = qdf(f"""
    SELECT DATE_TRUNC('month', CAST(dispatched_at AS TIMESTAMP))::DATE AS m,
           COUNT(DISTINCT customer_external_id||'_'||CAST(dispatched_at::DATE AS VARCHAR)) AS sent,
           COUNT(DISTINCT CASE WHEN delivered IS NOT NULL
                 THEN customer_external_id||'_'||CAST(dispatched_at::DATE AS VARCHAR) END) AS delivered
    FROM {scan_c} GROUP BY 1 ORDER BY 1;
""")
monthly["label"] = pd.to_datetime(monthly.m).dt.strftime("%b %y")
monthly["rate"]  = (monthly.delivered/monthly.sent*100).round(2)

cost = qdf(f"""
    SELECT DATE_TRUNC('month', CAST(dispatched_at AS TIMESTAMP))::DATE AS m,
           COUNT(DISTINCT CASE WHEN delivered IS NOT NULL
                 THEN customer_external_id||'_'||CAST(dispatched_at::DATE AS VARCHAR) END) AS delivered
    FROM {scan_c} GROUP BY 1 ORDER BY 1;
""")
d = cost.delivered
cost["label"]     = pd.to_datetime(cost.m).dt.strftime("%b %y")
cost["meta"]      = (d*0.96*0.0107 + d*0.04*0.0014).round(2)
cost["connectly"] = (d*0.9*0.0123 + 500).round(2)

st.subheader("📈 Monthly Messaging & Cost Overview")
fig,(ax1,ax2)=plt.subplots(1,2,figsize=(12,4),facecolor=BG)
x,w=range(len(monthly)),.35
ax1.bar([i-w/2 for i in x],monthly.sent,w,color="#00b4d8")
ax1.bar([i+w/2 for i in x],monthly.delivered,w,color="#ffb703")
for i,r in monthly.iterrows():
    ax1.text(i-w/2,r.sent,f"{r.sent/1e6:.1f}M",ha='center',va='bottom',fontsize=8)
    ax1.text(i+w/2,r.delivered,f"{r.rate:.0f}%",ha='center',va='bottom',fontsize=8)
ax1.set_xticks(x);ax1.set_xticklabels(monthly.label,rotation=45);ax1.set_title("Sent vs Delivered")
x2=range(len(cost))
ax2.plot(x2,cost.meta,marker="o",color="#00b4d8");ax2.plot(x2,cost.connectly,marker="o",color="#ffb703")
for i,r in cost.iterrows():
    ax2.text(i,r.meta,f"${r.meta:,.0f}",ha='center',va='bottom',color="#00b4d8",fontsize=8)
    ax2.text(i,r.connectly,f"${r.connectly:,.0f}",ha='center',va='bottom',color="#ffb703",fontsize=8)
ax2.set_xticks(x2);ax2.set_xticklabels(cost.label,rotation=45);ax2.set_title("Monthly Cost")
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
FROM base WHERE product<>'Unknown' GROUP BY 1
""")
tot=funnel.iloc[:,1:3].sum();tot["product"]="Total"
funnel=pd.concat([funnel, pd.DataFrame([tot])],ignore_index=True)
funnel["delivery_rate (%)"]=(funnel.messages_delivered/funnel.messages_sent*100).round(2)
st.subheader("📦 Funnel by Product (all)")
st.dataframe(funnel,use_container_width=True)

# ─── product picker ─────────────────────────────────────────────
prod_df=qdf(f"""
    SELECT DISTINCT mp.product
    FROM {scan_c} m
    LEFT JOIN parquet_scan('{MAP_FILE}') mp
      ON CAST(m.business_id AS VARCHAR)=CAST(mp.business_id AS VARCHAR)
    WHERE dispatched_at::DATE BETWEEN DATE '{sd}' AND DATE '{ed}'
      AND mp.product IS NOT NULL
""")
products=sorted(prod_df["product"].unique())
prod_sel=st.selectbox("Filter product for charts below",["All"]+products)

join_mp=f"""
  LEFT JOIN parquet_scan('{MAP_FILE}') mp
  ON CAST(m.business_id AS VARCHAR)=CAST(mp.business_id AS VARCHAR)
"""
prod_clause="" if prod_sel=="All" else f"AND mp.product = '{prod_sel}'"

# ─── nudges vs activity (%) ────────────────────────────────────
nudge_dist=qdf(f"""
WITH nudges AS (
  SELECT customer_external_id AS user
  FROM {scan_c} m
  {join_mp}
  WHERE dispatched_at::DATE BETWEEN DATE '{sd}' AND DATE '{ed}' {prod_clause}
  GROUP BY 1
)
SELECT COALESCE(a.active_days,0) AS active_days, COUNT(*) AS users
FROM nudges
LEFT JOIN (
  SELECT user, COUNT(DISTINCT activity_date) AS active_days
  FROM act_src
  WHERE activity_date BETWEEN DATE '{sd}' AND DATE '{ed}'
  GROUP BY 1
) a USING (user)
GROUP BY 1 ORDER BY 1
""")
cnt=nudge_dist.set_index("active_days").users
pct=(cnt/cnt.sum()*100).round(0).astype(int)

st.subheader("📊 Nudges vs User Activity (%)")
fig_h,ax_h=plt.subplots(figsize=(8,4),facecolor=BG)
bars=ax_h.bar(cnt.index,cnt.values,color="#90e0ef")
for b,val in zip(bars,pct[cnt.index]):
    ax_h.text(b.get_x()+b.get_width()/2,b.get_height()+.5,f"{val}%",
              ha='center',va='bottom',color=TXT,fontsize=8,rotation=0)  # ← horizontal
ax_h.set_xlabel("Active days");ax_h.set_ylabel("Users")
st.pyplot(fig_h)

# ─── campaign performance ───────────────────────────────────────
camp=qdf(f"""
WITH msgs AS (
  SELECT sendout_name,
         customer_external_id AS user,
         customer_external_id||'_'||CAST(dispatched_at::DATE AS VARCHAR) AS msg_id,
         delivered, button_responses, link_clicks
  FROM {scan_c} m
  {join_mp}
  WHERE dispatched_at::DATE BETWEEN DATE '{sd}' AND DATE '{ed}' {prod_clause}
),
base AS (
  SELECT sendout_name,
         COUNT(DISTINCT msg_id)                                     AS messages_sent,
         COUNT(*) FILTER (WHERE delivered IS NOT NULL)              AS messages_delivered,
         COUNT(button_responses)+COUNT(link_clicks)                 AS total_clicks
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
        FROM act_src WHERE activity_date BETWEEN DATE '{sd}' AND DATE '{ed}'
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
FROM base b LEFT JOIN seg_pct s USING (sendout_name)
""")

# compute & format rates
camp["delivery_rate (%)"]=(camp.messages_delivered/camp.messages_sent*100).round(1)
camp["click_rate (%)"]   =(camp.total_clicks/camp.messages_sent*100).round(1)
camp["% of Total"]       =(camp.messages_sent/camp.messages_sent.sum()*100).round(1)

for col in ["delivery_rate (%)","click_rate (%)","% of Total",
            "inactive_pct","active_pct","high_pct"]:
    camp[col]=camp[col].fillna(0).round(1).astype(str)+"%"

camp=camp.rename(columns={
    "inactive_pct":"Inactive %","active_pct":"Active %","high_pct":"Highly Active %"
})

camp=camp[[
    "sendout_name","messages_sent","messages_delivered",
    "delivery_rate (%)","click_rate (%)","% of Total",
    "Inactive %","Active %","Highly Active %"
]].sort_values("messages_sent",ascending=False)

st.subheader("🎯 Campaign Performance")
st.dataframe(camp,use_container_width=True)
