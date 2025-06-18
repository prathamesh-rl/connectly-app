# -----------------------------------------------------------------
#  Connectly Messaging Dashboard · memory-capped, multi-user build
# -----------------------------------------------------------------
import streamlit as st, duckdb, pandas as pd
import matplotlib.pyplot as plt, matplotlib.style as style
import datetime, glob, itertools, textwrap, pathlib, gc

# ─── page / theme ────────────────────────────────────────────────
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

def expand(patterns):
    return list(itertools.chain.from_iterable(glob.glob(p) for p in patterns))

FILES_CAM = expand(PAT_CAM)
FILES_ACT = expand(PAT_ACT)

if not FILES_CAM:
    st.error("❌ No campaign Parquet shards found"); st.stop()

lit = lambda paths: "[" + ", ".join(f"'{pathlib.Path(p).as_posix()}'" for p in paths) + "]"
scan_c = f"read_parquet({lit(FILES_CAM)}, union_by_name=true)"
scan_a = f"read_parquet({lit(FILES_ACT)}, union_by_name=true)" if FILES_ACT else None

@st.cache_resource(show_spinner=False)
def get_con():
    con = duckdb.connect()
    con.execute("PRAGMA memory_limit='850MB'")   # was 600 MB
    con.execute("PRAGMA temp_directory='/tmp'")  # allow spilling
    return con


con = get_con()
qdf = lambda sql: con.sql(textwrap.dedent(sql)).df()

# ─── activity temp-view (schema-adaptive) ───────────────────────
if scan_a:
    cols = list(con.sql(f"SELECT * FROM {scan_a} LIMIT 0").df().columns)
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

# ═════════════════════════ SECTION 1 – Monthly Overview ══════════════════════
@st.cache_data(ttl=900)
def monthly_df():
    return qdf(f"""
      SELECT DATE_TRUNC('month', CAST(dispatched_at AS TIMESTAMP))::DATE AS m,
             COUNT(DISTINCT customer_external_id||'_'||CAST(dispatched_at::DATE AS VARCHAR)) AS sent,
             COUNT(DISTINCT CASE WHEN delivered IS NOT NULL
                   THEN customer_external_id||'_'||CAST(dispatched_at::DATE AS VARCHAR) END) AS delivered
      FROM (SELECT dispatched_at, delivered, customer_external_id FROM {scan_c})
      GROUP BY 1 ORDER BY 1;
    """)

monthly = monthly_df()
monthly["label"] = pd.to_datetime(monthly.m).dt.strftime("%b %y")
monthly["rate"]  = (monthly.delivered/monthly.sent*100).round(1)

cost = monthly[["m","delivered"]].copy()
d = cost.delivered
cost["label"]     = pd.to_datetime(cost.m).dt.strftime("%b %y")
cost["meta"]      = (d*0.96*0.0107 + d*0.04*0.0014).round(0)
cost["connectly"] = (d*0.9*0.0123  + 500).round(0)

st.subheader("📈 Monthly Messaging & Cost Overview")
fig,(ax1,ax2)=plt.subplots(1,2,figsize=(12,4),facecolor=BG)
x,w=range(len(monthly)),.35
ax1.bar([i-w/2 for i in x],monthly.sent,w,color="#00b4d8")
ax1.bar([i+w/2 for i in x],monthly.delivered,w,color="#ffb703")
for i,r in monthly.iterrows():
    ax1.text(i-w/2,r.sent,f"{r.sent/1e6:.1f}M",ha='center',va='bottom',fontsize=8)
    ax1.text(i+w/2,r.delivered,f"{r.rate:.0f}%",ha='center',va='bottom',fontsize=8)
ax1.set_xticks(x);ax1.set_xticklabels(monthly.label,rotation=45);ax1.set_title("Sent vs Delivered")
ax2.plot(range(len(cost)),cost.meta,marker="o",color="#00b4d8",label="Meta cost")
ax2.plot(range(len(cost)),cost.connectly,marker="o",color="#ffb703",label="Connectly cost")
for i,r in cost.iterrows():
    ax2.text(i,r.meta,f"₹{r.meta:,.0f}",ha='center',va='bottom',color="#00b4d8",fontsize=8)
    ax2.text(i,r.connectly,f"₹{r.connectly:,.0f}",ha='center',va='bottom',color="#ffb703",fontsize=8)
ax2.set_xticks(range(len(cost)));ax2.set_xticklabels(cost.label,rotation=45);ax2.legend();ax2.set_title("Monthly Cost")
st.pyplot(fig)
del monthly, cost, fig; gc.collect()

# ═════════════════════════ SECTION 2 – Date filters ══════════════════════════
c1,c2=st.columns(2)
sd=c1.date_input("Start date",MAX_D-datetime.timedelta(days=30),min_value=MIN_D,max_value=MAX_D)
ed=c2.date_input("End date",MAX_D,min_value=MIN_D,max_value=MAX_D)

# ═════════════════════════ SECTION 3 – Funnel by Product ═════════════════════
@st.cache_data(ttl=900)
def funnel_df(sd, ed):
    return qdf(f"""
      WITH base AS (
        SELECT COALESCE(mp.product,'Unknown') AS product,
               customer_external_id           AS user,
               delivered,
               button_responses>0 OR link_clicks>0 AS clicked
        FROM (SELECT business_id, customer_external_id, delivered,
                     button_responses, link_clicks, dispatched_at
              FROM {scan_c}
              WHERE dispatched_at::DATE BETWEEN '{sd}' AND '{ed}') m
        LEFT JOIN read_parquet('{MAP_FILE}') mp USING (business_id)
      )
      SELECT product,
             COUNT(DISTINCT user)                                            AS sent,
             COUNT(DISTINCT CASE WHEN delivered IS NOT NULL THEN user END)   AS delivered,
             COUNT(DISTINCT CASE WHEN clicked  THEN user END)                AS clicked
      FROM base WHERE product<>'Unknown'
      GROUP BY 1 ORDER BY sent DESC;
    """)

funnel=funnel_df(sd,ed)
total=funnel[["sent","delivered"]].sum().to_frame().T
total.insert(0,"product","Total")
total["delivery_rate"]=round(total.delivered*100/total.sent,1)
total["click_rate"]=round(funnel.clicked.mul(funnel.sent).sum()*100/total.sent,1)
funnel=pd.concat([total,funnel],ignore_index=True)

st.subheader("🪜 Funnel by Product")
st.dataframe(funnel.style.format({"delivery_rate":"{:.1f}%","click_rate":"{:.1f}%"}),
             use_container_width=True)

# ═════════════════════════ SECTION 4 – Product filter ════════════════════════
prod_opts=[p for p in funnel_df(sd,ed).product]
sel_prod=st.multiselect("Filter products (affects charts below)",prod_opts,default=prod_opts)
prod_clause="AND COALESCE(product,'Unknown') IN ("+",".join("'"+p+"'" for p in sel_prod)+")"

# ═════════════════════════ SECTION 5 – Nudges vs Activity ════════════════════
@st.cache_data(ttl=900)
def nudge_dist(sd, ed, clause):
    return qdf(f"""
      WITH nudges AS (
        SELECT customer_external_id AS user
        FROM (SELECT business_id, customer_external_id, dispatched_at
              FROM {scan_c}
              WHERE dispatched_at::DATE BETWEEN '{sd}' AND '{ed}') m
        LEFT JOIN read_parquet('{MAP_FILE}') mp USING (business_id)
        WHERE 1=1 {clause}
        GROUP BY 1
      ), act AS (
        SELECT user, COUNT(DISTINCT activity_date) AS days
        FROM act_src WHERE activity_date BETWEEN '{sd}' AND '{ed}'
        GROUP BY 1
      )
      SELECT COALESCE(days,0) AS days, COUNT(*) AS users
      FROM nudges LEFT JOIN act USING(user)
      GROUP BY 1 ORDER BY 1;
    """)

nv=nudge_dist(sd,ed,prod_clause)
nv["pct"]=(nv.users/nv.users.sum()*100).round(0).astype(int)

st.subheader("📊 Nudges vs User Activity")
fig_h,ax=plt.subplots(figsize=(8,4),facecolor=BG)
bars=ax.bar(nv.days,nv.users,color="#90e0ef")
for b,val in zip(bars,nv.pct):
    ax.text(b.get_x()+b.get_width()/2,b.get_height()+1,f"{val}%",
            ha='center',va='bottom',fontsize=8)
ax.set_xlabel("Active days");ax.set_ylabel("Users")
st.pyplot(fig_h)
del nv, fig_h; gc.collect()

# ═════════════════════════ SECTION 6 – Campaign table ════════════════════════
@st.cache_data(ttl=900)
def campaign_df(sd, ed, clause):
    return qdf(f"""
      WITH msgs AS (
        SELECT sendout_name,
               customer_external_id AS user,
               delivered IS NOT NULL                  AS deliv,
               button_responses+link_clicks           AS clicks
        FROM (SELECT sendout_name, business_id, customer_external_id, delivered,
                     button_responses, link_clicks, dispatched_at
              FROM {scan_c}
              WHERE dispatched_at::DATE BETWEEN '{sd}' AND '{ed}') m
        LEFT JOIN read_parquet('{MAP_FILE}') mp USING (business_id)
        WHERE 1=1 {clause}
      ), base AS (
        SELECT sendout_name,
               COUNT(DISTINCT user)                            AS sent,
               COUNT(DISTINCT CASE WHEN deliv THEN user END)   AS delivered,
               SUM(clicks)                                     AS clicks
        FROM msgs GROUP BY 1
      ), seg AS (
        SELECT DISTINCT sendout_name, user FROM msgs
      ), bucket AS (
        SELECT s.sendout_name,
               CASE WHEN COALESCE(a.days,0)=0                THEN 'Inactive'
                    WHEN COALESCE(a.days,0) BETWEEN 1 AND 10 THEN 'Active'
                    ELSE                                         'Highly Active'
               END AS seg
        FROM seg s
        LEFT JOIN (
          SELECT user, COUNT(DISTINCT activity_date) AS days
          FROM act_src
          WHERE activity_date BETWEEN '{sd}' AND '{ed}'
          GROUP BY 1
        ) a USING (user)
      ), pct AS (
        SELECT sendout_name,
               ROUND(SUM(seg='Inactive')      *100.0/COUNT(*),1) AS inactive_pct,
               ROUND(SUM(seg='Active')        *100.0/COUNT(*),1) AS active_pct,
               ROUND(SUM(seg='Highly Active') *100.0/COUNT(*),1) AS high_pct
        FROM bucket GROUP BY 1
      )
      SELECT b.*, p.*
      FROM base b LEFT JOIN pct p USING (sendout_name)
    """)

camp=campaign_df(sd,ed,prod_clause)
camp["delivery_rate (%)"]=(camp.delivered/camp.sent*100).round(1)
camp["click_rate (%)"]   =(camp.clicks   /camp.sent*100).round(1)
camp["% of Total"]       =(camp.sent     /camp.sent.sum()*100).round(1)
for col in ["inactive_pct","active_pct","high_pct","delivery_rate (%)","click_rate (%)","% of Total"]:
    camp[col]=camp[col].fillna(0).astype(str)+"%"
camp=camp.rename(columns={"inactive_pct":"Inactive %","active_pct":"Active %","high_pct":"Highly Active %"})\
         .sort_values("sent",ascending=False)

st.subheader("🎯 Campaign Performance")
st.dataframe(camp,use_container_width=True)
del camp; gc.collect()

# ════════════════════════════════════════════════════════════════
st.caption("© 2025 Rocket Learning • internal dashboard")
