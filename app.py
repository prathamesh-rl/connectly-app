# -----------------------------------------------------------------
#  Connectly Messaging Dashboard · Slim DuckDB (zero heavy SQL)
# -----------------------------------------------------------------
import streamlit as st, duckdb, pandas as pd, matplotlib.pyplot as plt, matplotlib.style as style
import datetime, gc

# Theme / page ----------------------------------------------------
style.use("dark_background")
BG, TXT = "#0e1117", "#d3d3d3"
plt.rcParams["text.color"] = TXT
st.set_page_config(page_title="Connectly Dashboard", layout="wide")
st.title("📊 Connectly Messaging Dashboard")

# DuckDB connection ----------------------------------------------
@st.cache_resource(show_spinner=False)
def get_con():
    return duckdb.connect("connectly_slim.duckdb", read_only=True)
con = get_con()
qdf = lambda q: con.sql(q).df()

# ── Month & product options -------------------------------------
months = qdf("SELECT DISTINCT month FROM monthly_metrics ORDER BY month").month
month_labels = pd.to_datetime(months).strftime("%b %Y")
products = qdf("SELECT DISTINCT product FROM funnel_by_product ORDER BY product").product

c1, c2 = st.columns(2)
sel_months = c1.multiselect("📅 Months", list(month_labels), default=month_labels[-6:])
sel_products = c2.multiselect("🛍️ Products", list(products), default=list(products))

# Helper to convert label→date
sel_month_dates = tuple(
    months[month_labels.get_loc(m)] if isinstance(month_labels, pd.Series)
    else months[list(month_labels).index(m)]
    for m in sel_months
)
month_clause = f"month IN {sel_month_dates}"
prod_clause  = f"product IN {tuple(sel_products)}"

# ═════ 1. Monthly overview (no filters) ═════════════════════════=
monthly = qdf("SELECT * FROM monthly_metrics ORDER BY month")
monthly["label"] = pd.to_datetime(monthly.month).dt.strftime("%b %y")

st.subheader("📈 Monthly Messaging & Cost Overview")
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4), facecolor=BG)
x, w = range(len(monthly)), .35
ax1.bar([i - w/2 for i in x], monthly.sent,       w, color="#00b4d8")
ax1.bar([i + w/2 for i in x], monthly.delivered,  w, color="#ffb703")
for i, r in monthly.iterrows():
    ax1.text(i - w/2, r.sent,       f"{r.sent/1e6:.1f}M", ha='center', va='bottom', fontsize=8)
    ax1.text(i + w/2, r.delivered,  f"{r.delivery_rate:.0f}%", ha='center', va='bottom', fontsize=8)
ax1.set_xticks(x); ax1.set_xticklabels(monthly.label, rotation=45); ax1.set_title("Sent vs Delivered")
ax2.plot(x, monthly.meta_cost,       marker="o", label="Meta ₹")
ax2.plot(x, monthly.connectly_cost,  marker="o", label="Connectly ₹")
ax2.set_xticks(x); ax2.set_xticklabels(monthly.label, rotation=45)
ax2.legend(); ax2.set_title("Monthly Cost")
st.pyplot(fig); del monthly, fig; gc.collect()

# ═════ 2. Funnel by product ═════════════════════════════════════
funnel = qdf(f"""
    SELECT product, 
           SUM(sent) AS sent,
           SUM(delivered) AS delivered,
           SUM(clicked) AS clicked,
           ROUND(SUM(delivered)*100.0/SUM(sent),1) AS delivery_rate,
           ROUND(SUM(clicked)*100.0/SUM(sent),1)   AS click_rate
    FROM funnel_by_product
    WHERE {month_clause} AND {prod_clause}
    GROUP BY 1
    ORDER BY sent DESC
""")
tot = funnel[["sent","delivered","clicked"]].sum().to_frame().T
tot.insert(0,"product","Total")
tot["delivery_rate"] = (tot.delivered*100/tot.sent).round(1)
tot["click_rate"]    = (tot.clicked*100/tot.sent).round(1)
funnel = pd.concat([tot,funnel], ignore_index=True)

st.subheader("🪜 Funnel by Product")
st.dataframe(funnel.style.format({"delivery_rate":"{:.1f}%","click_rate":"{:.1f}%"}), use_container_width=True)

# ═════ 3. Nudges vs user activity ═══════════════════════════════
activity = qdf(f"""
    SELECT active_bucket AS days, SUM(users) AS users
    FROM nudge_vs_activity
    WHERE {month_clause} AND {prod_clause}
    GROUP BY 1 ORDER BY
      CASE days WHEN '0' THEN 0 WHEN '1-10' THEN 1 ELSE 2 END
""")
activity["pct"] = (activity.users/activity.users.sum()*100).round(0).astype(int)

st.subheader("📊 Nudges vs User Activity")
fig_h, ax = plt.subplots(figsize=(8,4), facecolor=BG)
bars = ax.bar(activity.days, activity.users, color="#90e0ef")
for b,val in zip(bars, activity.pct):
    ax.text(b.get_x()+b.get_width()/2, b.get_height()+1, f"{val}%", ha='center', va='bottom', fontsize=8)
ax.set_xlabel("Active days bucket"); ax.set_ylabel("Users")
st.pyplot(fig_h); del activity, fig_h; gc.collect()

# ═════ 4. Campaign performance ═════════════════════════════════=
campaigns = qdf(f"""
    SELECT
        sendout_name,
        SUM(sent)       AS sent,
        SUM(delivered)  AS delivered,
        SUM(clicks)     AS clicks,
        ROUND(SUM(delivered)*100.0/SUM(sent),1)  AS delivery_rate_pct,
        ROUND(SUM(clicks)*100.0/SUM(sent),1)     AS click_rate_pct,
        ROUND(AVG(inactive_pct),1)               AS inactive_pct,
        ROUND(AVG(active_pct),1)                 AS active_pct,
        ROUND(AVG(high_pct),1)                   AS high_pct
    FROM campaign_perf
    WHERE {month_clause} AND {prod_clause}
    GROUP BY 1
    ORDER BY sent DESC
""")
st.subheader("🎯 Campaign Performance")
st.dataframe(campaigns, use_container_width=True)

st.caption("© 2025 Rocket Learning • internal dashboard")
