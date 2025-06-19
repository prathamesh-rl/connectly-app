# Connectly Messaging Dashboard – Render-Optimized
import streamlit as st, duckdb, pandas as pd, matplotlib.pyplot as plt, matplotlib.style as style
import datetime, gc

# ─── Setup ────────────────────────────────────────────────
style.use("dark_background")
BG, TXT = "#0e1117", "#d3d3d3"
plt.rcParams["text.color"] = TXT
st.set_page_config(page_title="Connectly Dashboard", layout="wide")
st.title("📊 Connectly Messaging Dashboard")

# ─── DB connection (remote HuggingFace) ───────────────────
@st.cache_resource(show_spinner=False)
def get_con():
    con = duckdb.connect(database=':memory:')
    con.execute("""
        INSTALL httpfs;
        LOAD httpfs;
        SET enable_object_cache=true;
        SET s3_region='auto';
        SET s3_url_style='path';
    """)
    con.execute("""
        ATTACH 'https://huggingface.co/datasets/pbhumble/connectly-parquet/resolve/main/connectly_slim.duckdb'
        AS conn (READ_ONLY)
    """)
    return con

con = get_con()
qdf = lambda q: con.sql(q).df()

# ─── Monthly Messaging & Cost Overview ─────────────────────
monthly = qdf("SELECT * FROM conn.monthly_metrics ORDER BY month")
monthly["label"] = pd.to_datetime(monthly.month).dt.strftime("%b %Y")

st.subheader("📈 Monthly Messaging & Cost Overview")
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4), facecolor=BG)
x, w = range(len(monthly)), .35
ax1.bar([i - w/2 for i in x], monthly.sent,      w, color="#00b4d8", label="Sent")
ax1.bar([i + w/2 for i in x], monthly.delivered, w, color="#ffb703", label="Delivered")
for i, r in monthly.iterrows():
    ax1.text(i - w/2, r.sent,       f"{r.sent/1e6:.1f}M", ha='center', va='bottom', fontsize=8)
    ax1.text(i + w/2, r.delivered,  f"{r.delivery_rate:.0f}%", ha='center', va='bottom', fontsize=8)
ax1.set_xticks(x); ax1.set_xticklabels(monthly.label, rotation=45)
ax1.set_ylabel("Messages"); ax1.set_title("Sent vs Delivered"); ax1.legend()

ax2.plot(x, monthly.meta_cost,      marker="o", label="Meta ₹")
ax2.plot(x, monthly.connectly_cost, marker="o", label="Connectly ₹")
for i, r in monthly.iterrows():
    ax2.text(i, r.meta_cost,      f"₹{int(r.meta_cost):,}",      ha='center', va='bottom', fontsize=8)
    ax2.text(i, r.connectly_cost, f"₹{int(r.connectly_cost):,}", ha='center', va='bottom', fontsize=8)
ax2.set_xticks(x); ax2.set_xticklabels(monthly.label, rotation=45)
ax2.set_ylabel("Cost (₹)"); ax2.set_title("Monthly Cost"); ax2.legend()

st.pyplot(fig); del monthly, fig; gc.collect()

# ─── Filters Section (below the chart) ─────────────────────
def to_sql_in(values):
    return "(" + ",".join(f"'{v}'" for v in values) + ")"

months = qdf("SELECT DISTINCT month FROM conn.monthly_metrics ORDER BY month")["month"]
month_labels = pd.to_datetime(months).dt.strftime("%b %Y")
products = qdf("SELECT DISTINCT product FROM conn.funnel_by_product ORDER BY product")["product"]

c1, c2 = st.columns(2)
default_month = "May 2025"
sel_months = c1.multiselect("🗓️ Months", list(month_labels),
                            default=[default_month],
                            label_visibility="visible")
sel_products = c2.multiselect("🛍️ Products", list(products),
                              default=list(products),
                              label_visibility="visible")

sel_month_dates = [months[month_labels.get_loc(m)] for m in sel_months]
month_clause = f"month IN {to_sql_in(sel_month_dates)}"
prod_clause = f"product IN {to_sql_in(sel_products)}"

# ─── Funnel by Product ─────────────────────────────────────
funnel = qdf(f"""
    SELECT product,
           SUM(sent) AS sent,
           SUM(delivered) AS delivered,
           SUM(clicked) AS clicked,
           ROUND(SUM(delivered)*100.0/SUM(sent),1) AS delivery_rate,
           ROUND(SUM(clicked)*100.0/SUM(sent),1) AS click_rate
    FROM conn.funnel_by_product
    WHERE {month_clause} AND {prod_clause}
    GROUP BY 1 ORDER BY sent DESC
""")
tot = funnel[["sent","delivered","clicked"]].sum().to_frame().T
tot.insert(0, "product", "Total")
tot["delivery_rate"] = (tot.delivered*100/tot.sent).round(1)
tot["click_rate"] = (tot.clicked*100/tot.sent).round(1)
funnel = pd.concat([tot, funnel], ignore_index=True)

st.subheader("🪜 Funnel by Product")
st.dataframe(funnel.style.format({"delivery_rate":"{:.1f}%", "click_rate":"{:.1f}%"}),
             use_container_width=True)

# ─── Nudges vs User Activity ───────────────────────────────
activity = qdf(f"""
    SELECT active_bucket AS days, SUM(users) AS users
    FROM conn.nudge_vs_activity
    WHERE {month_clause} AND {prod_clause}
    GROUP BY 1 ORDER BY CASE days WHEN '0' THEN 0 WHEN '1-10' THEN 1 ELSE 2 END
""")
activity["pct"] = (activity.users / activity.users.sum() * 100).round(0).astype(int)

st.subheader("📊 Nudges vs User Activity")
fig_h, ax = plt.subplots(figsize=(8,4), facecolor=BG)
bars = ax.bar(activity.days, activity.users, color="#90e0ef")
for b, val in zip(bars, activity.pct):
    ax.text(b.get_x() + b.get_width()/2, b.get_height() + 1, f"{val}%",
            ha='center', va='bottom', fontsize=8)
ax.set_xlabel("Active days bucket"); ax.set_ylabel("Users")
st.pyplot(fig_h); del activity, fig_h; gc.collect()

# ─── Campaign Performance ──────────────────────────────────
campaigns = qdf(f"""
    SELECT sendout_name,
           SUM(sent) AS sent,
           SUM(delivered) AS delivered,
           SUM(clicks) AS clicks,
           ROUND(SUM(delivered)*100.0/SUM(sent),1) AS delivery_rate_pct,
           ROUND(SUM(clicks)*100.0/SUM(sent),1) AS click_rate_pct,
           ROUND(AVG(inactive_pct),1) AS inactive_pct,
           ROUND(AVG(active_pct),1) AS active_pct,
           ROUND(AVG(high_pct),1) AS high_pct
    FROM conn.campaign_perf
    WHERE {month_clause} AND {prod_clause}
    GROUP BY 1 ORDER BY sent DESC
""")
st.subheader("🎯 Campaign Performance")
st.dataframe(campaigns, use_container_width=True)

st.caption("© 2025 Rocket Learning • internal dashboard")
