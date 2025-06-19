# -----------------------------------------------------------------
#  Connectly Messaging Dashboard · Streamlit + DuckDB
# -----------------------------------------------------------------
import streamlit as st, duckdb, pandas as pd, matplotlib.pyplot as plt, matplotlib.style as style
import datetime, gc, os, requests

# Theme / page ----------------------------------------------------
style.use("dark_background")
BG, TXT = "#0e1117", "#d3d3d3"
plt.rcParams["text.color"] = TXT
st.set_page_config(page_title="Connectly Dashboard", layout="wide")
st.title("📊 Connectly Messaging Dashboard")

# Download DuckDB file if not present -----------------------------
DB_PATH = "connectly_slim_new.duckdb"
DB_URL = "https://huggingface.co/datasets/pbhumble/connectly-parquet/resolve/main/connectly_slim_new.duckdb"

if not os.path.exists(DB_PATH):
    with st.spinner("Downloading database..."):
        with open(DB_PATH, "wb") as f:
            f.write(requests.get(DB_URL).content)

# DuckDB connection -----------------------------------------------
@st.cache_resource(show_spinner=False)
def get_con():
    return duckdb.connect(DB_PATH, read_only=True)
con = get_con()
qdf = lambda q: con.sql(q).df()

# ── Month filter options (No filter on cost overview) ------------
months = qdf("SELECT DISTINCT month FROM monthly_metrics ORDER BY month").month
month_labels = pd.to_datetime(months).strftime("%b %Y").tolist()
month_map = dict(zip(month_labels, months))
default_months = ["May 2025"]

# ═════ 1. Monthly Messaging & Cost Overview ══════════════════════
monthly = qdf("SELECT * FROM monthly_metrics ORDER BY month")
monthly["label"] = pd.to_datetime(monthly.month).dt.strftime("%b %y")

# Hardcoded delivery & cost
hardcoded = {
    "Jan 25": 1843291,
    "Feb 25": 2475248,
    "Mar 25": 4025949,
    "Apr 25": 3566647,
    "May 25": 4796896,
    "Jun 25": 2517590,
}
monthly["delivered"] = monthly.label.map(hardcoded)
monthly["delivery_rate"] = (monthly.delivered / monthly.sent * 100).round(1)
monthly["meta_cost"] = (monthly.delivered * 0.96 * 0.0107 + monthly.delivered * 0.04 * 0.0014).round(0)
monthly["connectly_cost"] = (monthly.delivered * 0.90 * 0.0123 + 500).round(0)

st.subheader("📈 Monthly Messaging & Cost Overview")
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4), facecolor=BG)
x, w = range(len(monthly)), .35
ax1.bar([i - w/2 for i in x], monthly.sent,       w, color="#00b4d8", label="Sent")
ax1.bar([i + w/2 for i in x], monthly.delivered,  w, color="#ffb703", label="Delivered")
for i, r in monthly.iterrows():
    ax1.text(i - w/2, r.sent,       f"{r.sent//1_000:,}", ha='center', va='bottom', fontsize=8)
    ax1.text(i + w/2, r.delivered,  f"{r.delivery_rate:.0f}%", ha='center', va='bottom', fontsize=8)
ax1.set_xticks(x); ax1.set_xticklabels(monthly.label, rotation=45); ax1.set_title("Sent vs Delivered")
ax2.plot(x, monthly.meta_cost,       marker="o", label="Meta $", color="#57cc99")
ax2.plot(x, monthly.connectly_cost,  marker="o", label="Connectly $", color="#f28482")
ax2.set_xticks(x); ax2.set_xticklabels(monthly.label, rotation=45)
ax2.legend(); ax2.set_title("Monthly Cost ($)")
st.pyplot(fig); del monthly, fig; gc.collect()

# Filters below
c1, c2 = st.columns(2)
sel_months = c1.multiselect("📅 Filter by Months", month_labels, default=default_months)
products = qdf("SELECT DISTINCT product FROM funnel_by_product ORDER BY product").product.tolist()
sel_products = c2.multiselect("🛍️ Filter by Products", products, default=products)

sel_month_dates = tuple(month_map[m] for m in sel_months)
month_clause = f"month IN {sel_month_dates}"
prod_clause  = f"product IN {tuple(sel_products)}"

# ═════ 2. Funnel by Product ══════════════════════════════════════
funnel = qdf(f"""
    SELECT product, 
           SUM(sent)       AS sent,
           SUM(delivered)  AS delivered,
           ROUND(SUM(delivered)*100.0/SUM(sent),1) AS delivery_rate,
           ROUND(SUM(clicked)*100.0/SUM(sent),1)   AS click_rate
    FROM funnel_by_product
    WHERE {month_clause} AND {prod_clause}
    GROUP BY 1
    ORDER BY sent DESC
""")
tot = funnel[["sent","delivered"]].sum().to_frame().T
tot.insert(0,"product","Total")
tot["delivery_rate"] = (tot.delivered*100/tot.sent).round(1)
tot["click_rate"]    = (funnel["click_rate"].mean()).round(1)
funnel = pd.concat([funnel, tot], ignore_index=True)

st.subheader("🪜 Funnel by Product")
st.dataframe(
    funnel.style.format({
        "sent": "{:.0f}", "delivered": "{:.0f}",
        "delivery_rate": "{:.1f}%", "click_rate": "{:.1f}%"
    }).apply(lambda x: ['background-color: #333' if x.name == len(funnel)-1 else '' for _ in x], axis=1),
    use_container_width=True
)

# Product filter box restyled
st.markdown("""
<style>
div[data-baseweb="select"] > div {
  background-color: #222 !important;
  color: #ccc !important;
}
</style>
""", unsafe_allow_html=True)

# ═════ 3. Nudges vs Activity w/ Frequency Layer ═════════════════
activity = qdf(f"""
    SELECT active_bucket, freq_bucket, SUM(users) AS users
    FROM nudge_vs_activity
    WHERE {month_clause} AND {prod_clause}
    GROUP BY 1,2
""")
activity["activity_label"] = activity.active_bucket.map({
    "0": "Inactive (0 Days)",
    "1-10": "Active (1–10 Days)",
    ">10": "Highly Active (>10 Days)"
})
pivoted = activity.pivot(index="activity_label", columns="freq_bucket", values="users").fillna(0)
pivoted = pivoted[["low", "medium", "high"]]  # Ensure column order
pivoted = pivoted.loc[["Inactive (0 Days)", "Active (1–10 Days)", "Highly Active (>10 Days)"]]  # Sort x-axis

st.subheader("📊 Nudges vs User Activity (with Frequency)")
fig, ax = plt.subplots(figsize=(8,4), facecolor=BG)
labels = pivoted.index.tolist()
low, med, high = pivoted["low"], pivoted["medium"], pivoted["high"]
ax.bar(labels, low, color="#90be6d", label="Low (<5×M)")
ax.bar(labels, med, bottom=low, color="#f9c74f", label="Mid (5–10×M)")
ax.bar(labels, high, bottom=low+med, color="#f94144", label="High (>10×M)")
ax.legend(); ax.set_ylabel("Users")
st.pyplot(fig); del activity, fig, pivoted; gc.collect()

# ═════ 4. Campaign Performance Table ════════════════════════════
campaigns = qdf(f"""
    SELECT
        sendout_name,
        SUM(sent) AS sent,
        SUM(delivered) AS delivered,
        ROUND(SUM(delivered)*0.96*0.0107 + SUM(delivered)*0.04*0.0014, 0) AS cost,
        ROUND(SUM(clicks)*100.0/SUM(sent), 1) AS click_rate,
        ROUND(AVG(inactive_pct),1) AS inactive,
        ROUND(AVG(active_pct),1) AS active,
        ROUND(AVG(high_pct),1) AS high,
        ROUND(AVG(inactive_low),1) AS inactive_low,
        ROUND(AVG(inactive_med),1) AS inactive_med,
        ROUND(AVG(inactive_high),1) AS inactive_high,
        ROUND(AVG(active_low),1) AS active_low,
        ROUND(AVG(active_med),1) AS active_med,
        ROUND(AVG(active_high),1) AS active_high,
        ROUND(AVG(high_low),1) AS high_low,
        ROUND(AVG(high_med),1) AS high_med,
        ROUND(AVG(high_high),1) AS high_high
    FROM campaign_perf
    WHERE {month_clause} AND {prod_clause}
    GROUP BY 1
    ORDER BY sent DESC
""")
st.subheader("🎯 Campaign Performance")
st.dataframe(
    campaigns.style.format({
        "sent": "{:.0f}", "delivered": "{:.0f}", "cost": "${:.0f}",
        "click_rate": "{:.1f}%",
        **{col: "{:.1f}%" for col in campaigns.columns if "low" in col or "med" in col or "high" in col or col in ["inactive","active","high"]}
    }),
    use_container_width=True
)

st.caption("© 2025 Rocket Learning • internal dashboard")
