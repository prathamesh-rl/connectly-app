# -----------------------------------------------------------------
#  Connectly Messaging Dashboard (Render-compatible version)
# -----------------------------------------------------------------
import streamlit as st, duckdb, pandas as pd, matplotlib.pyplot as plt, matplotlib.style as style
import urllib.request, os, gc

# Theme setup
style.use("dark_background")
BG, TXT = "#0e1117", "#d3d3d3"
plt.rcParams["text.color"] = TXT
st.set_page_config(page_title="Connectly Dashboard", layout="wide")
st.title("📊 Connectly Messaging Dashboard")

# Download & connect to DuckDB
@st.cache_resource(show_spinner=False)
def get_con():
    url = "https://huggingface.co/datasets/pbhumble/connectly-parquet/resolve/main/connectly_slim_new.duckdb"
    db_path = "/tmp/connectly_slim_new.duckdb"
    if not os.path.exists(db_path):
        urllib.request.urlretrieve(url, db_path)
    return duckdb.connect(db_path, read_only=True)

con = get_con()
qdf = lambda q: con.sql(q).df()

# Month & product filter setup
months_df = qdf("SELECT DISTINCT month FROM monthly_metrics ORDER BY month")
months = months_df['month'].tolist()
month_labels = pd.to_datetime(months).strftime("%b %Y").tolist()
month_map = dict(zip(month_labels, months))
products = qdf("SELECT DISTINCT product FROM funnel_by_product ORDER BY product")["product"].tolist()

# Filter UI (not applied to monthly graph)
st.markdown("### 📅 Months")
sel_months = st.multiselect("", month_labels, default=["May 2025"], key="month_filter", label_visibility="collapsed")

# Monthly messaging & cost overview
monthly = qdf("SELECT * FROM monthly_metrics ORDER BY month")
monthly["label"] = pd.to_datetime(monthly.month).dt.strftime("%b %y")

# Hardcoded delivery for chart
hardcoded = {
    "Jan 2025": 1843291,
    "Feb 2025": 2475248,
    "Mar 2025": 4025949,
    "Apr 2025": 3566647,
    "May 2025": 4796896,
    "Jun 2025": 2517590,
}
monthly["delivered"] = monthly["label"].map(hardcoded)
monthly["delivery_rate"] = (monthly["delivered"] / monthly["sent"] * 100).round(1)
monthly["meta_cost"] = (monthly["delivered"] * (0.96 * 0.0107 + 0.04 * 0.0014)).round()
monthly["connectly_cost"] = (monthly["delivered"] * 0.90 * 0.0123 + 500).round()

# Plotting
st.subheader("📈 Monthly Messaging & Cost Overview")
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4), facecolor=BG)
x = range(len(monthly)); w = 0.35
ax1.bar([i - w/2 for i in x], monthly.sent, w, color="#00b4d8")
ax1.bar([i + w/2 for i in x], monthly.delivered, w, color="#fb8500")
for i, r in monthly.iterrows():
    ax1.text(i - w/2, r.sent, f"{r.sent//1_000_000}M", ha='center', va='bottom', fontsize=8)
    ax1.text(i + w/2, r.delivered, f"{r.delivery_rate:.0f}%", ha='center', va='bottom', fontsize=8)
ax1.set_xticks(x); ax1.set_xticklabels(monthly.label, rotation=45); ax1.set_title("Sent vs Delivered")

ax2.plot(x, monthly.meta_cost, marker="o", label="Meta $")
ax2.plot(x, monthly.connectly_cost, marker="o", label="Connectly $")
ax2.set_xticks(x); ax2.set_xticklabels(monthly.label, rotation=45)
ax2.legend(); ax2.set_title("Monthly Cost")
st.pyplot(fig); del fig, monthly; gc.collect()

# Filter logic (for other sections)
sel_month_dates = [month_map[m] for m in sel_months]
month_clause = f"month IN {tuple(sel_month_dates)}"
prod_clause = f"product IN {tuple(products)}"

# Funnel by Product
funnel = qdf(f"""
SELECT product, SUM(sent) AS sent, SUM(delivered) AS delivered,
ROUND(SUM(clicked)*100.0/SUM(sent),1) AS click_rate,
ROUND(SUM(delivered)*100.0/SUM(sent),1) AS delivery_rate
FROM funnel_by_product
WHERE {month_clause}
GROUP BY 1 ORDER BY sent DESC;
""")
totals = funnel[["sent", "delivered"]].sum().to_frame().T
totals["click_rate"] = (funnel["click_rate"] * funnel["sent"]).sum() / funnel["sent"].sum()
totals["delivery_rate"] = (funnel["delivery_rate"] * funnel["sent"]).sum() / funnel["sent"].sum()
totals.insert(0, "product", "Total")
funnel = pd.concat([funnel, totals], ignore_index=True)
funnel[["sent", "delivered"]] = funnel[["sent", "delivered"]].astype(int)

st.subheader("🪜 Funnel by Product")
st.dataframe(
    funnel.style.format({
        "click_rate": "{:.1f}%", "delivery_rate": "{:.1f}%"
    }).apply(lambda x: ['font-weight: bold; background-color: #333' if x.name == len(funnel)-1 else '' for _ in x], axis=1),
    use_container_width=True
)

# Product filter
sel_products = st.multiselect(
    "🔐 Products", options=products, default=products,
    key="product_filter", label_visibility="visible"
)
prod_clause = f"product IN {tuple(sel_products)}"

# Nudge vs Activity
nudge = qdf(f"""
SELECT active_bucket AS days, SUM(low_freq) AS low, SUM(mid_freq) AS mid, SUM(high_freq) AS high
FROM nudge_vs_activity
WHERE {month_clause} AND {prod_clause}
GROUP BY 1 ORDER BY CASE days WHEN '0' THEN 0 WHEN '1-10' THEN 1 ELSE 2 END
""")
nudge["total"] = nudge[["low", "mid", "high"]].sum(axis=1)

st.subheader("📊 Nudges vs User Activity")
fig, ax = plt.subplots(figsize=(8, 4), facecolor=BG)
bottom = [0]*len(nudge)
colors = ["#80cfa9", "#f2c94c", "#ef476f"]
labels = ["Low", "Mid", "High"]
for i, col in enumerate(["low", "mid", "high"]):
    ax.bar(nudge.days, nudge[col], bottom=bottom, label=labels[i], color=colors[i])
    bottom = [sum(x) for x in zip(bottom, nudge[col])]
ax.set_xlabel("Activity Tier"); ax.set_ylabel("Users"); ax.legend()
st.pyplot(fig); del fig

# Campaign Performance
campaigns = qdf(f"""
SELECT sendout_name, SUM(sent) AS sent, SUM(delivered) AS delivered,
ROUND(SUM(clicked)*100.0/SUM(sent),1) AS click_rate,
ROUND(SUM(delivered)*0.96*0.0107 + SUM(delivered)*0.04*0.0014, 2) AS cost,
ROUND(SUM(inactive_low*100.0/inactive),1) AS inact_low,
ROUND(SUM(inactive_mid*100.0/inactive),1) AS inact_mid,
ROUND(SUM(inactive_high*100.0/inactive),1) AS inact_high,
ROUND(SUM(active_low*100.0/active),1) AS act_low,
ROUND(SUM(active_mid*100.0/active),1) AS act_mid,
ROUND(SUM(active_high*100.0/active),1) AS act_high,
ROUND(SUM(high_low*100.0/high),1) AS high_low,
ROUND(SUM(high_mid*100.0/high),1) AS high_mid,
ROUND(SUM(high_high*100.0/high),1) AS high_high
FROM campaign_perf
WHERE {month_clause} AND {prod_clause}
GROUP BY 1 ORDER BY sent DESC
""")

st.subheader("🎯 Campaign Performance")
st.dataframe(
    campaigns.style.format({
        "click_rate": "{:.1f}%", "cost": "${:.2f}",
        "inact_low": "{:.0f}%", "inact_mid": "{:.0f}%", "inact_high": "{:.0f}%",
        "act_low": "{:.0f}%", "act_mid": "{:.0f}%", "act_high": "{:.0f}%",
        "high_low": "{:.0f}%", "high_mid": "{:.0f}%", "high_high": "{:.0f}%"
    }),
    use_container_width=True
)

st.caption("© 2025 Rocket Learning · Internal Dashboard")
