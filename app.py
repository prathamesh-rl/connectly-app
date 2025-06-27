import streamlit as st, duckdb, pandas as pd, matplotlib.pyplot as plt
import matplotlib.style as style, gc, requests, os

# ─── Configuration ─────────────────────────────────────────────
style.use("default")  # use matplotlib's default light theme
BG, TXT = "#ffffff", "#000000"  # white background, black text
plt.rcParams["text.color"] = TXT
plt.rcParams["axes.labelcolor"] = TXT
plt.rcParams["xtick.color"] = TXT
plt.rcParams["ytick.color"] = TXT
plt.rcParams["axes.edgecolor"] = TXT
st.set_page_config(layout="wide", page_title="Connectly Dashboard")

st.title("📊 Connectly Dashboard")

DB_URL = "https://huggingface.co/datasets/pbhumble/connectly-parquet/resolve/main/connectly_slim_new.duckdb"
DB_PATH = "connectly_slim_new.duckdb"

@st.cache_resource(show_spinner=False)
def get_con():
    if not os.path.exists(DB_PATH):
        with open(DB_PATH, "wb") as f:
            f.write(requests.get(DB_URL).content)
    return duckdb.connect(DB_PATH, read_only=True)

con = get_con()
qdf = lambda q: con.sql(q).df()

# ─── Monthly Messaging (unfiltered) ────────────────────────────
monthly = qdf("SELECT * FROM connectly_slim_new.monthly_metrics ORDER BY month")
monthly["label"] = pd.to_datetime(monthly.month).dt.strftime("%b %y")

delivered_map = {
    "2025-01-01": 1990000,
    "2025-02-01": 2475248,
    "2025-03-01": 4025949,
    "2025-04-01": 3566647,
    "2025-05-01": 4400000,
    "2025-06-01": 2517590
}
monthly["delivered"] = monthly.month.astype(str).map(delivered_map).fillna(0).astype(int)
monthly["meta_cost"] = (monthly.delivered * 0.96 * 0.0107 + monthly.delivered * 0.04 * 0.0014).round()
monthly["connectly_cost"] = (monthly.delivered * 0.90 * 0.0123 + 500).round()

try:
    sent_total = qdf("SELECT * FROM connectly_slim_new.monthly_sent_total ORDER BY month")
    sent_total_dict = dict(zip(sent_total.month.astype(str), sent_total.total_sent))
    monthly["sent_total"] = monthly.month.astype(str).map(sent_total_dict).fillna(monthly.delivered)
except:
    monthly["sent_total"] = monthly.delivered

# Monthly Messaging & Cost Overview
st.subheader("📈 Monthly Messaging & Cost Overview")
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4), facecolor=BG)
x = range(len(monthly))

# --- Left: Delivered ---
bars = ax1.bar(x, monthly.delivered, width=0.5, color="#4A90E2", label="Delivered")
for i, r in enumerate(monthly.delivered):
    ax1.text(i, r, f"{r/1e6:.1f}M", ha='center', va='bottom', fontsize=8, color='black')
ax1.set_xticks(x)
ax1.set_xticklabels(monthly.label, rotation=45)
ax1.set_title("Delivered Messages")
ax1.legend()

# --- Right: Costs ---
ax2.plot(x, monthly.meta_cost, marker="o", label="Meta $", color="#1F77B4")      # softer blue
ax2.plot(x, monthly.connectly_cost, marker="o", label="Connectly $", color="#FF7F0E")  # orange
for i in x:
    ax2.text(i, monthly.meta_cost[i], f"${monthly.meta_cost[i]:,.0f}", ha='center', va='bottom', fontsize=8)
    ax2.text(i, monthly.connectly_cost[i], f"${monthly.connectly_cost[i]:,.0f}", ha='center', va='bottom', fontsize=8)
ax2.set_xticks(x)
ax2.set_xticklabels(monthly.label, rotation=45)
ax2.set_title("Monthly Cost")
ax2.legend()

st.pyplot(fig)
del monthly, fig
gc.collect()


# ─── Filters (Below Graphs) ────────────────────────────────────
months_df = qdf("SELECT DISTINCT month FROM connectly_slim_new.funnel_by_product ORDER BY month")
months = months_df["month"].tolist()
month_labels = pd.to_datetime(months).strftime("%b %Y").tolist()

sel_months = st.multiselect("📅 Months", month_labels, default=["May 2025"])
sel_month_dates = [months[month_labels.index(m)] for m in sel_months]
month_clause = "month IN (" + ", ".join([f"DATE '{d}'" for d in sel_month_dates]) + ")"

# Fetch product list and show filter (even if UI comes later, sel_products must be defined early)
products_df = qdf("SELECT DISTINCT product FROM connectly_slim_new.funnel_by_product ORDER BY product")
products = products_df["product"].tolist()
sel_products = products  # default to all products initially

# This prevents sel_products from being undefined before funnel block
if not sel_month_dates or not sel_products:
    st.warning("Please select at least one month and one product.")
    st.stop()
# ─── Funnel by Product ─────────────────────────────────────────
funnel = qdf(f"""
    SELECT product AS Product,
           SUM(sent)::INT AS Sent,
           SUM(delivered)::INT AS Delivered,
           ROUND(SUM(delivered)*100.0/SUM(sent), 1) AS "Delivery Rate"
    FROM connectly_slim_new.funnel_by_product
    WHERE {month_clause}
    GROUP BY 1 ORDER BY sent DESC
""")
total = funnel[["Sent", "Delivered"]].sum().to_frame().T
total["Delivery Rate"] = (funnel["Delivered"].sum() * 100 / funnel["Sent"].sum()).round(1)
total.insert(0, "Product", "Total")
funnel = pd.concat([funnel, total], ignore_index=True)

st.subheader("🪜 Funnel by Product")
st.dataframe(
    funnel.style.format({
        "Sent": "{:,.0f}", "Delivered": "{:,.0f}", "Delivery Rate": "{:.1f}%"
    }).apply(
        lambda x: ['background-color: #f0f0f0' if x.name == funnel.index[-1] else '' for _ in x],
    axis=1
    ),
    use_container_width=True
)

# Now fetch product list and show filter
products_df = qdf("SELECT DISTINCT product FROM connectly_slim_new.funnel_by_product ORDER BY product")
products = products_df["product"].tolist()

sel_products = st.multiselect("🛍️ Products", products, default=products)

# Clause for remaining queries
prod_clause = "product IN (" + ", ".join([f"'{p}'" for p in sel_products]) + ")"


# Load the table (filtered by month and product)
act = qdf(f"""
    SELECT * FROM connectly_slim_new.nudge_vs_activity
    WHERE {month_clause} AND {prod_clause}
""")

# Pivot the data to wide format
if "users" not in act.columns:
    st.warning("No user data found for selected filters.")
    st.stop()

act["users"] = act["users"].astype(int)

pivot = act.pivot_table(
    index="active_bucket",
    values="users",
    aggfunc="sum"
)

# Rename the buckets for display
bucket_labels = {
    '0': "Inactive (0 Days)",
    '1-10': "Active (1-10 Days)",
    '>10': "Highly Active (>10 Days)"
}
pivot.index = pivot.index.map(bucket_labels)

# Ensure all expected rows are present
pivot = pivot.reindex(["Inactive (0 Days)", "Active (1-10 Days)", "Highly Active (>10 Days)"], fill_value=0)

# Plot
st.subheader("📊 Nudge Frequency × User Activity")
fig, ax = plt.subplots(figsize=(10, 5), facecolor="white")

bars = ax.bar(pivot.index, pivot["users"], color=["#bde0fe", "#ffd60a", "#ff5a5f"])

for bar in bars:
    height = bar.get_height()
    ax.text(bar.get_x() + bar.get_width() / 2, height + 500, f"{height:,}", ha="center", va="bottom", fontsize=8)

ax.set_ylabel("Users")
ax.set_title("User Activity Buckets by Nudge Exposure")
st.pyplot(fig)



# ─── Campaign Performance Table ────────────────────────────────
campaigns = qdf(f"""
    SELECT sendout_name AS "Campaign Name",
           SUM(sent)::INT AS Sent,
           SUM(delivered)::INT AS Delivered,
           ROUND(SUM(delivered)*100.0/SUM(sent),1) AS "Delivery Rate",
           ROUND(SUM(delivered)*0.96*0.0107 + SUM(delivered)*0.04*0.0014) AS Cost,
           ROUND(AVG(inactive_pct),1) AS "Inactive %",
           ROUND(AVG(active_pct),1) AS "Active %",
           ROUND(AVG(high_pct),1) AS "Highly Active %",
           ROUND(AVG(inactive_low),1) AS "Inactive: 1-4 ",
           ROUND(AVG(inactive_med),1) AS "Inactive: 5-10",
           ROUND(AVG(inactive_high),1) AS "Inactive: >10",
           ROUND(AVG(active_low),1) AS "Active: 1-4",
           ROUND(AVG(active_med),1) AS "Active: 5-10",
           ROUND(AVG(active_high),1) AS "Active: >10",
           ROUND(AVG(high_low),1) AS "High: 1-4",
           ROUND(AVG(high_med),1) AS "High: 5-10",
           ROUND(AVG(high_high),1) AS "High: >10"
    FROM connectly_slim_new.campaign_perf
    WHERE {month_clause} AND {prod_clause}
    GROUP BY 1 ORDER BY sent DESC
""")

st.subheader("🎯 Campaign Performance")
st.dataframe(
    campaigns.style.format({
        "Delivery Rate": "{:.1f}%",
        "Cost": "${:,.0f}",
        **{col: "{:.1f}%" for col in campaigns.columns if "%" in col or ":" in col}
    }),
    use_container_width=True
)

st.caption("© 2025 Rocket Learning · Internal Dashboard")
