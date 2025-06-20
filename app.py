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


# ─── Funnel by Product ─────────────────────────────────────────
funnel = qdf(f"""
    SELECT product,
           SUM(sent)::INT AS sent,
           SUM(delivered)::INT AS delivered,
           ROUND(SUM(delivered)*100.0/SUM(sent), 1) AS delivery_rate
    FROM connectly_slim_new.funnel_by_product
    WHERE {month_clause}
    GROUP BY 1 ORDER BY sent DESC
""")
total = funnel[["sent", "delivered"]].sum().to_frame().T
total["delivery_rate"] = (funnel["delivered"].sum() * 100 / funnel["sent"].sum()).round(1)
total.insert(0, "product", "Total")
funnel = pd.concat([funnel, total], ignore_index=True)

st.subheader("🪜 Funnel by Product")
st.dataframe(
    funnel.style.format({
        "sent": "{:,.0f}", "delivered": "{:,.0f}", "delivery_rate": "{:.1f}%"
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


# ─── Nudge vs Activity (Layered Bar, With % and Labels) ───────────────
act = qdf(f"SELECT * FROM connectly_slim_new.nudge_vs_activity WHERE {month_clause} AND {prod_clause}")
month_count = len(sel_month_dates)

agg = act.groupby("active_bucket")[["low_freq", "med_freq", "high_freq"]].sum().astype(int)
agg = agg.loc[["Inactive (0 Days)", "Active (1-10 Days)", "Highly Active (>10 Days)"]]
agg["total"] = agg.sum(axis=1)

colors = ["#bde0fe", "#ffd60a", "#ff5a5f"]  # pastel blue, yellow, red
labels = ["Low", "Medium", "High"]

st.subheader("📊 Nudge Frequency × User Activity")
fig, ax = plt.subplots(figsize=(10, 5), facecolor="white")
bottom = None

for i, col in enumerate(["low_freq", "med_freq", "high_freq"]):
    bars = ax.bar(agg.index, agg[col], bottom=bottom, label=labels[i], color=colors[i])
    for bar in bars:
        height = bar.get_height()
        if height > 0:
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_y() + height / 2, f"{height:,}",
                    ha="center", va="center", fontsize=8, color="black")
    bottom = agg[col] if bottom is None else bottom + agg[col]

# Add % labels on top
total_users = agg["total"].sum()
for i, v in enumerate(agg["total"]):
    ax.text(i, v + 5000, f"{v * 100 / total_users:.1f}%", ha="center", fontsize=9, fontweight="bold")

ax.set_ylabel("Users")
ax.set_title("Frequency of Nudges by Activity Level")
ax.legend(title="Nudge Frequency")
plt.xticks(rotation=0)
st.pyplot(fig)
del agg, fig; gc.collect()



# ─── Campaign Performance Table ────────────────────────────────
campaigns = qdf(f"""
    SELECT sendout_name,
           SUM(sent)::INT AS sent,
           SUM(delivered)::INT AS delivered,
           ROUND(SUM(delivered)*100.0/SUM(sent),1) AS delivery_rate,
           ROUND(SUM(delivered)*0.96*0.0107 + SUM(delivered)*0.04*0.0014) AS cost,
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
        "delivery_rate": "{:.1f}%",
        "cost": "${:,.0f}",
        **{col: "{:.1f}%" for col in campaigns.columns if "%" in col or ":" in col}
    }),
    use_container_width=True
)

st.caption("© 2025 Rocket Learning · Internal Dashboard")
