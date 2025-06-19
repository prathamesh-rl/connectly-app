import streamlit as st, duckdb, pandas as pd, matplotlib.pyplot as plt
import matplotlib.style as style, gc
import requests, os

# Always use dark theme
style.use("dark_background")
BG, TXT = "#0e1117", "#d3d3d3"
plt.rcParams["text.color"] = TXT
st.set_page_config(page_title="Connectly Dashboard", layout="wide")
st.markdown("""
    <style>
    body { background-color: #0e1117; color: #d3d3d3; }
    .stMultiSelect [data-baseweb="tag"] { background-color: #444 !important; color: white; }
    </style>
""", unsafe_allow_html=True)

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

# ─── Monthly Messaging & Cost Overview (Unfiltered) ─────────────
monthly = qdf("SELECT * FROM monthly_metrics ORDER BY month")
monthly["label"] = pd.to_datetime(monthly.month).dt.strftime("%b %y")

delivered_map = {
    "2025-01-01": 1843291,
    "2025-02-01": 2475248,
    "2025-03-01": 4025949,
    "2025-04-01": 3566647,
    "2025-05-01": 4796896,
    "2025-06-01": 2517590
}
monthly["delivered"] = monthly.month.astype(str).map(delivered_map).fillna(0).astype(int)
monthly["delivery_rate"] = (monthly.delivered / monthly.sent * 100).round(1)
monthly["meta_cost"] = (monthly.delivered * 0.96 * 0.0107 + monthly.delivered * 0.04 * 0.0014).round()
monthly["connectly_cost"] = (monthly.delivered * 0.90 * 0.0123 + 500).round()

st.subheader("📈 Monthly Messaging & Cost Overview")
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4), facecolor=BG)
x, w = range(len(monthly)), 0.35
ax1.bar([i - w/2 for i in x], monthly.sent, w, color="#00b4d8")
ax1.bar([i + w/2 for i in x], monthly.delivered, w, color="#ffb703")
for i, r in monthly.iterrows():
    ax1.text(i - w/2, r.sent, f"{int(r.sent/1e6)}M", ha='center', va='bottom', fontsize=8)
    ax1.text(i + w/2, r.delivered, f"{int(r.delivery_rate)}%", ha='center', va='bottom', fontsize=8)
ax1.set_xticks(x); ax1.set_xticklabels(monthly.label, rotation=45); ax1.set_title("Sent vs Delivered")
ax2.plot(x, monthly.meta_cost, marker="o", label="Meta $", color="#90e0ef")
ax2.plot(x, monthly.connectly_cost, marker="o", label="Connectly $", color="#f4f1bb")
ax2.set_xticks(x); ax2.set_xticklabels(monthly.label, rotation=45)
ax2.set_title("Monthly Cost"); ax2.legend()
st.pyplot(fig); del monthly, fig; gc.collect()

# ─── Filters ───────────────────────────────────────────────────
months_df = qdf("SELECT DISTINCT month FROM funnel_by_product ORDER BY month")
products_df = qdf("SELECT DISTINCT product FROM funnel_by_product ORDER BY product")
months = months_df["month"].tolist()
month_labels = pd.to_datetime(months).strftime("%b %Y").tolist()
products = products_df["product"].tolist()

c1, c2 = st.columns(2)
sel_months = c1.multiselect("📅 Months", month_labels, default=["May 2025"])
sel_products = c2.multiselect("🛍️ Products", products, default=products)

sel_month_dates = [months[month_labels.index(m)] for m in sel_months]
month_clause = f"""month IN ({', '.join([f"DATE '{d}'" for d in sel_month_dates])})"""
prod_clause = f"product IN ({', '.join(repr(p) for p in sel_products)})"


# ─── Funnel by Product ─────────────────────────────────────────
funnel = qdf(f"""
    SELECT product,
           SUM(sent)::INT AS sent,
           SUM(delivered)::INT AS delivered,
           ROUND(SUM(clicked)*100.0/SUM(sent),1) AS click_rate
    FROM conn.funnel_by_product
    WHERE {month_clause} AND {prod_clause}
    GROUP BY 1 ORDER BY sent DESC
""")
total = funnel[["sent", "delivered"]].sum().to_frame().T
total["click_rate"] = (funnel["click_rate"] * funnel["sent"]).sum() / funnel["sent"].sum()
total.insert(0, "product", "Total")
funnel = pd.concat([funnel, total], ignore_index=True)

st.subheader("🪜 Funnel by Product")
st.dataframe(
    funnel.style.format({
        "sent": "{:,.0f}",
        "delivered": "{:,.0f}",
        "click_rate": "{:.1f}%"
    }).apply(lambda x: ["background-color: #222" if v == "Total" else "" for v in x], axis=1, subset=["product"]),
    use_container_width=True
)

# (You can continue the rest of the Nudges + Campaign table logic unchanged below)

# ─── Funnel Table ───────────────────────────────────────────────
funnel = qdf(f"""
    SELECT product,
           SUM(sent)::INT AS sent,
           SUM(delivered)::INT AS delivered,
           ROUND(SUM(clicked)*100.0/SUM(sent),1) AS click_rate
    FROM conn.funnel_by_product
    WHERE {month_clause} AND {prod_clause}
    GROUP BY 1 ORDER BY sent DESC
""")
total = funnel[["sent", "delivered"]].sum().to_frame().T
total["click_rate"] = (funnel["click_rate"] * funnel["sent"]).sum() / funnel["sent"].sum()
total.insert(0, "product", "Total")
funnel = pd.concat([funnel, total], ignore_index=True)

st.subheader("🪜 Funnel by Product")
st.dataframe(
    funnel.style.format({"click_rate": "{:.1f}%"}).format({"sent": "{:,.0f}", "delivered": "{:,.0f}"}).apply(
        lambda x: ["background-color: #222" if val == "Total" else "" for val in x], axis=1, subset=["product"]
    ),
    use_container_width=True
)

# ─── Nudge vs Activity (layered bar) ────────────────────────────
act = qdf(f"SELECT * FROM conn.nudge_vs_activity WHERE {month_clause} AND {prod_clause}")
month_count = len(sel_month_dates)
thresholds = [5 * month_count, 10 * month_count]

agg = act.groupby(["activity", "freq_bucket"], as_index=False).agg({"users": "sum"})
pivot = agg.pivot(index="activity", columns="freq_bucket", values="users").fillna(0)
pivot = pivot[["low", "med", "high"]] if all(k in pivot.columns for k in ["low", "med", "high"]) else pivot
pivot = pivot.astype(int)

st.subheader("📊 Nudge Frequency × User Activity")
fig, ax = plt.subplots(figsize=(8, 4), facecolor=BG)
bottom = None
colors = ["#90ee90", "#f7d674", "#f77f7f"]
labels = ["Low", "Medium", "High"]

for i, col in enumerate(pivot.columns):
    ax.bar(pivot.index, pivot[col], label=labels[i], bottom=bottom, color=colors[i])
    bottom = pivot[col] if bottom is None else bottom + pivot[col]

ax.set_ylabel("Users"); ax.legend(title="Nudge Frequency")
st.pyplot(fig); del pivot, fig; gc.collect()

# ─── Campaign Performance Table ─────────────────────────────────
campaigns = qdf(f"""
    SELECT sendout_name,
           SUM(sent)::INT AS sent,
           SUM(delivered)::INT AS delivered,
           ROUND(SUM(delivered)*100.0/SUM(sent),1) AS delivery_rate,
           ROUND(SUM(clicks)*100.0/SUM(sent),1)    AS click_rate,
           ROUND(SUM(delivered)*0.96*0.0107 + SUM(delivered)*0.04*0.0014) AS cost,
           ROUND(AVG(inactive_low),1) AS "Inactive: Low",
           ROUND(AVG(inactive_med),1) AS "Inactive: Med",
           ROUND(AVG(inactive_high),1) AS "Inactive: High",
           ROUND(AVG(active_low),1) AS "Active: Low",
           ROUND(AVG(active_med),1) AS "Active: Med",
           ROUND(AVG(active_high),1) AS "Active: High",
           ROUND(AVG(high_low),1) AS "High: Low",
           ROUND(AVG(high_med),1) AS "High: Med",
           ROUND(AVG(high_high),1) AS "High: High"
    FROM conn.campaign_perf
    WHERE {month_clause} AND {prod_clause}
    GROUP BY 1 ORDER BY sent DESC
""")

st.subheader("🎯 Campaign Performance")
st.dataframe(
    campaigns.style.format({
        "delivery_rate": "{:.1f}%", "click_rate": "{:.1f}%", "cost": "${:,.0f}",
        **{col: "{:.1f}%" for col in campaigns.columns if ":" in col}
    }),
    use_container_width=True,
    column_config={"sendout_name": st.column_config.Column("Sendout Name", frozen=True)}
)

st.caption("© 2025 Rocket Learning · Internal Dashboard")
