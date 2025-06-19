import streamlit as st, duckdb, pandas as pd, matplotlib.pyplot as plt
import matplotlib.style as style, gc, requests, os

# ─── Configuration ─────────────────────────────────────────────
style.use("dark_background")
BG, TXT = "#0e1117", "#d3d3d3"
plt.rcParams["text.color"] = TXT
st.set_page_config(layout="wide", page_title="Connectly Dashboard")

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

# Try loading sent_total if exists
try:
    sent_total = qdf("SELECT * FROM connectly_slim_new.monthly_sent_total ORDER BY month")
    sent_total_dict = dict(zip(sent_total.month.astype(str), sent_total.total_sent))
    monthly["sent_total"] = monthly.month.astype(str).map(sent_total_dict).fillna(monthly.sent)
except:
    monthly["sent_total"] = monthly.sent

st.subheader("📈 Monthly Messaging & Cost Overview")
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4), facecolor=BG)

x = range(len(monthly))
w = 0.35

# Sent vs Delivered bar chart
ax1.bar([i - w/2 for i in x], monthly.sent_total, w, color="#00b4d8", label="Total Sent")
ax1.bar([i + w/2 for i in x], monthly.delivered, w, color="#ffb703", label="Delivered")
for i, r in monthly.iterrows():
    ax1.text(i - w/2, r.sent_total, f"{int(r.sent_total/1e6)}M", ha='center', va='bottom', fontsize=8)
    ax1.text(i + w/2, r.delivered, f"{int(r.delivery_rate)}%", ha='center', va='bottom', fontsize=8)
ax1.set_xticks(list(x)); ax1.set_xticklabels(monthly.label, rotation=45); ax1.set_title("Sent vs Delivered")
ax1.legend()

# Cost line chart
ax2.plot(x, monthly.meta_cost, marker="o", label="Meta $", color="#90e0ef")
ax2.plot(x, monthly.connectly_cost, marker="o", label="Connectly $", color="#f4f1bb")
for i, r in monthly.iterrows():
    ax2.text(i, r.meta_cost, f"${int(r.meta_cost)}", ha='center', va='bottom', fontsize=8)
    ax2.text(i, r.connectly_cost, f"${int(r.connectly_cost)}", ha='center', va='bottom', fontsize=8)
ax2.set_xticks(list(x)); ax2.set_xticklabels(monthly.label, rotation=45)
ax2.set_title("Monthly Cost"); ax2.legend()

st.pyplot(fig); del monthly, fig; gc.collect()

# ─── Filters (Placed below Graph) ──────────────────────────────
months_df = qdf("SELECT DISTINCT month FROM connectly_slim_new.funnel_by_product ORDER BY month")
products_df = qdf("SELECT DISTINCT product FROM connectly_slim_new.funnel_by_product ORDER BY product")

months = months_df["month"].tolist()
month_labels = pd.to_datetime(months).strftime("%b %Y").tolist()
products = products_df["product"].tolist()

c1, c2 = st.columns(2)
sel_months = c1.multiselect("📅 Months", month_labels, default=["May 2025"])
sel_products = c2.multiselect("🛍️ Products", products, default=products, label_visibility="visible")

sel_month_dates = [months[month_labels.index(m)] for m in sel_months]
month_clause = "month IN ({})".format(", ".join([f"DATE '{d}'" for d in sel_month_dates]))
prod_clause = "product IN ({})".format(", ".join([repr(p) for p in sel_products]))

# ─── Funnel by Product ─────────────────────────────────────────
funnel = qdf(f"""
    SELECT product,
           SUM(sent)::INT AS sent,
           SUM(delivered)::INT AS delivered,
           ROUND(SUM(clicked)*100.0/SUM(sent),1) AS click_rate
    FROM connectly_slim_new.funnel_by_product
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
    }).map(lambda v: "background-color: #222" if v == "Total" else "", subset=pd.IndexSlice[funnel.index[-1:], :])

)

# ─── Nudge vs Activity (layered bar) ────────────────────────────
try:
    act = qdf(f"SELECT * FROM connectly_slim_new.nudge_vs_activity WHERE {month_clause} AND {prod_clause}")
    if "activity" in act.columns and "freq_bucket" in act.columns:
        month_count = len(sel_month_dates)
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
except Exception as e:
    st.warning(f"⚠️ Could not load Nudge vs Activity chart: {e}")

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
    FROM connectly_slim_new.campaign_perf
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
    column_config={"sendout_name": "Sendout Name"}

)

st.caption("© 2025 Rocket Learning · Internal Dashboard")
