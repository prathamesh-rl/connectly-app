import streamlit as st, duckdb, pandas as pd, matplotlib.pyplot as plt
import matplotlib.style as style, gc, requests, os

# ─── Configuration ───
style.use("default")
BG, TXT = "#ffffff", "#000000"
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

# ─── Monthly Messaging (unfiltered) ───
monthly = qdf("SELECT * FROM connectly_slim_new.monthly_metrics ORDER BY month")
monthly["label"] = pd.to_datetime(monthly.month).dt.strftime("%b %y")

...  # (unchanged top code remains here)

# ─── Nudge vs Activity (robust version) ───
# First detect valid month-product combos
valid_combos = qdf("SELECT DISTINCT month, product FROM connectly_slim_new.nudge_vs_activity")
valid_set = set(zip(valid_combos.month.astype(str), valid_combos.product))

# Filter the current selections to only valid combinations
selected_combos = [(m, p) for m in sel_month_dates for p in sel_products if (str(m), p) in valid_set]

if not selected_combos:
    st.warning("No user data available for the selected months and products.")
    st.stop()

# Build WHERE clause with only valid pairs
combo_clauses = [f"(month = DATE '{m}' AND product = '{p}')" for m, p in selected_combos]
combo_clause = " OR ".join(combo_clauses)

act = qdf(f"""
    SELECT * FROM connectly_slim_new.nudge_vs_activity
    WHERE {combo_clause}
""")

if act.empty:
    st.warning("No user data found for selected filters.")
    st.stop()

act["users"] = act["users"].astype(int)
pivot = act.pivot_table(index="active_bucket", values="users", aggfunc="sum")
pivot.index = pivot.index.map({
    '0': "Inactive (0 Days)",
    '1-10': "Active (1-10 Days)",
    '>10': "Highly Active (>10 Days)"
})
pivot = pivot.groupby(pivot.index).sum()
pivot = pivot.reindex(["Inactive (0 Days)", "Active (1-10 Days)", "Highly Active (>10 Days)"], fill_value=0)

st.subheader("📊 Nudge Frequency × User Activity")
fig, ax = plt.subplots(figsize=(10, 5), facecolor="white")
bars = ax.bar(pivot.index, pivot["users"], color=["#bde0fe", "#ffd60a", "#ff5a5f"])
for bar in bars:
    height = bar.get_height()
    ax.text(bar.get_x() + bar.get_width() / 2, height + 500, f"{height:,}", ha="center", va="bottom", fontsize=8)
ax.set_ylabel("Users")
ax.set_title("User Activity Buckets by Nudge Exposure")
st.pyplot(fig)

# ─── Campaign Performance Table ───
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

st.subheader("🌟 Campaign Performance")
st.dataframe(
    campaigns.style.format({
        "Delivery Rate": "{:.1f}%",
        "Cost": "${:,.0f}",
        **{col: "{:.1f}%" for col in campaigns.columns if "%" in col or ":" in col}
    }),
    use_container_width=True
)

st.caption("\u00a9 2025 Rocket Learning \u00b7 Internal Dashboard")
