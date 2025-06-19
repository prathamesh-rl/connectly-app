# -----------------------------------------------------------------
#  Connectly Messaging Dashboard (Final Version)
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
    return duckdb.connect("connectly_slim_new.duckdb", read_only=True)
con = get_con()
qdf = lambda q: con.sql(q).df()

# ── Month options (for filters) ----------------------------------
months_df = qdf("SELECT DISTINCT month FROM monthly_metrics ORDER BY month")
months = months_df.month.astype(str)
month_labels = pd.to_datetime(months).dt.strftime("%b %Y")
month_dict = dict(zip(month_labels, months))

# Default to May 2025 only
default_months = ["May 2025"] if "May 2025" in month_labels.values else month_labels[-1:]
sel_months = st.multiselect("📅 Months", list(month_labels), default=default_months, key="month_filter")
sel_month_dates = tuple(month_dict[m] for m in sel_months)
month_clause = f"month IN {sel_month_dates}"

# ═════ 1. Monthly Messaging & Cost Overview (no filters) ════════
st.subheader("📈 Monthly Messaging & Cost Overview")
monthly = qdf("SELECT * FROM monthly_metrics ORDER BY month")
monthly["label"] = pd.to_datetime(monthly.month).dt.strftime("%b %y")

# Hardcoded delivered numbers and costs
manual_delivered = {
    "2025-01-01": 1843291,
    "2025-02-01": 2475248,
    "2025-03-01": 4025949,
    "2025-04-01": 3566647,
    "2025-05-01": 4796896,
    "2025-06-01": 2517590
}
monthly["manual_delivered"] = monthly["month"].map(manual_delivered).fillna(0).astype(int)
monthly["manual_delivery_rate"] = (monthly.manual_delivered * 100 / monthly.sent).round(1)
monthly["meta_cost"] = ((monthly.manual_delivered * 0.96 * 0.0107) + (monthly.manual_delivered * 0.04 * 0.0014)).round(0)
monthly["connectly_cost"] = ((monthly.manual_delivered * 0.90 * 0.0123) + 500).round(0)

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4), facecolor=BG)
x, w = range(len(monthly)), .35
ax1.bar([i - w/2 for i in x], monthly.sent,              w, color="#00b4d8", label="Sent")
ax1.bar([i + w/2 for i in x], monthly.manual_delivered,  w, color="#ffb703", label="Delivered")
for i, r in monthly.iterrows():
    ax1.text(i - w/2, r.sent, f"{r.sent//1000}K", ha='center', va='bottom', fontsize=8)
    ax1.text(i + w/2, r.manual_delivered, f"{r.manual_delivery_rate:.0f}%", ha='center', va='bottom', fontsize=8)
ax1.set_xticks(x); ax1.set_xticklabels(monthly.label, rotation=45); ax1.set_title("Sent vs Delivered")

ax2.plot(x, monthly.meta_cost,      marker="o", label="Meta $", linewidth=2)
ax2.plot(x, monthly.connectly_cost, marker="o", label="Connectly $", linewidth=2, linestyle="--")
ax2.set_xticks(x); ax2.set_xticklabels(monthly.label, rotation=45)
ax2.legend(); ax2.set_title("Monthly Cost ($)")
st.pyplot(fig); del fig; gc.collect()

# ── Product filter after monthly section ------------------------
products = qdf("SELECT DISTINCT product FROM funnel_by_product ORDER BY product").product
sel_products = st.multiselect("🛍️ Products", list(products), default=list(products), key="product_filter")
prod_clause = f"product IN {tuple(sel_products)}"

# ═════ 2. Funnel by Product ═════════════════════════════════════
funnel = qdf(f"""
    SELECT product, 
           SUM(sent) AS sent,
           SUM(delivered) AS delivered,
           ROUND(SUM(delivered)*100.0/SUM(sent),1) AS delivery_rate,
           ROUND(SUM(clicked)*100.0/SUM(sent),1)   AS click_rate
    FROM funnel_by_product
    WHERE {month_clause} AND {prod_clause}
    GROUP BY 1
    ORDER BY sent DESC
""")

tot = funnel[["sent","delivered"]].sum().to_frame().T
tot.insert(0,"product","Total")
tot["delivery_rate"] = (tot.delivered * 100 / tot.sent).round(1)
tot["click_rate"] = funnel["click_rate"].mean().round(1)  # Approx average
funnel = pd.concat([funnel, tot], ignore_index=True)

st.subheader("🪜 Funnel by Product")
st.dataframe(
    funnel.style
        .format({"sent":"{:.0f}","delivered":"{:.0f}","delivery_rate":"{:.1f}%","click_rate":"{:.1f}%"})
        .apply(lambda x: ["background-color: #222" if v=="Total" else "" for v in x], subset=["product"]),
    use_container_width=True
)

# ═════ 3. Nudges vs User Activity ═══════════════════════════════
st.subheader("📊 Nudges vs User Activity")
activity = qdf(f"""
    SELECT active_bucket, low_freq_pct, mid_freq_pct, high_freq_pct, users
    FROM nudge_vs_activity
    WHERE {month_clause} AND {prod_clause}
    ORDER BY CASE active_bucket WHEN 'Inactive (0 Days)' THEN 0 WHEN 'Active (1-10 Days)' THEN 1 ELSE 2 END
""")
activity["low"] = (activity.users * activity.low_freq_pct / 100).round(0)
activity["mid"] = (activity.users * activity.mid_freq_pct / 100).round(0)
activity["high"] = (activity.users * activity.high_freq_pct / 100).round(0)

fig_h, ax = plt.subplots(figsize=(8,4), facecolor=BG)
bar1 = ax.bar(activity.active_bucket, activity["low"], color="#90be6d", label="Low Freq")
bar2 = ax.bar(activity.active_bucket, activity["mid"], bottom=activity["low"], color="#f9c74f", label="Med Freq")
bar3 = ax.bar(activity.active_bucket, activity["high"], bottom=activity["low"]+activity["mid"], color="#f94144", label="High Freq")
ax.set_ylabel("Users"); ax.set_xlabel("Activity Bucket")
ax.legend()
st.pyplot(fig_h); del fig_h; gc.collect()

# ═════ 4. Campaign Performance ═════════════════════════════════= 
st.subheader("🎯 Campaign Performance")
campaigns = qdf(f"""
    SELECT sendout_name, sent, delivered,
           ROUND((delivered * 0.96 * 0.0107 + delivered * 0.04 * 0.0014), 2) AS cost,
           ROUND(100.0*clicks/sent,1) AS click_rate,
           inactive_pct, active_pct, high_pct,
           inactive_low_pct, inactive_mid_pct, inactive_high_pct,
           active_low_pct, active_mid_pct, active_high_pct,
           high_low_pct, high_mid_pct, high_high_pct
    FROM campaign_perf
    WHERE {month_clause} AND {prod_clause}
    ORDER BY sent DESC
""")
st.dataframe(
    campaigns.style.format({
        "sent": "{:.0f}", "delivered": "{:.0f}", "cost": "$ {:.2f}",
        "click_rate": "{:.1f}%",
        **{col: "{:.1f}%" for col in campaigns.columns if col.endswith("pct")}
    }),
    use_container_width=True,
    hide_index=True
)

st.caption("© 2025 Rocket Learning • internal dashboard")
