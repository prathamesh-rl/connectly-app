# -----------------------------------------------------------------
#  Connectly Messaging Dashboard · Slim DuckDB (fully optimized)
# -----------------------------------------------------------------
import streamlit as st, duckdb, pandas as pd, matplotlib.pyplot as plt, matplotlib.style as style
import datetime, gc

# --- Dark mode enforcement ---
style.use("dark_background")
BG, TXT = "#0e1117", "#d3d3d3"
plt.rcParams["text.color"] = TXT
plt.rcParams["axes.facecolor"] = BG
plt.rcParams["savefig.facecolor"] = BG
st.set_page_config(page_title="Connectly Dashboard", layout="wide", initial_sidebar_state="collapsed")
st.markdown("<style>body { background-color: #0e1117 !important; }</style>", unsafe_allow_html=True)

st.title("📊 Connectly Messaging Dashboard")

# --- DuckDB connection ---
@st.cache_resource(show_spinner=False)
def get_con():
    con = duckdb.connect(database=':memory:', read_only=False)
    con.execute(f"""
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_region='auto';
        SET enable_http_metadata_cache=true;
        ATTACH 'https://huggingface.co/datasets/pbhumble/connectly-parquet/resolve/main/connectly_slim_new.duckdb' AS conn (READ_ONLY);
    """)
    return con
con = get_con()
qdf = lambda q: con.sql(q).df()

# --- 1. Monthly Messaging & Cost Overview ---
monthly = qdf("SELECT * FROM conn.monthly_metrics ORDER BY month")
monthly["label"] = pd.to_datetime(monthly.month).dt.strftime("%b %y")

hardcoded = {
    'Jan 25': 1843291, 'Feb 25': 2475248, 'Mar 25': 4025949,
    'Apr 25': 3566647, 'May 25': 4796896, 'Jun 25': 2517590
}
monthly["delivered"] = monthly["label"].map(hardcoded).fillna(0).astype(int)
monthly["delivery_rate"] = (monthly["delivered"] / monthly["sent"] * 100).round(1)
monthly["meta_cost"] = (monthly["delivered"] * 0.96 * 0.0107 + monthly["delivered"] * 0.04 * 0.0014).round(2)
monthly["connectly_cost"] = (monthly["delivered"] * 0.90 * 0.0123 + 500).round(2)

st.subheader("📈 Monthly Messaging & Cost Overview")
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4), facecolor=BG)
x, w = range(len(monthly)), 0.35
ax1.bar([i - w/2 for i in x], monthly.sent, w, color="#00b4d8")
ax1.bar([i + w/2 for i in x], monthly.delivered, w, color="#ffb703")
for i, r in monthly.iterrows():
    ax1.text(i - w/2, r.sent, f"{r.sent//1_000_000}M", ha='center', va='bottom', fontsize=8)
ax1.set_xticks(x)
ax1.set_xticklabels(monthly.label, rotation=45)
ax1.set_title("Sent vs Delivered")

ax2.plot(x, monthly.meta_cost, marker="o", label="Meta $", color="#90e0ef")
ax2.plot(x, monthly.connectly_cost, marker="o", label="Connectly $", color="#f9f871")
ax2.set_xticks(x)
ax2.set_xticklabels(monthly.label, rotation=45)
ax2.legend()
ax2.set_title("Monthly Cost")
st.pyplot(fig)
del monthly, fig; gc.collect()

# --- 2. Filters (after cost overview) ---
months_raw = qdf("SELECT DISTINCT month FROM conn.monthly_metrics ORDER BY month").month
month_labels = pd.to_datetime(months_raw).strftime("%b %Y")
default_months = ["May 2025"]

c1, c2 = st.columns(2)
sel_months = c1.multiselect("📅 Months", list(month_labels), default=default_months)
products = qdf("SELECT DISTINCT product FROM conn.funnel_by_product ORDER BY product").product.tolist()
sel_products = c2.multiselect("💼 Products", products, default=products)

sel_month_dates = [f"'{months_raw[list(month_labels).index(m)]}'" for m in sel_months]
month_clause = f"month IN ({', '.join(sel_month_dates)})"
prod_clause = f"product IN ({', '.join([f'\'{p}\'' for p in sel_products])})"

# --- 3. Funnel by Product ---
funnel = qdf(f"""
    SELECT product, 
           SUM(sent) AS sent,
           SUM(delivered) AS delivered,
           ROUND(SUM(delivered)*100.0/SUM(sent),1) AS delivery_rate,
           ROUND(SUM(clicked)*100.0/SUM(sent),1)   AS click_rate
    FROM conn.funnel_by_product
    WHERE {month_clause} AND {prod_clause}
    GROUP BY 1 ORDER BY sent DESC
""")
tot = funnel[["sent", "delivered"]].sum().to_frame().T

tot.insert(0, "product", "Total")
tot["delivery_rate"] = (tot.delivered * 100 / tot.sent).round(1)
tot["click_rate"] = (funnel.sent * funnel.click_rate / 100).sum() * 100 / funnel.sent.sum()
tot["click_rate"] = tot["click_rate"].round(1)
funnel = pd.concat([funnel, tot], ignore_index=True)

st.subheader("🩜 Funnel by Product")
st.dataframe(
    funnel.style.format({
        "sent": "{:,.0f}", "delivered": "{:,.0f}",
        "delivery_rate": "{:.1f}%", "click_rate": "{:.1f}%"
    }),
    use_container_width=True
)

# --- 4. Nudges vs User Activity ---
st.subheader("📊 Nudges vs User Activity")
activity = qdf(f"""
    SELECT active_bucket AS days, low, med, high, total FROM conn.nudge_vs_activity
    WHERE {month_clause} AND {prod_clause}
""")
fig, ax = plt.subplots(figsize=(8, 4), facecolor=BG)
labels = activity.days.replace({"0": "Inactive (0 Days)", "1-10": "Active (1-10 Days)", ">10": "Highly Active (>10 Days)"})
bottoms = [0] * len(activity)
for col, color in zip(["low", "med", "high"], ["#90ee90", "#f9c74f", "#f94144"]):
    ax.bar(labels, activity[col], bottom=bottoms, label=col.capitalize()+" freq", color=color)
    bottoms = [sum(x) for x in zip(bottoms, activity[col])]
    for i, val in enumerate(activity[col]):
        if val > 0:
            ax.text(i, bottoms[i]-val/2, f"{val}%", ha='center', fontsize=8)
ax.set_ylabel("% of Users")
ax.legend()
st.pyplot(fig)

# --- 5. Campaign Performance ---
st.subheader(" 🎯 Campaign Performance")
campaigns = qdf(f"""
    SELECT
        sendout_name,
        SUM(sent) AS sent,
        SUM(delivered) AS delivered,
        ROUND(SUM(delivered)*100.0/SUM(sent),1) AS delivery_rate,
        ROUND(SUM(clicks)*100.0/SUM(sent),1)    AS click_rate,
        ROUND(SUM(delivered)*0.96*0.0107 + SUM(delivered)*0.04*0.0014, 2) AS cost,
        ROUND(AVG(inactive_pct),1) AS inactive_pct,
        ROUND(AVG(active_pct),1) AS active_pct,
        ROUND(AVG(high_pct),1) AS high_pct,
        ROUND(AVG(inactive_low),1) AS "Inactive: Low Freq",
        ROUND(AVG(inactive_med),1) AS "Inactive: Med Freq",
        ROUND(AVG(inactive_high),1) AS "Inactive: High Freq",
        ROUND(AVG(active_low),1) AS "Active: Low Freq",
        ROUND(AVG(active_med),1) AS "Active: Med Freq",
        ROUND(AVG(active_high),1) AS "Active: High Freq",
        ROUND(AVG(high_low),1) AS "Highly Active: Low",
        ROUND(AVG(high_med),1) AS "Highly Active: Med",
        ROUND(AVG(high_high),1) AS "Highly Active: High"
    FROM conn.campaign_perf
    WHERE {month_clause} AND {prod_clause}
    GROUP BY 1 ORDER BY sent DESC
""")
st.dataframe(
    campaigns.style.format({
        "delivery_rate": "{:.1f}%", "click_rate": "{:.1f}%", "cost": "$ {:.2f}"
    }).format(na_rep="-"),
    use_container_width=True,
    hide_index=True
)

st.caption("\u00a9 2025 Rocket Learning • internal dashboard")
