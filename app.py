# ---------------------------------------------------------------
#  Connectly Messaging Dashboard   •   robust & union-safe build
# ---------------------------------------------------------------
import streamlit as st, duckdb, pandas as pd, matplotlib.pyplot as plt, matplotlib.style as style
import datetime, glob, os

# ───────────  Theme  ───────────
style.use("dark_background")
BG, TXT = "#0e1117", "#d3d3d3"
st.set_page_config(page_title="Connectly Dashboard", layout="wide")
st.title("📊 Connectly Messaging Dashboard")

# Tell DuckDB to merge columns by NAME when many Parquets differ slightly
SCAN_ARGS = ", union_by_name=True"      # <─── key fix for InvalidInputException

# ───────── Helper: discover Parquet files safely ─────────
def brace_list(patterns, label):
    """
    Return '{file1,file2,...}' ready for DuckDB.
    Stop with Streamlit error if no pattern matches.
    """
    for pat in patterns:
        files = sorted(glob.glob(pat))
        if files:
            st.write(f"✅ {label}: {len(files)} files via pattern '{pat}'")
            return "{" + ",".join(files) + "}"
    st.error(f"❌ No Parquet files found for {label}. Tried: {patterns}")
    st.stop()

# Patterns that match your repo layout
CAMPAIGN_PATTERNS = [
    "parquet_trim/dispatch_date=*/data_0.parquet",
    "parquet_output/dispatch_date=*/data_0.parquet",
]
ACTIVITY_PATTERNS = [
    "activity_chunks/activity_data_*.parquet",
    "activity_trim/act_*.parquet",
]
MAP_FILE = "connectly_business_id_mapping.parquet"

PARQUET_FILES  = brace_list(CAMPAIGN_PATTERNS, "campaign data")
ACTIVITY_FILES = brace_list(ACTIVITY_PATTERNS, "activity data")

# ───────── Date bounds for widgets ─────────
min_d, max_d = datetime.date(2025, 1, 1), datetime.date(2025, 6, 8)

# ───────── Cached DuckDB loaders ─────────
@st.cache_data(ttl=600)
def monthly_sent_delivered():
    q = f"""
    SELECT DATE_TRUNC('month', dispatched_at)::DATE AS m,
           COUNT(DISTINCT customer_external_id || '_' ||
                 CAST(dispatched_at::DATE AS VARCHAR))         AS sent,
           COUNT(DISTINCT CASE WHEN delivered IS NOT NULL THEN
                 customer_external_id || '_' ||
                 CAST(dispatched_at::DATE AS VARCHAR) END)     AS delivered
    FROM parquet_scan('{PARQUET_FILES}'{SCAN_ARGS})
    GROUP BY 1 ORDER BY 1;
    """
    df = duckdb.sql(q).df()
    df["label"] = pd.to_datetime(df["m"]).dt.strftime("%b %y")
    df["rate"]  = (df.delivered / df.sent * 100).round(2)
    return df

@st.cache_data(ttl=600)
def monthly_cost():
    q = f"""
    SELECT DATE_TRUNC('month', dispatched_at)::DATE AS m,
           COUNT(DISTINCT CASE WHEN delivered IS NOT NULL THEN
                 customer_external_id || '_' ||
                 CAST(dispatched_at::DATE AS VARCHAR) END) AS delivered
    FROM parquet_scan('{PARQUET_FILES}'{SCAN_ARGS})
    GROUP BY 1 ORDER BY 1;
    """
    df = duckdb.sql(q).df()
    d = df.delivered
    df["label"] = pd.to_datetime(df["m"]).dt.strftime("%b %y")
    df["meta"]      = (d*0.96*0.0107 + d*0.04*0.0014).round(2)
    df["connectly"] = (d*0.9*0.0123 + 500).round(2)
    return df

def load_msgs(sd, ed):
    q = f"""
    SELECT DISTINCT
           customer_external_id,
           dispatched_at::DATE AS msg_date,
           customer_external_id || '_' ||
           CAST(dispatched_at::DATE AS VARCHAR) AS message_id,
           delivered, button_responses, link_clicks, sendout_name,
           COALESCE(mp.product,'Unknown') AS product
    FROM parquet_scan('{PARQUET_FILES}'{SCAN_ARGS}) m
    LEFT JOIN parquet_scan('{MAP_FILE}') mp
      ON CAST(m.business_id AS VARCHAR)=CAST(mp.business_id AS VARCHAR)
    WHERE msg_date BETWEEN DATE '{sd}' AND DATE '{ed}';
    """
    return duckdb.sql(q).df()

def load_act(sd, ed):
    q = f"""
    SELECT user_phone, product, CAST(activity_date AS DATE) AS activity_date
    FROM parquet_scan('{ACTIVITY_FILES}'{SCAN_ARGS})
    WHERE activity_date::DATE BETWEEN DATE '{sd}' AND DATE '{ed}';
    """
    return duckdb.sql(q).df()

# ───────── Monthly charts ─────────
msum, mcost = monthly_sent_delivered(), monthly_cost()
st.subheader("📈 Monthly Messaging & Cost Overview")
fig,(ax1,ax2) = plt.subplots(1,2, figsize=(12,4), facecolor=BG)

x, w = range(len(msum)), .35
ax1.bar([i-w/2 for i in x], msum.sent,      w, color="#00b4d8", label="Sent")
ax1.bar([i+w/2 for i in x], msum.delivered, w, color="#ffb703", label="Delivered")
for i,r in msum.iterrows():
    ax1.text(i-w/2, r.sent,      f"{r.sent/1e6:.1f}M", ha='center', va='bottom', color=TXT, fontsize=8)
    ax1.text(i+w/2, r.delivered, f"{r.rate}%",          ha='center', va='bottom', color=TXT, fontsize=8)
ax1.set_xticks(x); ax1.set_xticklabels(msum.label, rotation=45)
ax1.set_title("Sent vs Delivered", color=TXT); ax1.tick_params(colors=TXT); ax1.set_facecolor(BG)

x2 = range(len(mcost))
ax2.plot(x2, mcost.meta,      marker="o", color="#00b4d8", label="Meta")
ax2.plot(x2, mcost.connectly, marker="o", color="#ffb703", label="Connectly")
for i,r in mcost.iterrows():
    ax2.text(i, r.meta,      f"${r.meta:,.0f}",      ha='center', va='bottom', color="#00b4d8", fontsize=8)
    ax2.text(i, r.connectly, f"${r.connectly:,.0f}", ha='center', va='bottom', color="#ffb703", fontsize=8)
ax2.set_xticks(x2); ax2.set_xticklabels(mcost.label, rotation=45)
ax2.set_title("Monthly Cost", color=TXT); ax2.tick_params(colors=TXT); ax2.set_facecolor(BG)
st.pyplot(fig)

# ───────── Date widgets ─────────
col_s,col_e = st.columns(2)
start_date = col_s.date_input("Start date", msum.m.min(), min_value=min_d, max_value=max_d)
end_date   = col_e.date_input("End date",   msum.m.max(), min_value=min_d, max_value=max_d)

# ───────── Load filtered data ─────────
msgs = load_msgs(start_date, end_date)
acts = load_act(start_date, end_date)

# ───────── Funnel ─────────
st.subheader("📦 Funnel by Product")
mask = msgs.product != "Unknown"
funnel = (msgs.loc[mask].groupby("product")
          .agg(messages_sent      = ("message_id","nunique"),
               messages_delivered = ("delivered","count"),
               button_clicks      = ("button_responses","count"),
               link_clicks        = ("link_clicks","count"))
          .reset_index())
tot = funnel.iloc[:,1:5].sum(); tot["product"] = "Total"
funnel = pd.concat([funnel, pd.DataFrame([tot])], ignore_index=True)
funnel["delivery_rate (%)"] = (funnel.messages_delivered / funnel.messages_sent * 100).round(2)
funnel["click_rate (%)"]    = ((funnel.button_clicks + funnel.link_clicks) / funnel.messages_sent * 100).round(2)
st.dataframe(funnel, use_container_width=True)

# ───────── Histogram (%) ─────────
products = sorted(msgs.product.unique())
prod_sel = st.selectbox("Histogram product", ["All"]+products)
msgs_sel = msgs if prod_sel=="All" else msgs[msgs.product == prod_sel]
acts_sel = acts if prod_sel=="All" else acts[acts.product == prod_sel]

nudges = msgs_sel.groupby("customer_external_id").size().reset_index(name="n")
active = acts_sel.groupby("user_phone")["activity_date"].nunique().reset_index(name="days")
merged = nudges.merge(active, left_on="customer_external_id",
                      right_on="user_phone", how="left").fillna(0)
merged["days"] = merged.days.astype(int)

st.subheader("📊 Nudges vs User Activity (%)")
pct = merged.days.value_counts(normalize=True).sort_index()*100
fig_h, ax_h = plt.subplots(figsize=(8,4), facecolor=BG)
bars = ax_h.bar(pct.index, pct.values, color="#90e0ef")
for b in bars:
    ax_h.text(b.get_x()+b.get_width()/2, b.get_height()+.5,
              f"{b.get_height():.0f}%", ha='center', color=TXT)
ax_h.set_xlabel("Active days", color=TXT); ax_h.set_ylabel("% users", color=TXT)
ax_h.set_title(f"Activity distribution ({prod_sel})", color=TXT)
ax_h.tick_params(colors=TXT); ax_h.set_facecolor(BG)
st.pyplot(fig_h)

# ───────── Campaign performance ─────────
st.subheader("🎯 Campaign Performance")
camp = (msgs_sel.groupby("sendout_name")
        .agg(messages_sent      = ("message_id","nunique"),
             messages_delivered = ("delivered","count"),
             button_clicks      = ("button_responses","count"),
             link_clicks        = ("link_clicks","count"))
        ).reset_index()
camp["delivery_rate (%)"] = (camp.messages_delivered / camp.messages_sent * 100).round(2)
camp["click_rate (%)"]    = ((camp.button_clicks + camp.link_clicks) / camp.messages_sent * 100).round(2)

merged["segment"] = pd.cut(merged.days, bins=[-1,0,10,merged.days.max()],
                           labels=["Inactive","Active","Highly Active"])
seg_tbl = merged[["customer_external_id","segment"]].merge(
    msgs_sel[["customer_external_id","sendout_name"]], how="left")
ct = pd.crosstab(seg_tbl.sendout_name, seg_tbl.segment, normalize="index")*100
ct = ct.reindex(columns=["Inactive","Active","Highly Active"], fill_value=0).round(1)
for col in ct.columns:
    ct[col] = ct[col].astype(str) + "%"
camp_final = camp.merge(ct.reset_index(), on="sendout_name", how="left")
st.dataframe(camp_final, use_container_width=True)
