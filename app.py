# ------------------------------------------------------------------
#  Connectly Messaging Dashboard • rock-solid build  (18-Jun-2025)
# ------------------------------------------------------------------
import streamlit as st, duckdb, pandas as pd, matplotlib.pyplot as plt
import matplotlib.style as style, datetime, glob, os, textwrap, pathlib

# ── theme ─────────────────────────────────────────────────────────
style.use("dark_background")
BG, TXT = "#0e1117", "#d3d3d3"
plt.rcParams["text.color"] = TXT
st.set_page_config(page_title="Connectly Dashboard", layout="wide")
st.title("📊 Connectly Messaging Dashboard")

# ── helper: resolve good parquet shards (skip zero-byte/corrupt) ──
def good_shards(patterns, min_bytes=1024):
    for pat in patterns:
        paths = [p for p in glob.glob(pat) if os.path.getsize(p) >= min_bytes]
        if paths:
            # Convert Windows paths → posix so DuckDB understands them
            paths = [pathlib.Path(p).as_posix() for p in paths]
            return "{" + ",".join(paths) + "}"
    return None

# ── file locations ────────────────────────────────────────────────
CAM_PATTS = (
    "parquet_trim/dispatch_date=*/data_0.parquet",
    "parquet_trim/msg_*.parquet",
    "parquet_output/dispatch_date=*/data_0*.parquet",
)
ACT_PATTS = (
    "activity_trim/act_*.parquet",
    "activity_chunks/activity_data_*.parquet",
)
MAP_FILE = "connectly_business_id_mapping.parquet"

CAM_FILES = good_shards(CAM_PATTS)
ACT_FILES = good_shards(ACT_PATTS)
if CAM_FILES is None:
    st.error("❌ No usable campaign Parquet shards found."); st.stop()

# ── date limits & widgets ─────────────────────────────────────────
MIN_D, MAX_D = datetime.date(2025, 1, 1), datetime.date(2025, 6, 8)
c1, c2 = st.columns(2)
sd = c1.date_input("Start date", MAX_D - datetime.timedelta(days=30),
                   MIN_D, MAX_D)
ed = c2.date_input("End date",   MAX_D, MIN_D, MAX_D)

# ── duckdb connection + small helper ──────────────────────────────
con = duckdb.connect()
scan_c = f"parquet_scan('{CAM_FILES}', union_by_name=true)"
scan_a = f"parquet_scan('{ACT_FILES}', union_by_name=true)" if ACT_FILES else None
qdf    = lambda sql: con.sql(textwrap.dedent(sql)).df()

# ── monthly sent/delivered & cost (all dates, full shards) ────────
monthly = qdf(f"""
SELECT DATE_TRUNC('month', CAST(dispatched_at AS TIMESTAMP))::DATE AS m,
       COUNT(DISTINCT customer_external_id||'_'||
             CAST(dispatched_at::DATE AS VARCHAR))                 AS sent,
       COUNT(DISTINCT CASE WHEN delivered IS NOT NULL
             THEN customer_external_id||'_'||
             CAST(dispatched_at::DATE AS VARCHAR) END)            AS delivered
FROM {scan_c} GROUP BY 1 ORDER BY 1
""")
monthly["label"] = pd.to_datetime(monthly.m).dt.strftime("%b %y")
monthly["rate"]  = (monthly.delivered / monthly.sent * 100).round(2)

cost = qdf(f"""
SELECT DATE_TRUNC('month', CAST(dispatched_at AS TIMESTAMP))::DATE AS m,
       COUNT(DISTINCT CASE WHEN delivered IS NOT NULL
             THEN customer_external_id||'_'||
             CAST(dispatched_at::DATE AS VARCHAR) END) AS delivered
FROM {scan_c} GROUP BY 1 ORDER BY 1
""")
d = cost.delivered
cost["label"]      = pd.to_datetime(cost.m).dt.strftime("%b %y")
cost["meta"]       = (d*0.96*0.0107 + d*0.04*0.0014).round(2)
cost["connectly"]  = (d*0.9*0.0123 + 500).round(2)

# ── monthly charts ────────────────────────────────────────────────
st.subheader("📈 Monthly Messaging & Cost Overview")
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4), facecolor=BG)
x, w = range(len(monthly)), .35
ax1.bar([i - w/2 for i in x], monthly.sent,      w, color="#00b4d8")
ax1.bar([i + w/2 for i in x], monthly.delivered, w, color="#ffb703")
for i, r in monthly.iterrows():
    ax1.text(i - w/2, r.sent,      f"{r.sent/1e6:.1f}M", ha="center", va="bottom", fontsize=8)
    ax1.text(i + w/2, r.delivered, f"{r.rate:.0f}%",     ha="center", va="bottom", fontsize=8)
ax1.set_xticks(x); ax1.set_xticklabels(monthly.label, rotation=45)
ax1.set_title("Sent vs Delivered")

x2 = range(len(cost))
ax2.plot(x2, cost.meta,      marker="o", color="#00b4d8", label="Meta")
ax2.plot(x2, cost.connectly, marker="o", color="#ffb703", label="Connectly")
for i, r in cost.iterrows():
    ax2.text(i, r.meta,      f"${r.meta:,.0f}",      color="#00b4d8", fontsize=8, ha="center", va="bottom")
    ax2.text(i, r.connectly, f"${r.connectly:,.0f}", color="#ffb703", fontsize=8, ha="center", va="bottom")
ax2.set_xticks(x2); ax2.set_xticklabels(cost.label, rotation=45)
ax2.set_title("Monthly Cost")
st.pyplot(fig)

# ── product list (for current window) ─────────────────────────────
prod_df = qdf(f"""
SELECT DISTINCT mp.product
FROM {scan_c} m
LEFT JOIN parquet_scan('{MAP_FILE}') mp
  ON CAST(m.business_id AS VARCHAR)=CAST(mp.business_id AS VARCHAR)
WHERE dispatched_at::DATE BETWEEN DATE '{sd}' AND DATE '{ed}'
  AND mp.product IS NOT NULL
""")
products = sorted(prod_df["product"].dropna().unique().tolist())
prod_sel = st.selectbox("Product filter (Activity & Campaign)", ["All"] + products, index=0)
prod_clause = "" if prod_sel == "All" else f"AND mp.product = '{prod_sel}'"

# ── campaign messages (filtered) ─────────────────────────────────
msgs = qdf(f"""
SELECT customer_external_id,
       sendout_name,
       delivered,
       button_responses, link_clicks,
       customer_external_id||'_'||CAST(dispatched_at::DATE AS VARCHAR) AS msg_id,
       CAST(dispatched_at::DATE AS DATE) AS msg_date,
       COALESCE(mp.product,'Unknown') AS product
FROM {scan_c} m
LEFT JOIN parquet_scan('{MAP_FILE}') mp
  ON CAST(m.business_id AS VARCHAR)=CAST(mp.business_id AS VARCHAR)
WHERE msg_date BETWEEN DATE '{sd}' AND DATE '{ed}' {prod_clause}
""")

# ── activity rows (filtered) ─────────────────────────────────────
if scan_a:
    cols = con.sql(f"DESCRIBE {scan_a}").df()["column_name"].tolist()
    if "guardian_phone" in cols or "moderator_phone" in cols:
        active = qdf(f"""
        SELECT COALESCE(guardian_phone, moderator_phone, user_phone) AS user,
               CAST(activity_date AS DATE) AS activity_date
        FROM {scan_a}
        WHERE activity_date::DATE BETWEEN DATE '{sd}' AND DATE '{ed}' {prod_clause}
        """)
    else:
        active = qdf(f"""
        SELECT user_phone AS user,
               CAST(activity_date AS DATE) AS activity_date
        FROM {scan_a}
        WHERE activity_date::DATE BETWEEN DATE '{sd}' AND DATE '{ed}' {prod_clause}
        """)
else:
    active = pd.DataFrame(columns=["user", "activity_date"])

# ── funnel (all products) ────────────────────────────────────────
st.subheader("📦 Funnel by Product")
base = msgs.assign(msg_id=msgs.msg_id)      # ensure copy
funnel = (base[base.product != "Unknown"]
          .groupby("product", as_index=False)
          .agg(messages_sent      = ("msg_id", "nunique"),
               messages_delivered = ("delivered", "count")))
tot = funnel.iloc[:, 1:3].sum(); tot["product"] = "Total"
funnel = pd.concat([funnel, pd.DataFrame([tot])], ignore_index=True)
funnel["delivery_rate (%)"] = (funnel.messages_delivered / funnel.messages_sent * 100).round(2)
st.dataframe(funnel, use_container_width=True)

# ── nudge vs activity histogram ──────────────────────────────────
nudges = msgs.groupby("customer_external_id").size().reset_index(name="nudge_cnt")
act_days = active.groupby("user")["activity_date"].nunique().reset_index(name="active_days")
merged  = nudges.merge(act_days, left_on="customer_external_id", right_on="user", how="left").fillna(0)
merged["active_days"] = merged.active_days.astype(int)

st.subheader("📊 Nudges vs User Activity")
cnt = merged.active_days.value_counts().sort_index()
pct = (cnt / cnt.sum() * 100).round(0).astype(int)

fig_h, ax_h = plt.subplots(figsize=(8, 4), facecolor=BG)
bars = ax_h.bar(cnt.index, cnt.values, color="#90e0ef")
for b, val in zip(bars, pct[cnt.index]):
    ax_h.text(b.get_x() + b.get_width()/2, b.get_height() + .5,
              f"{val}%", ha="center", va="bottom", fontsize=8)
ax_h.set_xlabel("Active days"); ax_h.set_ylabel("Users")
st.pyplot(fig_h)

# ── campaign performance (filtered by product selector) ──────────
camp = (msgs.groupby("sendout_name", as_index=False)
        .agg(messages_sent      = ("msg_id", "nunique"),
             messages_delivered = ("delivered", "count"),
             button_clicks      = ("button_responses", "count"),
             link_clicks        = ("link_clicks", "count")))
camp["delivery_rate (%)"] = (camp.messages_delivered / camp.messages_sent * 100).round(1)
camp["click_rate (%)"]    = ((camp.button_clicks + camp.link_clicks) / camp.messages_sent * 100).round(1)
camp["% of Total"]        = (camp.messages_sent / camp.messages_sent.sum() * 100).round(1)

# build activity-segment %
merged["segment"] = pd.cut(merged.active_days,
                           bins=[-1, 0, 10, merged.active_days.max()],
                           labels=["Inactive", "Active", "Highly Active"])
seg_tbl = pd.merge(
    msgs[["customer_external_id", "sendout_name"]],
    merged[["customer_external_id", "segment"]],
    on="customer_external_id", how="left"
)
ct = (pd.crosstab(seg_tbl.sendout_name, seg_tbl.segment, normalize="index")*100
      ).reindex(columns=["Inactive", "Active", "Highly Active"], fill_value=0).round(1)
for c in ct.columns:
    ct[c] = ct[c].astype(str) + "%"

final = (camp
         .merge(ct.reset_index(), on="sendout_name", how="left")
         .sort_values("messages_sent", ascending=False)
         .drop(columns=["button_clicks", "link_clicks"]))

# move % columns right after messages_delivered
cols = ["sendout_name", "messages_sent", "messages_delivered",
        "delivery_rate (%)", "click_rate (%)", "% of Total",
        "Inactive", "Active", "Highly Active"]
final = final[cols]

st.subheader("🎯 Campaign Performance")
st.dataframe(final, use_container_width=True)
