# ------------------------------------------------------------------
# build_duckdb.py · Pre-computes all analytics into connectly_slim.db
# ------------------------------------------------------------------
"""
Run locally or in a GitHub Action:

    python build_duckdb.py
"""

import duckdb, os

RAW_CAMP = "parquet_trim/dispatch_date=*/data_0.parquet"
RAW_ACT  = "activity_chunks/activity_data_*.parquet"
MAP_FILE = "business_product_map.parquet"   # adjust if needed
OUT_DB   = "connectly_slim.duckdb"

# ── (re)create slim DB ────────────────────────────────────────────
if os.path.exists(OUT_DB):
    os.remove(OUT_DB)
con = duckdb.connect(OUT_DB)

# 1️⃣ Campaigns  ----------------------------------------------------
con.execute(f"""
CREATE TABLE camp_raw AS
SELECT
    sendout_name,
    business_id,
    customer_external_id            AS user,
    CAST(dispatched_at AS TIMESTAMP) AS dispatched_at,
    delivered::BOOLEAN              AS delivered,
    button_responses::INT           AS button_responses,
    link_clicks::INT                AS link_clicks
FROM read_parquet('{RAW_CAMP}');
""")

con.execute(f"""
CREATE TABLE product_map AS
SELECT business_id, product
FROM read_parquet('{MAP_FILE}');
""")

con.execute("""
CREATE TABLE camp AS
SELECT
    DATE_TRUNC('month', dispatched_at)::DATE          AS month,
    COALESCE(pm.product,'Unknown')                    AS product,
    sendout_name,
    user,
    delivered,
    button_responses + link_clicks                    AS clicks
FROM camp_raw
LEFT JOIN product_map pm USING (business_id);
""")

# 2️⃣ Activity  -----------------------------------------------------
con.execute(f"""
CREATE TABLE act AS
SELECT
    user_phone                              AS user,
    CAST(activity_date AS DATE)             AS activity_date,
    DATE_TRUNC('month', activity_date)::DATE AS month,
    product
FROM read_parquet('{RAW_ACT}');
""")

# 3️⃣ monthly_metrics  ---------------------------------------------
con.execute("""
CREATE TABLE monthly_metrics AS
SELECT
    month,
    COUNT(*)                                    AS sent,
    COUNT(*) FILTER (WHERE delivered)           AS delivered,
    ROUND(COUNT(*) FILTER (WHERE delivered)*100.0/COUNT(*),1) AS delivery_rate,
    ROUND((delivered*0.96*0.0107) + (delivered*0.04*0.0014))  AS meta_cost,
    ROUND((delivered*0.90*0.0123) + 500)                      AS connectly_cost
FROM camp
GROUP BY 1 ORDER BY 1;
""")

# 4️⃣ funnel_by_product  -------------------------------------------
con.execute("""
CREATE TABLE funnel_by_product AS
SELECT
    month,
    product,
    COUNT(DISTINCT user)                               AS sent,
    COUNT(DISTINCT CASE WHEN delivered THEN user END)  AS delivered,
    COUNT(DISTINCT CASE WHEN clicks>0  THEN user END)  AS clicked,
    ROUND(COUNT(DISTINCT CASE WHEN delivered THEN user END)*100.0
          /COUNT(DISTINCT user),1)                     AS delivery_rate,
    ROUND(COUNT(DISTINCT CASE WHEN clicks>0 THEN user END)*100.0
          /COUNT(DISTINCT user),1)                     AS click_rate
FROM camp
WHERE product<>'Unknown'
GROUP BY 1,2;
""")

# 5️⃣ nudge_vs_activity  -------------------------------------------
con.execute("""
CREATE TABLE nudge_vs_activity AS
WITH act_days AS (
    SELECT user, month, product,
           COUNT(DISTINCT activity_date) AS days
    FROM act GROUP BY 1,2,3
),
nudged AS (
    SELECT DISTINCT month, product, user FROM camp
)
SELECT
    n.month,
    n.product,
    CASE
        WHEN COALESCE(a.days,0)=0            THEN '0'
        WHEN a.days BETWEEN 1 AND 10         THEN '1-10'
        ELSE                                      '>10'
    END                           AS active_bucket,
    COUNT(*)                      AS users
FROM nudged n
LEFT JOIN act_days a USING (user, month, product)
GROUP BY 1,2,3;
""")

# 6️⃣ campaign_perf  -----------------------------------------------
con.execute("""
CREATE TABLE campaign_perf AS
WITH base AS (
    SELECT
        month, product, sendout_name,
        COUNT(DISTINCT user)                         AS sent,
        COUNT(DISTINCT CASE WHEN delivered THEN user END) AS delivered,
        SUM(clicks)                                  AS clicks
    FROM camp
    GROUP BY 1,2,3
),
seg AS (
    SELECT
        c.month, c.product, c.sendout_name, c.sent,
        ROUND(SUM(CASE WHEN days=0                THEN 1 END)*100.0/c.sent,1) AS inactive_pct,
        ROUND(SUM(CASE WHEN days BETWEEN 1 AND 10 THEN 1 END)*100.0/c.sent,1) AS active_pct,
        ROUND(SUM(CASE WHEN days>10              THEN 1 END)*100.0/c.sent,1) AS high_pct
    FROM base c
    LEFT JOIN (
        SELECT user, month, product, COUNT(DISTINCT activity_date) AS days
        FROM act GROUP BY 1,2,3
    ) a USING (month, product, user)
    GROUP BY 1,2,3,4
)
SELECT b.*, s.inactive_pct, s.active_pct, s.high_pct
FROM base b JOIN seg s USING (month, product, sendout_name);
""")

con.close()
print(f"✅ Built {OUT_DB} successfully")
