import duckdb
import os

RAW_CAMP = "parquet_trim/dispatch_date=*/data_0.parquet"
RAW_ACT = "activity_chunks/activity_data_*.parquet"
MAP_FILE = "connectly_business_id_mapping.parquet"
OUT_DB = "connectly_slim.duckdb"

# Delete if exists
if os.path.exists(OUT_DB):
    os.remove(OUT_DB)
con = duckdb.connect(OUT_DB)

# ─── 1. Raw campaign table ─────────────────────────────────────
con.execute(f"""
CREATE TABLE camp_raw AS
SELECT
    customer_external_id              AS user,
    business_id,
    sendout_name,
    CAST(dispatched_at AS TIMESTAMP)  AS dispatched_at,
    delivered,
    TRY_CAST(button_responses AS INT) AS button_responses,
    TRY_CAST(link_clicks AS INT)      AS link_clicks
FROM read_parquet('{RAW_CAMP}');
""")

# ─── 2. Map business_id to product ─────────────────────────────
con.execute(f"""
CREATE TABLE product_map AS
SELECT business_id, product
FROM read_parquet('{MAP_FILE}');
""")

# ─── 3. Final campaign table with product & month ──────────────
con.execute("""
CREATE TABLE camp AS
SELECT
    DATE_TRUNC('month', dispatched_at)::DATE       AS month,
    COALESCE(p.product, 'Unknown')                 AS product,
    cr.sendout_name,
    cr.user,
    (cr.delivered IS NOT NULL)                     AS delivered,
    cr.button_responses + cr.link_clicks           AS clicks
FROM camp_raw cr
LEFT JOIN product_map p USING (business_id);
""")

# ─── 4. Activity table ─────────────────────────────────────────
con.execute(f"""
CREATE TABLE act AS
SELECT
    user_phone                              AS user,
    product,
    CAST(activity_date AS DATE)             AS activity_date,
    DATE_TRUNC('month', activity_date)::DATE AS month
FROM read_parquet('{RAW_ACT}');
""")

# ─── 5. Monthly metrics ────────────────────────────────────────
con.execute("""
CREATE TABLE monthly_metrics AS
SELECT
    month,
    COUNT(*)                                    AS sent,
    COUNT(*) FILTER (WHERE delivered)           AS delivered,
    ROUND(COUNT(*) FILTER (WHERE delivered)*100.0/COUNT(*), 1) AS delivery_rate,
    ROUND(
        COUNT(*) FILTER (WHERE delivered)*0.96*0.0107 +
        COUNT(*) FILTER (WHERE delivered)*0.04*0.0014
    )                                           AS meta_cost,
    ROUND(
        COUNT(*) FILTER (WHERE delivered)*0.90*0.0123 + 500
    )                                           AS connectly_cost
FROM camp
GROUP BY 1 ORDER BY 1;
""")

# ─── 6. Funnel by product ──────────────────────────────────────
con.execute("""
CREATE TABLE funnel_by_product AS
SELECT
    month,
    product,
    COUNT(DISTINCT user)                             AS sent,
    COUNT(DISTINCT CASE WHEN delivered THEN user END) AS delivered,
    COUNT(DISTINCT CASE WHEN clicks > 0 THEN user END) AS clicked,
    ROUND(COUNT(DISTINCT CASE WHEN delivered THEN user END)*100.0
          /COUNT(DISTINCT user),1)                   AS delivery_rate,
    ROUND(COUNT(DISTINCT CASE WHEN clicks > 0 THEN user END)*100.0
          /COUNT(DISTINCT user),1)                   AS click_rate
FROM camp
WHERE product <> 'Unknown'
GROUP BY 1,2;
""")

# ─── 7. Nudges vs activity ─────────────────────────────────────
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
LEFT JOIN act_days a USING(user, month, product)
GROUP BY 1,2,3;
""")

# ─── 8. Campaign performance ───────────────────────────────────
con.execute("""
CREATE TABLE campaign_perf AS
WITH msgs AS (
    SELECT
        month, product, sendout_name, user, delivered, clicks
    FROM camp
),
activity_seg AS (
    SELECT
        user,
        month,
        product,
        COUNT(DISTINCT activity_date) AS days
    FROM act
    GROUP BY 1,2,3
),
segmented AS (
    SELECT
        m.month,
        m.product,
        m.sendout_name,
        m.user,
        m.delivered,
        m.clicks,
        COALESCE(a.days, 0) AS days
    FROM msgs m
    LEFT JOIN activity_seg a
    ON m.user = a.user AND m.month = a.month AND m.product = a.product
)
SELECT
    month,
    product,
    sendout_name,
    COUNT(DISTINCT user) AS sent,
    COUNT(DISTINCT CASE WHEN delivered THEN user END) AS delivered,
    SUM(clicks) AS clicks,
    ROUND(COUNT(*) FILTER (WHERE days = 0) * 100.0 / COUNT(*), 1) AS inactive_pct,
    ROUND(COUNT(*) FILTER (WHERE days BETWEEN 1 AND 10) * 100.0 / COUNT(*), 1) AS active_pct,
    ROUND(COUNT(*) FILTER (WHERE days > 10) * 100.0 / COUNT(*), 1) AS high_pct
FROM segmented
GROUP BY 1,2,3;

""")
# Print final table sizes
for table in ["monthly_metrics", "funnel_by_product", "nudge_vs_activity", "campaign_perf"]:
    print(f"{table}: ", con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])

# Clean up heavy tables to slim the DB
con.execute("DROP TABLE IF EXISTS camp_raw;")
con.execute("DROP TABLE IF EXISTS camp;")
con.execute("DROP TABLE IF EXISTS act;")
con.execute("DROP TABLE IF EXISTS product_map;")

# ✅ Close connection at the very end
con.close()
print("✅ Built connectly_slim.duckdb successfully")

