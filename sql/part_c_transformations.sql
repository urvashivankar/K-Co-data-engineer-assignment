-- ============================================================================
-- Part C: SQL Transformations
-- K&Co Cloud Cost Intelligence Platform
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Query 1: Create Unified Table (AWS + GCP)
-- ----------------------------------------------------------------------------
-- Purpose: Combine AWS and GCP billing data into a single standardized table
-- with cloud_provider column and consistent field names

CREATE OR REPLACE VIEW vw_unified_cloud_billing AS
SELECT 
    date,
    'AWS' AS cloud_provider,
    account_id AS cloud_account_id,
    service,
    team,
    env AS environment,
    cost_usd,
    CASE WHEN cost_usd < 0 THEN TRUE ELSE FALSE END AS is_credit
FROM aws_line_items_12mo

UNION ALL

SELECT 
    date,
    'GCP' AS cloud_provider,
    project_id AS cloud_account_id,
    service,
    team,
    env AS environment,
    cost_usd,
    CASE WHEN cost_usd < 0 THEN TRUE ELSE FALSE END AS is_credit
FROM gcp_billing_12mo;

-- Verify unified table
SELECT 
    cloud_provider,
    COUNT(*) AS record_count,
    COUNT(DISTINCT cloud_account_id) AS unique_accounts,
    SUM(cost_usd) AS total_cost_usd,
    MIN(date) AS min_date,
    MAX(date) AS max_date
FROM vw_unified_cloud_billing
GROUP BY cloud_provider;

-- ----------------------------------------------------------------------------
-- Query 2: Monthly Spend by Cloud Provider
-- ----------------------------------------------------------------------------
-- Purpose: Calculate total monthly spend for each cloud provider

SELECT 
    cloud_provider,
    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(MONTH FROM date) AS month,
    TO_CHAR(date, 'Month YYYY') AS month_name,
    COUNT(*) AS transaction_count,
    SUM(cost_usd) AS total_cost_usd,
    AVG(cost_usd) AS avg_cost_per_transaction,
    SUM(CASE WHEN is_credit THEN cost_usd ELSE 0 END) AS total_credits,
    SUM(CASE WHEN NOT is_credit THEN cost_usd ELSE 0 END) AS total_charges
FROM vw_unified_cloud_billing
GROUP BY 
    cloud_provider,
    EXTRACT(YEAR FROM date),
    EXTRACT(MONTH FROM date),
    TO_CHAR(date, 'Month YYYY')
ORDER BY 
    year,
    month,
    cloud_provider;

-- Alternative version with month-over-month growth
WITH monthly_spend AS (
    SELECT 
        cloud_provider,
        DATE_TRUNC('month', date) AS month,
        SUM(cost_usd) AS total_cost_usd
    FROM vw_unified_cloud_billing
    GROUP BY cloud_provider, DATE_TRUNC('month', date)
)
SELECT 
    cloud_provider,
    month,
    total_cost_usd,
    LAG(total_cost_usd) OVER (PARTITION BY cloud_provider ORDER BY month) AS prev_month_cost,
    total_cost_usd - LAG(total_cost_usd) OVER (PARTITION BY cloud_provider ORDER BY month) AS mom_change,
    ROUND(
        (total_cost_usd - LAG(total_cost_usd) OVER (PARTITION BY cloud_provider ORDER BY month)) 
        / NULLIF(LAG(total_cost_usd) OVER (PARTITION BY cloud_provider ORDER BY month), 0) * 100,
        2
    ) AS mom_change_pct
FROM monthly_spend
ORDER BY month, cloud_provider;

-- ----------------------------------------------------------------------------
-- Query 3: Monthly Spend by Team & Environment
-- ----------------------------------------------------------------------------
-- Purpose: Break down monthly costs by team and environment for chargeback

SELECT 
    team,
    environment,
    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(MONTH FROM date) AS month,
    TO_CHAR(date, 'Mon YYYY') AS month_label,
    COUNT(*) AS transaction_count,
    SUM(cost_usd) AS total_cost_usd,
    ROUND(AVG(cost_usd), 2) AS avg_daily_cost,
    COUNT(DISTINCT cloud_account_id) AS accounts_used,
    COUNT(DISTINCT service) AS services_used
FROM vw_unified_cloud_billing
GROUP BY 
    team,
    environment,
    EXTRACT(YEAR FROM date),
    EXTRACT(MONTH FROM date),
    TO_CHAR(date, 'Mon YYYY')
ORDER BY 
    year,
    month,
    team,
    environment;

-- Pivot version showing environments as columns
SELECT 
    team,
    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(MONTH FROM date) AS month,
    SUM(CASE WHEN environment = 'prod' THEN cost_usd ELSE 0 END) AS prod_cost,
    SUM(CASE WHEN environment = 'staging' THEN cost_usd ELSE 0 END) AS staging_cost,
    SUM(CASE WHEN environment = 'dev' THEN cost_usd ELSE 0 END) AS dev_cost,
    SUM(cost_usd) AS total_cost,
    ROUND(
        SUM(CASE WHEN environment = 'prod' THEN cost_usd ELSE 0 END) / NULLIF(SUM(cost_usd), 0) * 100,
        2
    ) AS prod_pct
FROM vw_unified_cloud_billing
GROUP BY 
    team,
    EXTRACT(YEAR FROM date),
    EXTRACT(MONTH FROM date)
ORDER BY 
    year,
    month,
    total_cost DESC;

-- ----------------------------------------------------------------------------
-- Query 4: Top 5 Most Expensive Services (Across Both Clouds)
-- ----------------------------------------------------------------------------
-- Purpose: Identify highest-cost services for optimization opportunities

SELECT 
    service,
    cloud_provider,
    COUNT(*) AS transaction_count,
    SUM(cost_usd) AS total_cost_usd,
    ROUND(AVG(cost_usd), 2) AS avg_cost_per_transaction,
    MIN(cost_usd) AS min_cost,
    MAX(cost_usd) AS max_cost,
    COUNT(DISTINCT team) AS teams_using,
    COUNT(DISTINCT environment) AS environments_using
FROM vw_unified_cloud_billing
WHERE NOT is_credit  -- Exclude credits for true cost analysis
GROUP BY service, cloud_provider
ORDER BY total_cost_usd DESC
LIMIT 5;

-- Alternative: Top 5 services overall (combining both clouds)
SELECT 
    service,
    COUNT(*) AS transaction_count,
    SUM(cost_usd) AS total_cost_usd,
    ROUND(AVG(cost_usd), 2) AS avg_cost_per_transaction,
    SUM(CASE WHEN cloud_provider = 'AWS' THEN cost_usd ELSE 0 END) AS aws_cost,
    SUM(CASE WHEN cloud_provider = 'GCP' THEN cost_usd ELSE 0 END) AS gcp_cost,
    ROUND(
        SUM(cost_usd) / (SELECT SUM(cost_usd) FROM vw_unified_cloud_billing WHERE NOT is_credit) * 100,
        2
    ) AS pct_of_total_spend
FROM vw_unified_cloud_billing
WHERE NOT is_credit
GROUP BY service
ORDER BY total_cost_usd DESC
LIMIT 5;

-- ----------------------------------------------------------------------------
-- Additional Useful Queries
-- ----------------------------------------------------------------------------

-- Query 5: Daily Cost Trend (for anomaly detection)
SELECT 
    date,
    cloud_provider,
    SUM(cost_usd) AS daily_cost,
    COUNT(*) AS transaction_count,
    ROUND(AVG(SUM(cost_usd)) OVER (
        PARTITION BY cloud_provider 
        ORDER BY date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2) AS rolling_7day_avg
FROM vw_unified_cloud_billing
GROUP BY date, cloud_provider
ORDER BY date, cloud_provider;

-- Query 6: Cost by Service Category (would require service mapping)
-- This assumes we've created the dim_service table from Part B
SELECT 
    s.service_category,
    SUM(f.cost_usd) AS total_cost_usd,
    COUNT(*) AS transaction_count,
    ROUND(
        SUM(f.cost_usd) / (SELECT SUM(cost_usd) FROM vw_unified_cloud_billing) * 100,
        2
    ) AS pct_of_total
FROM vw_unified_cloud_billing f
LEFT JOIN dim_service s ON f.service = s.service_name AND f.cloud_provider = s.cloud_provider
GROUP BY s.service_category
ORDER BY total_cost_usd DESC;

-- Query 7: Account-level spend summary
SELECT 
    cloud_provider,
    cloud_account_id,
    COUNT(DISTINCT date) AS days_with_activity,
    SUM(cost_usd) AS total_cost_usd,
    ROUND(AVG(cost_usd), 2) AS avg_transaction_cost,
    COUNT(DISTINCT service) AS unique_services,
    COUNT(DISTINCT team) AS unique_teams
FROM vw_unified_cloud_billing
GROUP BY cloud_provider, cloud_account_id
ORDER BY total_cost_usd DESC;

-- Query 8: Credits and refunds analysis
SELECT 
    cloud_provider,
    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(MONTH FROM date) AS month,
    COUNT(*) AS credit_count,
    SUM(cost_usd) AS total_credits,
    ABS(SUM(cost_usd)) AS total_credit_amount,
    service,
    team
FROM vw_unified_cloud_billing
WHERE is_credit = TRUE
GROUP BY 
    cloud_provider,
    EXTRACT(YEAR FROM date),
    EXTRACT(MONTH FROM date),
    service,
    team
ORDER BY year, month, total_credit_amount DESC;

-- ============================================================================
-- End of SQL Transformations
-- ============================================================================
