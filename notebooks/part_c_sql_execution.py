"""
Part C: SQL Query Execution
Execute SQL transformations and generate sample outputs
"""

import pandas as pd
import sqlite3
from datetime import datetime

def main():
    print("=" * 80)
    print("PART C: SQL TRANSFORMATIONS - EXECUTION")
    print("=" * 80)
    print()
    
    # Load data
    print("Loading datasets...")
    aws_df = pd.read_csv('data/aws_line_items_12mo.csv')
    gcp_df = pd.read_csv('data/gcp_billing_12mo.csv')
    print(f"✓ Loaded {len(aws_df):,} AWS records")
    print(f"✓ Loaded {len(gcp_df):,} GCP records")
    print()
    
    # Create in-memory SQLite database
    conn = sqlite3.connect(':memory:')
    
    # Load data into SQLite
    aws_df.to_sql('aws_line_items_12mo', conn, index=False, if_exists='replace')
    gcp_df.to_sql('gcp_billing_12mo', conn, index=False, if_exists='replace')
    print("✓ Data loaded into SQLite")
    print()
    
    # ========================================================================
    # Query 1: Create Unified Table
    # ========================================================================
    print("=" * 80)
    print("QUERY 1: UNIFIED CLOUD BILLING TABLE")
    print("=" * 80)
    print()
    
    query1 = """
    CREATE VIEW vw_unified_cloud_billing AS
    SELECT 
        date,
        'AWS' AS cloud_provider,
        account_id AS cloud_account_id,
        service,
        team,
        env AS environment,
        cost_usd,
        CASE WHEN cost_usd < 0 THEN 1 ELSE 0 END AS is_credit
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
        CASE WHEN cost_usd < 0 THEN 1 ELSE 0 END AS is_credit
    FROM gcp_billing_12mo
    """
    
    conn.execute(query1)
    
    # Verify unified table
    verify_query = """
    SELECT 
        cloud_provider,
        COUNT(*) AS record_count,
        COUNT(DISTINCT cloud_account_id) AS unique_accounts,
        ROUND(SUM(cost_usd), 2) AS total_cost_usd,
        MIN(date) AS min_date,
        MAX(date) AS max_date
    FROM vw_unified_cloud_billing
    GROUP BY cloud_provider
    """
    
    result1 = pd.read_sql_query(verify_query, conn)
    print("Unified Table Summary:")
    print(result1.to_string(index=False))
    print()
    
    # ========================================================================
    # Query 2: Monthly Spend by Cloud Provider
    # ========================================================================
    print("=" * 80)
    print("QUERY 2: MONTHLY SPEND BY CLOUD PROVIDER")
    print("=" * 80)
    print()
    
    query2 = """
    SELECT 
        cloud_provider,
        CAST(strftime('%Y', date) AS INTEGER) AS year,
        CAST(strftime('%m', date) AS INTEGER) AS month,
        strftime('%Y-%m', date) AS month_label,
        COUNT(*) AS transaction_count,
        ROUND(SUM(cost_usd), 2) AS total_cost_usd,
        ROUND(AVG(cost_usd), 2) AS avg_cost_per_transaction,
        ROUND(SUM(CASE WHEN is_credit THEN cost_usd ELSE 0 END), 2) AS total_credits,
        ROUND(SUM(CASE WHEN NOT is_credit THEN cost_usd ELSE 0 END), 2) AS total_charges
    FROM vw_unified_cloud_billing
    GROUP BY 
        cloud_provider,
        strftime('%Y', date),
        strftime('%m', date),
        strftime('%Y-%m', date)
    ORDER BY 
        year,
        month,
        cloud_provider
    """
    
    result2 = pd.read_sql_query(query2, conn)
    print("Monthly Spend by Cloud Provider:")
    print(result2.to_string(index=False))
    print()
    
    # ========================================================================
    # Query 3: Monthly Spend by Team & Environment
    # ========================================================================
    print("=" * 80)
    print("QUERY 3: MONTHLY SPEND BY TEAM & ENVIRONMENT")
    print("=" * 80)
    print()
    
    query3 = """
    SELECT 
        team,
        environment,
        CAST(strftime('%Y', date) AS INTEGER) AS year,
        CAST(strftime('%m', date) AS INTEGER) AS month,
        strftime('%Y-%m', date) AS month_label,
        COUNT(*) AS transaction_count,
        ROUND(SUM(cost_usd), 2) AS total_cost_usd,
        ROUND(AVG(cost_usd), 2) AS avg_cost,
        COUNT(DISTINCT cloud_account_id) AS accounts_used,
        COUNT(DISTINCT service) AS services_used
    FROM vw_unified_cloud_billing
    GROUP BY 
        team,
        environment,
        strftime('%Y', date),
        strftime('%m', date),
        strftime('%Y-%m', date)
    ORDER BY 
        year,
        month,
        total_cost_usd DESC
    LIMIT 20
    """
    
    result3 = pd.read_sql_query(query3, conn)
    print("Monthly Spend by Team & Environment (Top 20):")
    print(result3.to_string(index=False))
    print()
    
    # Pivot version
    query3_pivot = """
    SELECT 
        team,
        CAST(strftime('%Y', date) AS INTEGER) AS year,
        CAST(strftime('%m', date) AS INTEGER) AS month,
        ROUND(SUM(CASE WHEN environment = 'prod' THEN cost_usd ELSE 0 END), 2) AS prod_cost,
        ROUND(SUM(CASE WHEN environment = 'staging' THEN cost_usd ELSE 0 END), 2) AS staging_cost,
        ROUND(SUM(CASE WHEN environment = 'dev' THEN cost_usd ELSE 0 END), 2) AS dev_cost,
        ROUND(SUM(cost_usd), 2) AS total_cost,
        ROUND(
            SUM(CASE WHEN environment = 'prod' THEN cost_usd ELSE 0 END) * 100.0 / NULLIF(SUM(cost_usd), 0),
            2
        ) AS prod_pct
    FROM vw_unified_cloud_billing
    GROUP BY 
        team,
        strftime('%Y', date),
        strftime('%m', date)
    ORDER BY 
        year,
        month,
        total_cost DESC
    LIMIT 15
    """
    
    result3_pivot = pd.read_sql_query(query3_pivot, conn)
    print("\\nMonthly Spend by Team (Pivot by Environment - Top 15):")
    print(result3_pivot.to_string(index=False))
    print()
    
    # ========================================================================
    # Query 4: Top 5 Most Expensive Services
    # ========================================================================
    print("=" * 80)
    print("QUERY 4: TOP 5 MOST EXPENSIVE SERVICES")
    print("=" * 80)
    print()
    
    query4 = """
    SELECT 
        service,
        cloud_provider,
        COUNT(*) AS transaction_count,
        ROUND(SUM(cost_usd), 2) AS total_cost_usd,
        ROUND(AVG(cost_usd), 2) AS avg_cost_per_transaction,
        ROUND(MIN(cost_usd), 2) AS min_cost,
        ROUND(MAX(cost_usd), 2) AS max_cost,
        COUNT(DISTINCT team) AS teams_using,
        COUNT(DISTINCT environment) AS environments_using
    FROM vw_unified_cloud_billing
    WHERE NOT is_credit
    GROUP BY service, cloud_provider
    ORDER BY total_cost_usd DESC
    LIMIT 5
    """
    
    result4 = pd.read_sql_query(query4, conn)
    print("Top 5 Most Expensive Services (by Cloud Provider):")
    print(result4.to_string(index=False))
    print()
    
    # Combined across clouds
    query4_combined = """
    SELECT 
        service,
        COUNT(*) AS transaction_count,
        ROUND(SUM(cost_usd), 2) AS total_cost_usd,
        ROUND(AVG(cost_usd), 2) AS avg_cost_per_transaction,
        ROUND(SUM(CASE WHEN cloud_provider = 'AWS' THEN cost_usd ELSE 0 END), 2) AS aws_cost,
        ROUND(SUM(CASE WHEN cloud_provider = 'GCP' THEN cost_usd ELSE 0 END), 2) AS gcp_cost,
        ROUND(
            SUM(cost_usd) * 100.0 / (SELECT SUM(cost_usd) FROM vw_unified_cloud_billing WHERE NOT is_credit),
            2
        ) AS pct_of_total_spend
    FROM vw_unified_cloud_billing
    WHERE NOT is_credit
    GROUP BY service
    ORDER BY total_cost_usd DESC
    LIMIT 5
    """
    
    result4_combined = pd.read_sql_query(query4_combined, conn)
    print("\\nTop 5 Services Overall (Combined AWS + GCP):")
    print(result4_combined.to_string(index=False))
    print()
    
    # ========================================================================
    # Additional Analysis
    # ========================================================================
    print("=" * 80)
    print("ADDITIONAL INSIGHTS")
    print("=" * 80)
    print()
    
    # Total spend summary
    total_query = """
    SELECT 
        ROUND(SUM(cost_usd), 2) AS total_spend,
        ROUND(SUM(CASE WHEN is_credit THEN ABS(cost_usd) ELSE 0 END), 2) AS total_credits,
        ROUND(SUM(CASE WHEN NOT is_credit THEN cost_usd ELSE 0 END), 2) AS total_charges,
        COUNT(*) AS total_records,
        COUNT(DISTINCT date) AS unique_dates
    FROM vw_unified_cloud_billing
    """
    
    total_result = pd.read_sql_query(total_query, conn)
    print("Overall Summary:")
    print(total_result.to_string(index=False))
    print()
    
    conn.close()
    
    print("=" * 80)
    print("SQL EXECUTION COMPLETE")
    print("=" * 80)
    
    return {
        'unified_summary': result1,
        'monthly_by_provider': result2,
        'monthly_by_team_env': result3,
        'monthly_by_team_pivot': result3_pivot,
        'top_services': result4,
        'top_services_combined': result4_combined,
        'total_summary': total_result
    }

if __name__ == "__main__":
    results = main()
