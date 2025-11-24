"""
Part A: Data Understanding & Quality Checks
K&Co Cloud Cost Intelligence Platform

This script performs comprehensive data profiling on AWS and GCP billing data.
"""

import pandas as pd
import numpy as np
from datetime import datetime
import json

def main():
    print("=" * 80)
    print("K&CO DATA PROFILING ANALYSIS")
    print("=" * 80)
    print()
    
    # Load datasets
    print("Loading datasets...")
    aws_df = pd.read_csv('data/aws_line_items_12mo.csv')
    gcp_df = pd.read_csv('data/gcp_billing_12mo.csv')
    print(f"✓ AWS Data Loaded: {len(aws_df):,} records")
    print(f"✓ GCP Data Loaded: {len(gcp_df):,} records")
    print()
    
    # Convert dates
    aws_df['date'] = pd.to_datetime(aws_df['date'])
    gcp_df['date'] = pd.to_datetime(gcp_df['date'])
    
    # 1. Basic Profiling
    print("=" * 80)
    print("1. BASIC DATA PROFILING")
    print("=" * 80)
    print()
    
    profiling_summary = pd.DataFrame({
        'Dataset': ['AWS', 'GCP'],
        'Row Count': [len(aws_df), len(gcp_df)],
        'Column Count': [len(aws_df.columns), len(gcp_df.columns)],
        'Memory (MB)': [
            f"{aws_df.memory_usage(deep=True).sum() / 1024**2:.2f}",
            f"{gcp_df.memory_usage(deep=True).sum() / 1024**2:.2f}"
        ]
    })
    print(profiling_summary.to_string(index=False))
    print()
    
    # 2. Missing Values
    print("=" * 80)
    print("2. MISSING/NULL VALUES ANALYSIS")
    print("=" * 80)
    print()
    
    aws_missing = aws_df.isnull().sum().sum()
    gcp_missing = gcp_df.isnull().sum().sum()
    print(f"AWS Total Missing Values: {aws_missing}")
    print(f"GCP Total Missing Values: {gcp_missing}")
    print()
    
    # 3. Duplicate Records
    print("=" * 80)
    print("3. DUPLICATE RECORDS CHECK")
    print("=" * 80)
    print()
    
    aws_duplicates = aws_df.duplicated().sum()
    gcp_duplicates = gcp_df.duplicated().sum()
    aws_key_duplicates = aws_df.duplicated(subset=['date', 'account_id', 'service', 'team', 'env']).sum()
    gcp_key_duplicates = gcp_df.duplicated(subset=['date', 'project_id', 'service', 'team', 'env']).sum()
    
    print(f"AWS Complete Duplicate Rows: {aws_duplicates} ({aws_duplicates/len(aws_df)*100:.2f}%)")
    print(f"GCP Complete Duplicate Rows: {gcp_duplicates} ({gcp_duplicates/len(gcp_df)*100:.2f}%)")
    print()
    print(f"AWS Duplicates on Key Columns: {aws_key_duplicates}")
    print(f"GCP Duplicates on Key Columns: {gcp_key_duplicates}")
    print()
    
    # 4. Date Range Analysis
    print("=" * 80)
    print("4. DATE RANGE ANALYSIS")
    print("=" * 80)
    print()
    
    print("AWS Date Range:")
    print(f"  Min Date: {aws_df['date'].min().date()}")
    print(f"  Max Date: {aws_df['date'].max().date()}")
    print(f"  Date Span: {(aws_df['date'].max() - aws_df['date'].min()).days} days")
    print(f"  Unique Dates: {aws_df['date'].nunique()}")
    print()
    
    print("GCP Date Range:")
    print(f"  Min Date: {gcp_df['date'].min().date()}")
    print(f"  Max Date: {gcp_df['date'].max().date()}")
    print(f"  Date Span: {(gcp_df['date'].max() - gcp_df['date'].min()).days} days")
    print(f"  Unique Dates: {gcp_df['date'].nunique()}")
    print()
    
    # Check for date gaps
    aws_date_range = pd.date_range(start=aws_df['date'].min(), end=aws_df['date'].max(), freq='D')
    aws_missing_dates = set(aws_date_range) - set(aws_df['date'].unique())
    gcp_date_range = pd.date_range(start=gcp_df['date'].min(), end=gcp_df['date'].max(), freq='D')
    gcp_missing_dates = set(gcp_date_range) - set(gcp_df['date'].unique())
    
    print(f"AWS Missing Dates: {len(aws_missing_dates)}")
    print(f"GCP Missing Dates: {len(gcp_missing_dates)}")
    print()
    
    # 5. Environment Values
    print("=" * 80)
    print("5. ENVIRONMENT VALUES ANALYSIS")
    print("=" * 80)
    print()
    
    print("AWS Environments:")
    print(aws_df['env'].value_counts().to_string())
    print()
    
    print("GCP Environments:")
    print(gcp_df['env'].value_counts().to_string())
    print()
    
    # 6. Service Names
    print("=" * 80)
    print("6. SERVICE NAMES ANALYSIS")
    print("=" * 80)
    print()
    
    print(f"AWS Unique Services ({aws_df['service'].nunique()}): {sorted(aws_df['service'].unique())}")
    print()
    print(f"GCP Unique Services ({gcp_df['service'].nunique()}): {sorted(gcp_df['service'].unique())}")
    print()
    
    # 7. Team Analysis
    print("=" * 80)
    print("7. TEAM ANALYSIS")
    print("=" * 80)
    print()
    
    print(f"AWS Teams ({aws_df['team'].nunique()}): {sorted(aws_df['team'].unique())}")
    print(f"GCP Teams ({gcp_df['team'].nunique()}): {sorted(gcp_df['team'].unique())}")
    print()
    
    # 8. Cost Analysis
    print("=" * 80)
    print("8. COST ANALYSIS")
    print("=" * 80)
    print()
    
    print("AWS Cost Statistics:")
    print(aws_df['cost_usd'].describe().to_string())
    print(f"\nNegative Costs: {(aws_df['cost_usd'] < 0).sum()} records")
    print(f"Zero Costs: {(aws_df['cost_usd'] == 0).sum()} records")
    print()
    
    print("GCP Cost Statistics:")
    print(gcp_df['cost_usd'].describe().to_string())
    print(f"\nNegative Costs: {(gcp_df['cost_usd'] < 0).sum()} records")
    print(f"Zero Costs: {(gcp_df['cost_usd'] == 0).sum()} records")
    print()
    
    # 9. Account/Project IDs
    print("=" * 80)
    print("9. ACCOUNT/PROJECT ID ANALYSIS")
    print("=" * 80)
    print()
    
    print(f"AWS Unique Account IDs ({aws_df['account_id'].nunique()}): {sorted(aws_df['account_id'].unique())}")
    print()
    print(f"GCP Unique Project IDs ({gcp_df['project_id'].nunique()}): {sorted(gcp_df['project_id'].unique())}")
    print()
    
    # 10. Data Quality Risks
    print("=" * 80)
    print("10. DATA QUALITY RISKS IDENTIFIED")
    print("=" * 80)
    print()
    
    risks = [
        {
            'risk': 'Negative Cost Values',
            'description': f'Found {(aws_df["cost_usd"] < 0).sum()} negative costs in AWS and {(gcp_df["cost_usd"] < 0).sum()} in GCP',
            'impact': 'Can skew financial reporting and analytics',
            'remediation': 'Investigate if these are credits/refunds. Create separate column for credits or flag them explicitly.'
        },
        {
            'risk': 'Duplicate Records on Key Columns',
            'description': f'Found {aws_key_duplicates} duplicates in AWS and {gcp_key_duplicates} in GCP',
            'impact': 'Double-counting costs leading to inflated spend reports',
            'remediation': 'Implement deduplication logic based on composite key. Aggregate costs if legitimate.'
        },
        {
            'risk': 'Inconsistent Service Naming',
            'description': 'Same services appear in both AWS and GCP (EC2, RDS, S3, Lambda, EKS)',
            'impact': 'Confusion in cross-cloud analysis; GCP should use different service names',
            'remediation': 'Create service mapping table to standardize names across clouds.'
        },
        {
            'risk': 'Missing Date Continuity',
            'description': f'{len(aws_missing_dates)} missing dates in AWS, {len(gcp_missing_dates)} in GCP',
            'impact': 'Incomplete time-series analysis and trending',
            'remediation': 'Implement data completeness checks. Fill gaps with zero-cost records or flag missing days.'
        },
        {
            'risk': 'No Data Type Validation',
            'description': 'Columns loaded as generic types; no explicit validation of account_id, project_id formats',
            'impact': 'Invalid IDs could enter system undetected',
            'remediation': 'Implement schema validation with expected data types and regex patterns for IDs.'
        },
        {
            'risk': 'Wide Cost Range Without Outlier Detection',
            'description': f'AWS costs range from ${aws_df["cost_usd"].min():.2f} to ${aws_df["cost_usd"].max():.2f}',
            'impact': 'Anomalous spikes may go unnoticed without automated detection',
            'remediation': 'Implement statistical outlier detection (IQR, Z-score) and alerting.'
        },
        {
            'risk': 'No Referential Integrity Checks',
            'description': 'Team, service, env values not validated against master lists',
            'impact': 'Typos and invalid values can fragment reporting',
            'remediation': 'Create dimension tables with valid values. Enforce foreign key constraints.'
        }
    ]
    
    for idx, risk in enumerate(risks, 1):
        print(f"Risk #{idx}: {risk['risk']}")
        print(f"  Description: {risk['description']}")
        print(f"  Impact: {risk['impact']}")
        print(f"  Remediation: {risk['remediation']}")
        print()
    
    # Summary Statistics
    print("=" * 80)
    print("11. COMPREHENSIVE SUMMARY")
    print("=" * 80)
    print()
    
    summary_stats = pd.DataFrame({
        'Metric': [
            'Total Records',
            'Date Range',
            'Unique Dates',
            'Missing Dates',
            'Unique Services',
            'Unique Teams',
            'Unique Environments',
            'Unique Accounts/Projects',
            'Total Cost (USD)',
            'Average Daily Cost (USD)',
            'Negative Cost Records',
            'Duplicate Records (Key)',
            'Missing Values'
        ],
        'AWS': [
            f"{len(aws_df):,}",
            f"{aws_df['date'].min().date()} to {aws_df['date'].max().date()}",
            f"{aws_df['date'].nunique()}",
            f"{len(aws_missing_dates)}",
            f"{aws_df['service'].nunique()}",
            f"{aws_df['team'].nunique()}",
            f"{aws_df['env'].nunique()}",
            f"{aws_df['account_id'].nunique()}",
            f"${aws_df['cost_usd'].sum():,.2f}",
            f"${aws_df.groupby('date')['cost_usd'].sum().mean():,.2f}",
            f"{(aws_df['cost_usd'] < 0).sum()}",
            f"{aws_key_duplicates}",
            f"{aws_missing}"
        ],
        'GCP': [
            f"{len(gcp_df):,}",
            f"{gcp_df['date'].min().date()} to {gcp_df['date'].max().date()}",
            f"{gcp_df['date'].nunique()}",
            f"{len(gcp_missing_dates)}",
            f"{gcp_df['service'].nunique()}",
            f"{gcp_df['team'].nunique()}",
            f"{gcp_df['env'].nunique()}",
            f"{gcp_df['project_id'].nunique()}",
            f"${gcp_df['cost_usd'].sum():,.2f}",
            f"${gcp_df.groupby('date')['cost_usd'].sum().mean():,.2f}",
            f"{(gcp_df['cost_usd'] < 0).sum()}",
            f"{gcp_key_duplicates}",
            f"{gcp_missing}"
        ]
    })
    
    print(summary_stats.to_string(index=False))
    print()
    
    print("=" * 80)
    print("ANALYSIS COMPLETE")
    print("=" * 80)
    
    # Return data for summary document
    return {
        'aws_df': aws_df,
        'gcp_df': gcp_df,
        'summary_stats': summary_stats,
        'risks': risks,
        'aws_key_duplicates': aws_key_duplicates,
        'gcp_key_duplicates': gcp_key_duplicates,
        'aws_missing_dates': len(aws_missing_dates),
        'gcp_missing_dates': len(gcp_missing_dates)
    }

if __name__ == "__main__":
    results = main()
