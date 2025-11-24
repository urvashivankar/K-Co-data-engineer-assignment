# Part A: Data Understanding & Quality Checks
## K&Co Cloud Cost Intelligence Platform

---

## Executive Summary

This document presents the findings from comprehensive data profiling of AWS and GCP billing datasets covering 12 months of cloud cost data. The analysis identified **7 critical data quality risks** that require remediation before production deployment.

---

## 1. Data Profiling Results

### 1.1 Dataset Overview

| Metric | AWS | GCP |
|--------|-----|-----|
| **Total Records** | 2,943 | 2,908 |
| **Date Range** | 2025-01-01 to 2025-04-10 | 2025-01-01 to 2025-04-10 |
| **Unique Dates** | 100 | 100 |
| **Column Count** | 6 | 6 |
| **Memory Usage** | ~0.13 MB | ~0.12 MB |

### 1.2 Column Structure

**AWS Columns:**
- `date` (object → datetime)
- `account_id` (object)
- `service` (object)
- `team` (object)
- `env` (object)
- `cost_usd` (float64)

**GCP Columns:**
- `date` (object → datetime)
- `project_id` (object)
- `service` (object)
- `team` (object)
- `env` (object)
- `cost_usd` (float64)

---

## 2. Data Quality Analysis

### 2.1 Missing/Null Values

| Dataset | Total Missing Values | Status |
|---------|---------------------|--------|
| AWS | 0 | ✓ No missing values |
| GCP | 0 | ✓ No missing values |

**Finding:** Both datasets are complete with no null values across any columns.

### 2.2 Duplicate Records

| Dataset | Complete Duplicates | Key Column Duplicates* |
|---------|-------------------|----------------------|
| AWS | 0 (0.00%) | 0 |
| GCP | 0 (0.00%) | 0 |

*Key columns: date + account_id/project_id + service + team + env

**Finding:** No duplicate records detected, indicating clean data ingestion.

### 2.3 Date Range & Continuity

| Metric | AWS | GCP |
|--------|-----|-----|
| Start Date | 2025-01-01 | 2025-01-01 |
| End Date | 2025-04-10 | 2025-04-10 |
| Date Span | 100 days | 100 days |
| Unique Dates | 100 | 100 |
| Missing Dates | 0 | 0 |

**Finding:** Complete daily coverage with no gaps in the time series.

### 2.4 Categorical Values Analysis

#### Environments
Both datasets contain 3 environments:
- **prod** (production)
- **staging**
- **dev** (development)

Distribution is consistent across both clouds.

#### Services
Both datasets contain 5 services:
- **EC2** (Elastic Compute Cloud)
- **RDS** (Relational Database Service)
- **S3** (Simple Storage Service)
- **Lambda** (Serverless Functions)
- **EKS** (Elastic Kubernetes Service)

**⚠️ Issue:** GCP using AWS service names suggests synthetic/test data.

#### Teams
Both datasets contain 3 teams:
- **Core**
- **Web**
- **Data**

#### Account/Project IDs
- **AWS:** 3 unique account IDs (111111111111, 222222222222, 333333333333)
- **GCP:** 3 unique project IDs (proj-alpha, proj-beta, proj-gamma)

### 2.5 Cost Analysis

#### AWS Cost Statistics
```
Count:     2,943
Mean:      $127.89
Std Dev:   $124.66
Min:       $-12.56
25%:       $32.09
50%:       $89.19
75%:       $181.53
Max:       $547.27
```

- **Negative Costs:** 21 records (credits/refunds)
- **Zero Costs:** 1 record
- **Total Spend:** $376,476.27

#### GCP Cost Statistics
```
Count:     2,908
Mean:      $113.72
Std Dev:   $104.41
Min:       $-12.56
25%:       $42.30
50%:       $92.77
75%:       $157.66
Max:       $547.27
```

- **Negative Costs:** 15 records (credits/refunds)
- **Zero Costs:** 0 records
- **Total Spend:** $330,651.76

**Combined Total Spend:** $707,128.03 over 100 days

---

## 3. Data Quality Risks Identified

### Risk #1: Negative Cost Values
**Description:** Found 21 negative costs in AWS and 15 in GCP (ranging from -$12.56 to -$0.16)

**Impact:** Can skew financial reporting and analytics if not handled properly. Negative values may represent:
- Credits from reserved instance purchases
- Refunds for service issues
- Billing corrections

**Remediation:**
- Investigate source of negative values with cloud billing teams
- Create separate `credit_amount` column to track credits explicitly
- Add `is_credit` boolean flag for easy filtering
- Document credit types in metadata table

---

### Risk #2: Duplicate Records on Key Columns
**Description:** While no duplicates found in current dataset, the grain definition (daily per service/team/env/account) allows for potential duplicates

**Impact:** Without proper constraints, duplicate records could lead to:
- Double-counting costs
- Inflated spend reports
- Incorrect budget forecasting

**Remediation:**
- Implement unique constraint on composite key: `(date, cloud_provider, account_id, service, team, env)`
- Add deduplication logic in ETL pipeline
- If multiple records are legitimate, aggregate costs during ingestion
- Log duplicate detection events for monitoring

---

### Risk #3: Inconsistent Service Naming Across Clouds
**Description:** Same service names appear in both AWS and GCP (EC2, RDS, S3, Lambda, EKS) - this is unrealistic as GCP uses different naming (Compute Engine, Cloud SQL, Cloud Storage, Cloud Functions, GKE)

**Impact:**
- Confusion in cross-cloud cost analysis
- Inability to distinguish cloud-specific services
- Misleading service-level reporting

**Remediation:**
- Create `dim_service` table with standardized service names
- Map cloud-specific names to common categories (e.g., "Compute", "Database", "Storage")
- Add `cloud_provider` prefix to service names in fact table
- Implement service name validation against approved lists

---

### Risk #4: Missing Date Continuity Validation
**Description:** While current data has no gaps, there's no mechanism to detect missing dates in production

**Impact:**
- Incomplete time-series analysis
- Gaps in cost trending
- Missed anomaly detection
- Inaccurate month-over-month comparisons

**Remediation:**
- Implement daily data completeness checks
- Alert on missing dates (expected daily ingestion)
- Fill gaps with zero-cost placeholder records for continuity
- Track data freshness SLA (e.g., data should arrive within 24 hours)

---

### Risk #5: No Data Type Validation
**Description:** Columns loaded as generic object types; no explicit validation of:
- Account ID format (should be 12-digit number for AWS)
- Project ID format (should match GCP naming convention)
- Date format consistency
- Cost value ranges

**Impact:**
- Invalid data could enter system undetected
- Type conversion errors in downstream processing
- Inconsistent data formats across batches

**Remediation:**
- Implement schema validation with Pydantic or Great Expectations
- Define regex patterns for account/project IDs
- Enforce data type constraints (e.g., cost_usd must be numeric)
- Reject records that fail validation with detailed error logging

---

### Risk #6: Wide Cost Range Without Outlier Detection
**Description:** AWS costs range from -$12.56 to $547.27 with high standard deviation ($124.66)

**Impact:**
- Anomalous cost spikes may go unnoticed
- Fraudulent activity could be missed
- Budget overruns without early warning
- Difficulty distinguishing normal variance from true anomalies

**Remediation:**
- Implement statistical outlier detection:
  - IQR (Interquartile Range) method for daily costs
  - Z-score analysis for service-level costs
  - Time-series anomaly detection (Prophet, ARIMA)
- Set up automated alerts for costs exceeding 2σ from mean
- Create cost threshold rules per service/environment
- Weekly anomaly review process with FinOps team

---

### Risk #7: No Referential Integrity Checks
**Description:** Team, service, and environment values are not validated against master reference lists

**Impact:**
- Typos can create fragmented reporting (e.g., "prod" vs "production")
- New teams/services added without governance
- Inconsistent naming conventions
- Difficulty in aggregating costs by logical groups

**Remediation:**
- Create dimension tables with approved values:
  - `dim_team` (team_id, team_name, department, cost_center)
  - `dim_service` (service_id, service_name, category, cloud_provider)
  - `dim_environment` (env_id, env_name, env_type)
- Enforce foreign key constraints in data warehouse
- Implement data validation rules in ETL pipeline
- Require approval workflow for adding new dimension values

---

## 4. Summary Statistics

| Metric | AWS | GCP |
|--------|-----|-----|
| **Total Records** | 2,943 | 2,908 |
| **Date Range** | 2025-01-01 to 2025-04-10 | 2025-01-01 to 2025-04-10 |
| **Unique Dates** | 100 | 100 |
| **Missing Dates** | 0 | 0 |
| **Unique Services** | 5 | 5 |
| **Unique Teams** | 3 | 3 |
| **Unique Environments** | 3 | 3 |
| **Unique Accounts/Projects** | 3 | 3 |
| **Total Cost (USD)** | $376,476.27 | $330,651.76 |
| **Average Daily Cost (USD)** | $3,764.76 | $3,306.52 |
| **Negative Cost Records** | 21 | 15 |
| **Duplicate Records** | 0 | 0 |
| **Missing Values** | 0 | 0 |

---

## 5. Recommendations for Production

### Immediate Actions (Pre-Production)
1. ✓ **Investigate negative costs** - Confirm these are legitimate credits
2. ✓ **Fix service naming** - Map to actual GCP service names or document as test data
3. ✓ **Implement schema validation** - Define and enforce data contracts

### Short-term (First Sprint)
4. ✓ **Create dimension tables** - Establish referential integrity
5. ✓ **Add deduplication logic** - Prevent double-counting
6. ✓ **Set up data quality monitoring** - Daily validation checks

### Medium-term (First Quarter)
7. ✓ **Implement outlier detection** - Automated anomaly alerts
8. ✓ **Build data lineage tracking** - Understand data flow and transformations
9. ✓ **Establish SLAs** - Define data freshness and completeness targets

---

## 6. Conclusion

The AWS and GCP billing datasets are **structurally sound** with:
- ✓ No missing values
- ✓ No duplicate records
- ✓ Complete date coverage
- ✓ Consistent categorical values

However, **7 data quality risks** have been identified that must be addressed before production deployment. The most critical risks are:
1. Negative cost handling
2. Service name standardization
3. Outlier detection implementation

With proper remediation, this data will provide a solid foundation for K&Co's cloud cost intelligence platform.

---

**Document Version:** 1.0  
**Analysis Date:** November 24, 2025  
**Analyst:** Data Engineering Team  
**Next Steps:** Proceed to Part B (Data Modeling)
