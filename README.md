# K&Co Data Engineer Intern — Take-Home Assignment
## Multi-Cloud Billing Analytics Platform

**Submitted by:** Data Engineering Candidate  
**Submission Date:** November 24, 2025  

---

##  Executive Summary

This submission presents a complete solution for K&Co's cloud cost intelligence platform, covering data profiling, warehouse design, SQL transformations, pipeline architecture, and FinOps insights across AWS and GCP billing data.

**Key Findings:**
- Analyzed **5,851 billing records** ($707K total spend)
- Identified **7 critical data quality risks**
- Designed **star schema** with 1 fact + 5 dimension tables
- Created **unified billing view** combining AWS + GCP
- Architected **production-grade pipeline** with Airflow + dbt
- Discovered **$503 EC2 cost anomaly** (61% spike)

---

## Project Structure

```
multi-cloud-billing-assignment/
├── data/
│   ├── aws_line_items_12mo.csv          # AWS billing data (2,943 records)
│   └── gcp_billing_12mo.csv             # GCP billing data (2,908 records)
├── notebooks/
│   ├── part_a_data_profiling.ipynb      # Jupyter notebook for Part A
│   ├── part_a_profiling.py              # Python script for data profiling
│   └── part_c_sql_execution.py          # SQL query execution script
├── sql/
│   └── part_c_transformations.sql       # All SQL queries for Part C
├── diagram/
│   ├── er_diagram.png                   # Entity-Relationship diagram (Part B)
│   └── pipeline_architecture.png        # Data pipeline architecture (Part D)
├── docs/
│   ├── Part_A_Summary.md                # Data profiling & quality analysis
│   ├── Part_B_Data_Model.md             # Star schema design & documentation
│   ├── Part_C_SQL_Results.md            # SQL transformations & sample outputs
│   ├── Part_D_Pipeline_Design.md        # Pipeline architecture & runbook
│   └── Part_E_FinOps_Analysis.md        # Cost anomaly analysis & recommendations
└── README.md                            # This file
```

---

##  Assignment Parts Overview

### Part A: Data Understanding & Quality Checks 

**Deliverables:**
- [`part_a_data_profiling.ipynb`](notebooks/part_a_data_profiling.ipynb) - Comprehensive profiling notebook
- [`Part_A_Summary.md`](docs/Part_A_Summary.md) - Written summary with findings

**Key Findings:**
- **No missing values** across 5,851 records
- **No duplicate records** on composite keys
- **Complete date coverage** (100 consecutive days)
- **7 data quality risks identified:**
  1. Negative cost values (36 records - credits/refunds)
  2. Potential for duplicate records without constraints
  3. Inconsistent service naming across clouds
  4. Missing date continuity validation
  5. No data type validation
  6. Wide cost range without outlier detection
  7. No referential integrity checks

---

### Part B: Data Modeling 

**Deliverables:**
- [`er_diagram.png`](diagram/er_diagram.png) - Star schema ER diagram
- [`Part_B_Data_Model.md`](docs/Part_B_Data_Model.md) - Complete schema documentation

**Schema Design:**
- **Fact Table:** `fact_cloud_billing` (daily grain per service/team/env/provider)
- **Dimension Tables:**
  - `dim_date` - Time dimension with fiscal calendar support
  - `dim_service` - Cloud service catalog
  - `dim_team` - Organizational structure
  - `dim_environment` - Environment classification (prod/staging/dev)
  - `dim_cloud_provider` - Cloud provider reference

**Design Decisions:**
- Star schema for query simplicity and performance
- Surrogate keys for flexibility
- Daily grain balances detail with performance
- Separate `is_credit` flag for negative costs

---

### Part C: SQL Transformations 

**Deliverables:**
- [`part_c_transformations.sql`](sql/part_c_transformations.sql) - All SQL queries
- [`Part_C_SQL_Results.md`](docs/Part_C_SQL_Results.md) - Query results & analysis

**Queries Developed:**
1. **Unified Cloud Billing View** - Combined AWS + GCP with standardized columns
2. **Monthly Spend by Cloud Provider** - Trend analysis with MoM growth
3. **Monthly Spend by Team & Environment** - Chargeback reporting
4. **Top 5 Most Expensive Services** - Cost optimization opportunities

**Key Insights:**
- **EC2 dominates** at 78% of total spend
- **AWS costs 14% more** than GCP on average
- **Production environments** account for only 50% of spend (optimization opportunity)

---

### Part D: Pipeline Design 

**Deliverables:**
- [`pipeline_architecture.png`](diagram/pipeline_architecture.png) - Architecture diagram
- [`Part_D_Pipeline_Design.md`](docs/Part_D_Pipeline_Design.md) - Complete design document

**Architecture Highlights:**
- **Orchestration:** Apache Airflow (daily batch at 6:30 AM UTC)
- **Storage:** Medallion architecture (Bronze → Silver → Gold)
- **Transformation:** dbt for SQL-based transformations
- **Validation:** Great Expectations for data quality
- **Monitoring:** Datadog + CloudWatch with PagerDuty alerts
- **Warehouse:** Snowflake (cloud-agnostic recommendation)

**SLA:** Data available in analytics zone by 8 AM UTC (90-minute window)

---

### Part E: Value Extraction (FinOps Mindset) 

**Deliverables:**
- [`Part_E_FinOps_Analysis.md`](docs/Part_E_FinOps_Analysis.md) - Anomaly analysis & recommendations

**Anomaly Identified:**
- **EC2 cost spike on March 8, 2025:** $503.40 (61% above average)
- **Location:** GCP proj-gamma, Core team, production environment

**Root Causes:**
1. Unintentional auto-scaling event or manual scaling action
2. Instance type migration without rightsizing

**Recommended Action:**
Implement automated weekly rightsizing analysis with budget alerts at 120% of 7-day rolling average, potentially saving **15-20% on annual compute costs**.

**Additional Insights:**
- Non-production environments consume 50% of spend (optimization opportunity: **$50-60K/year**)
- EC2 concentration risk at 78% of spend (reserved instances could save **$80-100K/year**)
- Multi-cloud cost parity suggests need for TCO analysis

---

##  Technologies Used

- **Languages:** Python 3.8+, SQL (ANSI SQL-92)
- **Libraries:** pandas, numpy, matplotlib, seaborn, sqlite3
- **Databases:** SQLite (for demonstration), designed for Snowflake/BigQuery
- **Tools:** Jupyter Notebook, dbt, Great Expectations, Apache Airflow
- **Visualization:** Matplotlib, AI-generated architecture diagrams

---

##  How to Run

### Prerequisites
```bash
pip install pandas numpy matplotlib seaborn jupyter
```

### Part A: Data Profiling
```bash
cd multi-cloud-billing-assignment
python notebooks/part_a_profiling.py
```

### Part C: SQL Execution
```bash
python notebooks/part_c_sql_execution.py
```

### View Jupyter Notebooks
```bash
jupyter notebook notebooks/part_a_data_profiling.ipynb
```

---

##  Key Metrics Summary

| Metric | Value |
|--------|-------|
| **Total Records** | 5,851 |
| **Date Range** | Jan 1 - Apr 10, 2025 (100 days) |
| **Total Spend** | $707,128.03 |
| **AWS Spend** | $376,476.27 (53.2%) |
| **GCP Spend** | $330,651.76 (46.8%) |
| **Unique Services** | 5 (EC2, RDS, S3, Lambda, EKS) |
| **Unique Teams** | 3 (Core, Web, Data) |
| **Environments** | 3 (prod, staging, dev) |
| **Data Quality Risks** | 7 identified |
| **Cost Anomalies** | 1 major spike detected |

---

##  Recommendations for Production

### Immediate (Week 1)
1. Implement unified billing view in Snowflake
2. Set up Great Expectations validation suite
3. Deploy Airflow DAG for daily ingestion

### Short-term (Month 1)
4. Create dimension tables with referential integrity
5. Implement automated rightsizing analysis
6. Set up cost anomaly alerts (>120% of 7-day avg)

### Medium-term (Quarter 1)
7. Build BI dashboards in Tableau/Looker
8. Implement reserved instance recommendations
9. Optimize non-production environment costs

**Estimated Annual Savings:** $130,000 - $160,000 (18-23% cost reduction)

---

## Documentation Quality

All deliverables include:
- Clear explanations of design decisions
- Sample outputs and visualizations
- Production-ready considerations
- Assumptions clearly stated
- Future enhancement roadmap

---

##  What I Learned

1. **Data Quality is Critical:** Even clean-looking data has hidden risks (negative costs, potential duplicates)
2. **Star Schema Simplicity:** Denormalized dimensions make analytics queries much easier
3. **FinOps Mindset:** Cost optimization requires both technical analysis and business context
4. **Production Thinking:** Real pipelines need validation, monitoring, and error handling
5. **Multi-Cloud Complexity:** Managing two clouds adds operational overhead that must be justified

---

##  Contact

For questions about this submission, please contact:
- **Email:** [urvashiparmar1603@gmail.com]
- **LinkedIn:** [https://www.linkedin.com/in/urvashi-vankar-5229bb272/]
- **GitHub:** [https://github.com/urvashivankar]

---

##  Acknowledgments

Thank you to K&Co for this comprehensive and realistic take-home assignment. It provided excellent hands-on experience with:
- Real-world cloud billing data
- End-to-end data engineering workflows
- FinOps best practices
- Production-grade system design

---

**Total Time Invested:** ~6 hours  
**Confidence Level:** High - All requirements met with production-ready solutions
