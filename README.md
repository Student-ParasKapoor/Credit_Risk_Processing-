# PySpark Credit Risk Analytics Pipeline

## 📌 Overview
This project implements a **PySpark-based time-series credit risk analytics pipeline** that simulates real-world banking and financial data processing workflows.  
It focuses on **loan-level risk metrics**, **data quality validation**, and **time-series completeness checks** using distributed data processing.

The pipeline ingests raw loan data, computes portfolio-level risk metrics, validates data quality, and prepares outputs suitable for downstream analytics or reporting.

---

## 🏦 Business Problem
Banks manage large volumes of **loan-level periodic data** for:
- Portfolio exposure monitoring
- Credit risk assessment
- Regulatory and internal reporting

Data quality issues such as **duplicate records**, **null values**, or **missing reporting periods** can lead to incorrect risk calculations and financial exposure.  
This project demonstrates how such data can be **processed, validated, and analyzed at scale** using PySpark.

---

## 🛠️ Tech Stack
- **Python**
- **PySpark (Spark DataFrame API, Spark SQL)**
- **SQL-style aggregations**
- **Parquet / CSV**
- **VS Code**
- **Git**

---

## 📂 Project Structure
```text
pyspark_credit_risk_pipeline/
├── README.md
├── data/
│   └── raw/
│       └── credit_risk_data.csv
├── outputs/
│   ├── metrics/
│   │   ├── exposure/
│   │   ├── default_rate/
│   │   └── delinquency_rate/
│   └── exceptions/
│       ├── duplicates/
│       ├── nulls/
│       └── missing_months/
└── src/
    ├── spark_session.py
    ├── ingest.py
    ├── transform.py
    ├── validate.py
```
## 📥 Data Ingestion

- Raw loan-level data is ingested from CSV using **PySpark**
- Schema is inferred automatically
- Each row represents **one loan in one reporting month**

**Key columns:**
- `loan_id`
- `customer_id`
- `report_month`
- `outstanding_balance`
- `default_flag`
- `days_past_due`
- `product_type`

---

## 📊 Risk Metrics Implemented

customer_id

report_month

outstanding_balance

default_flag

days_past_due

product_type

### 1️⃣ Monthly Portfolio Exposure

Aggregates total outstanding balance per month

Represents overall portfolio exposure

### 2️⃣ Monthly Default Rate
Calculated as:
defaulted_loans / total_loans

- Based on loan-level default flag

### 3️⃣ Monthly Delinquency Rate (DPD > 30)
- Measures early-stage credit risk  
- Flags loans with `days_past_due > 30`

---

## ✅ Data Quality & Validation

### 🔹 Duplicate Detection
- Identifies duplicate records using:
loan_id + report_month

- Records are **flagged, not deleted**

### 🔹 Null Checks
Flags records with null values in critical fields:
- `loan_id`
- `customer_id`
- `report_month`
- `outstanding_balance`
- `default_flag`

### 🔹 Missing Month Validation
- Performs loan-level time-series completeness checks
- Generates expected months between first and last reporting period
- Detects gaps using left joins
- Prevents false positives by respecting loan lifecycle

---

## 💾 Output Persistence

- Clean metrics and exception datasets are written separately

**Output formats:**
- **Parquet** (recommended for Linux / production environments)
- **CSV** (can be used for local Windows execution)

> ⚠️ **Note:** On Windows systems, Spark may require additional Hadoop configuration (`winutils.exe`) for Parquet writes.  
> This limitation does not apply to Linux-based Spark clusters used in production.

---

## ▶️ How to Run

### 1️⃣ Activate virtual environment
```bash
venv\Scripts\activate
```
### 2️⃣ Run transformations
```bash
python src/transform.py
```
### 3️⃣ Run validations
```bash
python src/validate.py
```
---

## 🧠 Key Concepts Demonstrated

- Distributed data processing with Spark
- Lazy evaluation
- Time-series aggregation
- Loan-level vs portfolio-level analytics
- Data quality validation in regulated environments
- Separation of clean outputs and exception datasets

---

## 🚀 Future Enhancements

- Customer-level aggregation
- Cloud storage integration (S3 / ADLS)
- Pipeline orchestration and scheduling
- Monitoring and alerting for data quality
- Unit tests for validation logic

---
## 👤 Author

**Paras Kapoor**  
Data / Risk Analytics  
Python | PySpark | SQL
