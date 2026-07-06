# IAM-Security-Analytics-Pipeline
# Identity & Access Management (IAM) Anomaly Detection Pipeline using Microsoft Fabric

## Project Overview

This project simulates an Identity and Access Management (IAM) environment and builds an end-to-end data engineering pipeline using the Medallion Architecture (Bronze, Silver, Gold) in Microsoft Fabric.

The pipeline generates synthetic IAM data, cleans and validates it, detects security anomalies using rule-based logic, and stores curated datasets in Delta Lake tables for reporting and analytics.

---

## Architecture

```
                Synthetic Data Generation
                         │
                         ▼
                  Bronze Layer
          Raw data ingestion into Delta
                         │
                         ▼
                  Silver Layer
     Data cleaning, validation & enrichment
                         │
                         ▼
                   Gold Layer
      Rule-based anomaly detection engine
                         │
                         ▼
             Curated Delta Tables
                         │
                         ▼
          Power BI / Reporting / Analytics
```

---

## Medallion Architecture

### Bronze Layer
- Generates synthetic IAM datasets using Faker
- Creates:
  - Users
  - Login Events
  - Access Requests
- Stores raw data in Bronze Delta tables
- Adds ingestion timestamps
- Pipeline logging
- Data quality validation
- Pipeline summary

---

### Silver Layer
Cleans and standardizes the raw datasets.

Transformations include:

- Remove duplicate records
- Standardize department names
- Normalize account status values
- Convert dates to proper data types
- Calculate password age
- Create password stale flag
- Extract login hour
- Extract day of week
- Extract login date

Data Quality Checks:

- Duplicate user IDs
- Duplicate event IDs
- Duplicate request IDs
- Null primary keys
- Valid account status values
- Valid login status values
- Valid request status values

Outputs:

- silver_users
- silver_login_events
- silver_access_requests

---

### Gold Layer

Implements rule-based anomaly detection.

Security rules include:

1. Suspended account successfully logged in
2. Login from high-risk country
3. Privileged user logged in after business hours
4. Multiple failed login attempts (possible brute force attack)
5. Privileged user using personal device
6. Active account with stale password

Outputs:

- gold_anomalies

---

## Final Curated Tables

The final notebook loads cleaned data into Delta tables:

- users
- login_events
- access_requests
- anomalies

These tables are ready for reporting and analytics.

---

## Technologies Used

- Microsoft Fabric
- PySpark
- Delta Lake
- SQL
- Pandas
- NumPy
- Faker
- Python Logging

---

## Data Pipeline

| Notebook | Purpose |
|-----------|----------|
| nb_DataGeneration | Generate synthetic IAM datasets |
| nb_dataCleaningSilver | Clean and validate data |
| nb_AnomalyDetectionGold| Detect suspicious activity |
| CreateTables | Create Delta tables |
| nb_loadToDelta | Load curated data into Delta tables |

---

## Logging

Each notebook includes:

- Pipeline start/end logging
- Progress tracking
- Data quality validation
- Error handling
- Execution summary
- Row counts

---

## Data Quality Checks

Implemented validations include:

- Duplicate detection
- Null value validation
- Domain validation
- Row count verification
- Table creation verification

---

## Sample Datasets

### Users
- User ID
- Username
- Department
- Role
- Account Status
- Password Information

### Login Events
- Login Time
- Country
- Device Type
- Login Status
- Weekend Flag
- After-hours Flag

### Access Requests
- Requested Role
- Approval Status
- Request Date

---

## Future Improvements

Potential enhancements include:

- Incremental loading
- Streaming ingestion
- Delta Live Tables
- Machine Learning anomaly detection
- Fabric Data Pipelines orchestration
- CI/CD using GitHub Actions

---

## Repository Structure

```
notebooks/
│
├── 01_nb_DataGeneration.ipynb
├── 02_nb_dataCleaningSilver.ipynb
├── 03_nb_AnomalyDetectionGold.ipynb
├── 04_CreateTables.sql
├── 05_nb_loadToDelta.ipynb
│
README.md
```

---

## Author

Developed as a Microsoft Fabric Data Engineering portfolio project demonstrating:

- Medallion Architecture
- Delta Lake
- Data Engineering best practices
- Data Quality validation
- Rule-based security analytics
