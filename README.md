# IAM Security Analytics Pipeline using Microsoft Fabric

An end-to-end Identity & Access Management (IAM) Security Analytics solution built on **Microsoft Fabric** using the **Medallion Architecture (Bronze, Silver, Gold)**. The project generates synthetic IAM data, performs data engineering transformations, detects security anomalies using rule-based logic, stores curated datasets in Delta Lake, and visualizes security insights through an interactive **Power BI dashboard**.

---

# Project Overview

This project simulates an enterprise IAM environment by generating realistic user, authentication, and access request data. The pipeline applies data quality validations and security rules to identify suspicious activities before publishing curated datasets for reporting.

The project demonstrates the complete analytics lifecycle:

* Synthetic data generation
* Bronze, Silver, and Gold Medallion Architecture
* Data quality validation
* Rule-based anomaly detection
* Delta Lake storage
* Interactive Power BI reporting
* Git version control


# Architecture

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
              Power BI Dashboard

# Medallion Architecture

## Bronze Layer

Purpose:

* Generate synthetic IAM datasets
* Store raw data
* Capture ingestion metadata

Datasets generated:

* Users
* Login Events
* Access Requests

Features:

* Raw Delta tables
* Ingestion timestamp
* Pipeline logging
* Data quality validation
* Pipeline execution summary

Outputs:

* bronze_users
* bronze_login_events
* bronze_access_requests

## Silver Layer

Purpose:

Clean, standardize and enrich the raw datasets.

Transformations:

* Remove duplicate records
* Standardize department names
* Normalize account status values
* Convert date columns
* Calculate password age
* Generate password stale flag
* Extract login hour
* Extract login date
* Extract day of week

Data Quality Checks:

* Duplicate User IDs
* Duplicate Event IDs
* Duplicate Request IDs
* Null primary keys
* Valid account status values
* Valid login status values
* Valid request status values
* Row count validation

Outputs:

* silver_users
* silver_login_events
* silver_access_requests

## Gold Layer

Purpose:

Identify suspicious security events using rule-based anomaly detection.

Implemented Security Rules:

* Suspended account successfully logged in
* Login from high-risk country
* Privileged user login after business hours
* Multiple failed logins (possible brute-force attack)
* Privileged user using a personal device
* Active account with stale password

Output:

* gold_anomalies

# Final Curated Delta Tables

The Gold datasets are loaded into reporting tables for analytics.

Tables:

* users
* login_events
* access_requests
* anomalies

These tables serve as the reporting layer for Power BI.

# Power BI Dashboard

An interactive security analytics dashboard was built on top of the curated Delta tables.

### Page 1 – Executive Overview

* Total Users
* Total Login Events
* Total Anomalies
* Total Access Requests
* Users by Department
* Account Status Distribution
* Privileged vs Standard Users



### Page 2 – Login Activity

* Login Trends
* Successful vs Failed Logins
* Login Distribution by Country
* Device Usage
* Business Hours vs After Hours Logins

### Page 3 – Security Anomalies

* Total Anomalies
* Critical / High / Medium Risk KPIs
* Anomalies by Risk Level
* Anomaly Types
* Risk Distribution
* Detailed Anomaly Table

https://github.com/SandyaDuggana06/IAM-Security-Analytics-Pipeline/blob/main/AccessRiskAnalyticsPowerBIReport_SecurityAnomalyDashboard.png

### Page 4 – Access Management

* Total Requests
* Approval Status Distribution
* Requested Roles
* Access Request Trends
* Detailed Access Request Table

# Technologies Used

* Microsoft Fabric
* OneLake
* Lakehouse
* Delta Lake
* PySpark
* SQL
* Power BI
* Python
* Pandas
* NumPy
* Faker
* Python Logging
* Git & GitHub


# Pipeline Components

| Notebook                   | Purpose                                              |
| -------------------------- | ---------------------------------------------------- |
| 01_nb_DataGeneration       | Generate synthetic IAM datasets and Bronze ingestion |
| 02_nb_DataCleaningSilver   | Data cleaning, validation and enrichment             |
| 03_nb_AnomalyDetectionGold | Rule-based anomaly detection                         |
| 04_CreateTables            | Create Delta tables                                  |
| 05_nb_LoadToDelta          | Load curated datasets into reporting tables          |


# Logging & Observability

Each notebook includes:

* Pipeline start/end logging
* Progress tracking
* Data quality validation
* Execution summary
* Row count verification
* Error handling

# Data Quality Checks

Implemented validations include:

* Duplicate detection
* Null value validation
* Domain validation
* Row count verification
* Table verification

# Sample Datasets

### Users

* User ID
* Username
* Department
* Role
* Account Status
* Password Information

### Login Events

* Login Time
* Country
* Device Type
* Login Status
* Weekend Flag
* After-hours Flag

### Access Requests

* Requested Role
* Approval Status
* Request Date

### Anomalies

* User ID
* Anomaly Type
* Risk Level
* Detection Timestamp

---

# Repository Structure

notebooks/
│
├── 01_nb_DataGeneration.ipynb
├── 02_nb_DataCleaningSilver.ipynb
├── 03_nb_AnomalyDetectionGold.ipynb
├── 04_CreateTables.sql
├── 05_nb_LoadToDelta.ipynb
│
powerbi/
│
├── IAM_Security_Analytics.pbix
│
images/
│
├── architecture.png
├── dashboard_overview.png
├── login_activity.png
├── anomalies_dashboard.png
├── access_management.png
│
README.md
```

---

# Future Improvements

Potential enhancements include:

* Incremental loading
* Streaming ingestion
* Delta Live Tables
* Microsoft Fabric Data Pipelines
* Machine Learning-based anomaly detection
* Real-time security monitoring
* CI/CD using GitHub Actions
* Automated alerting using Power Automate

# Key Skills Demonstrated

* Microsoft Fabric
* Medallion Architecture
* Lakehouse Design
* Delta Lake
* Data Engineering
* Data Quality Validation
* PySpark
* SQL
* Rule-based Security Analytics
* Power BI Dashboard Development
* Git Version Control

# Author

Developed as a Microsoft Fabric Data Engineering portfolio project demonstrating end-to-end data engineering, security analytics, and business intelligence using Microsoft Fabric and Power BI.

