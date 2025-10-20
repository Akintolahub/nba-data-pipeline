# Nba Data Pipeline
Building a modern data pipeline, including ETL processes, data modelling and analytics. 

Welcome to the **Data Pipeline and Analytics Project** repository! 🚀
This project demonstrates a comprehensive data warehousing and analytics solution, from building a data warehouse to generating actionable insights. Designed as a portfolio project, it highlights industry best practices in data engineering and analytics.

---
## 🏗️ Data Architecture

The data architecture for this project follows Medallion Architecture **Landing**, **Staging**, and **Reporting** layers:
[Data Architecture](https://github.com/Akintolahub/nba-data-pipeline/blob/4449465c7c65469a47ce0e9cff1794adde2b0009/docs/High%20Level%20Architechture.png)

1. **Data Lake**: Stores data from source system. Data is transformed from pandas dataframes to CSV Files and loaded into Google Cloud Storage Buckets.
2. **Landing Layer**: Stores raw data from the source systems. Data is ingested from CSV Files into Google BigQuery.
3. **Staging Layer**: This layer includes a heavy transfromation processes to prepare data for analysis.
4. **Reporting Layer**: Houses user-ready data modeled into a star schema required for reporting and analytics.

---

## 🚀 Project Requirements

### Building the Data Warehouse (Data Engineering)

#### Objective
Develop a modern data warehouse using SQL Server to consolidate NBA data, enabling analytical reporting and informed decision-making.

#### Specifications
- **Data Source**: Import data from a source system (NBA_API) provided as Dataframes.
- **Data Quality**: Cleanse and resolve data quality issues prior to analysis.
- **Integration**: Combine both sources into a single, user-friendly data model designed for analytical queries.
- **Scope**: Focus on the latest dataset only; historization of data is not required.
- **Documentation**: Provide clear documentation of the data model to support analytics teams.

---

### BI: Analytics & Reporting (Data Analysis)

#### Objective
Develop SQL-based analytics to deliver detailed insights into:

- **Daily top NBA perfomers**
- **Top NBA perfomers over their last 5 games**
- **Top fantasy performimg outliers**

These insights empower me with key sport metrics, enabling strategic decision-making in my fantasy team.

---

## 🛡️ License

This project is licensed under the [MIT License](LICENSE). You are free to use, modify, and share this project with proper attribution.

## 🌟 About Me
Hi there! I'm **Oluwatofunmi Akintola**. I’m an Data professional and passionatate and aspiring Data Engineer, looking to share some personal projects I have executed.
