# SQL Data Warehouse Project

Building a modern data warehouse with **MS SQL Server**, including **ETL processes**, **data modeling**, and **analytics**.

This project showcases an **end-to-end data warehousing and analytics solution**, covering everything from warehouse development to deriving actionable insights. Built as a **portfolio piece**, it emphasizes **industry-standard best practices** in data engineering and analytics.

---

## 📊 Data Architecture

<img width="1135" height="749" alt="image" src="https://github.com/user-attachments/assets/c5dd97a3-3702-4705-8880-e9b91d90f7dd" />

The project adopts the **Medallion Architecture** with three layers:

1. **Bronze Layer**: Stores raw source data ingested from CSV files into a SQL Server database.  
2. **Silver Layer**: Performs data cleansing, standardization, and normalization to prepare data for analysis.  
3. **Gold Layer**: Contains business-ready, analytics-optimized data organized in a star schema for reporting and insights.  

---

## 📌 Project Overview

This project demonstrates the end-to-end development of a **modern data warehouse** using the Medallion Architecture (Bronze, Silver, Gold layers).

### Key Components:
- **Data Architecture**: Designing a scalable data warehouse with structured layers.  
- **ETL Pipelines**: Building processes to extract, transform, and load data from source systems.  
- **Data Modeling**: Creating fact and dimension tables optimized for analytical queries.  
- **Analytics & Reporting**: Delivering SQL-based reports and dashboards that generate actionable insights.  

🎯 **Who Can Benefit from This Repository?**
- SQL Developers  
- Data Architects  
- Data Engineers  
- ETL Pipeline Developers  
- Data Modelers  
- Data Analysts  

---

## 🛠️ Project Requirements

### Data Engineering – Building the Data Warehouse
**Objective:**  
Design and implement a modern data warehouse in **SQL Server** to consolidate sales data for analytical reporting and informed decision-making.

**Specifications:**
- **Data Sources**: Import sales data from two source systems (ERP and CRM) provided as CSV files.  
- **Data Quality**: Cleanse, standardize, and resolve quality issues prior to analysis.  
- **Integration**: Merge both sources into a single, user-friendly data model optimized for analytical queries.  
- **Scope**: Focus on the most recent dataset only (no historization required).  
- **Documentation**: Provide clear documentation of the data model to assist business stakeholders and analytics teams.  

### Data Analysis – BI, Analytics & Reporting
**Objective:**  
Develop SQL-based analytics and reporting solutions to deliver actionable insights into:
- **Customer Behavior**  
- **Product Performance**  
- **Sales Trends**  

These insights provide stakeholders with **key business metrics**, supporting **data-driven strategic decisions**.

📄 For more details, see **docs/requirements.md**.

---

## 📂 Project Structure

```
data-warehouse-project/
│
├── datasets/                           # Raw datasets used for the project (ERP and CRM data)
│
├── docs/                               # Project documentation and architecture details
│   ├── etl.drawio                      # ETL process diagram
│   ├── data_architecture.drawio        # Project architecture diagram
│   ├── data_catalog.md                 # Dataset catalog with field descriptions and metadata
│   ├── data_flow.drawio                # Data flow diagram
│   ├── data_models.drawio              # Data models (star schema)
│   ├── naming-conventions.md           # Naming guidelines for tables, columns, and files
│
├── scripts/                            # SQL scripts for ETL and transformations
│   ├── bronze/                         # Scripts for extracting and loading raw data
│   ├── silver/                         # Scripts for cleaning and transforming data
│   ├── gold/                           # Scripts for creating analytical models
│
├── tests/                              # Test scripts and quality checks
│
├── README.md                           # Project overview and instructions
├── LICENSE                             # License information for the repository
├── .gitignore                          # Files and directories to ignore in Git
└── requirements.txt                    # Project dependencies
```

---

## 📜 License

This project is licensed under the **MIT License**. You are free to use, modify, and distribute this project, provided that proper attribution is given.
