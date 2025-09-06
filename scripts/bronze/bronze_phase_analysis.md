# Bronze Phase Analysis: Key Questions

This document serves as a reference template for analyzing the **Bronze layer** of a data warehouse. It combines important questions around **data ownership, architecture, integration, and ETL considerations**.

---

## 1. Data Ownership
- Who owns the data in each source system?  
- Who is responsible for data accuracy, completeness, and security?  
- Are there any restrictions on using this data?

---

## 2. Supported Business Processes
- Which business processes does this data support?  
- How critical is the raw data for downstream reporting and analytics?  
- Are there any operational dependencies on this data?

---

## 3. System & Data Documentation
- What are the source systems providing this data?  
- How is the data extracted (e.g., CSV, API, database connection)?  
- Are there existing system or data dictionaries available?  
- How frequently is the data updated?

---

## 4. Data Model & Data Catalog
- What are the raw tables or entities in the source systems?  
- What fields and data types exist in each table?  
- Are there primary keys or unique identifiers?  
- Is there a data catalog available to track metadata and lineage?

---

## 5. Architecture & Technology Stack
- Where is the data stored? (SQL Server, Oracle, MySQL, PostgreSQL, AWS, Azure, on-premises, hybrid)  
- How is raw, staged, and transformed data organized?  
- What are the storage limitations, retention policies, and backup mechanisms?  
- How is data ingested from source systems? (API, Kafka, File Extract, Direct DB, Streaming)  
- Are there batch or real-time integration requirements?  
- How are data transformations and quality checks handled during ingestion?

---

## 6. Extract & Load Considerations
- Will the data be loaded as **full load** or **incremental load**?  
- How are changed records identified in the source system?  
- What time period does the extract cover? (latest dataset vs full history)  
- Are historical records required in the warehouse?  
- What is the expected size of each extract?  
- Are there any limitations on data volume?  
- How can extraction be scheduled to avoid impacting source system performance?  
- How will data access be secured? (API tokens, SSH keys, VPN, IP whitelisting, database credentials)  
- Are there audit or compliance requirements for access?  
- How are credentials managed and rotated?

---

## Notes
- These questions form a **comprehensive checklist** for the Bronze phase of a data warehouse.  
- Clear answers help guide ETL design, schema creation, and integration planning.
