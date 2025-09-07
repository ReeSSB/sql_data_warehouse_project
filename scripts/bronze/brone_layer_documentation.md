# Bronze Layer Documentation

## Overview

The **Bronze Layer** is the *raw data ingestion zone* in the data
warehouse. It acts as the foundation of the entire pipeline, where data
from different source systems is stored **as-is**, with minimal or no
transformation.

The purpose of this layer is to create a **single, centralized landing
zone** for all incoming data so that downstream processes (Silver & Gold
layers) can clean, enrich, and transform it.

------------------------------------------------------------------------

## Key Principles

1.  **Raw Storage**
    -   Data is ingested directly from source systems (CRM, ERP, APIs,
        flat files, etc.) into the bronze schema.\
    -   No business logic or transformations are applied here.
2.  **Traceability**
    -   Preserves the original form of the data.\
    -   Helps in debugging, auditing, and replaying ETL/ELT processes.
3.  **Separation of Concerns**
    -   Bronze = Raw landing zone\
    -   Silver = Cleansed/standardized data\
    -   Gold = Curated data for analytics & reporting

------------------------------------------------------------------------

## Source Systems in This Project

-   **CRM (Customer Relationship Management)**
    -   `crm_cust_info`: Customer profile details.\
    -   `crm_prd_info`: Product master data.\
    -   `crm_sales_details`: Sales transactions.
-   **ERP (Enterprise Resource Planning)**
    -   `erp_loc_a101`: Location and country reference data.\
    -   `erp_cust_az12`: Customer demographic attributes (e.g.,
        birthdate, gender).\
    -   `erp_px_cat_g1v2`: Product category and maintenance
        classification.

------------------------------------------------------------------------

## Table Design Notes

-   Tables mirror source system structures.\
-   Data types are aligned with source definitions.\
-   No business rules, aggregations, or cleansing applied.\
-   Tables may include **batch metadata** (e.g., load time, batch ID) if
    required for operational tracking.

------------------------------------------------------------------------

## Responsibilities at Bronze Layer

-   **Data Ingestion:** Capture full or incremental loads from source
    systems.\
-   **Data Storage:** Keep historical snapshots if needed.\
-   **Auditability:** Enable root cause analysis by preserving unaltered
    records.

------------------------------------------------------------------------

⚠️ **Important:**\
- Do not apply transformations in the bronze layer.\
- Keep schema aligned with the source system to maintain integrity.\
- Handle sensitive data according to governance and compliance policies.

------------------------------------------------------------------------

👉 Next steps: The **Silver Layer** will build on this raw data by
cleaning, standardizing, and resolving duplicates/keys.
