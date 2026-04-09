# Inferyx

A data engineering and ML deployment project built on top of the **Inferyx platform** (v6.4.9), designed for financial services use cases. The repository contains Iceberg table DDL scripts for a multi-domain data lakehouse (LMS, CMS, LOS), a personality-based ML dataset, and a Jupyter deployment notebook for running AML predictions using a Gradient Boosted Tree model.

---

## What's in this repo

| File | Description |
|------|-------------|
| `LMS_Bronze_Data_Dictionary_Create_Table_Statements.sql` | Iceberg DDL for raw (Bronze) LMS tables — account balances, holders, profiles, customers, loans, repayments, disbursements, write-offs, etc. |
| `LMS_Silver_Data_Dictionary_Create_Table_Statements.sql` | Cleaned/transformed Silver-layer LMS tables for analytics consumption |
| `CMS_Silver_Data_Dictionary_Create_Table_Statements.sql` | Silver-layer CMS tables — account master, TP mapping, action history, customer info |
| `LOS_Silver_Data_Dictionary_Create_Table_Statements.sql` | Silver-layer LOS (Loan Origination System) tables |
| `Deployment.ipynb` | Jupyter notebook for deploying/running the AML prediction pipeline on the Inferyx platform |
| `predict.txt` | Inferyx `predict` metadata object (JSON) — defines an AML Suspicious Transaction Activity model using GBT, including feature-attribute mapping, dataset/model UUIDs, and platform config |
| `dt.json` | Decision tree or deployment config JSON |
| `personality_datasert.csv` | Personality dataset used for ML model training/testing |
| `test.csv` | Test dataset for model evaluation |

---

## Domain Overview

### Data Lakehouse — Bronze / Silver Layers

The SQL scripts follow a **medallion architecture** (`jfl_catalog.jfl_bronze` / `jfl_catalog.jfl_silver`) using **Apache Iceberg** as the table format. Three source systems are modelled:

- **LMS (Loan Management System)** — loan accounts, disbursements, demands, repayments, fees, write-offs, products, customers, branches, working registers
- **CMS (Collections Management System)** — account master, TP (third-party) mapping, action history archive, customer information
- **LOS (Loan Origination System)** — origination pipeline tables

All tables include standard audit and ingestion columns: `business_date`, `load_id`, `load_date`, `operation_type`, `timestamp`.

### ML / AML Use Case

The `predict.txt` file is an Inferyx platform export of a **Predict** object with the following properties:

- **Model:** AML — Suspicious Transaction Activity (Gradient Boosted Tree)
- **Platform version:** Inferyx 6.4.9
- **Input:** Dataset UUID `dc265559-bc3a-4b8b-9187-26478048d2b6`
- **Model UUID:** `f5597dc7-963a-4376-b945-33ea51a0c6d1`
- **Feature mapping:** 23 features mapped from dataset attributes to model inputs
- **Output:** File target (prediction results written to file)

The `Deployment.ipynb` notebook handles execution of this predict pipeline.

---

## Tech Stack

- **Apache Iceberg** — open table format for all data lake tables
- **Inferyx Platform** (v6.4.9) — ML lifecycle management (model training, predict objects, deployment)
- **Gradient Boosted Trees (GBT)** — algorithm used for AML suspicious transaction classification
- **Python / Jupyter Notebook** — deployment and orchestration
- **SQL** — table DDL and schema definitions

---

## Getting Started

### 1. Set up Iceberg tables

Run the SQL scripts against your catalog (replace `jfl_catalog` with your own if needed):
````sql
-- Bronze LMS tables
source LMS_Bronze_Data_Dictionary_Create_Table_Statements.sql

-- Silver LMS tables
source LMS_Silver_Data_Dictionary_Create_Table_Statements.sql

-- Silver CMS tables
source CMS_Silver_Data_Dictionary_Create_Table_Statements.sql

-- Silver LOS tables
source LOS_Silver_Data_Dictionary_Create_Table_Statements.sql
````

### 2. Import the Predict object into Inferyx

Import `predict.txt` into the Inferyx platform as a Predict metadata object and verify the dataset and model UUIDs resolve correctly in your environment.

### 3. Run the deployment notebook

Open `Deployment.ipynb` in Jupyter and execute cells to trigger the AML prediction pipeline.

---

## Data Model Highlights

**LMS Bronze — key tables**

- `stg_lms_account_balances` — account-level balance tracking with reconciliation fields
- `stg_lms_customers` — full customer KYC including identity docs, geo-location, income, nominee
- `stg_lms_loan_od_account_view` — consolidated loan view with DPD, interest rates, disbursements
- `stg_lms_loan_od_demands` — installment-level demand schedule
- `stg_lms_loan_od_repayments` — repayment transactions with interest/principal/penal breakdown
- `stg_lms_loan_od_write_offs` — write-off records with colender splits

**CMS Silver — key tables**

- `cms_account_master` — full loan account snapshot with collection and overdue fields
- `cms_account_master_tp_mapping` — third-party collector assignment and contact tracking
- `cms_action_history_arch` — historical collection action log
- `cms_customer_information` — customer contact and identity details for collections

---

## Notes

- All Iceberg tables use `USING ICEBERG` clause and are partitioned logically by `business_date`.
- The `predict.txt` object references UUIDs specific to the source Inferyx environment — update them before importing into a different tenant.
- `personality_datasert.csv` appears to be used for a separate classification model unrelated to AML.

---

## Author

[parthrudrawar](https://github.com/parthrudrawar)
