# Business Intelligence Development Stages For Customers of Perumda Dashboard

## 1. Database Source

The process begins with the **operational database** stored in **PostgreSQL**.

* Main tables: customers, transactions, water consumption, complaints, disconnections, and new customer registrations.
* This database serves as the **source system** for all subsequent ETL processes.

**Goal:** Ensure availability of operational data as input for the BI system.

---

## 2. ETL Pipeline

### a. Prototype in Jupyter Notebook

* **Extract:** Retrieve data from PostgreSQL using SQLAlchemy and pandas.
* **Transform:** Clean null values, normalize tables, and build fact and dimension structures.
* **Load:** Save transformed data into staging format (CSV/Parquet).

> Jupyter is used because it is flexible for exploration, validation, and documenting the ETL workflow.

### b. Implementation in VS Code (System ETL)

* Convert notebook logic into **modular Python scripts** (`extract.py`, `transform.py`, `load.py`).
* Run ETL pipelines automatically via cron jobs, schedulers, or workflow orchestrators (Airflow/Prefect).
* Data is periodically updated in the **data warehouse**.

**Goal:** Make ETL **automated, scalable, and production-ready**.

---

## 3. Datamart

After data is loaded into the data warehouse, a **datamart** is built.

* **Schema:** Snowflake schema with fact tables (transactions, complaints, consumption, registration) and dimension tables (customer, region, tariff, time, status).
* **SCD (Slowly Changing Dimension):** Maintains historical changes (e.g., customer status updates).

**Goal:** Provide a structured, consistent, and analysis-ready dataset.

---

## 4. Analytical Models

### Forecasting

* Algorithm: **XGBoost** with time-series features (lag, trend, seasonality).
* Evaluation: **MAE** and **RMSE**.
* Output: Forecast of water usage, revenue, or demand for upcoming periods.

---

## 5. Dashboard
This project integrates **ETL pipelines** with **Streamlit-based Business Intelligence dashboards**, providing descriptive, diagnostic, predictive, and prescriptive insights. Below is the explanation of each dashboard view:

### 1. Customer Dashboard

<img width="1414" height="934" alt="dashboard pelanggan" src="https://github.com/user-attachments/assets/716056f8-c821-4ecd-bb0d-5e52293c734f" />  

Displays key metrics related to customer management:

* Total number of active customers.
* Customer segmentation by type (residential, commercial, industrial).
* Trends in new customer registrations.
* Geographic distribution of customers across service areas.

**Purpose:** Helps monitor growth, retention, and customer distribution.

### 2. Water Consumption Dashboard

<img width="1414" height="752" alt="dashboard pemakaian air" src="https://github.com/user-attachments/assets/2813106c-bf4d-4746-9753-3242ce65cab4" />  

Highlights water usage patterns:

* Total consumption by region and customer segment.
* Monthly and yearly trends in water usage.
* Identification of peak consumption periods.
* Comparative analysis between forecasted and actual usage.

**Purpose:** Supports resource allocation and demand planning.

### 3. Revenue Dashboard

<img width="1414" height="943" alt="dashboard pendapatan" src="https://github.com/user-attachments/assets/63d84a9c-e2c0-4664-9131-96143d211c67" />  

Shows revenue performance from customer billing:

* Total revenue by year, month, and customer category.
* Outstanding bills and payment completion rates.
* Contribution of each region to overall revenue.
* Comparison between expected vs actual revenue collection.

**Purpose:** Ensures financial transparency and helps identify revenue gaps.

### 4. Complaints Dashboard

<img width="1414" height="861" alt="dashboard pengaduan" src="https://github.com/user-attachments/assets/e04a9d5d-26e5-4a4b-a669-b04a494f419d" />  

Monitors customer service and complaint management:

* Number of complaints received over time.
* Complaint categories (e.g., water quality, service interruptions, billing issues).
* Response time and resolution performance.
* Regional distribution of complaints.

**Purpose:** Improves customer satisfaction by identifying recurring issues and service bottlenecks.

### 5. Disconnection Dashboard

<img width="1414" height="943" alt="dashboard pemutusan" src="https://github.com/user-attachments/assets/c1ea262d-d57d-45de-ac1a-de72d7a1f2ea" />  

Focuses on service disconnections due to non-payment or other reasons:

* Number of customers disconnected monthly.
* Trends of reconnections after bill settlement.
* Regional breakdown of disconnection cases.
* Comparison between disconnected vs total customers.

**Purpose:** Tracks customer payment behavior and supports policy enforcement.

### 6. New Connections Dashboard

<img width="1414" height="961" alt="dashboard sbbaru" src="https://github.com/user-attachments/assets/add8c429-8548-4789-b7b7-8d53a96f272d" />  

Provides insights into newly installed water connections:

* Growth in new installations by month and year.
* Distribution by customer segment and region.
* Correlation between new connections and overall revenue growth.

**Purpose:** Supports expansion strategy and service penetration monitoring.

### 7. Forecasting Dashboard

<img width="1414" height="731" alt="dashboard forecasting" src="https://github.com/user-attachments/assets/bcef7f2c-d0c5-4c76-8feb-e0732076ea41" />  

Presents predictive analytics using time-series forecasting:

* Predicted water consumption for upcoming periods.
* Revenue forecasting trends.
* Confidence intervals to measure forecast uncertainty.
* Comparison of actual vs forecasted results.

**Purpose:** Enables proactive decision-making for supply planning and financial forecasting.

### 8. ETL Process Dashboard

<img width="1414" height="746" alt="proses etl" src="https://github.com/user-attachments/assets/4b9b1557-0f68-4f93-bedd-926d80675a93" />  

Visualizes the **Extract-Transform-Load (ETL)** workflow:

* Data sources (customer, billing, consumption, complaints).
* Transformation steps (cleaning, normalization, validation).
* Loading into a PostgreSQL data mart.

**Purpose:** Ensures data consistency and transparency in the data pipeline.

### 9. ETL Process History

<img width="1414" height="746" alt="riwayat proses etl" src="https://github.com/user-attachments/assets/a9d7566c-1765-422f-aa84-3623198c2ca8" />  

Tracks the execution history of ETL jobs:

* Timestamp of each ETL run.
* Success or failure logs.
* Data volume processed in each cycle.
* Error handling and recovery records.

**Purpose:** Provides auditability and reliability for data management processes.
