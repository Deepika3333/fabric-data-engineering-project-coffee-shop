

# ☕ Microsoft Fabric Data Engineering Project – Coffee Shop Analytics

## 🔍 Project Overview

This project demonstrates an **end-to-end data engineering pipeline** built using **Microsoft Fabric**, following the **Medallion Architecture (Landing → Bronze → Silver → Gold)** to support **business decision-making** through **Power BI analytics**.

The solution processes raw coffee shop transaction data and delivers **clean, analytics-ready fact and dimension tables** optimized for reporting and insights.

---

## 🧱 Architecture

**Landing → Bronze → Silver → Gold**

* **Landing**: Raw CSV ingestion (audit-safe, no transformations)
* **Bronze**: Delta ingestion with schema enforcement and month-based partitioning
* **Silver**: Data quality checks, cleaning, standardization, and business transformations
* **Gold**: Star schema (fact and dimension tables) optimized for Power BI

---

## 🛠️ Tech Stack

* Microsoft Fabric
* OneLake (Lakehouse)
* PySpark
* Delta Lake
* Power BI
* Medallion Architecture

---

## 📊 Business Objectives Solved

* Sales performance by store location
* Top-selling products and categories
* Peak transaction hours and staffing optimization
* Average Transaction Value (ATV) analysis
* Weekday vs weekend sales trends
* Underperforming product detection
* Pricing consistency checks across stores
* Payment method usage analysis

---

## 🧪 Data Quality & Engineering Highlights

* ✔ Month-based incremental processing
* ✔ Rerun-safe pipelines using `replaceWhere`
* ✔ Composite-key duplicate handling
* ✔ Strong data lineage (`file_name`, `date_processed`)
* ✔ Spark capacity throttling handled using **Wait Activities**
* ✔ Power BI Direct Lake issues diagnosed and resolved

---

## 🗂️ Gold Layer – Star Schema

### Fact Table

* `fact_sales`

### Dimension Tables

* `dim_store`
* `dim_product`
* `dim_date`
* `dim_time`
* `dim_payment_method`

---

## 📈 Power BI Insights

* Astoria and Hell’s Kitchen outperform other locations
* Beverages generate the highest revenue
* Morning and afternoon are peak transaction periods
* Several products consistently underperform
* Upselling opportunities identified through ATV analysis
