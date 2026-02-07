Microsoft Fabric Data Engineering Project – Coffee Shop Analytics
🔍 Project Overview

This project demonstrates an end-to-end data engineering pipeline built using Microsoft Fabric following the Medallion Architecture (Landing → Bronze → Silver → Gold) to support business decision-making through Power BI analytics.

The solution processes raw coffee shop transaction data and delivers clean, analytics-ready fact and dimension tables optimized for reporting.

🧱 Architecture

Landing → Bronze → Silver → Gold

Landing: Raw CSV ingestion (audit-safe, no transformations)

Bronze: Delta ingestion with schema enforcement & month-based partitioning

Silver: Data quality checks, cleaning, standardization & business transformations

Gold: Star schema (fact & dimension tables) optimized for Power BI

🛠️ Tech Stack

Microsoft Fabric

OneLake (Lakehouse)

PySpark

Delta Lake

Power BI

Medallion Architecture

📊 Business Objectives Solved

Sales performance by store location

Top-selling products & categories

Peak transaction hours & staffing optimization

Average transaction value (ATV)

Weekday vs weekend trends

Underperforming product detection

Pricing consistency checks

Payment method analysis

🧪 Data Quality & Engineering Highlights

✔ Month-based incremental processing
✔ Rerun-safe pipelines using replaceWhere
✔ Composite-key duplicate handling
✔ Strong data lineage (file_name, date_processed)
✔ Spark capacity throttling handled with Wait Activities
✔ Direct Lake Power BI issue diagnosed & resolved

🗂️ Gold Layer – Star Schema

Fact Table

fact_sales

Dimensions

dim_store

dim_product

dim_date

dim_time

dim_payment_method

📈 Power BI Insights

Astoria & Hell’s Kitchen outperform other locations

Beverages drive the highest revenue

Morning & afternoon are peak hours

Several products consistently underperform

Upselling opportunities identified via ATV analysis

📄 Full Technical Documentation

📘 Detailed implementation (code, screenshots, pipeline design):
➡️ docs/Project_08_Fabric_Data_Engineering.pdf

👩‍💻 Author

Deepika Mandapalli
Azure Data Engineer | Microsoft Fabric | Delta Lake | Power BI
