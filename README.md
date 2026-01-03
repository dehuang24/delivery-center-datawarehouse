# Delivery Center Data Engineering Project 🚀

A comprehensive **Data Warehouse (DW)** and **Data Science Pipeline** project for a Food Delivery company.

This project simulates a real-world workflow, ranging from processing raw data (Raw CSV), cleaning (ETL), building a data warehouse (Galaxy Schema), to applying Machine Learning for clustering and revenue forecasting.

---

## 📚 Project Overview

### Objectives
1.  **Build a Data Warehouse:** Transform discrete transaction data (OLTP) into a centralized analytical model (OLAP) to serve BI reporting.
2.  **Automate ETL:** Use Python to automate the data cleaning, normalization, and loading processes.
3.  **Data Enrichment:** Utilize geospatial libraries to enrich the data with City/State information derived from GPS coordinates.
4.  **Advanced Analytics:** Apply K-Means Clustering for market segmentation and Facebook Prophet for demand forecasting.

### System Architecture
* **Data Source:** CSV files (Orders, Deliveries, Stores, etc.).
* **Staging Area (MySQL `delivery_food`):** Stores data after preliminary cleaning via Python.
* **Data Warehouse (MySQL `delivery_dw`):** Normalized data warehouse following the Galaxy Schema (Extended Star Schema) model.
* **Analytics:** Python (Scikit-learn, Prophet) connects directly to the DW to run models.

---

## 🛠 Tech Stack

| Area | Technology / Library |
| :--- | :--- |
| **Language** | Python 3.9+ |
| **Database** | MySQL 8.0 |
| **ETL & Data manipulation** | Pandas, SQLAlchemy, PyMySQL |
| **Geo-Spatial** | Geopy, Reverse-geocoder (Offline) |
| **Machine Learning** | Scikit-learn (K-Means), Facebook Prophet (Time-series) |
| **Visualization** | Matplotlib, Seaborn, Folium (Maps) |
| **Orchestration** | Apache Airflow (Optional/Planned) |

---

## 📂 Data Source

The project's raw data is sourced from a public Kaggle dataset: **Brazilian Delivery Center**.

👉 **Download Link:** [Brazilian Delivery Center on Kaggle](https://www.kaggle.com/datasets/nosbielcs/brazilian-delivery-center)

