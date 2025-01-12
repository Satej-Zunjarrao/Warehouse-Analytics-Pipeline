# Warehouse Analytics Pipeline

Streamlining data collection, transformation, and visualization to optimize warehouse operations.

---

## Overview

The **Warehouse Analytics Pipeline** is a Python-based solution designed to enhance warehouse efficiency by collecting, processing, and visualizing operational data. The pipeline leverages advanced ETL workflows, real-time data processing, and data visualization techniques to provide actionable insights into key metrics like inventory levels, order processing times, and delivery schedules.

This project includes a modular pipeline for data integration, cleaning, KPI analysis, dashboard preparation, and automation.

---

## Key Features

- **Data Collection**: Integrates data from SQL databases, IoT devices, and flat files.
- **Data Cleaning**: Preprocesses and standardizes raw data using Pandas.
- **KPI Analysis**: Calculates metrics like inventory turnover, order accuracy, and storage utilization.
- **Visualization**: Prepares data for Tableau and Power BI dashboards.
- **Alerts**: Automates real-time notifications for predefined thresholds.
- **Automation**: Orchestrates the pipeline with scheduled runs for near real-time updates.

---

## Directory Structure

```plaintext
project/
│
├── data_extraction.py          # Handles data extraction from SQL, IoT, and flat files
├── data_transformation.py      # Cleans and preprocesses data
├── data_loading.py             # Loads data into Snowflake data warehouse
├── kpi_analysis.py             # Calculates key performance indicators
├── dashboard_setup.py          # Prepares data for visualization tools
├── alerts_automation.py        # Implements real-time notification system
├── pipeline_scheduler.py       # Orchestrates the ETL pipeline with scheduling
├── performance_monitoring.py   # Logs performance metrics and execution times
├── README.md                   # Project documentation
```

## Modules

### 1. data_extraction.py
- Extracts data from SQL Server, Oracle databases, IoT devices, and flat files.
- Outputs raw data in Pandas DataFrame format for further processing.

### 2. data_transformation.py
- Preprocesses raw data by handling missing values, normalizing formats, and aggregating metrics.
- Outputs cleaned and transformed data for analysis.

### 3. data_loading.py
- Loads the processed data into Snowflake.
- Ensures optimized query performance with appropriate schema definitions.

### 4. kpi_analysis.py
- Calculates critical KPIs such as inventory turnover, order accuracy, and storage utilization.
- Outputs actionable metrics for operational insights.

### 5. dashboard_setup.py
- Prepares and structures data for visualization in Tableau and Power BI.
- Exports dashboard-ready data as a CSV file.

### 6. alerts_automation.py
- Monitors metrics and triggers alerts for predefined thresholds.
- Sends real-time email notifications for low inventory or high utilization.

### 7. pipeline_scheduler.py
- Schedules and automates the ETL pipeline tasks.
- Ensures consistent and timely updates for real-time analytics.

### 8. performance_monitoring.py
- Logs execution times and tracks errors for debugging and optimization.
- Provides performance reports for pipeline monitoring.

---

## Contact

For queries or collaboration, feel free to reach out:

- **Name**: Satej Zunjarrao  
- **Email**: zsatej1028@gmail.com  

