"""
pipeline_scheduler.py

This module orchestrates the ETL pipeline by scheduling and executing tasks for data extraction,
transformation, loading, and monitoring. It ensures that the pipeline runs at defined intervals.
Author: Satej
"""

import schedule  # For scheduling tasks
import time  # For managing sleep intervals
import os  # For triggering scripts as subprocesses

# Paths to the other pipeline scripts
DATA_EXTRACTION_SCRIPT = "/home/satej/pipeline/data_extraction.py"
DATA_TRANSFORMATION_SCRIPT = "/home/satej/pipeline/data_transformation.py"
DATA_LOADING_SCRIPT = "/home/satej/pipeline/data_loading.py"
KPI_ANALYSIS_SCRIPT = "/home/satej/pipeline/kpi_analysis.py"

def run_data_extraction():
    """
    Executes the data extraction script.
    """
    try:
        os.system(f"python {DATA_EXTRACTION_SCRIPT}")
        print("Data extraction task completed.")
    except Exception as e:
        print(f"Error during data extraction task: {e}")

def run_data_transformation():
    """
    Executes the data transformation script.
    """
    try:
        os.system(f"python {DATA_TRANSFORMATION_SCRIPT}")
        print("Data transformation task completed.")
    except Exception as e:
        print(f"Error during data transformation task: {e}")

def run_data_loading():
    """
    Executes the data loading script.
    """
    try:
        os.system(f"python {DATA_LOADING_SCRIPT}")
        print("Data loading task completed.")
    except Exception as e:
        print(f"Error during data loading task: {e}")

def run_kpi_analysis():
    """
    Executes the KPI analysis script.
    """
    try:
        os.system(f"python {KPI_ANALYSIS_SCRIPT}")
        print("KPI analysis task completed.")
    except Exception as e:
        print(f"Error during KPI analysis task: {e}")

def schedule_pipeline():
    """
    Schedules the ETL pipeline to run at defined intervals.
    """
    # Define the schedule (e.g., every day at 2:00 AM)
    schedule.every().day.at("02:00").do(run_data_extraction)
    schedule.every().day.at("02:30").do(run_data_transformation)
    schedule.every().day.at("03:00").do(run_data_loading)
    schedule.every().day.at("03:30").do(run_kpi_analysis)

    print("Pipeline scheduler initialized. Waiting for tasks to execute...")
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    schedule_pipeline()
