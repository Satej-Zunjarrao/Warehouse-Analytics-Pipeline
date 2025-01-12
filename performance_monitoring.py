"""
performance_monitoring.py

This module monitors the performance of the ETL pipeline by logging execution times,
tracking errors, and generating summary reports for debugging and optimization.
Author: Satej
"""

import time  # For tracking execution time
import logging  # For logging performance metrics

# Configure logging
LOG_FILE = "/home/satej/logs/pipeline_performance.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def log_performance(task_name, execution_time):
    """
    Logs the performance of a specific task.

    Args:
        task_name (str): Name of the task (e.g., "Data Extraction").
        execution_time (float): Time taken to complete the task in seconds.
    """
    try:
        logging.info(f"Task: {task_name} | Execution Time: {execution_time:.2f} seconds")
        print(f"Logged performance for task: {task_name}")
    except Exception as e:
        print(f"Error logging performance for task '{task_name}': {e}")

def log_error(task_name, error_message):
    """
    Logs errors encountered during task execution.

    Args:
        task_name (str): Name of the task where the error occurred.
        error_message (str): Detailed error message.
    """
    try:
        logging.error(f"Task: {task_name} | Error: {error_message}")
        print(f"Logged error for task: {task_name}")
    except Exception as e:
        print(f"Error logging error for task '{task_name}': {e}")

def monitor_task(task_name, function, *args, **kwargs):
    """
    Wraps a task execution with performance monitoring and error logging.

    Args:
        task_name (str): Name of the task being monitored.
        function (callable): The function to execute.
        *args: Positional arguments for the function.
        **kwargs: Keyword arguments for the function.
    """
    start_time = time.time()
    try:
        # Execute the task
        function(*args, **kwargs)
        execution_time = time.time() - start_time
        log_performance(task_name, execution_time)
    except Exception as e:
        log_error(task_name, str(e))

if __name__ == "__main__":
    # Example usage of performance monitoring
    def sample_task():
        """Example task to simulate a workload."""
        time.sleep(2)  # Simulate task delay

    # Monitor the execution of a sample task
    monitor_task("Sample Task", sample_task)
