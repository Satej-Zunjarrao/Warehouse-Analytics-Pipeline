"""
data_extraction.py

This module handles data extraction from various sources, including SQL databases, IoT devices,
and flat files. It is the first step in the ETL pipeline for the warehouse analytics project.
Author: Satej
"""

import pyodbc  # For database connection
import pandas as pd  # For handling dataframes
import os  # For handling file paths

# Define constants for database connection
SQL_SERVER_CONNECTION_STRING = "Driver={SQL Server};Server=SATEJ-SQL-SERVER;Database=WarehouseDB;UID=your_username;PWD=your_password"
ORACLE_CONNECTION_STRING = "Driver={Oracle};DBQ=//SATEJ-ORACLE-SERVER:1521/ORCLPDB;Uid=your_username;Pwd=your_password"
IOT_DATA_PATH = "/home/satej/data/iot_data/"
FLAT_FILE_PATH = "/home/satej/data/flat_files/"

def extract_from_sql_server():
    """
    Extract data from SQL Server database.

    Returns:
        pd.DataFrame: Extracted data in a Pandas DataFrame format.
    """
    try:
        # Establish connection
        conn = pyodbc.connect(SQL_SERVER_CONNECTION_STRING)
        query = "SELECT * FROM Inventory"  # Example query
        df = pd.read_sql(query, conn)
        conn.close()
        print("Data successfully extracted from SQL Server.")
        return df
    except Exception as e:
        print(f"Error while extracting data from SQL Server: {e}")
        return pd.DataFrame()

def extract_from_oracle():
    """
    Extract data from Oracle database.

    Returns:
        pd.DataFrame: Extracted data in a Pandas DataFrame format.
    """
    try:
        # Establish connection
        conn = pyodbc.connect(ORACLE_CONNECTION_STRING)
        query = "SELECT * FROM Orders"  # Example query
        df = pd.read_sql(query, conn)
        conn.close()
        print("Data successfully extracted from Oracle database.")
        return df
    except Exception as e:
        print(f"Error while extracting data from Oracle: {e}")
        return pd.DataFrame()

def extract_from_iot():
    """
    Extract data from IoT-enabled devices stored in CSV files.

    Returns:
        pd.DataFrame: Combined data from all IoT device files.
    """
    try:
        # Load all CSV files in the IoT data directory
        files = [os.path.join(IOT_DATA_PATH, f) for f in os.listdir(IOT_DATA_PATH) if f.endswith('.csv')]
        dataframes = [pd.read_csv(file) for file in files]
        combined_df = pd.concat(dataframes, ignore_index=True)
        print("Data successfully extracted from IoT devices.")
        return combined_df
    except Exception as e:
        print(f"Error while extracting data from IoT devices: {e}")
        return pd.DataFrame()

def extract_from_flat_files():
    """
    Extract data from flat files (e.g., CSV, Excel).

    Returns:
        pd.DataFrame: Extracted data in a Pandas DataFrame format.
    """
    try:
        file_path = os.path.join(FLAT_FILE_PATH, "warehouse_data.xlsx")
        df = pd.read_excel(file_path)
        print("Data successfully extracted from flat files.")
        return df
    except Exception as e:
        print(f"Error while extracting data from flat files: {e}")
        return pd.DataFrame()

if __name__ == "__main__":
    # Example usage
    sql_data = extract_from_sql_server()
    oracle_data = extract_from_oracle()
    iot_data = extract_from_iot()
    flat_file_data = extract_from_flat_files()

    # Combine all data into a single DataFrame for further processing
    combined_data = pd.concat([sql_data, oracle_data, iot_data, flat_file_data], ignore_index=True)
    print("Data extraction completed. Combined dataset size:", combined_data.shape)
