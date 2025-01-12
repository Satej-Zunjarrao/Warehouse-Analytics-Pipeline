"""
data_loading.py

This module handles loading cleaned and transformed data into the Snowflake data warehouse.
It includes connection setup, schema definition, and batch loading for optimized performance.
Author: Satej
"""

import snowflake.connector  # Snowflake connection library
from snowflake.connector.pandas_tools import write_pandas  # For Pandas DataFrame loading
import pandas as pd  # For handling data

# Snowflake connection credentials (replace with secure access in production)
SNOWFLAKE_CONFIG = {
    "user": "SATEJ_USER",
    "password": "your_password",
    "account": "SATEJ_ACCOUNT",
    "warehouse": "COMPUTE_WH",
    "database": "WAREHOUSE_DB",
    "schema": "PUBLIC"
}

def connect_to_snowflake():
    """
    Establishes a connection to the Snowflake data warehouse.

    Returns:
        snowflake.connector.connection.SnowflakeConnection: Active Snowflake connection.
    """
    try:
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        print("Connected to Snowflake successfully.")
        return conn
    except Exception as e:
        print(f"Error connecting to Snowflake: {e}")
        return None

def create_table_if_not_exists(conn, table_name, schema):
    """
    Creates a table in Snowflake if it does not already exist.

    Args:
        conn (SnowflakeConnection): Active Snowflake connection.
        table_name (str): Name of the table to create.
        schema (str): Schema definition for the table.
    """
    try:
        cursor = conn.cursor()
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {schema}
        )
        """
        cursor.execute(create_table_query)
        print(f"Table '{table_name}' is ready in Snowflake.")
    except Exception as e:
        print(f"Error creating table '{table_name}': {e}")

def load_data_to_snowflake(conn, df, table_name):
    """
    Loads a Pandas DataFrame into a Snowflake table.

    Args:
        conn (SnowflakeConnection): Active Snowflake connection.
        df (pd.DataFrame): DataFrame to load.
        table_name (str): Name of the table in Snowflake.
    """
    try:
        success, num_rows, _ = write_pandas(conn, df, table_name.upper())
        if success:
            print(f"Data successfully loaded to Snowflake table '{table_name}'. Rows inserted: {num_rows}")
        else:
            print("Failed to load data into Snowflake.")
    except Exception as e:
        print(f"Error loading data to Snowflake: {e}")

if __name__ == "__main__":
    # Example usage
    transformed_data_path = "/home/satej/data/transformed_data.csv"
    transformed_data = pd.read_csv(transformed_data_path)

    # Connect to Snowflake
    connection = connect_to_snowflake()

    if connection:
        # Define table schema (adjust as per your data)
        table_name = "WAREHOUSE_ANALYTICS"
        schema = """
            warehouse_id VARCHAR,
            date DATE,
            total_quantity INT,
            average_order_value FLOAT
        """

        # Ensure table exists
        create_table_if_not_exists(connection, table_name, schema)

        # Load data
        load_data_to_snowflake(connection, transformed_data, table_name)

        # Close connection
        connection.close()
