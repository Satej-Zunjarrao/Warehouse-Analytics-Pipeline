"""
kpi_analysis.py

This module calculates key performance indicators (KPIs) for warehouse operations.
The metrics include inventory turnover rates, order accuracy, and storage utilization.
Author: Satej
"""

import pandas as pd  # For data manipulation

def calculate_inventory_turnover(df):
    """
    Calculates inventory turnover rates.

    Formula:
        Inventory Turnover = Cost of Goods Sold / Average Inventory

    Args:
        df (pd.DataFrame): DataFrame containing inventory and cost data.

    Returns:
        pd.DataFrame: DataFrame with inventory turnover calculated.
    """
    try:
        df['inventory_turnover'] = df['cost_of_goods_sold'] / df['average_inventory']
        print("Inventory turnover calculation completed.")
        return df
    except KeyError as e:
        print(f"Missing required column for inventory turnover calculation: {e}")
        return df
    except Exception as e:
        print(f"Error calculating inventory turnover: {e}")
        return df

def calculate_order_accuracy(df):
    """
    Calculates order accuracy as a percentage.

    Formula:
        Order Accuracy (%) = (Correct Orders / Total Orders) * 100

    Args:
        df (pd.DataFrame): DataFrame containing order data.

    Returns:
        pd.DataFrame: DataFrame with order accuracy calculated.
    """
    try:
        df['order_accuracy'] = (df['correct_orders'] / df['total_orders']) * 100
        print("Order accuracy calculation completed.")
        return df
    except KeyError as e:
        print(f"Missing required column for order accuracy calculation: {e}")
        return df
    except Exception as e:
        print(f"Error calculating order accuracy: {e}")
        return df

def calculate_storage_utilization(df):
    """
    Calculates storage utilization as a percentage.

    Formula:
        Storage Utilization (%) = (Used Space / Total Space) * 100

    Args:
        df (pd.DataFrame): DataFrame containing storage data.

    Returns:
        pd.DataFrame: DataFrame with storage utilization calculated.
    """
    try:
        df['storage_utilization'] = (df['used_space'] / df['total_space']) * 100
        print("Storage utilization calculation completed.")
        return df
    except KeyError as e:
        print(f"Missing required column for storage utilization calculation: {e}")
        return df
    except Exception as e:
        print(f"Error calculating storage utilization: {e}")
        return df

if __name__ == "__main__":
    # Example usage
    kpi_data_path = "/home/satej/data/kpi_data.csv"
    kpi_data = pd.read_csv(kpi_data_path)

    # Calculate KPIs
    kpi_data = calculate_inventory_turnover(kpi_data)
    kpi_data = calculate_order_accuracy(kpi_data)
    kpi_data = calculate_storage_utilization(kpi_data)

    print("KPI Analysis Results:")
    print(kpi_data.head())
