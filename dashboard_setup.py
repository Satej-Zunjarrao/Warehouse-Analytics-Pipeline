"""
dashboard_setup.py

This module prepares and exports data for integration with Tableau and Power BI dashboards.
It focuses on structuring the data for visualization and saving it in appropriate formats.
Author: Satej
"""

import pandas as pd  # For data manipulation

# File path for saving dashboard-ready data
DASHBOARD_EXPORT_PATH = "/home/satej/data/dashboard_ready_data.csv"

def prepare_dashboard_data(df):
    """
    Prepares data for dashboard visualization by ensuring appropriate structure and aggregations.

    Args:
        df (pd.DataFrame): Transformed data for dashboard preparation.

    Returns:
        pd.DataFrame: Data structured for Tableau or Power BI dashboards.
    """
    try:
        # Add calculated columns for visualization
        df['inventory_utilization'] = (df['total_quantity'] / df['storage_capacity']) * 100

        # Rename columns for clarity in dashboards
        df.rename(columns={
            'total_quantity': 'Total Inventory',
            'average_order_value': 'Average Order Value',
            'date': 'Date',
            'warehouse_id': 'Warehouse ID'
        }, inplace=True)

        # Ensure date sorting for time-series visualizations
        df = df.sort_values(by='Date')

        print("Dashboard data preparation completed.")
        return df
    except Exception as e:
        print(f"Error during dashboard data preparation: {e}")
        return pd.DataFrame()

def export_to_csv(df, export_path=DASHBOARD_EXPORT_PATH):
    """
    Exports the prepared data to a CSV file for Tableau or Power BI integration.

    Args:
        df (pd.DataFrame): DataFrame to export.
        export_path (str): File path for exporting the CSV file.
    """
    try:
        df.to_csv(export_path, index=False)
        print(f"Dashboard-ready data successfully exported to: {export_path}")
    except Exception as e:
        print(f"Error exporting dashboard data to CSV: {e}")

if __name__ == "__main__":
    # Example usage
    transformed_data_path = "/home/satej/data/transformed_data.csv"
    transformed_data = pd.read_csv(transformed_data_path)

    # Prepare data for dashboards
    dashboard_data = prepare_dashboard_data(transformed_data)

    # Export data for visualization tools
    export_to_csv(dashboard_data)
