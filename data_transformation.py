"""
data_transformation.py

This module handles data cleaning and transformation, including handling inconsistencies,
normalizing formats, and aggregating metrics for warehouse analytics.
Author: Satej
"""

import pandas as pd  # For data manipulation
import numpy as np  # For numerical computations

def clean_data(df):
    """
    Cleans the input DataFrame by handling missing values, standardizing formats, and normalizing units.

    Args:
        df (pd.DataFrame): Raw data to be cleaned.

    Returns:
        pd.DataFrame: Cleaned data.
    """
    try:
        # Drop rows with missing essential values
        df = df.dropna(subset=['product_id', 'warehouse_id', 'quantity'])
        
        # Fill missing numeric columns with 0
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        df[numeric_cols] = df[numeric_cols].fillna(0)
        
        # Standardize date formats
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
        
        print("Data cleaning completed.")
        return df
    except Exception as e:
        print(f"Error during data cleaning: {e}")
        return pd.DataFrame()

def transform_data(df):
    """
    Transforms the cleaned DataFrame by aggregating data and deriving key metrics.

    Args:
        df (pd.DataFrame): Cleaned data to be transformed.

    Returns:
        pd.DataFrame: Transformed data with aggregated metrics.
    """
    try:
        # Aggregate data to daily summaries
        df['date'] = pd.to_datetime(df['date'])
        aggregated_df = df.groupby(['warehouse_id', 'date']).agg({
            'quantity': 'sum',
            'order_value': 'mean'
        }).reset_index()

        # Rename columns for better understanding
        aggregated_df.rename(columns={
            'quantity': 'total_quantity',
            'order_value': 'average_order_value'
        }, inplace=True)

        print("Data transformation completed.")
        return aggregated_df
    except Exception as e:
        print(f"Error during data transformation: {e}")
        return pd.DataFrame()

if __name__ == "__main__":
    # Example usage
    # Load a sample dataset for testing
    sample_data_path = "/home/satej/data/sample_raw_data.csv"
    raw_data = pd.read_csv(sample_data_path)

    # Clean the data
    cleaned_data = clean_data(raw_data)

    # Transform the data
    transformed_data = transform_data(cleaned_data)

    print("Transformed data preview:")
    print(transformed_data.head())
