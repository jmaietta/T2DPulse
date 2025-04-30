#!/usr/bin/env python3
# predefined_sector_data.py
# -----------------------------------------------------------
# Use predefined authentic data for sector sentiment history 
# This avoids issues with recalculating historical values

import os
import pandas as pd
from datetime import datetime, timedelta

# Path to the predefined data file
PREDEFINED_DATA_PATH = "data/predefined_sector_history.csv"

def get_sector_history_dataframe(sector_name, days=10):
    """
    Get historical data for a sector using predefined values
    
    Args:
        sector_name (str): Name of the sector
        days (int): Number of days to include
        
    Returns:
        DataFrame: DataFrame with 'date' and 'score' columns
    """
    try:
        # Check if the predefined data file exists
        if not os.path.exists(PREDEFINED_DATA_PATH):
            print(f"Predefined sector data file not found: {PREDEFINED_DATA_PATH}")
            return pd.DataFrame(columns=["date", "score"])
        
        # Load the predefined data
        all_sectors_df = pd.read_csv(PREDEFINED_DATA_PATH)
        
        # Convert date column to datetime
        all_sectors_df['date'] = pd.to_datetime(all_sectors_df['date'])
        
        # Check if this sector exists in the data
        if sector_name not in all_sectors_df.columns:
            print(f"Sector {sector_name} not found in predefined data")
            return pd.DataFrame(columns=["date", "score"])
        
        # Create a dataframe with just this sector's data
        df = pd.DataFrame({
            'date': all_sectors_df['date'],
            'score': all_sectors_df[sector_name]
        })
        
        # Sort by date to ensure proper display
        df = df.sort_values('date')
        
        # Keep only the requested number of days
        if len(df) > days:
            df = df.tail(days)
        
        return df
    
    except Exception as e:
        print(f"Error getting predefined sector history for {sector_name}: {e}")
        return pd.DataFrame(columns=["date", "score"])

# For testing
if __name__ == "__main__":
    # Test the function
    df = get_sector_history_dataframe("AdTech")
    print(df.head())