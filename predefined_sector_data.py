#!/usr/bin/env python3
# predefined_sector_data.py
# -----------------------------------------------------------
# Use predefined authentic historical sector sentiment data
# instead of calculating it on the fly

import os
import pandas as pd

def get_predefined_sector_history(all_sectors=True):
    """
    Load the predefined sector history from CSV file
    
    Args:
        all_sectors (bool): Whether to return all sectors or just the ones in sentiment_engine.SECTORS
        
    Returns:
        dict: Dictionary with date-indexed DataFrames for each sector
    """
    csv_path = "data/predefined_sector_history.csv"
    
    # If the predefined data doesn't exist yet, return empty dictionary
    if not os.path.exists(csv_path):
        print(f"Warning: Predefined sector history file not found at {csv_path}")
        return {}
        
    try:
        # Read the CSV file
        df = pd.read_csv(csv_path)
        
        # Convert date column to datetime
        df['date'] = pd.to_datetime(df['date'])
        
        # Dictionary to store results
        sector_history = {}
        
        # Get all column names except 'date'
        sector_columns = [col for col in df.columns if col != 'date']
        
        # Create a DataFrame for each sector
        for sector in sector_columns:
            sector_df = df[['date', sector]].copy()
            sector_df.columns = ['date', 'value']  # Rename for consistency
            sector_df = sector_df.set_index('date')
            sector_history[sector] = sector_df
            
        return sector_history
        
    except Exception as e:
        print(f"Error loading predefined sector history: {e}")
        return {}
        
def get_sector_history_dates():
    """
    Get the list of dates for which we have sector history data
    
    Returns:
        list: List of date strings in YYYY-MM-DD format
    """
    csv_path = "data/predefined_sector_history.csv"
    
    if not os.path.exists(csv_path):
        return []
        
    try:
        df = pd.read_csv(csv_path)
        dates = df['date'].tolist()
        return dates
    except:
        return []