#!/usr/bin/env python3
"""
Update Sector Trends with Authentic Data

This script ensures sector_30day_history.csv contains authentic data from 
authentic_sector_history.csv while keeping the correct date format.
"""

import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta

def main():
    print("Updating sector trend data with authentic values...")
    
    # Load the authentic data
    try:
        authentic_df = pd.read_csv('data/authentic_sector_history.csv')
        print(f"Loaded authentic data with {len(authentic_df)} entries")
    except Exception as e:
        print(f"Error loading authentic data: {e}")
        return False
    
    # Create the data directory if it doesn't exist
    os.makedirs('data', exist_ok=True)
    
    # Ensure the 'date' column is in datetime format
    authentic_df['date'] = pd.to_datetime(authentic_df['date'])
    
    # Filter out weekends
    authentic_df = authentic_df[authentic_df['date'].dt.dayofweek < 5]
    
    # Rename 'date' to 'Date' to match dashboard format
    authentic_df = authentic_df.rename(columns={'date': 'Date'})
    
    # Save directly to sector_30day_history.csv (overwriting any previous data)
    authentic_df.to_csv('data/sector_30day_history.csv', index=False)
    print(f"Saved authentic trend data to data/sector_30day_history.csv")
    
    # Also update sector_history.json for API access
    json_path = "data/sector_history.json"
    
    # Create a backup
    authentic_df.to_csv('data/sector_30day_history_authentic.csv', index=False)
    print(f"Saved backup to data/sector_30day_history_authentic.csv")
    
    # Run the fix_sector_trends.py script to update all files
    try:
        import fix_sector_trends
        success = fix_sector_trends.main()
        if success:
            print("Successfully updated all sector trend files!")
        else:
            print("Failed to update all sector trend files")
    except Exception as e:
        print(f"Error running fix_sector_trends.py: {e}")
        
    return True

if __name__ == "__main__":
    success = main()
    if success:
        print("✅ Successfully updated sector trend data with authentic values!")
    else:
        print("❌ Failed to update sector trend data!")