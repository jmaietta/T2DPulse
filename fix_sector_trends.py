#!/usr/bin/env python3
"""
Fix sector trend visualizations in T2D Pulse dashboard

This script creates a bridge between the data/sector_30day_history.csv file
which contains the authentic market cap trend data and the
authentic_sector_history.csv file which is used by the dashboard
for rendering the sector trend charts.

The issue is that sector_30day_history.csv is being updated by convert_sector_data.py
but authentic_sector_history.csv is not being updated, so the charts remain blank.
"""

import pandas as pd
import os
import json
from datetime import datetime

def main():
    print("Fixing sector trend visualizations...")
    
    # Load the authentic market cap data
    try:
        sector_30day_df = pd.read_csv('data/sector_30day_history.csv')
        print(f"Loaded authentic data from sector_30day_history.csv with {len(sector_30day_df)} entries")
    except Exception as e:
        print(f"Error loading sector_30day_history.csv: {e}")
        return False
    
    # Create the data directory if it doesn't exist
    os.makedirs('data', exist_ok=True)
    
    # Convert to authentic_sector_history.csv format
    authentic_df = sector_30day_df.copy()
    
    # Ensure date is in proper format
    authentic_df['date'] = pd.to_datetime(authentic_df['Date'])
    authentic_df = authentic_df.drop(columns=['Date'])  # Remove original Date column
    
    # Save to authentic_sector_history.csv
    authentic_csv_path = "data/authentic_sector_history.csv"
    authentic_df.to_csv(authentic_csv_path, index=False)
    print(f"Saved authentic data to {authentic_csv_path}")
    
    # Also create a JSON version for easier access
    json_path = "data/authentic_sector_history.json"
    
    # Convert to dictionary format
    history_dict = {}
    for _, row in authentic_df.iterrows():
        date_str = row['date'].strftime('%Y-%m-%d')
        history_dict[date_str] = {sector: row[sector] for sector in authentic_df.columns if sector != 'date'}
    
    # Save to JSON
    with open(json_path, 'w') as f:
        json.dump(history_dict, f, indent=2)
    print(f"Saved authentic JSON data to {json_path}")
    
    # Also create a date-specific CSV for today
    today = datetime.now().strftime('%Y-%m-%d')
    today_csv_path = f"data/authentic_sector_history_{today}.csv"
    authentic_df.to_csv(today_csv_path, index=False)
    print(f"Saved date-specific authentic data to {today_csv_path}")
    
    return True

if __name__ == "__main__":
    success = main()
    if success:
        print("✅ Successfully fixed sector trend visualizations!")
    else:
        print("❌ Failed to fix sector trend visualizations")