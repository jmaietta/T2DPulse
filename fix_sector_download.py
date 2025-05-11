#!/usr/bin/env python3
"""
Fix sector history export to use authentic data
This script ensures that exports from sector_30day_history.csv contain authentic market cap data
and properly filter out weekends.
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime

def fix_sector_export():
    """
    Generate fixed sector export files based on the authentic data
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Load the authentic sector data
        authentic_data_path = "data/sector_30day_history.csv"
        if not os.path.exists(authentic_data_path):
            print(f"Error: {authentic_data_path} not found")
            return False
            
        # Load the data
        df = pd.read_csv(authentic_data_path)
        print(f"Loaded authentic data with {len(df)} entries")
        
        # Convert date column to datetime
        df['Date'] = pd.to_datetime(df['Date'])
        
        # Filter out weekends - we only want business days
        df = df[df['Date'].dt.dayofweek < 5]
        print(f"Filtered to {len(df)} business days")
        
        # Sort the data by date
        df = df.sort_values('Date')
        
        # Create the data directory if it doesn't exist
        os.makedirs('data', exist_ok=True)
        
        # Get today's date for the filename
        today = datetime.now().strftime('%Y-%m-%d')
        
        # Save to CSV (with date in filename)
        csv_file = f'data/sector_sentiment_history_{today}.csv'
        df.to_csv(csv_file, index=False)
        print(f"Exported sector history to CSV: {csv_file}")
        
        # Save to CSV (standard filename)
        csv_file_std = 'data/sector_sentiment_history.csv'
        df.to_csv(csv_file_std, index=False)
        print(f"Exported sector history to CSV: {csv_file_std}")
        
        # Save to Excel (with date in filename)
        excel_file = f'data/sector_sentiment_history_{today}.xlsx'
        df.to_excel(excel_file, index=False, engine='openpyxl')
        print(f"Exported sector history to Excel: {excel_file}")
        
        # Save to Excel (standard filename)
        excel_file_std = 'data/sector_sentiment_history.xlsx'
        df.to_excel(excel_file_std, index=False, engine='openpyxl')
        print(f"Exported sector history to Excel: {excel_file_std}")
        
        return True
    except Exception as e:
        print(f"Error fixing sector export: {e}")
        return False

if __name__ == "__main__":
    success = fix_sector_export()
    if success:
        print("Successfully fixed sector export!")
    else:
        print("Failed to fix sector export")