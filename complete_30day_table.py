"""
Generate a complete 30-day sector market cap table with date on left, sectors on top, and market cap values in the middle.
"""
import os
import pandas as pd
from datetime import datetime, timedelta

# Define directories
DATA_DIR = "data"

def create_full_30day_table():
    """Create a full 30-day market cap table with all dates and sectors"""
    # Load sector market caps
    market_cap_file = os.path.join(DATA_DIR, "sector_market_caps.parquet")
    
    if not os.path.exists(market_cap_file):
        print(f"Error: File not found - {market_cap_file}")
        return False
    
    try:
        # Load the parquet file
        sector_caps = pd.read_parquet(market_cap_file)
        
        # Get all sector columns (excluding weight columns and Total)
        sector_columns = [col for col in sector_caps.columns 
                         if not col.endswith('_weight_pct') and col != 'Total']
        
        # Convert to trillions for readability
        sector_caps_trillions = sector_caps[sector_columns] / 1_000_000_000_000
        
        # Get the last 30 days of data
        end_date = sector_caps.index.max()
        start_date = end_date - timedelta(days=30)
        recent_data = sector_caps_trillions[(sector_caps_trillions.index >= start_date)]
        
        # Sort by date (most recent first)
        recent_data = recent_data.sort_index(ascending=False)
        
        # Format the values with "$" and "T" for trillions
        formatted_data = recent_data.applymap(lambda x: f"${x:.2f}T")
        
        # Print complete table header
        print("\n===== COMPLETE 30-DAY SECTOR MARKET CAP HISTORY =====\n")
        
        # Print table header
        header = "| Date | " + " | ".join(sector_columns) + " |"
        separator = "|------|" + "|".join(["---"] * len(sector_columns)) + "|"
        print(header)
        print(separator)
        
        # Print all rows
        for date, row in formatted_data.iterrows():
            date_str = date.strftime("%Y-%m-%d")
            values = [row[col] for col in sector_columns]
            print(f"| {date_str} | " + " | ".join(values) + " |")
        
        # Save to file
        output_file = "30day_sector_marketcap_table.txt"
        with open(output_file, 'w') as f:
            f.write("\n===== COMPLETE 30-DAY SECTOR MARKET CAP HISTORY =====\n\n")
            f.write(header + "\n")
            f.write(separator + "\n")
            for date, row in formatted_data.iterrows():
                date_str = date.strftime("%Y-%m-%d")
                values = [row[col] for col in sector_columns]
                f.write(f"| {date_str} | " + " | ".join(values) + " |\n")
        
        print(f"\nTable saved to {output_file}")
        return True
        
    except Exception as e:
        print(f"Error: {e}")
        return False

if __name__ == "__main__":
    create_full_30day_table()