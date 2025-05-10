"""
Generate a simple markdown table of the 30-day market cap history for all sectors
"""
import os
import pandas as pd
from datetime import datetime, timedelta

# Define directories
DATA_DIR = "data"

def create_marketcap_table():
    """Create a simple 30-day market cap table"""
    # Load sector market caps from parquet
    market_cap_file = os.path.join(DATA_DIR, "sector_market_caps.parquet")
    
    if not os.path.exists(market_cap_file):
        print(f"Sector market cap file not found: {market_cap_file}")
        return False
    
    try:
        sector_caps = pd.read_parquet(market_cap_file)
        print(f"Loaded sector market caps from {market_cap_file}")
    except Exception as e:
        print(f"Error loading sector market caps: {e}")
        return False
    
    # Get all sector columns (excluding weight columns and Total)
    sector_columns = [col for col in sector_caps.columns 
                     if not col.endswith('_weight_pct') and col != 'Total']
    
    # Sort sectors by most recent market cap value (descending)
    latest_date = sector_caps.index.max()
    latest_values = sector_caps.loc[latest_date]
    sorted_sectors = sorted(sector_columns, key=lambda x: latest_values[x], reverse=True)
    
    # Convert to trillions for readability
    sector_caps_trillions = sector_caps[sorted_sectors] / 1_000_000_000_000
    
    # Filter to last 30 days and sort by date (descending)
    end_date = sector_caps.index.max()
    start_date = end_date - timedelta(days=30)
    recent_caps = sector_caps_trillions[(sector_caps_trillions.index >= start_date)]
    recent_caps = recent_caps.sort_index(ascending=False)
    
    # Format values with currency symbols
    formatted_df = pd.DataFrame(index=recent_caps.index)
    for column in sorted_sectors:
        formatted_df[column] = recent_caps[column].apply(lambda x: f"${x:.2f}T")
        
    # Print header
    print("\n# Sector Market Caps - 30 Day History ($ Trillions)\n")
    
    # Print markdown table header
    header = "| Date | " + " | ".join(sorted_sectors) + " |"
    divider = "|------|" + "|".join(["---"] * len(sorted_sectors)) + "|"
    print(header)
    print(divider)
    
    # Print rows
    for date in formatted_df.index:
        date_str = date.strftime("%Y-%m-%d")
        values = [formatted_df.loc[date, sector] for sector in sorted_sectors]
        row = f"| {date_str} | " + " | ".join(values) + " |"
        print(row)
    
    return True

if __name__ == "__main__":
    create_marketcap_table()