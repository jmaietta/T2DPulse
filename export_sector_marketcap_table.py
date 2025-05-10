"""
Export a clean table of sector market caps across dates to make it easy to verify
the corrected data.
"""
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Define directories
DATA_DIR = "data"

def load_sector_market_caps():
    """Load sector market cap data from parquet file"""
    market_cap_file = os.path.join(DATA_DIR, "sector_market_caps.parquet")
    
    if os.path.exists(market_cap_file):
        try:
            sector_caps = pd.read_parquet(market_cap_file)
            print(f"Loaded sector market caps from {market_cap_file}")
            return sector_caps
        except Exception as e:
            print(f"Error loading sector market caps: {e}")
            return None
    else:
        print(f"Sector market cap file not found: {market_cap_file}")
        return None

def create_market_cap_tables():
    """Create and save sector market cap tables in various formats"""
    # Load sector market caps
    sector_caps = load_sector_market_caps()
    if sector_caps is None:
        return False
    
    # Get all sector columns (excluding weight columns and Total)
    sector_columns = [col for col in sector_caps.columns 
                     if not col.endswith('_weight_pct') and col != 'Total']
    
    # Convert to trillions for readability
    sector_caps_trillions = sector_caps[sector_columns] / 1_000_000_000_000
    
    # Filter to last 30 days
    end_date = sector_caps.index.max()
    start_date = end_date - timedelta(days=30)
    recent_caps = sector_caps_trillions[(sector_caps_trillions.index >= start_date)]
    
    # Save as CSV for easy viewing
    csv_file = os.path.join(DATA_DIR, "sector_marketcap_table.csv")
    recent_caps.to_csv(csv_file)
    print(f"Saved sector market cap table to {csv_file}")
    
    # Save as Excel for better formatting
    excel_file = os.path.join(DATA_DIR, "sector_marketcap_table.xlsx")
    
    with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
        # Market cap table in trillions
        recent_caps.to_excel(writer, sheet_name='Market Cap (Trillions)')
        
        # Percentage of total table
        percentages = pd.DataFrame(index=recent_caps.index)
        for date in recent_caps.index:
            total = recent_caps.loc[date].sum()
            for sector in sector_columns:
                percentages.loc[date, sector] = (recent_caps.loc[date, sector] / total) * 100
        
        percentages.to_excel(writer, sheet_name='% of Total')
        
        # Last 7 days focused view
        last_7_days = recent_caps.iloc[-7:]
        last_7_days.to_excel(writer, sheet_name='Last 7 Days')
        
        # AdTech specific view with key stocks
        adtech_file = os.path.join(DATA_DIR, "adtech_marketcap_detail.csv")
        
        if os.path.exists(adtech_file):
            adtech_detail = pd.read_csv(adtech_file)
            adtech_detail.to_excel(writer, sheet_name='AdTech Detail')
    
    print(f"Saved detailed sector market cap tables to {excel_file}")
    
    # Print the table for the last 7 days (in trillions)
    last_7_days = recent_caps.iloc[-7:]
    print("\nSector Market Caps (Last 7 Days, in $ Trillions):")
    print(last_7_days.to_string(float_format='${:.2f}T'.format))
    
    # Print latest day with sector percentages
    latest_date = sector_caps.index.max()
    latest_caps = sector_caps_trillions.loc[latest_date]
    total_cap = latest_caps.sum()
    
    print(f"\nSector Market Caps on {latest_date.strftime('%Y-%m-%d')} (Total: ${total_cap:.2f}T):")
    
    for sector in sorted(sector_columns, key=lambda x: latest_caps[x], reverse=True):
        cap = latest_caps[sector]
        pct = (cap / total_cap) * 100
        print(f"{sector}: ${cap:.2f}T ({pct:.2f}%)")
    
    # Create text table for display in console or markdown
    print("\nText Table for Copy/Paste:")
    print("| Date | " + " | ".join(sector_columns) + " |")
    print("|------|" + "|".join(["---"] * len(sector_columns)) + "|")
    
    # Print last 7 days
    for date in sorted(last_7_days.index, reverse=True):
        date_str = date.strftime("%Y-%m-%d")
        values = [f"${last_7_days.loc[date, sector]:.2f}T" for sector in sector_columns]
        print(f"| {date_str} | " + " | ".join(values) + " |")
    
    return True

def create_focused_adtech_table():
    """Create a focused table just for AdTech market cap history"""
    # Load sector market caps
    sector_caps = load_sector_market_caps()
    if sector_caps is None:
        return False
    
    # Check if AdTech column exists
    if 'AdTech' not in sector_caps.columns:
        print(f"AdTech column not found in sector market caps")
        return False
    
    # Extract AdTech data and convert to trillions
    adtech_caps = sector_caps[['AdTech']] / 1_000_000_000_000
    
    # Filter to last 30 days
    end_date = sector_caps.index.max()
    start_date = end_date - timedelta(days=30)
    adtech_recent = adtech_caps[(adtech_caps.index >= start_date)]
    
    # Print AdTech market cap history
    print("\nAdTech Market Cap History (in $ Trillions):")
    print("| Date | Market Cap |")
    print("|------|------------|")
    
    for date in sorted(adtech_recent.index, reverse=True):
        date_str = date.strftime("%Y-%m-%d")
        value = adtech_recent.loc[date, 'AdTech']
        print(f"| {date_str} | ${value:.3f}T |")
    
    return True

if __name__ == "__main__":
    create_market_cap_tables()
    create_focused_adtech_table()