"""
Export a 30-day history of sector market caps with a focus on AdTech.
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

def export_adtech_30day_history():
    """Export a 30-day history table with AdTech market caps"""
    # Load sector market caps
    sector_caps = load_sector_market_caps()
    if sector_caps is None:
        return False
    
    # Filter to only include AdTech data and convert to trillions
    if 'AdTech' not in sector_caps.columns:
        print(f"AdTech column not found in sector market caps")
        return False
    
    # Convert to trillions for readability
    adtech_caps = sector_caps[['AdTech']] / 1_000_000_000_000  # to trillions
    
    # Get the last 30 days of data
    dates = sorted(adtech_caps.index)[-30:]
    recent_caps = adtech_caps.loc[dates]
    
    # Print the 30-day history table
    print("\nAdTech Market Cap - 30 Day History:")
    print("| Date | Market Cap (Trillions) |")
    print("|------|------------------------|")
    
    for date in sorted(recent_caps.index, reverse=True):
        print(f"| {date.strftime('%Y-%m-%d')} | ${recent_caps.loc[date, 'AdTech']:.3f}T |")
    
    # Calculate some analytics
    first_value = recent_caps.iloc[0]['AdTech']
    last_value = recent_caps.iloc[-1]['AdTech']
    change = ((last_value / first_value) - 1) * 100
    
    min_value = recent_caps['AdTech'].min()
    max_value = recent_caps['AdTech'].max()
    volatility = ((max_value / min_value) - 1) * 100
    
    print(f"\n30-Day Change: {change:.1f}%")
    print(f"30-Day Range: ${min_value:.3f}T - ${max_value:.3f}T")
    print(f"30-Day Volatility: {volatility:.1f}%")
    
    return True

def export_full_sector_30day_history():
    """Export the full 30-day history of all sector market caps"""
    # Load sector market caps
    sector_caps = load_sector_market_caps()
    if sector_caps is None:
        return False
    
    # Get all sector columns (excluding weight columns and Total)
    sector_columns = [col for col in sector_caps.columns 
                     if not col.endswith('_weight_pct') and col != 'Total']
    
    # Convert to trillions for readability
    sector_caps_trillions = sector_caps[sector_columns] / 1_000_000_000_000
    
    # Get the last 30 days of data
    dates = sorted(sector_caps_trillions.index)[-30:]
    recent_caps = sector_caps_trillions.loc[dates]
    
    # Save as CSV for easy viewing
    csv_file = os.path.join(DATA_DIR, "sector_marketcap_30days.csv")
    recent_caps.to_csv(csv_file)
    print(f"Saved 30-day sector market cap history to {csv_file}")
    
    # Save as Excel for better formatting
    excel_file = os.path.join(DATA_DIR, "sector_marketcap_30days.xlsx")
    recent_caps.to_excel(excel_file)
    print(f"Saved 30-day sector market cap history to {excel_file}")
    
    # Print the table in chunks to avoid exceeding terminal space
    print("\n30-Day Sector Market Cap History ($ Trillions):")
    
    chunks = 6  # Number of dates per chunk
    dates_list = sorted(recent_caps.index, reverse=True)
    
    for i in range(0, len(dates_list), chunks):
        chunk_dates = dates_list[i:i+chunks]
        
        # Print header for this chunk
        print("\n| Date | " + " | ".join(sector_columns) + " |")
        print("|------|" + "|".join(["---"] * len(sector_columns)) + "|")
        
        # Print data for each date in this chunk
        for date in chunk_dates:
            date_str = date.strftime("%Y-%m-%d")
            values = [f"${recent_caps.loc[date, sector]:.2f}T" for sector in sector_columns]
            print(f"| {date_str} | " + " | ".join(values) + " |")
    
    return True

def calculate_sector_avg_marketcaps():
    """Calculate average market caps for each sector over the past 30 days"""
    # Load sector market caps
    sector_caps = load_sector_market_caps()
    if sector_caps is None:
        return False
    
    # Get all sector columns (excluding weight columns and Total)
    sector_columns = [col for col in sector_caps.columns 
                     if not col.endswith('_weight_pct') and col != 'Total']
    
    # Convert to trillions for readability
    sector_caps_trillions = sector_caps[sector_columns] / 1_000_000_000_000
    
    # Get the last 30 days of data
    dates = sorted(sector_caps_trillions.index)[-30:]
    recent_caps = sector_caps_trillions.loc[dates]
    
    # Calculate averages and other statistics
    avg_caps = recent_caps.mean()
    std_caps = recent_caps.std()
    min_caps = recent_caps.min()
    max_caps = recent_caps.max()
    
    # Calculate total market cap (average over 30 days)
    total_avg = avg_caps.sum()
    
    # Calculate average weight of each sector
    weights = {}
    for sector in sector_columns:
        weight = (avg_caps[sector] / total_avg) * 100
        weights[sector] = weight
    
    # Print summary
    print("\n30-Day Average Sector Market Caps:")
    print("| Sector | Avg Market Cap | % of Total | Min | Max | Std Dev |")
    print("|--------|----------------|------------|-----|-----|---------|")
    
    for sector in sorted(sector_columns, key=lambda x: avg_caps[x], reverse=True):
        print(f"| {sector} | ${avg_caps[sector]:.2f}T | {weights[sector]:.2f}% | ${min_caps[sector]:.2f}T | ${max_caps[sector]:.2f}T | ${std_caps[sector]:.3f}T |")
    
    print(f"\nTotal Market: ${total_avg:.2f}T")
    
    return True

if __name__ == "__main__":
    # Export AdTech 30-day history
    export_adtech_30day_history()
    
    # Export full sector 30-day history
    export_full_sector_30day_history()
    
    # Calculate sector average market caps
    calculate_sector_avg_marketcaps()