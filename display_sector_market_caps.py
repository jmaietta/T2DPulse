"""
Display the sector market caps for the past 30 days
"""

import os
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import time

# Constants
SECTOR_CAPS_CSV = "sector_market_caps.csv"
MAX_WAIT_TIME = 300  # 5 minutes maximum waiting time

def wait_for_file():
    """Wait for the sector_market_caps.csv file to be generated"""
    start_time = time.time()
    while not os.path.exists(SECTOR_CAPS_CSV):
        elapsed = time.time() - start_time
        if elapsed > MAX_WAIT_TIME:
            print(f"Timeout waiting for {SECTOR_CAPS_CSV} to be generated")
            return False
        
        print(f"Waiting for {SECTOR_CAPS_CSV} to be generated... ({int(elapsed)}s)")
        time.sleep(10)
    
    return True

def load_sector_market_caps():
    """Load the sector market cap data from CSV"""
    if not os.path.exists(SECTOR_CAPS_CSV):
        print(f"Error: {SECTOR_CAPS_CSV} not found")
        return None
    
    try:
        df = pd.read_csv(SECTOR_CAPS_CSV)
        if df.empty:
            print(f"Warning: {SECTOR_CAPS_CSV} is empty")
            return None
        
        return df
    except Exception as e:
        print(f"Error loading {SECTOR_CAPS_CSV}: {e}")
        return None

def display_sector_market_caps(days=30):
    """Display the sector market caps for the past X days"""
    df = load_sector_market_caps()
    if df is None:
        return
    
    # Convert date column to datetime
    df['date'] = pd.to_datetime(df['date'])
    
    # Calculate cutoff date
    today = datetime.now().date()
    cutoff_date = today - timedelta(days=days)
    
    # Filter to data within the last X days
    recent_df = df[df['date'] >= cutoff_date.isoformat()]
    
    if recent_df.empty:
        print(f"No data available for the past {days} days")
        return
    
    # Convert to pivot table with dates as rows and sectors as columns
    pivot_df = recent_df.pivot(index='date', columns='sector', values='market_cap')
    
    # Convert to billions for readability
    pivot_df = pivot_df / 1_000_000_000
    
    # Round to 2 decimal places
    pivot_df = pivot_df.round(2)
    
    # Sort columns by the most recent market cap value (descending)
    last_date = pivot_df.index.max()
    if not pd.isna(last_date):
        try:
            column_order = pivot_df.loc[last_date].sort_values(ascending=False).index
            pivot_df = pivot_df[column_order]
        except:
            # Just use original order if sorting fails
            pass
    
    # Display the data
    print(f"\nSector Market Caps for the Past {days} Days (Billions USD)")
    print("="*100)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    print(pivot_df)
    
    # Display the latest values
    print("\nLatest Sector Market Caps (Billions USD)")
    print("="*60)
    latest_date = pivot_df.index.max()
    if not pd.isna(latest_date):
        latest_values = pivot_df.loc[latest_date].sort_values(ascending=False)
        for sector, value in latest_values.items():
            print(f"{sector:<25} ${value:<15.2f}")
    
    # Save formatted data to a nice CSV file
    output_file = "formatted_sector_market_caps_30days.csv"
    pivot_df.to_csv(output_file)
    print(f"\nSaved formatted data to {output_file}")

if __name__ == "__main__":
    print("Checking for sector market cap data...")
    if wait_for_file():
        display_sector_market_caps(30)
    else:
        print("Could not find sector market cap data. The calculation may still be running.")
        
        # Use the legacy data as fallback if available
        fallback_files = [
            "corrected_sector_market_caps.csv",
            "corrected_sector_market_caps_detailed.csv",
            "final_sector_market_caps.csv"
        ]
        
        for file in fallback_files:
            if os.path.exists(file):
                print(f"\nFallback: Using existing data from {file}")
                df = pd.read_csv(file)
                print("\nSector Market Caps (Billions USD):")
                print("="*60)
                print(df)
                break
        else:
            print("No fallback data available. Please wait for the calculation to complete.")