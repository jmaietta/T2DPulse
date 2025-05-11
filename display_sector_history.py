#!/usr/bin/env python3
"""
Display the 30-day market cap history for all sectors in a readable format.
"""

import pandas as pd
import os
import glob
from datetime import datetime, timedelta

def find_ticker_history_file():
    """Find the most recent full ticker history file"""
    history_files = glob.glob('*Ticker_History*.csv')
    if not history_files:
        print("No ticker history files found")
        return None
    
    return max(history_files, key=os.path.getctime)

def find_sector_history_file():
    """Find the most recent sector history file"""
    sector_files = glob.glob('*sector*history*.csv')
    if not sector_files:
        print("No sector history files found")
        return None
    
    return max(sector_files, key=os.path.getctime)

def format_market_cap(value):
    """Format a market cap value in billions"""
    return f"${value/1e9:.2f}B"

def main():
    # First try to find and load the sector history file
    sector_file = find_sector_history_file()
    if sector_file:
        print(f"Loading sector history from: {sector_file}")
        try:
            df = pd.read_csv(sector_file)
            
            # Check if it has a date column
            date_col = None
            for col in ['date', 'Date']:
                if col in df.columns:
                    date_col = col
                    break
            
            if date_col:
                # Convert date to datetime
                df[date_col] = pd.to_datetime(df[date_col])
                
                # Sort by date
                df = df.sort_values(by=date_col)
                
                # Display the data
                print(f"\nSector history from {df[date_col].min()} to {df[date_col].max()}")
                print(f"Found {len(df)} entries")
                
                # Print the complete data
                print("\nSector scores by date:")
                print("-" * 80)
                print(df.to_string())
                
                # Calculate and print average scores for each sector
                print("\nAverage sector scores:")
                print("-" * 80)
                
                # Get all columns except the date column
                sector_cols = [col for col in df.columns if col != date_col]
                
                for sector in sector_cols:
                    avg_score = df[sector].mean()
                    print(f"{sector}: {avg_score:.2f}")
            else:
                print(f"No date column found in {sector_file}")
                
        except Exception as e:
            print(f"Error loading {sector_file}: {e}")
    
    # Try to find actual market cap values
    market_cap_file = 'authentic_sector_market_caps.csv'
    if os.path.exists(market_cap_file):
        print(f"\nLoading market cap data from: {market_cap_file}")
        try:
            mc_df = pd.read_csv(market_cap_file)
            print("\nSector market caps (most recent):")
            print("-" * 80)
            
            # Sort by market cap
            if 'Market Cap (Billions USD)' in mc_df.columns:
                mc_df = mc_df.sort_values(by='Market Cap (Billions USD)', ascending=False)
            
            print(mc_df.to_string())
        except Exception as e:
            print(f"Error loading {market_cap_file}: {e}")
    
    # Try to find daily market cap history
    daily_file = find_ticker_history_file()
    if daily_file:
        print(f"\nLoading ticker history from: {daily_file}")
        try:
            ticker_df = pd.read_csv(daily_file)
            
            # Check for date and sector columns
            if 'date' in ticker_df.columns and 'sector' in ticker_df.columns and 'market_cap' in ticker_df.columns:
                # Convert date to datetime
                ticker_df['date'] = pd.to_datetime(ticker_df['date'])
                
                # Create a pivot table of market cap by sector and date
                pivot = ticker_df.pivot_table(
                    index='date',
                    columns='sector',
                    values='market_cap',
                    aggfunc='sum'
                )
                
                # Convert to billions for readability
                pivot = pivot / 1e9
                
                print("\nDaily market cap by sector (Billions USD):")
                print("-" * 80)
                print(pivot.tail(30).to_string(float_format="${:.2f}B".format))
            else:
                print(f"Required columns not found in {daily_file}")
                print(f"Columns: {ticker_df.columns.tolist()}")
        except Exception as e:
            print(f"Error loading {daily_file}: {e}")

if __name__ == "__main__":
    main()