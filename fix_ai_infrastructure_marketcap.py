#!/usr/bin/env python3
"""
Fix the inconsistency in AI Infrastructure market cap data.
This script ensures the historical market cap values match the 
authentic market cap totals from the complete ticker dataset.
"""

import pandas as pd
import os
from datetime import datetime, timedelta
import numpy as np

def load_ticker_mapping():
    """Load the mapping of tickers to sectors"""
    coverage_file = 'T2D_Pulse_93_tickers_coverage.csv'
    
    if not os.path.exists(coverage_file):
        print(f"Error: {coverage_file} not found")
        return None
    
    # Skip header rows
    mapping_df = pd.read_csv(coverage_file, skiprows=7)
    
    # Create a mapping of tickers to sectors
    ticker_to_sectors = {}
    for _, row in mapping_df.iterrows():
        ticker = row['Ticker']
        sector = row['Sector']
        
        if ticker not in ticker_to_sectors:
            ticker_to_sectors[ticker] = []
        
        if sector not in ticker_to_sectors[ticker]:
            ticker_to_sectors[ticker].append(sector)
    
    return ticker_to_sectors

def load_ticker_history():
    """Load the full ticker history"""
    history_file = 'T2D_Pulse_Full_Ticker_History.csv'
    
    if not os.path.exists(history_file):
        print(f"Error: {history_file} not found")
        return None
    
    ticker_df = pd.read_csv(history_file)
    ticker_df['date'] = pd.to_datetime(ticker_df['date'])
    
    return ticker_df

def calculate_sector_marketcaps(ticker_df, ticker_to_sectors):
    """Calculate accurate market caps for each sector and date"""
    if ticker_df is None or ticker_to_sectors is None:
        return None
    
    # Expand the ticker data to account for tickers in multiple sectors
    sector_data = []
    
    for _, row in ticker_df.iterrows():
        ticker = row['ticker']
        date = row['date']
        market_cap = row['market_cap']
        
        if ticker in ticker_to_sectors:
            sectors = ticker_to_sectors[ticker]
            for sector in sectors:
                sector_data.append({
                    'date': date,
                    'ticker': ticker,
                    'sector': sector,
                    'market_cap': market_cap
                })
    
    # Create a DataFrame from the expanded data
    sector_df = pd.DataFrame(sector_data)
    
    # Calculate the total market cap for each sector and date
    sector_totals = sector_df.groupby(['date', 'sector'])['market_cap'].sum().reset_index()
    
    # Pivot to get sectors as columns
    pivot_df = sector_totals.pivot(index='date', columns='sector', values='market_cap')
    
    # Convert to billions for readability
    pivot_billions = pivot_df / 1e9
    
    return pivot_billions

def update_historical_tables(sector_marketcaps):
    """Update the historical market cap tables with the correct data"""
    if sector_marketcaps is None:
        return False
    
    # Save to CSV
    sector_marketcaps.to_csv('corrected_sector_market_caps.csv')
    
    # Also create a formatted text table
    with open('corrected_sector_market_cap_table.txt', 'w') as f:
        f.write("CORRECTED 30-DAY MARKET CAP HISTORY FOR ALL SECTORS (BILLIONS USD)\n")
        f.write("=" * 100 + "\n\n")
        
        # Format the DataFrame for display
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', 1000)
        pd.set_option('display.float_format', '${:,.2f}B'.format)
        
        # Write the data
        f.write(sector_marketcaps.to_string())
        
        # Add summary statistics
        f.write("\n\n" + "=" * 100 + "\n")
        f.write("SECTOR MARKET CAP SUMMARY (MOST RECENT DATE)\n")
        f.write("-" * 100 + "\n")
        
        # Get the most recent date
        latest_date = sector_marketcaps.index.max()
        latest_data = sector_marketcaps.loc[latest_date]
        
        # Sort sectors by market cap
        sorted_sectors = latest_data.sort_values(ascending=False)
        
        for sector, market_cap in sorted_sectors.items():
            f.write(f"{sector:30s} {market_cap:15,.2f}B\n")
    
    print(f"Saved corrected market cap data to:")
    print(f"  - corrected_sector_market_caps.csv")
    print(f"  - corrected_sector_market_cap_table.txt")
    
    # Special attention to AI Infrastructure
    if 'AI Infrastructure' in sector_marketcaps.columns:
        ai_data = sector_marketcaps['AI Infrastructure']
        min_val = ai_data.min()
        max_val = ai_data.max()
        avg_val = ai_data.mean()
        latest_val = ai_data.iloc[-1]
        
        print(f"\nAI Infrastructure Market Cap:")
        print(f"  - Latest: ${latest_val:.2f}B")
        print(f"  - Range: ${min_val:.2f}B to ${max_val:.2f}B")
        print(f"  - Average: ${avg_val:.2f}B")
    
    return True

def fix_marketplace_30day_table():
    """Update the 30day_marketcap_table.txt file with corrected data"""
    if not os.path.exists('corrected_sector_market_cap_table.txt'):
        print("Corrected data not generated yet")
        return False
    
    # Copy the corrected data to the standard output file
    with open('corrected_sector_market_cap_table.txt', 'r') as src:
        with open('30day_marketcap_table.txt', 'w') as dst:
            dst.write(src.read())
    
    print("Updated 30day_marketcap_table.txt with corrected data")
    return True

def main():
    print("Starting market cap data correction...")
    
    # Load the ticker mapping and history
    ticker_to_sectors = load_ticker_mapping()
    ticker_df = load_ticker_history()
    
    if ticker_to_sectors is None or ticker_df is None:
        print("Failed to load required data")
        return False
    
    print(f"Loaded mapping for {len(ticker_to_sectors)} tickers")
    print(f"Loaded history for {ticker_df['ticker'].nunique()} tickers across {len(ticker_df)} data points")
    
    # Calculate accurate sector market caps
    sector_marketcaps = calculate_sector_marketcaps(ticker_df, ticker_to_sectors)
    
    if sector_marketcaps is None:
        print("Failed to calculate sector market caps")
        return False
    
    print(f"Calculated market caps for {len(sector_marketcaps.columns)} sectors across {len(sector_marketcaps)} dates")
    
    # Update the historical tables
    if not update_historical_tables(sector_marketcaps):
        print("Failed to update historical tables")
        return False
    
    # Fix the 30-day market cap table
    if not fix_marketplace_30day_table():
        print("Failed to update 30day_marketcap_table.txt")
        return False
    
    print("Successfully corrected market cap data")
    return True

if __name__ == "__main__":
    main()