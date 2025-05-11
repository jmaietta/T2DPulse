#!/usr/bin/env python3
"""
Export and display the complete market cap data for all sectors over the past 30 days.
"""

import pandas as pd
import os
from datetime import datetime, timedelta
import numpy as np

def format_market_cap(value):
    """Format market cap in billions with 2 decimal places"""
    if pd.isna(value):
        return "N/A"
    return f"${value/1e9:.2f}B"

def main():
    print("T2D PULSE - COMPLETE HISTORICAL MARKET CAP DATA")
    print("=" * 80)
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("-" * 80)
    
    # Load the full ticker history with complete market cap data
    ticker_file = 'T2D_Pulse_Full_Ticker_History.csv'
    mapping_file = 'T2D_Pulse_93_tickers_coverage.csv'
    
    if not os.path.exists(ticker_file):
        print(f"Error: {ticker_file} not found")
        return
        
    if not os.path.exists(mapping_file):
        print(f"Error: {mapping_file} not found")
        return
    
    # Load the data
    ticker_df = pd.read_csv(ticker_file)
    mapping_df = pd.read_csv(mapping_file, skiprows=7)  # Skip the header rows and empty line
    
    print(f"Loaded {len(ticker_df)} market cap data points for {ticker_df['ticker'].nunique()} tickers")
    print(f"Loaded sector mappings for {mapping_df.shape[0]} rows across {mapping_df['Sector'].nunique()} sectors")
    
    # Convert dates to datetime
    ticker_df['date'] = pd.to_datetime(ticker_df['date'])
    mapping_df['Date'] = pd.to_datetime(mapping_df['Date'])
    
    # Create a mapping dictionary from ticker to sector
    ticker_to_sector = {}
    for _, row in mapping_df.iterrows():
        ticker = row['Ticker']  # Column name from the CSV
        sector = row['Sector']  # Column name from the CSV
        if ticker not in ticker_to_sector:
            ticker_to_sector[ticker] = []
        ticker_to_sector[ticker].append(sector)
    
    # Add sector column to ticker_df, handling tickers that belong to multiple sectors
    ticker_sectors = []
    for _, row in ticker_df.iterrows():
        ticker = row['ticker']
        if ticker in ticker_to_sector:
            sectors = ticker_to_sector[ticker]
            for sector in sectors:
                ticker_sectors.append({
                    'date': row['date'],
                    'ticker': ticker,
                    'sector': sector,
                    'market_cap': row['market_cap']
                })
    
    # Create a new DataFrame with the sector information
    sector_df = pd.DataFrame(ticker_sectors)
    
    # Calculate the total market cap by sector and date
    sector_totals = sector_df.groupby(['date', 'sector'])['market_cap'].sum().reset_index()
    
    # Pivot to get sectors as columns
    pivot_df = sector_totals.pivot(index='date', columns='sector', values='market_cap')
    
    # Sort by date
    pivot_df = pivot_df.sort_index()
    
    # Calculate sum of all sectors by date
    pivot_df['Total'] = pivot_df.sum(axis=1)
    
    # Convert to billions for readability
    pivot_billions = pivot_df / 1e9
    
    # Save the detailed data to CSV
    pivot_billions.to_csv('sector_market_cap_history_detailed.csv')
    
    # Display 30-day data
    latest_date = pivot_df.index.max()
    thirty_days_ago = latest_date - timedelta(days=30)
    recent_data = pivot_billions.loc[pivot_billions.index >= thirty_days_ago]
    
    # Display the data
    print("\nMARKET CAP BY SECTOR (IN BILLIONS USD) - LAST 30 DAYS:")
    print("-" * 80)
    
    # Format the data for display with consistent columns
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    pd.set_option('display.float_format', '${:,.2f}B'.format)
    
    print(recent_data)
    
    # Get the most recent date's data for a sector breakdown
    latest_data = pivot_billions.loc[latest_date]
    
    print("\nLATEST SECTOR MARKET CAPS (IN BILLIONS USD):")
    print("-" * 80)
    
    # Sort sectors by market cap
    sorted_sectors = latest_data.sort_values(ascending=False)
    
    for sector, market_cap in sorted_sectors.items():
        if sector != 'Total':
            pct = (market_cap / latest_data['Total']) * 100
            print(f"{sector}: {market_cap:.2f}B ({pct:.2f}%)")
    
    print(f"Total: {latest_data['Total']:.2f}B")
    
    # Calculate and display growth rates
    print("\nSECTOR GROWTH RATES (LAST 30 DAYS):")
    print("-" * 80)
    
    earliest_date = recent_data.index.min()
    earliest_data = pivot_billions.loc[earliest_date]
    
    for sector in pivot_df.columns:
        if sector == 'Total':
            continue
            
        if pd.notna(earliest_data[sector]) and earliest_data[sector] > 0:
            growth = ((latest_data[sector] / earliest_data[sector]) - 1) * 100
            print(f"{sector}: {growth:.2f}%")
        else:
            print(f"{sector}: N/A")
    
    # Save the complete report to a text file
    output_file = 'sector_market_cap_full_report.txt'
    with open(output_file, 'w') as f:
        f.write("T2D PULSE - COMPLETE HISTORICAL MARKET CAP DATA\n")
        f.write("=" * 80 + "\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("-" * 80 + "\n\n")
        
        f.write("MARKET CAP BY SECTOR (IN BILLIONS USD) - LAST 30 DAYS:\n")
        f.write("-" * 80 + "\n")
        f.write(recent_data.to_string() + "\n\n")
        
        f.write("LATEST SECTOR MARKET CAPS (IN BILLIONS USD):\n")
        f.write("-" * 80 + "\n")
        
        for sector, market_cap in sorted_sectors.items():
            if sector != 'Total':
                pct = (market_cap / latest_data['Total']) * 100
                f.write(f"{sector}: {market_cap:.2f}B ({pct:.2f}%)\n")
        
        f.write(f"Total: {latest_data['Total']:.2f}B\n\n")
        
        f.write("SECTOR GROWTH RATES (LAST 30 DAYS):\n")
        f.write("-" * 80 + "\n")
        
        for sector in pivot_df.columns:
            if sector == 'Total':
                continue
                
            if pd.notna(earliest_data[sector]) and earliest_data[sector] > 0:
                growth = ((latest_data[sector] / earliest_data[sector]) - 1) * 100
                f.write(f"{sector}: {growth:.2f}%\n")
            else:
                f.write(f"{sector}: N/A\n")
    
    print(f"\nFull report saved to {output_file}")
    print(f"Detailed data saved to sector_market_cap_history_detailed.csv")

if __name__ == "__main__":
    main()