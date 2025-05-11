#!/usr/bin/env python3
"""
Generate a sector market cap report for the last 30 days
"""

import pandas as pd
import os
from datetime import datetime

def format_marketcap(value):
    """Format market cap value in trillions or billions as appropriate"""
    if value >= 1e12:
        return f"${value/1e12:.2f}T"
    else:
        return f"${value/1e9:.2f}B"

def main():
    # Load sector market cap data
    df = pd.read_csv('sector_market_caps.csv')
    
    # Get unique sectors and dates
    sectors = sorted(df['sector'].unique())
    dates = sorted(df['date'].unique())
    
    print("======================= T2D PULSE SECTOR MARKET CAP REPORT =======================")
    print(f"Report generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Data timespan: {dates[0]} to {dates[-1]}")
    print("===============================================================================\n")
    
    # Print the latest market cap for each sector
    print("LATEST SECTOR MARKET CAPS:\n")
    latest_date = dates[-1]
    latest_data = df[df['date'] == latest_date].sort_values(by='market_cap', ascending=False)
    
    for _, row in latest_data.iterrows():
        sector = row['sector']
        market_cap = row['market_cap']
        print(f"{sector:<25}: {format_marketcap(market_cap)}")
    
    print("\n30-DAY HISTORY FOR KEY SECTORS:\n")
    
    # Key sectors to highlight
    key_sectors = ['AI Infrastructure', 'Cloud Infrastructure', 'Enterprise SaaS', 'Semiconductors', 'Consumer Internet']
    
    for sector in key_sectors:
        sector_data = df[df['sector'] == sector].sort_values('date')
        if len(sector_data) == 0:
            print(f"No data available for {sector}")
            continue
            
        print(f"{sector} 30-Day Market Cap History:")
        
        # Calculate first and last values to show percentage change
        first_cap = None
        last_cap = None
        
        # Print the data for every 3rd date (for brevity)
        for i, (_, row) in enumerate(sector_data.iterrows()):
            if i == 0:
                first_cap = row['market_cap']
            if i == len(sector_data) - 1:
                last_cap = row['market_cap']
                
            date = row['date']
            cap = row['market_cap']
            
            # Print only every 3rd date for brevity, but always print first and last
            if i % 3 == 0 or i == len(sector_data) - 1:
                print(f"  {date}: {format_marketcap(cap)}")
        
        # Calculate percent change
        if first_cap and last_cap:
            pct_change = (last_cap - first_cap) / first_cap * 100
            print(f"  30-Day Change: {pct_change:.1f}%\n")
        else:
            print("  Insufficient data to calculate change\n")
    
    # Summary table for all sectors on key dates
    print("\nSECTOR MARKET CAP SUMMARY TABLE (in Trillions):\n")
    
    # Choose dates (first, middle, last)
    if len(dates) >= 3:
        display_dates = [dates[0], dates[len(dates)//2], dates[-1]]
    else:
        display_dates = dates
    
    # Print header
    print(f"{'Sector':<25}", end="")
    for date in display_dates:
        print(f"{date:>12}", end="")
    print()
    
    print("-" * 25, end="")
    for _ in display_dates:
        print("-" * 12, end="")
    print()
    
    # Print data for each sector
    for sector in sectors:
        sector_data = df[df['sector'] == sector]
        print(f"{sector:<25}", end="")
        
        for date in display_dates:
            date_data = sector_data[sector_data['date'] == date]
            if len(date_data) > 0:
                value = date_data['market_cap'].values[0]
                if value >= 1e12:
                    print(f"{value/1e12:>12.2f}", end="")
                else:
                    print(f"{value/1e9:>12.2f}B", end="")
            else:
                print(f"{'N/A':>12}", end="")
        print()

if __name__ == "__main__":
    main()