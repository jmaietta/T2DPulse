"""
Recalculate market cap values for all sectors using all tickers in each sector
instead of just the representative tickers.
"""

import os
import pandas as pd
from datetime import datetime
import csv

def main():
    print("Recalculating market cap values using all tickers...")
    
    # Load full ticker data with market caps
    coverage_data = pd.read_csv("T2D_Pulse_93_tickers_coverage.csv", skiprows=7)
    
    # Extract the latest date
    latest_date = coverage_data['Date'].iloc[0]
    print(f"Using data from {latest_date}")
    
    # Group by sector and calculate total market cap
    sector_totals = {}
    for sector, group in coverage_data.groupby('Sector'):
        # Filter out rows with missing market cap
        valid_data = group[group['Market Cap (M)'].notna()]
        
        # Sum market caps in millions and convert to billions
        total_market_cap_billions = valid_data['Market Cap (M)'].sum() / 1000
        sector_totals[sector] = total_market_cap_billions
        
        # Print ticker breakdown
        print(f"\n{sector}:")
        ticker_data = []
        for _, row in valid_data.iterrows():
            market_cap_billions = row['Market Cap (M)'] / 1000
            ticker_data.append((row['Ticker'], market_cap_billions))
            
        # Sort by market cap, descending
        ticker_data.sort(key=lambda x: x[1], reverse=True)
        
        # Print ticker breakdown
        for ticker, mcap in ticker_data:
            print(f"  {ticker}: ${mcap:.2f}B")
            
        print(f"  TOTAL: ${total_market_cap_billions:.2f}B")
    
    # Create CSV with corrected market caps
    output_file = "corrected_sector_market_caps.csv"
    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Sector', 'Market Cap (Billions)'])
        
        # Sort sectors alphabetically
        for sector in sorted(sector_totals.keys()):
            writer.writerow([sector, f"{sector_totals[sector]:.2f}"])
    
    print(f"\nCorrect market cap data saved to {output_file}")
    
    # Create another file with a simplified table view
    output_table = "corrected_sector_market_cap_table.txt"
    with open(output_table, 'w') as f:
        f.write("Sector Market Capitalization Data (Corrected, Values in Billions USD)\n\n")
        f.write("Sector                  Market Cap\n")
        f.write("--------------------------------------\n")
        
        # Sort sectors by market cap, descending
        sorted_sectors = sorted(sector_totals.items(), key=lambda x: x[1], reverse=True)
        for sector, market_cap in sorted_sectors:
            f.write(f"{sector:<25} ${market_cap:.2f}B\n")
            
        # Calculate total
        total = sum(sector_totals.values())
        f.write("--------------------------------------\n")
        f.write(f"TOTAL                    ${total:.2f}B\n")
    
    print(f"Formatted table saved to {output_table}")
    
    return sector_totals

if __name__ == "__main__":
    main()