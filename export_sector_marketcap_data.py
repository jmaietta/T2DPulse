#!/usr/bin/env python3
"""
Export authentic sector market cap data to a more readable format
for verification and analysis.
"""

import pandas as pd
import os
import glob
from datetime import datetime, timedelta
import numpy as np

def format_market_cap(value):
    """Format market cap values in billions with 2 decimal places"""
    return f"${value/1e9:.2f}B"

def get_latest_sector_data():
    """Get the latest sector market cap data from CSV files"""
    
    # First try the main authentic data file
    if os.path.exists('authentic_sector_market_caps.csv'):
        sector_totals = pd.read_csv('authentic_sector_market_caps.csv')
        print(f"Loaded sector totals from authentic_sector_market_caps.csv: {len(sector_totals)} sectors")
        return sector_totals
    
    # Otherwise try to find historical data
    print("Looking for sector history files...")
    history_files = glob.glob('*sector*history*.csv')
    
    if not history_files:
        print("No sector history files found")
        return None
    
    print(f"Found {len(history_files)} history files")
    latest_file = max(history_files, key=os.path.getctime)
    print(f"Using most recent file: {latest_file}")
    
    history_df = pd.read_csv(latest_file)
    
    if 'date' not in history_df.columns and 'Date' not in history_df.columns:
        print(f"Invalid format in {latest_file} - no date column found")
        return None
    
    # Get the most recent date's data
    date_col = 'date' if 'date' in history_df.columns else 'Date'
    latest_date = history_df[date_col].max()
    latest_data = history_df[history_df[date_col] == latest_date]
    
    print(f"Using data from {latest_date}")
    return latest_data

def get_sector_history():
    """Get historical market cap data for each sector"""
    
    # Try to find comprehensive historical data
    if os.path.exists('T2D_Pulse_Full_Ticker_History.csv'):
        print("Loading full ticker history...")
        history_df = pd.read_csv('T2D_Pulse_Full_Ticker_History.csv')
        # Process this file to get sector-level history
        # This requires sector mapping data
        
    # Otherwise look for pre-computed sector history
    sector_files = [f for f in glob.glob('*sector*market*cap*.csv') if 'history' in f.lower()]
    
    if sector_files:
        latest_file = max(sector_files, key=os.path.getctime)
        print(f"Using sector history from: {latest_file}")
        sector_history = pd.read_csv(latest_file)
        return sector_history
    
    # If all else fails, try to find daily data in the logs
    print("Looking for sector history in logs...")
    log_files = glob.glob('*sector*.log')
    
    if log_files:
        # Extract data from logs (implementation would depend on log format)
        pass
    
    return None

def create_report():
    """Create a comprehensive sector market cap report"""
    
    # Get the latest sector data
    sector_totals = get_latest_sector_data()
    
    if sector_totals is None:
        print("Failed to load sector data")
        return
    
    # Try to get historical data for trends
    sector_history = get_sector_history()
    
    # Prepare the report
    report = []
    report.append("# T2D Pulse Sector Market Cap Report")
    report.append(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append("\n## Current Sector Market Caps")
    
    # Format current sector data
    if 'Sector' in sector_totals.columns and 'Market Cap (Billions USD)' in sector_totals.columns:
        # Data is in the expected format from authentic_sector_market_caps.csv
        market_cap_col = 'Market Cap (Billions USD)'
        sector_totals_sorted = sector_totals.sort_values(by=market_cap_col, ascending=False)
        
        table = []
        table.append("| Rank | Sector | Market Cap | % of Total |")
        table.append("|------|--------|------------|------------|")
        
        total_market_cap = sector_totals_sorted[market_cap_col].sum()
        
        for i, (_, row) in enumerate(sector_totals_sorted.iterrows()):
            sector = row['Sector']
            market_cap = row[market_cap_col]
            pct = market_cap / total_market_cap * 100
            
            table.append(f"| {i+1} | {sector} | ${market_cap:.2f}B | {pct:.2f}% |")
        
        report.extend(table)
        
    else:
        # Format might be different, adapt accordingly
        report.append("Sector data format not recognized")
    
    # Export all data to a CSV file
    output_file = "sector_market_cap_report.csv"
    
    if sector_history is not None:
        # If we have historical data, save it to the CSV
        sector_history.to_csv(output_file, index=False)
        report.append(f"\nHistorical data saved to {output_file}")
    
    # Write the report to a file
    with open("sector_market_cap_report.md", "w") as f:
        f.write("\n".join(report))
    
    # Also create a plain text version with just the market cap data
    with open("sector_market_caps_plain.txt", "w") as f:
        f.write("SECTOR MARKET CAPS (Billions USD)\n")
        f.write("-------------------------------\n")
        
        if 'Sector' in sector_totals.columns and 'Market Cap (Billions USD)' in sector_totals.columns:
            for _, row in sector_totals_sorted.iterrows():
                sector = row['Sector']
                market_cap = row[market_cap_col]
                f.write(f"{sector}: ${market_cap:.2f}B\n")
    
    print(f"Report generated: sector_market_cap_report.md")
    
    # Display the report content
    print("\n".join(report))

if __name__ == "__main__":
    create_report()