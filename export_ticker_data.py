#!/usr/bin/env python3
# export_ticker_data.py
# -----------------------------------------------------------
# Script to export all ticker data to a formatted CSV file

import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
import time

def get_official_tickers():
    """Get the complete list of official tickers"""
    tickers = []
    try:
        with open('official_tickers.csv', 'r') as f:
            for line in f:
                ticker = line.strip()
                if ticker:
                    tickers.append(ticker)
        return tickers
    except FileNotFoundError:
        print("Error: official_tickers.csv not found")
        return []

def load_ticker_data():
    """Load the historical ticker price and market cap data"""
    try:
        # Load data files
        price_df = pd.read_csv('data/historical_ticker_prices.csv', index_col=0)
        mcap_df = pd.read_csv('data/historical_ticker_marketcap.csv', index_col=0)
        
        return price_df, mcap_df
    except Exception as e:
        print(f"Error loading ticker data: {e}")
        return None, None

def get_sector_mapping():
    """Get mapping of tickers to sectors from config"""
    try:
        import config
        
        sector_mapping = {}
        for sector, tickers in config.SECTORS.items():
            for ticker in tickers:
                sector_mapping[ticker] = sector
        
        return sector_mapping
    except ImportError:
        print("Warning: Could not import config.py, sector mapping will not be available")
        return {}

def create_formatted_export(output_file='ticker_data_export.csv'):
    """Create a formatted export of all ticker data"""
    # Get official tickers
    tickers = get_official_tickers()
    if not tickers:
        print("Error: No official tickers found")
        return False
    
    # Load ticker data
    price_df, mcap_df = load_ticker_data()
    if price_df is None or mcap_df is None:
        print("Error: Failed to load ticker data")
        return False
    
    # Get sector mapping
    sector_mapping = get_sector_mapping()
    
    # Get latest date
    latest_date = price_df.index[-1]
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"Exporting data for date: {latest_date} (Generated: {timestamp})")
    
    # Create a new dataframe for the export
    export_data = []
    
    # For each ticker, add a row to the export
    for ticker in tickers:
        # Get price and market cap
        price = np.nan
        mcap = np.nan
        
        try:
            if ticker in price_df.columns:
                price = price_df.loc[latest_date, ticker]
        except Exception as e:
            print(f"Error getting price for {ticker}: {e}")
            
        try:
            if ticker in mcap_df.columns:
                mcap = mcap_df.loc[latest_date, ticker]
        except Exception as e:
            print(f"Error getting market cap for {ticker}: {e}")
        
        # Get sector
        sector = sector_mapping.get(ticker, "Unknown")
        
        # Determine data status
        if pd.notna(price) and pd.notna(mcap):
            status = 'Complete'
        elif pd.notna(price) and pd.isna(mcap):
            status = 'Missing Market Cap'
        elif pd.isna(price) and pd.notna(mcap):
            status = 'Missing Price'
        else:
            status = 'Missing All Data'
        
        # Add row to export data
        export_data.append({
            'Date': latest_date,
            'Ticker': ticker,
            'Sector': sector,
            'Price': price,
            'Market Cap': mcap,
            'Data Status': status
        })
    
    # Create DataFrame from export data
    export_df = pd.DataFrame(export_data)
    
    # Calculate coverage statistics
    total_tickers = len(export_df)
    complete_tickers = len(export_df[export_df['Data Status'] == 'Complete'])
    coverage_pct = complete_tickers / total_tickers * 100
    
    # Count sectors with 100% coverage
    sector_totals = {}
    sector_complete = {}
    
    for _, row in export_df.iterrows():
        sector = row['Sector']
        sector_totals[sector] = sector_totals.get(sector, 0) + 1
        if row['Data Status'] == 'Complete':
            sector_complete[sector] = sector_complete.get(sector, 0) + 1
    
    complete_sectors = 0
    total_sectors = len(sector_totals)
    
    for sector, total in sector_totals.items():
        if sector_complete.get(sector, 0) == total:
            complete_sectors += 1
    
    print(f"Overall Coverage: {complete_tickers}/{total_tickers} tickers ({coverage_pct:.1f}%)")
    print(f"Sectors at 100%: {complete_sectors}/{total_sectors} sectors ({complete_sectors/total_sectors*100:.1f}%)")
    
    # Sort by sector and ticker
    export_df = export_df.sort_values(['Sector', 'Ticker'])
    
    # Format market cap as millions
    export_df['Market Cap (M)'] = export_df['Market Cap'].apply(lambda x: x / 1000000 if pd.notna(x) else np.nan)
    
    # Add summary information at the start of the CSV
    # Create a metadata dataframe with summary information
    metadata = pd.DataFrame([
        {'Summary': f"T2D Pulse Ticker Coverage Report"},
        {'Summary': f"Generated: {timestamp}"},
        {'Summary': f"Data Date: {latest_date}"},
        {'Summary': f"Overall Coverage: {complete_tickers}/{total_tickers} tickers ({coverage_pct:.1f}%)"},
        {'Summary': f"Sectors at 100%: {complete_sectors}/{total_sectors} sectors ({complete_sectors/total_sectors*100:.1f}%)"},
        {'Summary': ""}
    ])
    
    # Save metadata and main data to CSV
    metadata.to_csv(output_file, index=False)
    export_df.to_csv(output_file, index=False, mode='a')
    
    print(f"Exported ticker data to {output_file}")
    
    # Print sector coverage
    print("\nSector Coverage:")
    sector_stats = export_df.groupby('Sector').agg(
        Total=('Ticker', 'count'),
        Complete=('Data Status', lambda x: (x == 'Complete').sum())
    )
    
    # Calculate coverage percentage
    coverage = []
    for sector, row in sector_stats.iterrows():
        cov_pct = row['Complete'] / row['Total'] * 100
        coverage.append((sector, row['Complete'], row['Total'], cov_pct))
    
    # Sort by coverage percentage (ascending)
    coverage.sort(key=lambda x: x[3])
    
    # Create a list of missing tickers by sector for documentation
    missing_by_sector = {}
    for _, row in export_df.iterrows():
        if row['Data Status'] != 'Complete':
            sector = row['Sector']
            if sector not in missing_by_sector:
                missing_by_sector[sector] = []
            missing_by_sector[sector].append(row['Ticker'])
    
    # Print sector coverage
    for sector, complete, total, cov_pct in coverage:
        print(f"{sector}: {complete}/{total} ({cov_pct:.1f}%)")
        if sector in missing_by_sector and len(missing_by_sector[sector]) > 0:
            print(f"  Missing: {', '.join(missing_by_sector[sector])}")
    
    return True

if __name__ == "__main__":
    # Get output filename from command line args, or use default
    output_file = sys.argv[1] if len(sys.argv) > 1 else 'ticker_data_export.csv'
    
    success = create_formatted_export(output_file)
    sys.exit(0 if success else 1)