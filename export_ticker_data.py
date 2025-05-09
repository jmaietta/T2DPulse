#!/usr/bin/env python3
# export_ticker_data.py
# -----------------------------------------------------------
# Script to export all ticker data to a formatted CSV file

import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys

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
    print(f"Exporting data for date: {latest_date}")
    
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
        
        # Add row to export data
        export_data.append({
            'Date': latest_date,
            'Ticker': ticker,
            'Sector': sector,
            'Price': price,
            'Market Cap': mcap,
            'Data Status': 'Complete' if (pd.notna(price) and pd.notna(mcap)) else 'Missing'
        })
    
    # Create DataFrame from export data
    export_df = pd.DataFrame(export_data)
    
    # Calculate coverage statistics
    total_tickers = len(export_df)
    complete_tickers = len(export_df[export_df['Data Status'] == 'Complete'])
    coverage_pct = complete_tickers / total_tickers * 100
    
    print(f"Coverage: {complete_tickers}/{total_tickers} tickers ({coverage_pct:.1f}%)")
    
    # Sort by sector and ticker
    export_df = export_df.sort_values(['Sector', 'Ticker'])
    
    # Format market cap as millions
    export_df['Market Cap (M)'] = export_df['Market Cap'].apply(lambda x: x / 1000000 if pd.notna(x) else np.nan)
    
    # Save to CSV
    export_df.to_csv(output_file, index=False)
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
    
    # Sort by coverage percentage
    coverage.sort(key=lambda x: x[3])
    
    for sector, complete, total, cov_pct in coverage:
        print(f"{sector}: {complete}/{total} ({cov_pct:.1f}%)")
    
    return True

if __name__ == "__main__":
    # Get output filename from command line args, or use default
    output_file = sys.argv[1] if len(sys.argv) > 1 else 'ticker_data_export.csv'
    
    success = create_formatted_export(output_file)
    sys.exit(0 if success else 1)