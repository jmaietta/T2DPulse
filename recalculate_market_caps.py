#!/usr/bin/env python3
"""
Recalculate all market caps using fully diluted share counts.

This script fixes market cap calculations by:
1. Loading historical stock price data
2. Getting fully diluted share counts for each ticker
3. Calculating market cap = price * fully diluted shares (NOT volume)
4. Aggregating by sector for the past 30 days
"""

import os
import sys
import pandas as pd
import numpy as np
import logging
from pathlib import Path
from datetime import datetime, timedelta
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('recalculate_market_caps.log')
    ]
)

# Define directories
DATA_DIR = "data"
PRICE_HISTORY_CSV = os.path.join(DATA_DIR, "historical_ticker_prices.csv")

def import_polygon_fully_diluted_shares():
    """Import the polygon_fully_diluted_shares module"""
    try:
        sys.path.append('.')
        from polygon_fully_diluted_shares import get_fully_diluted_share_count, SHARE_COUNT_OVERRIDES
        return get_fully_diluted_share_count, SHARE_COUNT_OVERRIDES
    except ImportError as e:
        logging.error(f"Error importing polygon_fully_diluted_shares: {e}")
        return None, {}

def load_historical_prices():
    """Load historical stock price data"""
    if not os.path.exists(PRICE_HISTORY_CSV):
        logging.error(f"Price history file not found: {PRICE_HISTORY_CSV}")
        return None
    
    try:
        # Load CSV, first column should be date 
        df = pd.read_csv(PRICE_HISTORY_CSV)
        
        # Make sure date column is datetime
        date_col = df.columns[0]
        df[date_col] = pd.to_datetime(df[date_col])
        
        # Set date as index
        df = df.set_index(date_col)
        
        logging.info(f"Loaded historical prices for {len(df.columns)} tickers")
        return df
    except Exception as e:
        logging.error(f"Error loading historical prices: {e}")
        return None

def load_sector_mappings():
    """Load mapping of tickers to sectors"""
    coverage_file = "T2D_Pulse_93_tickers_coverage.csv"
    
    if not os.path.exists(coverage_file):
        logging.error(f"Coverage file not found: {coverage_file}")
        return None
    
    try:
        # Skip header rows
        df = pd.read_csv(coverage_file, skiprows=7)
        
        # Create mapping of ticker to sector
        ticker_sector_map = {}
        for _, row in df.iterrows():
            ticker = row['Ticker']
            sector = row['Sector']
            ticker_sector_map[ticker] = sector
        
        logging.info(f"Loaded sector mappings for {len(ticker_sector_map)} tickers")
        return ticker_sector_map
    except Exception as e:
        logging.error(f"Error loading sector mappings: {e}")
        return None

def calculate_market_caps():
    """Calculate market caps using fully diluted share counts"""
    # Import the get_fully_diluted_share_count function
    get_share_count, overrides = import_polygon_fully_diluted_shares()
    if get_share_count is None:
        return None
    
    # Load historical prices
    prices_df = load_historical_prices()
    if prices_df is None:
        return None
    
    # Load sector mappings
    sector_map = load_sector_mappings()
    if sector_map is None:
        return None
    
    # Calculate market cap for each ticker
    market_caps = {}
    success_count = 0
    error_count = 0
    
    for ticker in prices_df.columns:
        try:
            # Get the fully diluted share count
            shares = get_share_count(ticker)
            
            if shares is not None:
                # Calculate market cap (price * shares)
                market_cap = prices_df[ticker] * shares
                market_caps[ticker] = market_cap
                success_count += 1
                logging.info(f"Calculated market cap for {ticker} using {shares:,} shares")
            else:
                logging.warning(f"No share count available for {ticker}, skipping market cap calculation")
                error_count += 1
        except Exception as e:
            logging.error(f"Error calculating market cap for {ticker}: {e}")
            error_count += 1
    
    if not market_caps:
        logging.error("No market caps could be calculated")
        return None
    
    # Convert to DataFrame
    mcap_df = pd.DataFrame(market_caps)
    
    # Limit to last 90 days to avoid unnecessary data
    cutoff_date = datetime.now() - timedelta(days=90)
    mcap_df = mcap_df[mcap_df.index >= cutoff_date]
    
    logging.info(f"Calculated market caps for {success_count} tickers, errors: {error_count}")
    
    # Aggregate by sector
    sector_caps = aggregate_by_sector(mcap_df, sector_map)
    
    return sector_caps

def aggregate_by_sector(mcap_df, sector_map):
    """Aggregate market caps by sector"""
    # Initialize dict to store sector aggregates
    sector_aggregates = {}
    
    # Group tickers by sector
    sector_tickers = {}
    for ticker, sector in sector_map.items():
        if sector not in sector_tickers:
            sector_tickers[sector] = []
        sector_tickers[sector].append(ticker)
    
    # Aggregate market caps by sector
    for sector, tickers in sector_tickers.items():
        # Get market caps for tickers in this sector
        sector_mcaps = mcap_df[[t for t in tickers if t in mcap_df.columns]]
        
        if not sector_mcaps.empty:
            # Sum across tickers for each date
            sector_aggregates[sector] = sector_mcaps.sum(axis=1)
            logging.info(f"Aggregated market cap for {sector} from {len(sector_mcaps.columns)} tickers")
        else:
            logging.warning(f"No market cap data for sector: {sector}")
    
    # Convert to DataFrame
    sector_df = pd.DataFrame(sector_aggregates)
    
    # Calculate total across all sectors
    sector_df['Total'] = sector_df.sum(axis=1)
    
    # Add weight columns
    for sector in sector_df.columns:
        if sector != 'Total':
            sector_df[f"{sector}_weight_pct"] = (sector_df[sector] / sector_df['Total']) * 100
    
    return sector_df

def create_30day_market_cap_table(sector_caps):
    """Create a 30-day market cap table for display"""
    if sector_caps is None:
        logging.error("No sector market cap data available")
        return None
    
    # Reset index to make date a column
    df = sector_caps.reset_index()
    
    # Rename index column to 'Date'
    df = df.rename(columns={df.columns[0]: 'Date'})
    
    # Get the last 30 days of data
    cutoff_date = datetime.now() - timedelta(days=30)
    df = df[df['Date'] >= cutoff_date]
    
    # Sort by date, most recent first
    df = df.sort_values('Date', ascending=False)
    
    # Format dates as YYYY-MM-DD
    df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
    
    # Keep only sector columns (not weights)
    keep_cols = ['Date'] + [col for col in df.columns if not col.endswith('_weight_pct') and col != 'Total']
    df = df[keep_cols]
    
    # Convert to trillions for readable display
    for col in df.columns:
        if col != 'Date':
            df[col] = df[col] / 1_000_000_000_000
            # Format with 2 decimal places and 'T' suffix
            df[col] = df[col].apply(lambda x: f"{x:.2f}T")
    
    return df

def save_results(sector_caps, table_df):
    """Save the calculated market caps to various files"""
    if sector_caps is None:
        return False
    
    # Ensure data directory exists
    os.makedirs('data', exist_ok=True)
    
    try:
        # Save full sector market caps
        parquet_file = os.path.join(DATA_DIR, "sector_market_caps.parquet")
        sector_caps.to_parquet(parquet_file)
        logging.info(f"Saved sector market caps to {parquet_file}")
        
        # Also save as CSV
        csv_file = os.path.join(DATA_DIR, "sector_market_caps.csv")
        sector_caps.to_csv(csv_file)
        logging.info(f"Saved sector market caps to {csv_file}")
        
        # Save 30-day table if available
        if table_df is not None:
            # Save to CSV with today's date
            today = datetime.now().strftime('%Y-%m-%d')
            
            # Save dated version
            dated_csv = os.path.join(DATA_DIR, f"sector_marketcap_30day_table_{today}.csv")
            table_df.to_csv(dated_csv, index=False)
            logging.info(f"Saved 30-day market cap table to {dated_csv}")
            
            # Save dated Excel
            dated_excel = os.path.join(DATA_DIR, f"sector_marketcap_30day_table_{today}.xlsx")
            table_df.to_excel(dated_excel, index=False)
            logging.info(f"Saved 30-day market cap table to {dated_excel}")
            
            # Save standard version
            std_csv = os.path.join(DATA_DIR, "sector_marketcap_30day_table.csv")
            table_df.to_csv(std_csv, index=False)
            logging.info(f"Saved 30-day market cap table to {std_csv}")
            
            # Save standard Excel
            std_excel = os.path.join(DATA_DIR, "sector_marketcap_30day_table.xlsx")
            table_df.to_excel(std_excel, index=False)
            logging.info(f"Saved 30-day market cap table to {std_excel}")
        
        return True
    except Exception as e:
        logging.error(f"Error saving results: {e}")
        return False

def main():
    """Main function"""
    print("Recalculating all market caps using fully diluted share counts...")
    
    # Calculate market caps
    sector_caps = calculate_market_caps()
    
    if sector_caps is None:
        print("ERROR: Failed to calculate market caps. Check logs for details.")
        return False
    
    # Create 30-day table
    table_df = create_30day_market_cap_table(sector_caps)
    
    # Save results
    if save_results(sector_caps, table_df):
        print("SUCCESS: Market caps recalculated successfully!")
        
        # Print the 30-day table
        if table_df is not None:
            print("\n===== 30-Day Market Cap Table (in Trillions USD) =====")
            print(table_df.head(10).to_string(index=False))
            print("...\n")
        
        return True
    else:
        print("ERROR: Failed to save results. Check logs for details.")
        return False

if __name__ == "__main__":
    main()