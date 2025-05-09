#!/usr/bin/env python3
# ensure_complete_data.py
# -----------------------------------------------------------
# Script to ensure 100% ticker data coverage by integrating with run_daily.py

import os
import subprocess
import sys
from datetime import datetime
import pytz
import pandas as pd
import config
from collect_complete_ticker_data import collect_complete_ticker_data

def get_all_tickers():
    """Get all unique tickers from all sectors"""
    all_tickers = set()
    for sector, tickers in config.SECTORS.items():
        for ticker in tickers:
            all_tickers.add(ticker)
    return sorted(list(all_tickers))

def check_data_coverage():
    """Check if we have 100% data coverage for today"""
    # Get the current date in Eastern time (US market timezone)
    eastern = pytz.timezone('US/Eastern')
    today = datetime.now(eastern).strftime('%Y-%m-%d')
    
    # File paths
    price_file = "data/historical_ticker_prices.csv"
    marketcap_file = "data/historical_ticker_marketcap.csv"
    
    # Check if files exist
    if not os.path.exists(price_file) or not os.path.exists(marketcap_file):
        print("Missing data files, need to collect data")
        return False
    
    # Load data files
    try:
        price_df = pd.read_csv(price_file, index_col=0)
        marketcap_df = pd.read_csv(marketcap_file, index_col=0)
        
        # Check if today's data exists
        if today not in price_df.index or today not in marketcap_df.index:
            print(f"No data for today ({today}) in one or both files")
            return False
        
        # Get all tickers
        all_tickers = get_all_tickers()
        
        # Check coverage for each ticker
        missing_price = []
        missing_marketcap = []
        
        for ticker in all_tickers:
            has_price = ticker in price_df.columns and not pd.isna(price_df.loc[today, ticker])
            has_marketcap = ticker in marketcap_df.columns and not pd.isna(marketcap_df.loc[today, ticker])
            
            if not has_price:
                missing_price.append(ticker)
            
            if not has_marketcap:
                missing_marketcap.append(ticker)
        
        if missing_price or missing_marketcap:
            print(f"Missing price data for {len(missing_price)} tickers")
            print(f"Missing market cap data for {len(missing_marketcap)} tickers")
            return False
        
        # If we got here, we have 100% coverage
        print(f"Verified 100% data coverage for all {len(all_tickers)} tickers on {today}")
        return True
    
    except Exception as e:
        print(f"Error checking data coverage: {e}")
        return False

def run_collection_if_needed():
    """Run data collection if we don't have 100% coverage"""
    # Check if we have 100% coverage
    if check_data_coverage():
        print("Already have 100% data coverage, no need to collect data")
        return True
    
    # If we don't have 100% coverage, run the collection script
    print("Missing complete data coverage, running collection script...")
    return collect_complete_ticker_data()

def main():
    """Main function"""
    print("Ensuring 100% ticker data coverage...")
    success = run_collection_if_needed()
    if success:
        print("Successfully ensured 100% ticker data coverage!")
        return 0
    else:
        print("Failed to ensure 100% ticker data coverage!")
        return 1

if __name__ == "__main__":
    sys.exit(main())