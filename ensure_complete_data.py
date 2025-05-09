#!/usr/bin/env python3
# ensure_complete_data.py
# -----------------------------------------------------------
# Script to check and ensure complete ticker data coverage
# Intended to be called by run_daily.py during daily updates

import os
import sys
import pandas as pd
import numpy as np
import time
import config
from collect_complete_ticker_data import fetch_ticker_data

# File paths
DATA_DIR = "data"
PRICE_FILE = os.path.join(DATA_DIR, "historical_ticker_prices.csv")
MARKETCAP_FILE = os.path.join(DATA_DIR, "historical_ticker_marketcap.csv")

def load_data():
    """Load the price and market cap data"""
    if not os.path.exists(PRICE_FILE) or not os.path.exists(MARKETCAP_FILE):
        print("Error: Historical ticker data files not found.")
        return None, None
    
    price_df = pd.read_csv(PRICE_FILE, index_col=0)
    marketcap_df = pd.read_csv(MARKETCAP_FILE, index_col=0)
    return price_df, marketcap_df

def get_sector_coverage(date=None):
    """Get the coverage for each sector for a specific date
    
    Args:
        date (str, optional): The date to check coverage for (e.g., '2025-05-09')
                              If None, use the latest date in the data
    
    Returns:
        dict: Dictionary with coverage information for each sector
    """
    price_df, marketcap_df = load_data()
    if price_df is None or marketcap_df is None:
        return {}
    
    if date is None:
        date = price_df.index[-1]  # Use latest date
    
    coverage = {}
    for sector, tickers in config.SECTORS.items():
        # Initialize counters
        total_tickers = len(tickers)
        price_coverage = 0
        marketcap_coverage = 0
        missing_tickers = []
        
        for ticker in tickers:
            has_price = False
            has_marketcap = False
            
            if ticker in price_df.columns:
                if date in price_df.index and not pd.isna(price_df.loc[date, ticker]):
                    price_coverage += 1
                    has_price = True
            
            if ticker in marketcap_df.columns:
                if date in marketcap_df.index and not pd.isna(marketcap_df.loc[date, ticker]):
                    marketcap_coverage += 1
                    has_marketcap = True
            
            if not (has_price and has_marketcap):
                missing_tickers.append(ticker)
        
        # Calculate coverage percentages
        price_pct = price_coverage / total_tickers * 100
        marketcap_pct = marketcap_coverage / total_tickers * 100
        
        coverage[sector] = {
            "total_tickers": total_tickers,
            "price_coverage": price_coverage,
            "price_pct": price_pct,
            "marketcap_coverage": marketcap_coverage,
            "marketcap_pct": marketcap_pct,
            "missing_tickers": missing_tickers
        }
    
    return coverage

def ensure_complete_data(date=None, max_per_sector=None):
    """Ensure complete data coverage for all tickers for a specific date
    
    Args:
        date (str, optional): The date to check coverage for (e.g., '2025-05-09')
                              If None, use the latest date in the data
        max_per_sector (int, optional): Maximum tickers to process per sector
                                        If None, process all missing tickers
    
    Returns:
        bool: True if all tickers have data after processing, False otherwise
    """
    price_df, marketcap_df = load_data()
    if price_df is None or marketcap_df is None:
        print("Error loading data. Aborting.")
        return False
    
    if date is None:
        date = price_df.index[-1]  # Use latest date
    
    print(f"Ensuring complete data for {date}...")
    
    # Get coverage before we start
    initial_coverage = get_sector_coverage(date)
    total_missing = sum(len(data["missing_tickers"]) for _, data in initial_coverage.items())
    
    # Calculate overall coverage percentage
    total_tickers = sum(data["total_tickers"] for _, data in initial_coverage.items())
    total_price_coverage = sum(data["price_coverage"] for _, data in initial_coverage.items())
    total_marketcap_coverage = sum(data["marketcap_coverage"] for _, data in initial_coverage.items())
    
    if total_tickers == 0:
        print("No tickers found in configuration. Aborting.")
        return False
    
    overall_price_pct = total_price_coverage / total_tickers * 100
    overall_marketcap_pct = total_marketcap_coverage / total_tickers * 100
    
    print(f"Initial coverage: {total_price_coverage}/{total_tickers} price ({overall_price_pct:.1f}%), "
          f"{total_marketcap_coverage}/{total_tickers} market cap ({overall_marketcap_pct:.1f}%)")
    print(f"Total missing tickers: {total_missing}")
    
    # If we already have 100% coverage, we're done
    if total_missing == 0:
        print("Already have 100% coverage for all tickers.")
        return True
    
    # Process each sector
    for sector, data in sorted(initial_coverage.items(), 
                               key=lambda x: (x[1]["price_pct"] + x[1]["marketcap_pct"]) / 2):
        missing_tickers = data["missing_tickers"]
        if not missing_tickers:
            print(f"Sector {sector} already has 100% coverage.")
            continue
        
        print(f"\n=== Processing {sector} ===")
        print(f"Found {len(missing_tickers)} tickers with missing data.")
        
        # Limit number of tickers if specified
        if max_per_sector and max_per_sector < len(missing_tickers):
            print(f"Limiting to {max_per_sector} tickers")
            missing_tickers = missing_tickers[:max_per_sector]
        
        # Process each missing ticker
        for i, ticker in enumerate(missing_tickers):
            print(f"\n[{i+1}/{len(missing_tickers)}] Processing {ticker}...")
            
            # Check if the ticker is in both dataframes
            if ticker not in price_df.columns:
                print(f"Warning: Ticker {ticker} not found in price data. Adding column.")
                price_df[ticker] = np.nan
            
            if ticker not in marketcap_df.columns:
                print(f"Warning: Ticker {ticker} not found in market cap data. Adding column.")
                marketcap_df[ticker] = np.nan
            
            # Fetch the data
            try:
                ticker_data = fetch_ticker_data(ticker)
                
                if ticker_data["price"] is not None:
                    price = ticker_data["price"]
                    print(f"  Got price: {price}")
                    price_df.loc[date, ticker] = price
                
                if ticker_data["market_cap"] is not None:
                    market_cap = ticker_data["market_cap"]
                    print(f"  Got market cap: {market_cap}")
                    marketcap_df.loc[date, ticker] = market_cap
                
                # Save after each ticker to avoid losing data
                price_df.to_csv(PRICE_FILE)
                marketcap_df.to_csv(MARKETCAP_FILE)
                print("  Saved data.")
                
                # Pause to avoid rate limiting
                time.sleep(1)
                
            except Exception as e:
                print(f"  Error fetching data for {ticker}: {e}")
    
    # Check final coverage
    final_coverage = get_sector_coverage(date)
    total_missing = sum(len(data["missing_tickers"]) for _, data in final_coverage.items())
    
    # Calculate overall coverage percentage
    total_tickers = sum(data["total_tickers"] for _, data in final_coverage.items())
    total_price_coverage = sum(data["price_coverage"] for _, data in final_coverage.items())
    total_marketcap_coverage = sum(data["marketcap_coverage"] for _, data in final_coverage.items())
    
    overall_price_pct = total_price_coverage / total_tickers * 100
    overall_marketcap_pct = total_marketcap_coverage / total_tickers * 100
    
    print(f"\nFinal coverage: {total_price_coverage}/{total_tickers} price ({overall_price_pct:.1f}%), "
          f"{total_marketcap_coverage}/{total_tickers} market cap ({overall_marketcap_pct:.1f}%)")
    print(f"Remaining missing tickers: {total_missing}")
    
    if total_missing == 0:
        print("Successfully achieved 100% coverage for all tickers!")
        return True
    else:
        print("Still have missing ticker data. Consider running again.")
        return False

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Ensure complete ticker data coverage')
    parser.add_argument('--date', type=str, help='Date to ensure coverage for (YYYY-MM-DD)')
    parser.add_argument('--max', type=int, help='Maximum tickers to process per sector')
    args = parser.parse_args()
    
    success = ensure_complete_data(date=args.date, max_per_sector=args.max)
    
    if success:
        print("Successfully ensured complete data coverage!")
        sys.exit(0)
    else:
        print("Could not achieve 100% coverage.")
        sys.exit(1)