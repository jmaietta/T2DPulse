#!/usr/bin/env python3
# process_sector_tickers.py
# -----------------------------------------------------------
# Script to process tickers sector by sector to ensure complete coverage

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

def get_sector_coverage():
    """Get the coverage for each sector"""
    price_df, marketcap_df = load_data()
    if price_df is None or marketcap_df is None:
        return {}
    
    latest_date = price_df.index[-1]
    
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
                if not pd.isna(price_df.loc[latest_date, ticker]):
                    price_coverage += 1
                    has_price = True
            
            if ticker in marketcap_df.columns:
                if not pd.isna(marketcap_df.loc[latest_date, ticker]):
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

def process_sector(sector, max_tickers=None):
    """Process a specific sector to fill in missing data"""
    print(f"\n=== Processing {sector} Sector ===")
    
    # Load data
    price_df, marketcap_df = load_data()
    if price_df is None or marketcap_df is None:
        print("Error loading data. Aborting.")
        return False
    
    # Get all dates in the dataframes
    all_dates = sorted(price_df.index)
    latest_date = all_dates[-1]
    
    # Get tickers for this sector
    if sector in config.SECTORS:
        tickers = config.SECTORS[sector]
    else:
        print(f"Error: Sector '{sector}' not found in config.")
        return False
    
    # Find missing tickers for this sector
    missing_tickers = []
    for ticker in tickers:
        price_missing = True
        marketcap_missing = True
        
        if ticker in price_df.columns:
            if not pd.isna(price_df.loc[latest_date, ticker]):
                price_missing = False
        
        if ticker in marketcap_df.columns:
            if not pd.isna(marketcap_df.loc[latest_date, ticker]):
                marketcap_missing = False
        
        if price_missing or marketcap_missing:
            missing_tickers.append(ticker)
    
    print(f"Found {len(missing_tickers)} tickers with missing data in {sector}.")
    
    # Limit number of tickers if specified
    if max_tickers and max_tickers < len(missing_tickers):
        print(f"Limiting to {max_tickers} tickers out of {len(missing_tickers)}")
        missing_tickers = missing_tickers[:max_tickers]
    
    print(f"Processing {len(missing_tickers)} tickers...")
    
    # Process each missing ticker
    success_count = 0
    failure_count = 0
    
    for i, ticker in enumerate(missing_tickers):
        print(f"\n[{i+1}/{len(missing_tickers)}] Processing {ticker}...")
        
        # Check if the ticker is in both dataframes
        if ticker not in price_df.columns:
            print(f"Warning: Ticker {ticker} not found in price data. Adding column.")
            price_df[ticker] = np.nan
        
        if ticker not in marketcap_df.columns:
            print(f"Warning: Ticker {ticker} not found in market cap data. Adding column.")
            marketcap_df[ticker] = np.nan
        
        # Check if we have any existing data for this ticker
        latest_price = None
        latest_marketcap = None
        
        # Try to use latest available data for filling
        if not pd.isna(price_df.loc[latest_date, ticker]):
            latest_price = price_df.loc[latest_date, ticker]
            print(f"  Using latest price from {latest_date}: {latest_price}")
        else:
            # Try to find last non-null value
            non_null_prices = price_df[ticker].dropna()
            if len(non_null_prices) > 0:
                latest_price = non_null_prices.iloc[-1]
                print(f"  Using latest available price: {latest_price}")
        
        if not pd.isna(marketcap_df.loc[latest_date, ticker]):
            latest_marketcap = marketcap_df.loc[latest_date, ticker]
            print(f"  Using latest market cap from {latest_date}: {latest_marketcap}")
        else:
            # Try to find last non-null value
            non_null_marketcaps = marketcap_df[ticker].dropna()
            if len(non_null_marketcaps) > 0:
                latest_marketcap = non_null_marketcaps.iloc[-1]
                print(f"  Using latest available market cap: {latest_marketcap}")
        
        # If we don't have any data, fetch it
        if latest_price is None or latest_marketcap is None:
            print(f"  Fetching data for {ticker}...")
            max_retries = 3
            retry_delay = 5  # seconds
            
            for attempt in range(max_retries):
                try:
                    ticker_data = fetch_ticker_data(ticker)
                    
                    if ticker_data["price"] is not None:
                        latest_price = ticker_data["price"]
                        print(f"  Got new price: {latest_price}")
                    
                    if ticker_data["market_cap"] is not None:
                        latest_marketcap = ticker_data["market_cap"]
                        print(f"  Got new market cap: {latest_marketcap}")
                    
                    # If we got both price and market cap, break out of retry loop
                    if latest_price is not None and latest_marketcap is not None:
                        break
                        
                    # If we're missing one or both, try again
                    if attempt < max_retries - 1:
                        missing_data = []
                        if latest_price is None:
                            missing_data.append("price")
                        if latest_marketcap is None:
                            missing_data.append("market cap")
                        
                        print(f"  Missing {', '.join(missing_data)} data. Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                    
                except Exception as e:
                    print(f"  Error fetching data for {ticker} (attempt {attempt+1}/{max_retries}): {e}")
                    
                    # Check if it's a rate limiting error
                    if "rate limit" in str(e).lower() or "too many requests" in str(e).lower():
                        wait_time = retry_delay * (attempt + 1)  # Exponential backoff
                        print(f"  Rate limit hit. Waiting {wait_time} seconds before retrying...")
                        time.sleep(wait_time)
                    elif attempt < max_retries - 1:
                        print(f"  Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                    else:
                        print(f"  Failed to fetch data for {ticker} after {max_retries} attempts.")
        
        # Fill in null values with the latest data
        if latest_price is not None:
            price_df.loc[price_df[ticker].isna(), ticker] = latest_price
            print(f"  Filled in price data for {ticker}")
        else:
            print(f"  No price data available for {ticker}")
        
        if latest_marketcap is not None:
            marketcap_df.loc[marketcap_df[ticker].isna(), ticker] = latest_marketcap
            print(f"  Filled in market cap data for {ticker}")
        else:
            print(f"  No market cap data available for {ticker}")
        
        # Count success or failure
        if latest_price is not None and latest_marketcap is not None:
            success_count += 1
        else:
            failure_count += 1
        
        # Save progress after each ticker to make sure we don't lose data
        print(f"Saving progress ({i+1}/{len(missing_tickers)} tickers processed)...")
        price_df.to_csv(PRICE_FILE)
        marketcap_df.to_csv(MARKETCAP_FILE)
        print("Progress saved.")
        
        # Pause to avoid rate limiting
        time.sleep(1)
    
    # Print summary
    print("\n=== Sector Processing Results ===")
    print(f"Sector: {sector}")
    print(f"Total tickers processed: {len(missing_tickers)}")
    print(f"Success: {success_count} ({success_count/len(missing_tickers)*100:.1f}% if any)")
    print(f"Failure: {failure_count} ({failure_count/len(missing_tickers)*100:.1f}% if any)")
    
    return True

def print_sector_coverage(coverage):
    """Print the coverage for each sector"""
    print("\n=== Sector Coverage ===")
    
    # Sort sectors by coverage from lowest to highest
    sorted_sectors = sorted(coverage.items(), key=lambda x: (x[1]["price_pct"] + x[1]["marketcap_pct"]) / 2)
    
    # Calculate overall coverage
    total_tickers = sum(data["total_tickers"] for _, data in coverage.items())
    total_price_coverage = sum(data["price_coverage"] for _, data in coverage.items())
    total_marketcap_coverage = sum(data["marketcap_coverage"] for _, data in coverage.items())
    
    overall_price_pct = total_price_coverage / total_tickers * 100
    overall_marketcap_pct = total_marketcap_coverage / total_tickers * 100
    
    print(f"Overall: {total_price_coverage}/{total_tickers} price ({overall_price_pct:.1f}%), "
          f"{total_marketcap_coverage}/{total_tickers} market cap ({overall_marketcap_pct:.1f}%)")
    
    for sector, data in sorted_sectors:
        missing_count = len(data["missing_tickers"])
        if missing_count > 0:
            missing_str = f", Missing: {missing_count} tickers"
        else:
            missing_str = ", Complete!"
            
        print(f"{sector}: {data['price_coverage']}/{data['total_tickers']} price ({data['price_pct']:.1f}%), "
              f"{data['marketcap_coverage']}/{data['total_tickers']} market cap ({data['marketcap_pct']:.1f}%){missing_str}")
    
    return

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Process ticker data for specific sectors')
    parser.add_argument('--sector', type=str, help='Specific sector to process')
    parser.add_argument('--max', type=int, help='Maximum number of tickers to process per sector')
    parser.add_argument('--list', action='store_true', help='List sectors and their coverage')
    parser.add_argument('--worst', action='store_true', help='Process the sector with worst coverage')
    args = parser.parse_args()
    
    # Get current coverage
    coverage = get_sector_coverage()
    
    if args.list:
        # Just print coverage and exit
        print_sector_coverage(coverage)
        sys.exit(0)
    
    if args.worst:
        # Find the sector with the worst coverage
        worst_sector = min(coverage.items(), key=lambda x: (x[1]["price_pct"] + x[1]["marketcap_pct"]) / 2)[0]
        print(f"Worst coverage sector: {worst_sector}")
        success = process_sector(worst_sector, args.max)
    elif args.sector:
        # Process the specified sector
        success = process_sector(args.sector, args.max)
    else:
        # Print usage and exit
        print("Please specify a sector to process with --sector or use --worst to process the worst sector.")
        print("Use --list to see a list of sectors and their coverage.")
        print_sector_coverage(coverage)
        sys.exit(1)
    
    # Get updated coverage
    updated_coverage = get_sector_coverage()
    print("\nUpdated sector coverage:")
    print_sector_coverage(updated_coverage)
    
    if success:
        print("\nSuccess!")
        sys.exit(0)
    else:
        print("\nErrors occurred.")
        sys.exit(1)