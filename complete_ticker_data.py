#!/usr/bin/env python3
# complete_ticker_data.py
# -----------------------------------------------------------
# Script to complete missing ticker data by filling in null values

import os
import sys
import pandas as pd
import numpy as np
import time
from collect_complete_ticker_data import fetch_ticker_data, get_all_tickers

# File paths
DATA_DIR = "data"
PRICE_FILE = os.path.join(DATA_DIR, "historical_ticker_prices.csv")
MARKETCAP_FILE = os.path.join(DATA_DIR, "historical_ticker_marketcap.csv")

def load_data():
    """Load the price and market cap data"""
    price_df = pd.read_csv(PRICE_FILE, index_col=0)
    marketcap_df = pd.read_csv(MARKETCAP_FILE, index_col=0)
    return price_df, marketcap_df

def get_null_tickers(price_df, marketcap_df):
    """Get tickers with null values"""
    price_nulls = {}
    marketcap_nulls = {}
    
    for ticker in price_df.columns:
        null_count = price_df[ticker].isna().sum()
        if null_count > 0:
            price_nulls[ticker] = null_count
    
    for ticker in marketcap_df.columns:
        null_count = marketcap_df[ticker].isna().sum()
        if null_count > 0:
            marketcap_nulls[ticker] = null_count
    
    # Sort by number of nulls
    price_nulls = {k: v for k, v in sorted(price_nulls.items(), key=lambda item: item[1], reverse=True)}
    marketcap_nulls = {k: v for k, v in sorted(marketcap_nulls.items(), key=lambda item: item[1], reverse=True)}
    
    return price_nulls, marketcap_nulls

def fill_missing_data():
    """Fill in missing data for tickers with null values"""
    price_df, marketcap_df = load_data()
    
    # Get tickers with null values
    price_nulls, marketcap_nulls = get_null_tickers(price_df, marketcap_df)
    
    print(f"Found {len(price_nulls)} tickers with null price values")
    print(f"Found {len(marketcap_nulls)} tickers with null market cap values")
    
    # Get unique tickers with any null values
    null_tickers = set(price_nulls.keys()) | set(marketcap_nulls.keys())
    print(f"Total tickers with any null values: {len(null_tickers)}")
    
    # Get all dates in the dataframes
    all_dates = sorted(price_df.index)
    latest_date = all_dates[-1]
    
    print(f"Processing {len(null_tickers)} tickers with null values...")
    
    # Process each ticker with null values
    success_count = 0
    failure_count = 0
    
    for i, ticker in enumerate(null_tickers):
        print(f"\n[{i+1}/{len(null_tickers)}] Processing {ticker}...")
        
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
            try:
                ticker_data = fetch_ticker_data(ticker)
                
                if ticker_data["price"] is not None:
                    latest_price = ticker_data["price"]
                    print(f"  Got new price: {latest_price}")
                
                if ticker_data["market_cap"] is not None:
                    latest_marketcap = ticker_data["market_cap"]
                    print(f"  Got new market cap: {latest_marketcap}")
                
            except Exception as e:
                print(f"  Error fetching data for {ticker}: {e}")
        
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
        
        # Save progress every 10 tickers or at the end
        if (i + 1) % 10 == 0 or i == len(null_tickers) - 1:
            print(f"Saving progress ({i+1}/{len(null_tickers)} tickers processed)...")
            price_df.to_csv(PRICE_FILE)
            marketcap_df.to_csv(MARKETCAP_FILE)
            print("Progress saved.")
            
            # Pause to avoid rate limiting
            time.sleep(1)
    
    # Final save
    price_df.to_csv(PRICE_FILE)
    marketcap_df.to_csv(MARKETCAP_FILE)
    
    print("\n--- Fill Missing Data Results ---")
    print(f"Total tickers processed: {len(null_tickers)}")
    print(f"Success: {success_count} ({success_count/len(null_tickers)*100:.1f}%)")
    print(f"Failure: {failure_count} ({failure_count/len(null_tickers)*100:.1f}%)")
    
    # Check final coverage
    price_df, marketcap_df = load_data()
    price_nulls, marketcap_nulls = get_null_tickers(price_df, marketcap_df)
    
    print("\n--- Final Data Coverage ---")
    print(f"Tickers with null price values: {len(price_nulls)}")
    print(f"Tickers with null market cap values: {len(marketcap_nulls)}")
    
    if len(price_nulls) == 0 and len(marketcap_nulls) == 0:
        print("\nSUCCESS: All ticker data is complete!")
        return True
    else:
        print("\nWARNING: Some tickers still have null values.")
        return False

if __name__ == "__main__":
    print("Starting to fill missing ticker data...")
    success = fill_missing_data()
    if success:
        print("Successfully filled all missing ticker data!")
        sys.exit(0)
    else:
        print("Some tickers still have missing data.")
        sys.exit(1)