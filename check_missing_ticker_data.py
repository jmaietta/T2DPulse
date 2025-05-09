#!/usr/bin/env python3
# check_missing_ticker_data.py
# -----------------------------------------------------------
# Check for missing stock price or market cap data for all tickers

import os
import pandas as pd
import config
from datetime import datetime, timedelta
import pytz

def get_all_tickers():
    """Get all unique tickers from all sectors"""
    all_tickers = set()
    for sector, tickers in config.SECTORS.items():
        for ticker in tickers:
            all_tickers.add(ticker)
    return list(all_tickers)

def check_ticker_data():
    """Check for missing stock price or market cap data for all tickers"""
    # File paths
    price_file = "data/historical_ticker_prices.csv"
    marketcap_file = "data/historical_ticker_marketcap.csv"
    
    # Get current date in Eastern time
    eastern = pytz.timezone('US/Eastern')
    today = datetime.now(eastern).date()
    
    # Define a range of dates to check (last 30 days)
    end_date = today
    start_date = end_date - timedelta(days=30)
    
    # Get all tickers
    all_tickers = get_all_tickers()
    print(f"Checking data for {len(all_tickers)} unique tickers")
    
    # Check if files exist
    if not os.path.exists(price_file):
        print(f"Warning: Price history file not found at {price_file}")
        return False
    
    if not os.path.exists(marketcap_file):
        print(f"Warning: Market cap history file not found at {marketcap_file}")
        return False
    
    # Load data files
    try:
        price_df = pd.read_csv(price_file)
        marketcap_df = pd.read_csv(marketcap_file)
        
        print(f"Loaded price data with {len(price_df)} rows")
        print(f"Loaded market cap data with {len(marketcap_df)} rows")
        
        # Convert date columns to datetime
        price_df['date'] = pd.to_datetime(price_df['date']).dt.date
        marketcap_df['date'] = pd.to_datetime(marketcap_df['date']).dt.date
        
        # Filter for recent dates (last 30 days)
        recent_price_df = price_df[price_df['date'] >= start_date]
        recent_marketcap_df = marketcap_df[marketcap_df['date'] >= start_date]
        
        print(f"Found {len(recent_price_df)} price data points in the last 30 days")
        print(f"Found {len(recent_marketcap_df)} market cap data points in the last 30 days")
        
        # Check missing data by ticker
        missing_price_tickers = []
        missing_marketcap_tickers = []
        missing_both_tickers = []
        
        for ticker in all_tickers:
            ticker_price_data = recent_price_df[recent_price_df['ticker'] == ticker]
            ticker_marketcap_data = recent_marketcap_df[recent_marketcap_df['ticker'] == ticker]
            
            has_price = len(ticker_price_data) > 0
            has_marketcap = len(ticker_marketcap_data) > 0
            
            if not has_price and not has_marketcap:
                missing_both_tickers.append(ticker)
            elif not has_price:
                missing_price_tickers.append(ticker)
            elif not has_marketcap:
                missing_marketcap_tickers.append(ticker)
        
        # Print results
        print("\n--- Missing Data Summary ---")
        if missing_both_tickers:
            print(f"\nTickers missing BOTH price and market cap data ({len(missing_both_tickers)}):")
            for ticker in sorted(missing_both_tickers):
                print(f"  {ticker}")
                # Find which sectors this ticker belongs to
                sectors = [sector for sector, tickers in config.SECTORS.items() if ticker in tickers]
                print(f"    Used in sectors: {', '.join(sectors)}")
        
        if missing_price_tickers:
            print(f"\nTickers missing price data only ({len(missing_price_tickers)}):")
            for ticker in sorted(missing_price_tickers):
                print(f"  {ticker}")
                # Find which sectors this ticker belongs to
                sectors = [sector for sector, tickers in config.SECTORS.items() if ticker in tickers]
                print(f"    Used in sectors: {', '.join(sectors)}")
        
        if missing_marketcap_tickers:
            print(f"\nTickers missing market cap data only ({len(missing_marketcap_tickers)}):")
            for ticker in sorted(missing_marketcap_tickers):
                print(f"  {ticker}")
                # Find which sectors this ticker belongs to
                sectors = [sector for sector, tickers in config.SECTORS.items() if ticker in tickers]
                print(f"    Used in sectors: {', '.join(sectors)}")
        
        if not missing_both_tickers and not missing_price_tickers and not missing_marketcap_tickers:
            print("\nAll tickers have both price and market cap data! 🎉")
        
        # Check data per sector
        print("\n--- Sector Data Coverage ---")
        for sector, tickers in config.SECTORS.items():
            sector_missing = []
            for ticker in tickers:
                ticker_price_data = recent_price_df[recent_price_df['ticker'] == ticker]
                ticker_marketcap_data = recent_marketcap_df[recent_marketcap_df['ticker'] == ticker]
                
                has_price = len(ticker_price_data) > 0
                has_marketcap = len(ticker_marketcap_data) > 0
                
                if not has_price or not has_marketcap:
                    sector_missing.append(ticker)
            
            total = len(tickers)
            missing = len(sector_missing)
            coverage = ((total - missing) / total) * 100 if total > 0 else 0
            
            print(f"{sector}: {coverage:.1f}% coverage ({total-missing}/{total} tickers)")
            if sector_missing:
                print(f"  Missing tickers: {', '.join(sector_missing)}")
        
        return True
    
    except Exception as e:
        print(f"Error checking ticker data: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("Checking for missing ticker data...")
    success = check_ticker_data()
    print(f"Data check {'completed' if success else 'failed'}")