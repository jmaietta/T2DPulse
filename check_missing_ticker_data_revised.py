#!/usr/bin/env python3
# check_missing_ticker_data_revised.py
# -----------------------------------------------------------
# Check for missing stock price or market cap data for all tickers

import os
import pandas as pd
import config
from datetime import datetime
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
        price_df = pd.read_csv(price_file, index_col=0)
        marketcap_df = pd.read_csv(marketcap_file, index_col=0)
        
        print(f"Loaded price data with shape {price_df.shape}")
        print(f"Loaded market cap data with shape {marketcap_df.shape}")
        
        # Find missing tickers
        price_tickers = price_df.columns.tolist()
        marketcap_tickers = marketcap_df.columns.tolist()
        
        missing_price_tickers = [ticker for ticker in all_tickers if ticker not in price_tickers]
        missing_marketcap_tickers = [ticker for ticker in all_tickers if ticker not in marketcap_tickers]
        missing_both_tickers = [ticker for ticker in all_tickers 
                              if ticker not in price_tickers and ticker not in marketcap_tickers]
        
        # Print results
        print("\n--- Missing Data Summary ---")
        if missing_both_tickers:
            print(f"\nTickers missing BOTH price and market cap data ({len(missing_both_tickers)}):")
            for ticker in sorted(missing_both_tickers):
                print(f"  {ticker}")
                # Find which sectors this ticker belongs to
                sectors = [sector for sector, tickers in config.SECTORS.items() if ticker in tickers]
                print(f"    Used in sectors: {', '.join(sectors)}")
        
        if [t for t in missing_price_tickers if t not in missing_both_tickers]:
            price_only = [t for t in missing_price_tickers if t not in missing_both_tickers]
            print(f"\nTickers missing price data only ({len(price_only)}):")
            for ticker in sorted(price_only):
                print(f"  {ticker}")
                # Find which sectors this ticker belongs to
                sectors = [sector for sector, tickers in config.SECTORS.items() if ticker in tickers]
                print(f"    Used in sectors: {', '.join(sectors)}")
        
        if [t for t in missing_marketcap_tickers if t not in missing_both_tickers]:
            mcap_only = [t for t in missing_marketcap_tickers if t not in missing_both_tickers]
            print(f"\nTickers missing market cap data only ({len(mcap_only)}):")
            for ticker in sorted(mcap_only):
                print(f"  {ticker}")
                # Find which sectors this ticker belongs to
                sectors = [sector for sector, tickers in config.SECTORS.items() if ticker in tickers]
                print(f"    Used in sectors: {', '.join(sectors)}")
        
        if not missing_both_tickers and not missing_price_tickers and not missing_marketcap_tickers:
            print("\nAll tickers have both price and market cap data! 🎉")
        
        # Check for null values in existing ticker data
        print("\n--- Checking for null values in existing ticker data ---")
        for ticker in [t for t in all_tickers if t in price_tickers]:
            if pd.isna(price_df[ticker]).any():
                print(f"WARNING: Ticker {ticker} has null price values")
        
        for ticker in [t for t in all_tickers if t in marketcap_tickers]:
            if pd.isna(marketcap_df[ticker]).any():
                print(f"WARNING: Ticker {ticker} has null market cap values")
        
        # Check data per sector
        print("\n--- Sector Data Coverage ---")
        for sector, tickers in config.SECTORS.items():
            sector_missing_price = [ticker for ticker in tickers if ticker not in price_tickers]
            sector_missing_mcap = [ticker for ticker in tickers if ticker not in marketcap_tickers]
            sector_missing_both = [ticker for ticker in tickers 
                                if ticker not in price_tickers and ticker not in marketcap_tickers]
            
            total = len(tickers)
            missing = len(set(sector_missing_price + sector_missing_mcap))
            coverage = ((total - missing) / total) * 100 if total > 0 else 0
            
            print(f"{sector}: {coverage:.1f}% coverage ({total-missing}/{total} tickers)")
            if sector_missing_both:
                print(f"  Missing both price & market cap: {', '.join(sector_missing_both)}")
            if [t for t in sector_missing_price if t not in sector_missing_both]:
                print(f"  Missing price only: {', '.join([t for t in sector_missing_price if t not in sector_missing_both])}")
            if [t for t in sector_missing_mcap if t not in sector_missing_both]:
                print(f"  Missing market cap only: {', '.join([t for t in sector_missing_mcap if t not in sector_missing_both])}")
            
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