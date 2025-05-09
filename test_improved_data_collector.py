#!/usr/bin/env python3
# test_improved_data_collector.py
# -----------------------------------------------------------
# Test script for improved multi-source data collector with AlphaVantage integration

import os
import time
import datetime
import pytz
import pandas as pd
from config import SECTORS
from improved_finnhub_data_collector import (
    collect_daily_sector_data,
    get_all_unique_tickers,
    update_historical_data,
    process_sector_data,
    fetch_market_cap_finnhub,
    fetch_market_cap_yfinance,
    fetch_market_cap_alphavantage,
    fetch_price_finnhub,
    fetch_price_yfinance,
    fetch_price_alphavantage
)

def test_market_cap_sources(ticker):
    """Test all market cap data sources for a ticker"""
    print(f"\nTesting market cap sources for {ticker}:")
    
    # Test Finnhub
    start_time = time.time()
    market_cap = fetch_market_cap_finnhub(ticker)
    finnhub_time = time.time() - start_time
    print(f"  Finnhub: {market_cap:,.2f if market_cap else None} ({finnhub_time:.2f}s)")
    
    # Test Yahoo Finance
    start_time = time.time()
    market_cap = fetch_market_cap_yfinance(ticker)
    yahoo_time = time.time() - start_time
    print(f"  Yahoo Finance: {market_cap:,.2f if market_cap else None} ({yahoo_time:.2f}s)")
    
    # Test AlphaVantage
    start_time = time.time()
    market_cap = fetch_market_cap_alphavantage(ticker)
    alpha_time = time.time() - start_time
    print(f"  AlphaVantage: {market_cap:,.2f if market_cap else None} ({alpha_time:.2f}s)")

def test_price_sources(ticker):
    """Test all price data sources for a ticker"""
    print(f"\nTesting price sources for {ticker}:")
    
    # Test Finnhub
    start_time = time.time()
    price = fetch_price_finnhub(ticker)
    finnhub_time = time.time() - start_time
    print(f"  Finnhub: {price:.2f if price else None} ({finnhub_time:.2f}s)")
    
    # Test Yahoo Finance
    start_time = time.time()
    price = fetch_price_yfinance(ticker)
    yahoo_time = time.time() - start_time
    print(f"  Yahoo Finance: {price:.2f if price else None} ({yahoo_time:.2f}s)")
    
    # Test AlphaVantage
    start_time = time.time()
    price = fetch_price_alphavantage(ticker)
    alpha_time = time.time() - start_time
    print(f"  AlphaVantage: {price:.2f if price else None} ({alpha_time:.2f}s)")

def load_previous_sector_scores():
    """Test loading previous sector scores for fallback"""
    authentic_history_file = os.path.join('data', 'authentic_sector_history.csv')
    previous_sector_scores = {}
    
    if os.path.exists(authentic_history_file):
        try:
            # Load the authentic sector history to get previous day's scores
            authentic_df = pd.read_csv(authentic_history_file)
            # Find the most recent date
            if not authentic_df.empty and 'Date' in authentic_df.columns:
                authentic_df['Date'] = pd.to_datetime(authentic_df['Date'])
                authentic_df = authentic_df.sort_values('Date', ascending=False)
                
                if not authentic_df.empty:
                    latest_row = authentic_df.iloc[0]
                    latest_date = latest_row['Date'].strftime('%Y-%m-%d')
                    
                    print(f"\nLoaded previous sector scores from {latest_date}:")
                    for sector in SECTORS:
                        if sector in latest_row:
                            previous_sector_scores[sector] = latest_row[sector]
                            print(f"  {sector}: {latest_row[sector]}")
        except Exception as e:
            print(f"Error loading authentic sector history: {e}")
    else:
        print(f"No authentic history file found at {authentic_history_file}")
    
    return previous_sector_scores

def main():
    """Test the improved multi-source data collector"""
    print("Testing improved data collector with AlphaVantage integration...")
    
    # Check for AlphaVantage API key
    alpha_key = os.environ.get("ALPHAVANTAGE_API_KEY", "")
    if not alpha_key:
        print("Warning: No AlphaVantage API key found in environment")
    else:
        print(f"AlphaVantage API key found (length: {len(alpha_key)})")
    
    # Test getting all unique tickers
    all_tickers = get_all_unique_tickers()
    print(f"Found {len(all_tickers)} unique tickers across all sectors")
    
    # Get a sample of tickers to test (up to 5)
    sample_tickers = all_tickers[:min(5, len(all_tickers))]
    print(f"Testing with sample tickers: {sample_tickers}")
    
    # Test each data source for the sample tickers
    for ticker in sample_tickers:
        test_market_cap_sources(ticker)
        test_price_sources(ticker)
        # Add a delay to avoid rate limiting
        time.sleep(2)
    
    # Test loading previous sector scores for fallback
    previous_scores = load_previous_sector_scores()
    
    # Test updating historical data for the sample tickers
    print("\nUpdating historical data for sample tickers...")
    historical_price_data, historical_marketcap_data = update_historical_data(sample_tickers)
    
    # Print sample of historical data
    if not historical_price_data.empty:
        print("\nHistorical price data sample:")
        print(historical_price_data.tail(3))
    
    if not historical_marketcap_data.empty:
        print("\nHistorical market cap data sample:")
        print(historical_marketcap_data.tail(3))
    
    # Test processing sector data
    print("\nTesting sector data processing with previous scores for fallback...")
    sector_data = process_sector_data(historical_price_data, historical_marketcap_data)
    
    # Print sector data
    if sector_data:
        print("\nProcessed sector data:")
        for sector, data in sector_data.items():
            print(f"{sector}: Market Cap = {data['market_cap']:,.2f}, " +
                  f"Momentum = {data['momentum']:.2f}%, " +
                  f"Tickers = {data.get('tickers_with_data', 'N/A')}/{data.get('total_tickers', 'N/A')}")
    
    # Choose whether to test full data collection
    test_full = input("\nRun full data collection test? (y/n): ").lower() == 'y'
    
    if test_full:
        print("\nTesting full data collection...")
        success = collect_daily_sector_data()
        
        if success:
            print("✅ Improved multi-source data collector test successful!")
        else:
            print("❌ Improved multi-source data collector test failed!")
            
        return success
    else:
        print("Skipping full data collection test")
        return True

if __name__ == "__main__":
    main()