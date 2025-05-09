#!/usr/bin/env python3
# test_it_services_tickers.py
# -----------------------------------------------------------
# Test script specifically for IT Services sector tickers

import os
import time
import config
from improved_finnhub_data_collector import (
    fetch_market_cap_finnhub,
    fetch_market_cap_yfinance,
    fetch_market_cap_alphavantage,
    calculate_market_cap_from_shares_and_price,
    fetch_price_finnhub,
    fetch_price_yfinance,
    fetch_price_alphavantage
)

def test_ticker(ticker):
    """Test all market cap and price data sources for a ticker"""
    print(f"\n--- Testing {ticker} ---")
    
    # Test market cap sources
    print(f"Market cap sources:")
    
    start_time = time.time()
    market_cap = fetch_market_cap_finnhub(ticker)
    finnhub_time = time.time() - start_time
    formatted_mcap = f"{market_cap:,.2f}" if market_cap is not None else "None"
    print(f"  Finnhub: {formatted_mcap} ({finnhub_time:.2f}s)")
    
    start_time = time.time()
    market_cap = fetch_market_cap_yfinance(ticker)
    yahoo_time = time.time() - start_time
    formatted_mcap = f"{market_cap:,.2f}" if market_cap is not None else "None"
    print(f"  Yahoo Finance: {formatted_mcap} ({yahoo_time:.2f}s)")
    
    start_time = time.time()
    market_cap = fetch_market_cap_alphavantage(ticker)
    alpha_time = time.time() - start_time
    formatted_mcap = f"{market_cap:,.2f}" if market_cap is not None else "None"
    print(f"  AlphaVantage: {formatted_mcap} ({alpha_time:.2f}s)")
    
    start_time = time.time()
    market_cap = calculate_market_cap_from_shares_and_price(ticker)
    calc_time = time.time() - start_time
    formatted_mcap = f"{market_cap:,.2f}" if market_cap is not None else "None"
    print(f"  Calculated (shares × price): {formatted_mcap} ({calc_time:.2f}s)")
    
    # Test price sources
    print(f"Price sources:")
    
    start_time = time.time()
    price = fetch_price_finnhub(ticker)
    finnhub_time = time.time() - start_time
    formatted_price = f"{price:.2f}" if price is not None else "None"
    print(f"  Finnhub: {formatted_price} ({finnhub_time:.2f}s)")
    
    start_time = time.time()
    price = fetch_price_yfinance(ticker)
    yahoo_time = time.time() - start_time
    formatted_price = f"{price:.2f}" if price is not None else "None"
    print(f"  Yahoo Finance: {formatted_price} ({yahoo_time:.2f}s)")
    
    start_time = time.time()
    price = fetch_price_alphavantage(ticker)
    alpha_time = time.time() - start_time
    formatted_price = f"{price:.2f}" if price is not None else "None"
    print(f"  AlphaVantage: {formatted_price} ({alpha_time:.2f}s)")

def main():
    """Test the IT Services tickers"""
    print("Testing IT Services sector tickers...")
    
    # Get IT Services tickers
    it_services_sector = "IT Services / Legacy Tech"
    if it_services_sector in config.SECTORS:
        tickers = config.SECTORS[it_services_sector]
        print(f"Found {len(tickers)} tickers for IT Services sector: {tickers}")
        
        # Test each ticker
        for ticker in tickers:
            test_ticker(ticker)
            # Add a delay to avoid rate limiting
            time.sleep(2)
    else:
        print(f"Error: {it_services_sector} not found in SECTORS")

if __name__ == "__main__":
    main()