#!/usr/bin/env python3
# find_missing_data_tickers.py
# -----------------------------------------------------------
# Identify tickers with missing market cap or price data

import os
import pandas as pd
import config
from improved_finnhub_data_collector import (
    get_eastern_date,
    fetch_market_cap_finnhub,
    fetch_market_cap_yfinance,
    fetch_market_cap_alphavantage,
    calculate_market_cap_from_shares_and_price,
    fetch_price_finnhub,
    fetch_price_yfinance, 
    fetch_price_alphavantage
)

def check_ticker_data(ticker):
    """Check if market cap and price data is available for a ticker from any source"""
    results = {
        'ticker': ticker,
        'market_cap': {
            'finnhub': None,
            'yfinance': None,
            'alphavantage': None,
            'calculated': None,
            'any_available': False
        },
        'price': {
            'finnhub': None,
            'yfinance': None,
            'alphavantage': None,
            'any_available': False
        }
    }
    
    # Check market cap sources
    print(f"Checking market cap sources for {ticker}:")
    
    # Finnhub
    market_cap = fetch_market_cap_finnhub(ticker)
    results['market_cap']['finnhub'] = market_cap is not None
    if market_cap is not None:
        print(f"  Finnhub: {market_cap:,.2f}")
        results['market_cap']['any_available'] = True
    else:
        print(f"  Finnhub: None")
    
    # Yahoo Finance
    market_cap = fetch_market_cap_yfinance(ticker)
    results['market_cap']['yfinance'] = market_cap is not None
    if market_cap is not None:
        print(f"  Yahoo Finance: {market_cap:,.2f}")
        results['market_cap']['any_available'] = True
    else:
        print(f"  Yahoo Finance: None")
    
    # AlphaVantage
    market_cap = fetch_market_cap_alphavantage(ticker)
    results['market_cap']['alphavantage'] = market_cap is not None
    if market_cap is not None:
        print(f"  AlphaVantage: {market_cap:,.2f}")
        results['market_cap']['any_available'] = True
    else:
        print(f"  AlphaVantage: None")
    
    # Calculated from shares and price
    market_cap = calculate_market_cap_from_shares_and_price(ticker)
    results['market_cap']['calculated'] = market_cap is not None
    if market_cap is not None:
        print(f"  Calculated (shares × price): {market_cap:,.2f}")
        results['market_cap']['any_available'] = True
    else:
        print(f"  Calculated (shares × price): None")
    
    # Check price sources
    print(f"Checking price sources for {ticker}:")
    
    # Finnhub
    price = fetch_price_finnhub(ticker)
    results['price']['finnhub'] = price is not None
    if price is not None:
        print(f"  Finnhub: {price:.2f}")
        results['price']['any_available'] = True
    else:
        print(f"  Finnhub: None")
    
    # Yahoo Finance
    price = fetch_price_yfinance(ticker)
    results['price']['yfinance'] = price is not None
    if price is not None:
        print(f"  Yahoo Finance: {price:.2f}")
        results['price']['any_available'] = True
    else:
        print(f"  Yahoo Finance: None")
    
    # AlphaVantage
    price = fetch_price_alphavantage(ticker)
    results['price']['alphavantage'] = price is not None
    if price is not None:
        print(f"  AlphaVantage: {price:.2f}")
        results['price']['any_available'] = True
    else:
        print(f"  AlphaVantage: None")
    
    return results

def main():
    """Find tickers with missing market cap or price data"""
    today = get_eastern_date()
    print(f"Finding tickers with missing data for {today}...")
    
    # Check if historical data files exist
    historical_price_file = os.path.join('data', 'historical_ticker_prices.csv')
    historical_marketcap_file = os.path.join('data', 'historical_ticker_marketcap.csv')
    
    historical_price_data = None
    historical_marketcap_data = None
    
    # Load historical data if available
    if os.path.exists(historical_price_file):
        try:
            historical_price_data = pd.read_csv(historical_price_file, index_col='date')
            print(f"Loaded historical price data for {len(historical_price_data.columns)} tickers")
        except Exception as e:
            print(f"Error loading historical price data: {e}")
    
    if os.path.exists(historical_marketcap_file):
        try:
            historical_marketcap_data = pd.read_csv(historical_marketcap_file, index_col='date')
            print(f"Loaded historical market cap data for {len(historical_marketcap_data.columns)} tickers")
        except Exception as e:
            print(f"Error loading historical market cap data: {e}")
    
    # Focus on IT Services sector
    focus_sector = "IT Services / Legacy Tech"
    print(f"Focusing on {focus_sector} sector")
    
    # Get tickers for the focus sector
    all_tickers = config.SECTORS.get(focus_sector, [])
    print(f"Found {len(all_tickers)} tickers in {focus_sector} sector: {all_tickers}")
    
    # Check each ticker
    missing_data = {
        'missing_market_cap': [],
        'missing_price': [],
        'completely_missing': [],
        'sector_missing_data': {}
    }
    
    for sector, tickers in config.SECTORS.items():
        missing_data['sector_missing_data'][sector] = []
    
    for ticker in all_tickers:
        # Check current API sources first
        results = check_ticker_data(ticker)
        
        has_historical_price = False
        has_historical_marketcap = False
        
        # Check if we have historical data
        if historical_price_data is not None and ticker in historical_price_data.columns:
            price_data = historical_price_data[ticker].dropna()
            has_historical_price = not price_data.empty
        
        if historical_marketcap_data is not None and ticker in historical_marketcap_data.columns:
            marketcap_data = historical_marketcap_data[ticker].dropna()
            has_historical_marketcap = not marketcap_data.empty
        
        # Track missing data
        missing_market_cap = not results['market_cap']['any_available'] and not has_historical_marketcap
        missing_price = not results['price']['any_available'] and not has_historical_price
        
        # Store results
        if missing_market_cap:
            missing_data['missing_market_cap'].append(ticker)
        
        if missing_price:
            missing_data['missing_price'].append(ticker)
        
        if missing_market_cap and missing_price:
            missing_data['completely_missing'].append(ticker)
        
        # Add to sector missing data if applicable
        if missing_market_cap or missing_price:
            for sector, tickers in config.SECTORS.items():
                if ticker in tickers:
                    missing_data['sector_missing_data'][sector].append({
                        'ticker': ticker,
                        'missing_market_cap': missing_market_cap,
                        'missing_price': missing_price
                    })
    
    # Print summary
    print("\n========== MISSING DATA SUMMARY ==========")
    print(f"Total unique tickers: {len(all_tickers)}")
    print(f"Tickers missing market cap data: {len(missing_data['missing_market_cap'])}")
    if missing_data['missing_market_cap']:
        print(f"  {', '.join(missing_data['missing_market_cap'])}")
    
    print(f"Tickers missing price data: {len(missing_data['missing_price'])}")
    if missing_data['missing_price']:
        print(f"  {', '.join(missing_data['missing_price'])}")
    
    print(f"Tickers completely missing data: {len(missing_data['completely_missing'])}")
    if missing_data['completely_missing']:
        print(f"  {', '.join(missing_data['completely_missing'])}")
    
    print("\n========== MISSING DATA BY SECTOR ==========")
    for sector, missing_tickers in missing_data['sector_missing_data'].items():
        if missing_tickers:
            print(f"{sector}: {len(missing_tickers)}/{len(config.SECTORS[sector])} tickers with missing data")
            for item in missing_tickers:
                print(f"  {item['ticker']} - Missing market cap: {item['missing_market_cap']}, Missing price: {item['missing_price']}")
        else:
            print(f"{sector}: All tickers have data available")

if __name__ == "__main__":
    main()