#!/usr/bin/env python3
# collect_complete_ticker_data.py
# -----------------------------------------------------------
# Comprehensive data collection for all tickers ensuring 100% coverage

import os
import requests
import time
import pandas as pd
import yfinance as yf
import json
from datetime import datetime, timedelta
import pytz
import config
import traceback
import sys

# Get API keys
FINNHUB_API_KEY = config.FINNHUB_API_KEY
ALPHAVANTAGE_API_KEY = os.environ.get("ALPHAVANTAGE_API_KEY")

# Ensure we have API keys
if not FINNHUB_API_KEY:
    print("Error: FINNHUB_API_KEY not found. Please set it in config.py")
    sys.exit(1)
if not ALPHAVANTAGE_API_KEY:
    print("Error: ALPHAVANTAGE_API_KEY not found in environment variables")
    sys.exit(1)

# Data file paths
DATA_DIR = "data"
PRICE_FILE = os.path.join(DATA_DIR, "historical_ticker_prices.csv")
MARKETCAP_FILE = os.path.join(DATA_DIR, "historical_ticker_marketcap.csv")

# Create data directory if it doesn't exist
os.makedirs(DATA_DIR, exist_ok=True)

# API rate limiting
FINNHUB_REQUESTS_PER_MINUTE = 30
ALPHAVANTAGE_REQUESTS_PER_MINUTE = 5
YAHOO_REQUESTS_PER_MINUTE = 2000  # Yahoo has a higher limit but we'll be conservative

# Function to get all tickers from config
def get_all_tickers():
    """Get all unique tickers across all sectors"""
    all_tickers = set()
    for sector, tickers in config.SECTORS.items():
        for ticker in tickers:
            all_tickers.add(ticker)
    return sorted(list(all_tickers))

# Finnhub API functions
def fetch_finnhub_ticker_data(ticker):
    """Fetch both price and market cap data for a ticker from Finnhub"""
    base_url = "https://finnhub.io/api/v1"
    
    # Fetch quote (current price)
    quote_url = f"{base_url}/quote"
    quote_params = {
        "symbol": ticker,
        "token": FINNHUB_API_KEY
    }
    
    # Fetch company profile (market cap)
    profile_url = f"{base_url}/stock/profile2"
    profile_params = {
        "symbol": ticker,
        "token": FINNHUB_API_KEY
    }
    
    try:
        # Get quote data
        quote_response = requests.get(quote_url, params=quote_params)
        if quote_response.status_code != 200:
            print(f"Finnhub API error (quote): {quote_response.status_code} for {ticker}")
            price = None
        else:
            quote_data = quote_response.json()
            price = quote_data.get('c')  # Current price
            if price == 0:  # Sometimes Finnhub returns 0 for valid tickers
                price = None
        
        # Wait to avoid rate limiting
        time.sleep(60 / FINNHUB_REQUESTS_PER_MINUTE)
        
        # Get profile data
        profile_response = requests.get(profile_url, params=profile_params)
        if profile_response.status_code != 200:
            print(f"Finnhub API error (profile): {profile_response.status_code} for {ticker}")
            market_cap = None
        else:
            profile_data = profile_response.json()
            market_cap = profile_data.get('marketCapitalization')
            if market_cap:
                market_cap = market_cap * 1000000  # Convert from millions to actual value
        
        # Wait to avoid rate limiting
        time.sleep(60 / FINNHUB_REQUESTS_PER_MINUTE)
        
        return {
            "price": price,
            "market_cap": market_cap,
            "source": "finnhub"
        }
    
    except Exception as e:
        print(f"Error fetching data from Finnhub for {ticker}: {e}")
        return {
            "price": None,
            "market_cap": None,
            "source": None
        }

# AlphaVantage API functions
def fetch_alphavantage_ticker_data(ticker):
    """Fetch both price and market cap data for a ticker from Alpha Vantage"""
    # Global quote for price
    quote_url = "https://www.alphavantage.co/query"
    quote_params = {
        "function": "GLOBAL_QUOTE",
        "symbol": ticker,
        "apikey": ALPHAVANTAGE_API_KEY
    }
    
    # Company overview for market cap
    overview_url = "https://www.alphavantage.co/query"
    overview_params = {
        "function": "OVERVIEW",
        "symbol": ticker,
        "apikey": ALPHAVANTAGE_API_KEY
    }
    
    try:
        # Get quote data
        quote_response = requests.get(quote_url, params=quote_params)
        if quote_response.status_code != 200:
            print(f"AlphaVantage API error (quote): {quote_response.status_code} for {ticker}")
            price = None
        else:
            quote_data = quote_response.json()
            if "Global Quote" in quote_data and "05. price" in quote_data["Global Quote"]:
                price = float(quote_data["Global Quote"]["05. price"])
            else:
                # Check for API limit or error messages
                if "Note" in quote_data:
                    print(f"AlphaVantage API limit reached: {quote_data['Note']}")
                else:
                    print(f"AlphaVantage API unexpected response (quote) for {ticker}: {quote_data}")
                price = None
        
        # Wait to avoid rate limiting
        time.sleep(60 / ALPHAVANTAGE_REQUESTS_PER_MINUTE)
        
        # Get overview data
        overview_response = requests.get(overview_url, params=overview_params)
        if overview_response.status_code != 200:
            print(f"AlphaVantage API error (overview): {overview_response.status_code} for {ticker}")
            market_cap = None
        else:
            overview_data = overview_response.json()
            if "MarketCapitalization" in overview_data:
                market_cap_str = overview_data["MarketCapitalization"]
                market_cap = float(market_cap_str) if market_cap_str else None
            else:
                # Check for API limit or error messages
                if "Note" in overview_data:
                    print(f"AlphaVantage API limit reached: {overview_data['Note']}")
                else:
                    print(f"AlphaVantage API unexpected response (overview) for {ticker}: {overview_data}")
                market_cap = None
        
        # Wait to avoid rate limiting
        time.sleep(60 / ALPHAVANTAGE_REQUESTS_PER_MINUTE)
        
        return {
            "price": price,
            "market_cap": market_cap,
            "source": "alphavantage"
        }
    
    except Exception as e:
        print(f"Error fetching data from AlphaVantage for {ticker}: {e}")
        return {
            "price": None,
            "market_cap": None,
            "source": None
        }

# Yahoo Finance functions
def fetch_yahoo_ticker_data(ticker):
    """Fetch both price and market cap data for a ticker from Yahoo Finance"""
    try:
        # Yahoo Finance ticker object
        yf_ticker = yf.Ticker(ticker)
        
        # Get ticker info
        info = yf_ticker.info
        
        # Extract price and market cap
        price = info.get('regularMarketPrice')
        market_cap = info.get('marketCap')
        
        # Handle None values and zeroes appropriately
        if price == 0:
            price = None
        if market_cap == 0:
            market_cap = None
        
        # Calculate market cap from shares outstanding and price if needed
        if market_cap is None and price is not None:
            shares_outstanding = info.get('sharesOutstanding')
            if shares_outstanding and shares_outstanding > 0:
                market_cap = shares_outstanding * price
        
        # Wait to avoid rate limiting
        time.sleep(60 / YAHOO_REQUESTS_PER_MINUTE)
        
        return {
            "price": price,
            "market_cap": market_cap,
            "source": "yahoo"
        }
    
    except Exception as e:
        print(f"Error fetching data from Yahoo Finance for {ticker}: {e}")
        return {
            "price": None,
            "market_cap": None,
            "source": None
        }

# Function to load existing data
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

# Function to fetch data for a single ticker
def fetch_ticker_data(ticker):
    """
    Fetch data for a single ticker using multiple sources
    Tries Finnhub first, then Alpha Vantage, then Yahoo Finance
    Returns all available data
    """
    print(f"\nFetching data for {ticker}...")
    
    # Try Finnhub first
    finnhub_data = fetch_finnhub_ticker_data(ticker)
    price = finnhub_data["price"]
    market_cap = finnhub_data["market_cap"]
    sources = {"price": None, "market_cap": None}
    
    if price is not None:
        sources["price"] = "finnhub"
        print(f"  ✓ Got price from Finnhub: {price:.2f}")
    else:
        print(f"  ✗ Failed to get price from Finnhub")
    
    if market_cap is not None:
        sources["market_cap"] = "finnhub"
        print(f"  ✓ Got market cap from Finnhub: {market_cap:,.2f}")
    else:
        print(f"  ✗ Failed to get market cap from Finnhub")
    
    # If we're missing data, try Alpha Vantage
    if price is None or market_cap is None:
        alphavantage_data = fetch_alphavantage_ticker_data(ticker)
        
        if price is None and alphavantage_data["price"] is not None:
            price = alphavantage_data["price"]
            sources["price"] = "alphavantage"
            print(f"  ✓ Got price from AlphaVantage: {price:.2f}")
        
        if market_cap is None and alphavantage_data["market_cap"] is not None:
            market_cap = alphavantage_data["market_cap"]
            sources["market_cap"] = "alphavantage"
            print(f"  ✓ Got market cap from AlphaVantage: {market_cap:,.2f}")
    
    # If we're still missing data, try Yahoo Finance
    if price is None or market_cap is None:
        yahoo_data = fetch_yahoo_ticker_data(ticker)
        
        if price is None and yahoo_data["price"] is not None:
            price = yahoo_data["price"]
            sources["price"] = "yahoo"
            print(f"  ✓ Got price from Yahoo Finance: {price:.2f}")
        
        if market_cap is None and yahoo_data["market_cap"] is not None:
            market_cap = yahoo_data["market_cap"]
            sources["market_cap"] = "yahoo"
            print(f"  ✓ Got market cap from Yahoo Finance: {market_cap:,.2f}")
    
    # If we still don't have a market cap but have price, try to calculate it
    if market_cap is None and price is not None:
        # Try one more specific attempt with Yahoo to get shares outstanding
        try:
            yf_ticker = yf.Ticker(ticker)
            shares_outstanding = yf_ticker.info.get('sharesOutstanding')
            if shares_outstanding and shares_outstanding > 0:
                market_cap = shares_outstanding * price
                sources["market_cap"] = "yahoo_calculated"
                print(f"  ✓ Calculated market cap from Yahoo shares: {market_cap:,.2f}")
        except Exception as e:
            print(f"  ✗ Failed to calculate market cap for {ticker}: {e}")
    
    # Final status report
    status = []
    if price is None:
        status.append("missing price")
    if market_cap is None:
        status.append("missing market cap")
    
    if not status:
        print(f"SUCCESS: Complete data for {ticker}")
    else:
        print(f"WARNING: Incomplete data for {ticker}: {', '.join(status)}")
    
    return {
        "ticker": ticker,
        "price": price,
        "market_cap": market_cap,
        "price_source": sources["price"],
        "market_cap_source": sources["market_cap"]
    }

# Main function to collect data for all tickers
def collect_complete_ticker_data():
    """Collect complete price and market cap data for all tickers"""
    # Get the current date in Eastern time (US market timezone)
    eastern = pytz.timezone('US/Eastern')
    today = datetime.now(eastern).strftime('%Y-%m-%d')
    
    print(f"Starting data collection for all tickers on {today}")
    print(f"Using Finnhub API key: {FINNHUB_API_KEY[:5]}... and AlphaVantage API key: {ALPHAVANTAGE_API_KEY[:5]}...")
    
    # Get all tickers
    all_tickers = get_all_tickers()
    print(f"Found {len(all_tickers)} unique tickers across all sectors")
    
    # Load existing data
    existing_price_df, existing_marketcap_df = load_existing_data()
    
    # Initialize the DataFrames if they don't exist
    if existing_price_df is None:
        price_df = pd.DataFrame(index=[today])
    else:
        # If we already have today's data, keep it; otherwise add a new row for today
        if today in existing_price_df.index:
            price_df = existing_price_df.copy()
        else:
            price_df = existing_price_df.copy()
            price_df.loc[today] = None
    
    if existing_marketcap_df is None:
        marketcap_df = pd.DataFrame(index=[today])
    else:
        # If we already have today's data, keep it; otherwise add a new row for today
        if today in existing_marketcap_df.index:
            marketcap_df = existing_marketcap_df.copy()
        else:
            marketcap_df = existing_marketcap_df.copy()
            marketcap_df.loc[today] = None
    
    # Track tickers to fetch
    tickers_to_fetch = []
    
    # Check which tickers we already have data for today
    for ticker in all_tickers:
        has_price = ticker in price_df.columns and not pd.isna(price_df.loc[today, ticker])
        has_marketcap = ticker in marketcap_df.columns and not pd.isna(marketcap_df.loc[today, ticker])
        
        if not has_price or not has_marketcap:
            tickers_to_fetch.append(ticker)
    
    print(f"Need to fetch data for {len(tickers_to_fetch)} tickers")
    
    # Iterate over tickers and fetch data
    results = []
    success_count = 0
    failure_count = 0
    
    for i, ticker in enumerate(tickers_to_fetch):
        print(f"\n[{i+1}/{len(tickers_to_fetch)}] Processing {ticker}...")
        
        result = fetch_ticker_data(ticker)
        results.append(result)
        
        # Update DataFrames
        if result["price"] is not None:
            price_df.loc[today, ticker] = result["price"]
            print(f"  Updated price_df with {ticker} = {result['price']}")
        
        if result["market_cap"] is not None:
            marketcap_df.loc[today, ticker] = result["market_cap"]
            print(f"  Updated marketcap_df with {ticker} = {result['market_cap']}")
        
        # Check if we successfully got both price and market cap
        if result["price"] is not None and result["market_cap"] is not None:
            success_count += 1
        else:
            failure_count += 1
        
        # Save after each ticker to avoid losing data
        price_df.to_csv(PRICE_FILE)
        marketcap_df.to_csv(MARKETCAP_FILE)
        print(f"  Saved current data to CSV files")
        
        # Progress report
        completion = (i + 1) / len(tickers_to_fetch) * 100
        print(f"Progress: {completion:.1f}% ({i+1}/{len(tickers_to_fetch)}) - Success: {success_count}, Failure: {failure_count}")
    
    # Final report
    print("\n--- Data Collection Results ---")
    print(f"Total tickers: {len(all_tickers)}")
    print(f"Tickers processed: {len(tickers_to_fetch)}")
    print(f"Success: {success_count} ({success_count/len(tickers_to_fetch)*100:.1f}%)")
    print(f"Failure: {failure_count} ({failure_count/len(tickers_to_fetch)*100:.1f}%)")
    
    # Check coverage for each sector
    print("\n--- Sector Coverage ---")
    for sector, tickers in config.SECTORS.items():
        sector_count = len(tickers)
        sector_success = 0
        
        for ticker in tickers:
            has_price = ticker in price_df.columns and not pd.isna(price_df.loc[today, ticker])
            has_marketcap = ticker in marketcap_df.columns and not pd.isna(marketcap_df.loc[today, ticker])
            
            if has_price and has_marketcap:
                sector_success += 1
        
        coverage = sector_success / sector_count * 100
        print(f"{sector}: {coverage:.1f}% coverage ({sector_success}/{sector_count} tickers)")
    
    # Final save
    price_df.to_csv(PRICE_FILE)
    marketcap_df.to_csv(MARKETCAP_FILE)
    
    print(f"\nFinal data saved to {PRICE_FILE} and {MARKETCAP_FILE}")
    
    return success_count == len(tickers_to_fetch)

if __name__ == "__main__":
    print("Starting comprehensive ticker data collection...")
    
    try:
        success = collect_complete_ticker_data()
        if success:
            print("Data collection completed successfully with 100% coverage!")
            sys.exit(0)
        else:
            print("Data collection completed with some missing data.")
            sys.exit(1)
    except Exception as e:
        print(f"Error during data collection: {e}")
        traceback.print_exc()
        sys.exit(1)