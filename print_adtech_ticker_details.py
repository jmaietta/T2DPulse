"""
Print detailed information about AdTech ticker share counts and market cap calculations.
"""
import os
import json
import pandas as pd
from datetime import datetime

# Define directory for data
DATA_DIR = "data"
CACHE_DIR = os.path.join(DATA_DIR, "cache")

def load_shares_outstanding():
    """
    Load shares outstanding data
    
    Returns:
        dict: Shares outstanding for each ticker
    """
    shares_cache = os.path.join(CACHE_DIR, "shares_outstanding.json")
    
    if os.path.exists(shares_cache):
        with open(shares_cache, 'r') as f:
            return json.load(f)
    else:
        print(f"Shares cache not found: {shares_cache}")
        return None

def load_historical_prices():
    """
    Load historical price data
    
    Returns:
        DataFrame: Historical price data for all tickers
    """
    price_cache = os.path.join(CACHE_DIR, "historical_prices.pkl")
    
    if os.path.exists(price_cache):
        price_dict = pd.read_pickle(price_cache)
        price_df = pd.DataFrame(price_dict)
        price_df.index = pd.to_datetime(price_df.index)
        price_df = price_df.sort_index()
        return price_df
    else:
        print(f"Price cache not found: {price_cache}")
        return None

def get_adtech_tickers():
    """Get tickers in the AdTech sector"""
    return ["GOOGL", "META", "APP", "TTD", "DV", "MGNI", "CRTO", "PUBM", "APPS"]

def print_adtech_ticker_details():
    """Print detailed information about AdTech ticker share counts and market caps"""
    # Load share counts
    shares_dict = load_shares_outstanding()
    if not shares_dict:
        print("Could not load share counts")
        return
    
    # Load price data
    price_df = load_historical_prices()
    if price_df is None:
        print("Could not load price data")
        return
    
    # Get latest date
    latest_date = price_df.index.max()
    
    # Get AdTech tickers
    adtech_tickers = get_adtech_tickers()
    
    # Print header
    print("\nAdTech Ticker Details (as of {}):\n".format(latest_date.strftime("%Y-%m-%d")))
    print("{:<10} {:<20} {:<15} {:<15} {:<15}".format(
        "Ticker", "Shares (millions)", "Price", "Market Cap ($B)", "% of Sector"
    ))
    print("-" * 75)
    
    # Calculate total sector market cap
    total_market_cap = 0
    ticker_data = []
    
    for ticker in adtech_tickers:
        if ticker in shares_dict and ticker in price_df.columns:
            shares = shares_dict[ticker]
            latest_price = price_df.loc[latest_date, ticker]
            market_cap = shares * latest_price
            
            ticker_data.append({
                "ticker": ticker,
                "shares": shares,
                "price": latest_price,
                "market_cap": market_cap
            })
            
            total_market_cap += market_cap
    
    # Print sorted by market cap
    for data in sorted(ticker_data, key=lambda x: x["market_cap"], reverse=True):
        ticker = data["ticker"]
        shares = data["shares"]
        price = data["price"]
        market_cap = data["market_cap"]
        percentage = (market_cap / total_market_cap) * 100
        
        print("{:<10} {:<20,.2f} ${:<14,.2f} ${:<14,.2f} {:<15.2f}%".format(
            ticker, shares / 1_000_000, price, market_cap / 1_000_000_000, percentage
        ))
    
    # Print total
    print("-" * 75)
    print("{:<10} {:<20} {:<15} ${:<14,.2f} {:<15}".format(
        "Total", "", "", total_market_cap / 1_000_000_000, "100.00%"
    ))

if __name__ == "__main__":
    print_adtech_ticker_details()