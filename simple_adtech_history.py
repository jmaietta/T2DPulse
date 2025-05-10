"""
Show a simple two-column table of AdTech market cap history:
Date | AdTech Market Cap
"""
import os
import json
import pandas as pd
from datetime import timedelta

# Define directory for data
DATA_DIR = "data"
CACHE_DIR = os.path.join(DATA_DIR, "cache")

def load_shares_outstanding():
    """Load shares outstanding data"""
    shares_cache = os.path.join(CACHE_DIR, "shares_outstanding.json")
    
    if os.path.exists(shares_cache):
        with open(shares_cache, 'r') as f:
            return json.load(f)
    else:
        print(f"Shares cache not found: {shares_cache}")
        return None

def load_historical_prices():
    """Load historical price data"""
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

def filter_business_days(df, days=30):
    """Filter to only include business days in the last N days"""
    if df is None:
        return None
        
    # Convert index to datetime if needed
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index)
    
    # Get end date (latest in the data)
    end_date = df.index.max()
    
    # Calculate start date (30 calendar days before end date)
    start_date = end_date - timedelta(days=days)
    
    # Filter to date range
    df_filtered = df[df.index >= start_date]
    
    # Filter to business days (Mon-Fri)
    df_filtered = df_filtered[df_filtered.index.dayofweek < 5]
    
    return df_filtered

def show_simple_adtech_history():
    """Show simple two-column history of AdTech market cap"""
    # Load share counts and prices
    shares_dict = load_shares_outstanding()
    price_df = load_historical_prices()
    
    if not shares_dict or price_df is None:
        print("Could not load share counts or price data")
        return
        
    # Filter to last 30 days
    price_df = filter_business_days(price_df, 30)
    
    # Get AdTech tickers
    adtech_tickers = get_adtech_tickers()
    
    # Calculate market caps
    market_caps = pd.DataFrame(index=price_df.index)
    
    for ticker in adtech_tickers:
        if ticker in shares_dict and ticker in price_df.columns:
            market_caps[ticker] = price_df[ticker] * shares_dict[ticker]
    
    # Calculate total AdTech market cap
    market_caps['Total'] = market_caps.sum(axis=1)
    
    # Convert to billions
    market_caps_billions = market_caps['Total'] / 1_000_000_000
    
    # Create a simple two-column table
    print("\nAdTech Market Cap History (in trillions USD)\n")
    print("{:<12} {:<15}".format("Date", "Market Cap"))
    print("-" * 30)
    
    # Print in reverse chronological order (newest first)
    for date in sorted(market_caps.index, reverse=True):
        date_str = date.strftime("%Y-%m-%d")
        market_cap = market_caps_billions[date] / 1000  # Convert to trillions
        
        print("{:<12} ${:<14,.3f}T".format(date_str, market_cap))
    
    # Print change
    first_date = market_caps.index.min()
    last_date = market_caps.index.max()
    
    start_value = market_caps_billions[first_date]
    end_value = market_caps_billions[last_date]
    
    print("\nSummary:")
    print(f"First day ({first_date.strftime('%Y-%m-%d')}): ${start_value/1000:.3f}T")
    print(f"Last day ({last_date.strftime('%Y-%m-%d')}): ${end_value/1000:.3f}T")
    
    change_pct = ((end_value / start_value) - 1) * 100
    print(f"Change: {change_pct:.2f}%")

if __name__ == "__main__":
    show_simple_adtech_history()