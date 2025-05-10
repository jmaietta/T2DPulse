"""
Show detailed AdTech Market Cap History for the past 30 days
"""
import os
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pytz

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

def calculate_market_caps():
    """Calculate market caps for each ticker and day"""
    # Load share counts and prices
    shares_dict = load_shares_outstanding()
    price_df = load_historical_prices()
    
    if not shares_dict or price_df is None:
        print("Could not load share counts or price data")
        return None
        
    # Filter to last 30 days
    price_df = filter_business_days(price_df, 30)
    
    # Get AdTech tickers
    adtech_tickers = get_adtech_tickers()
    
    # Calculate market caps
    market_caps = pd.DataFrame(index=price_df.index)
    
    for ticker in adtech_tickers:
        if ticker in shares_dict and ticker in price_df.columns:
            market_caps[ticker] = price_df[ticker] * shares_dict[ticker]
    
    return market_caps

def display_daily_adtech_history():
    """Display daily AdTech market cap history for all days"""
    # Calculate market caps
    market_caps = calculate_market_caps()
    
    if market_caps is None or market_caps.empty:
        print("No market cap data available")
        return
    
    # Get AdTech tickers
    adtech_tickers = get_adtech_tickers()
    
    # Calculate total market cap for each day
    market_caps['Total'] = market_caps.sum(axis=1)
    
    # Create DataFrame with percentage of total for each ticker on each day
    percentages = pd.DataFrame(index=market_caps.index)
    
    for ticker in adtech_tickers:
        if ticker in market_caps.columns:
            percentages[ticker] = (market_caps[ticker] / market_caps['Total']) * 100
    
    # Format market caps in billions
    market_caps_billions = market_caps / 1_000_000_000
    
    # Print daily breakdown
    print("\nAdTech Market Cap Daily History (in billions USD)\n")
    print("{:<12} {:<15} {:<15} {:<15} {:<15} {:<15} {:<15}".format(
        "Date", "GOOGL ($B)", "META ($B)", "APP ($B)", "TTD ($B)", "Others ($B)", "Total ($B)"
    ))
    print("-" * 100)
    
    for date in market_caps.index:
        date_str = date.strftime("%Y-%m-%d")
        
        # Get key ticker values
        googl = market_caps_billions.loc[date, 'GOOGL'] if 'GOOGL' in market_caps_billions.columns else 0
        meta = market_caps_billions.loc[date, 'META'] if 'META' in market_caps_billions.columns else 0
        app = market_caps_billions.loc[date, 'APP'] if 'APP' in market_caps_billions.columns else 0
        ttd = market_caps_billions.loc[date, 'TTD'] if 'TTD' in market_caps_billions.columns else 0
        
        # Calculate others (excluding key tickers)
        key_tickers = ['GOOGL', 'META', 'APP', 'TTD']
        others = sum(market_caps_billions.loc[date, ticker] for ticker in market_caps_billions.columns 
                     if ticker in adtech_tickers and ticker not in key_tickers)
        
        total = market_caps_billions.loc[date, 'Total']
        
        # Print row
        print("{:<12} ${:<14,.2f} ${:<14,.2f} ${:<14,.2f} ${:<14,.2f} ${:<14,.2f} ${:<14,.2f}".format(
            date_str, googl, meta, app, ttd, others, total
        ))
    
    # Print summary of changes
    first_date = market_caps.index.min()
    last_date = market_caps.index.max()
    
    print("\nSummary of Changes:")
    print("{:<15} {:<15} {:<15} {:<15}".format("Ticker", "Start ($B)", "End ($B)", "Change (%)"))
    print("-" * 60)
    
    tickers_to_show = ['GOOGL', 'META', 'APP', 'TTD', 'Total']
    
    for ticker in tickers_to_show:
        if ticker in market_caps_billions.columns:
            start_value = market_caps_billions.loc[first_date, ticker]
            end_value = market_caps_billions.loc[last_date, ticker]
            change_pct = ((end_value / start_value) - 1) * 100 if start_value > 0 else 0
            
            print("{:<15} ${:<14,.2f} ${:<14,.2f} {:<15.2f}%".format(
                ticker, start_value, end_value, change_pct
            ))
    
    # Calculate percentage of sector on first and last day
    print("\nSector Composition (% of total):")
    print("{:<15} {:<15} {:<15} {:<15}".format("Ticker", "Start", "End", "Change"))
    print("-" * 60)
    
    key_tickers = ['GOOGL', 'META', 'APP', 'TTD']
    
    for ticker in key_tickers:
        if ticker in percentages.columns:
            start_pct = percentages.loc[first_date, ticker]
            end_pct = percentages.loc[last_date, ticker]
            change = end_pct - start_pct
            
            print("{:<15} {:<14.2f}% {:<14.2f}% {:<15.2f}%".format(
                ticker, start_pct, end_pct, change
            ))
    
    # Calculate others percentage
    start_others_pct = sum(percentages.loc[first_date, ticker] for ticker in percentages.columns 
                          if ticker in adtech_tickers and ticker not in key_tickers)
    end_others_pct = sum(percentages.loc[last_date, ticker] for ticker in percentages.columns 
                        if ticker in adtech_tickers and ticker not in key_tickers)
    
    change_others = end_others_pct - start_others_pct
    
    print("{:<15} {:<14.2f}% {:<14.2f}% {:<15.2f}%".format(
        "Others", start_others_pct, end_others_pct, change_others
    ))

if __name__ == "__main__":
    display_daily_adtech_history()