"""
Show AdTech Market Cap History

This script displays the historical market cap data for the AdTech sector
and its constituent companies over the past 30 days using the corrected
fully diluted share counts.
"""
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import pytz
import json

# Define directory for data
DATA_DIR = "data"
CACHE_DIR = os.path.join(DATA_DIR, "cache")

def load_market_cap_data():
    """
    Load market cap data from the parquet file
    
    Returns:
        DataFrame: Market cap data for all sectors
    """
    sector_file = os.path.join(DATA_DIR, "sector_market_caps.parquet")
    
    if os.path.exists(sector_file):
        return pd.read_parquet(sector_file)
    else:
        print(f"Sector market cap file not found: {sector_file}")
        return None

def load_ticker_market_caps():
    """
    Load market cap data for individual tickers
    
    Returns:
        DataFrame: Market cap data for all tickers
    """
    ticker_file = os.path.join(DATA_DIR, "ticker_market_caps.parquet")
    
    if os.path.exists(ticker_file):
        return pd.read_parquet(ticker_file)
    else:
        # Calculate ticker market caps from price and share data
        prices = load_historical_prices()
        shares = load_shares_outstanding()
        
        if prices is not None and shares is not None:
            # Calculate market caps
            market_caps = pd.DataFrame(index=prices.index)
            for ticker in prices.columns:
                if ticker in shares:
                    market_caps[ticker] = prices[ticker] * shares[ticker]
            
            return market_caps
        else:
            print("Could not calculate ticker market caps")
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

def get_adtech_tickers():
    """
    Get tickers in the AdTech sector
    
    Returns:
        list: Tickers in the AdTech sector
    """
    return ["APP", "APPS", "CRTO", "DV", "GOOGL", "META", "MGNI", "PUBM", "TTD"]

def filter_business_days(df, days=30):
    """
    Filter to only include business days in the last N days
    
    Args:
        df (DataFrame): Data to filter
        days (int): Number of days to include
        
    Returns:
        DataFrame: Filtered data
    """
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

def convert_to_billions(value):
    """
    Convert a value to billions
    
    Args:
        value (float): Value to convert
        
    Returns:
        float: Value in billions
    """
    return value / 1_000_000_000

def display_adtech_market_cap_history(days=30):
    """
    Display AdTech market cap history for the past N days
    
    Args:
        days (int): Number of days to display
    """
    # Load market cap data
    sector_caps = load_market_cap_data()
    ticker_caps = load_ticker_market_caps()
    
    if sector_caps is None or ticker_caps is None:
        print("Could not load market cap data")
        return
    
    # Filter to business days in the last N days
    sector_caps = filter_business_days(sector_caps, days)
    ticker_caps = filter_business_days(ticker_caps, days)
    
    if sector_caps is None or ticker_caps is None or sector_caps.empty or ticker_caps.empty:
        print("No market cap data available for the specified period")
        return
    
    # Get AdTech tickers
    adtech_tickers = get_adtech_tickers()
    
    # Filter to AdTech tickers
    adtech_ticker_caps = ticker_caps[[t for t in adtech_tickers if t in ticker_caps.columns]]
    
    # Get AdTech sector cap
    adtech_sector_cap = sector_caps["AdTech"]
    
    # Print table header
    print("\nAdTech Market Cap History (in billions USD) - Past 30 Days\n")
    
    # Convert date index to string for display
    dates = adtech_sector_cap.index.strftime("%Y-%m-%d").tolist()
    
    # Create table rows
    rows = []
    
    # Add sector total row
    sector_values = [convert_to_billions(adtech_sector_cap[date]) for date in adtech_sector_cap.index]
    rows.append(["AdTech Sector Total"] + [f"${value:.2f}B" for value in sector_values])
    
    # Add ticker rows
    for ticker in adtech_tickers:
        if ticker in adtech_ticker_caps.columns:
            ticker_values = [convert_to_billions(adtech_ticker_caps.loc[date, ticker]) if date in adtech_ticker_caps.index else None for date in adtech_sector_cap.index]
            ticker_row = [ticker] + [f"${value:.2f}B" if value is not None else "N/A" for value in ticker_values]
            rows.append(ticker_row)
    
    # Calculate column widths
    col_widths = [max(len(str(row[i])) for row in rows) for i in range(len(rows[0]))]
    
    # Print column headers
    header = ["Company"] + dates
    header_str = "  ".join(str(header[i]).ljust(col_widths[i]) for i in range(len(header)))
    print(header_str)
    print("-" * len(header_str))
    
    # Print rows
    for row in rows:
        row_str = "  ".join(str(row[i]).ljust(col_widths[i]) for i in range(len(row)))
        print(row_str)
    
    # Print market cap breakdown as of latest date
    latest_date = adtech_sector_cap.index.max()
    latest_sector_cap = convert_to_billions(adtech_sector_cap[latest_date])
    
    print(f"\nAdTech Market Cap Breakdown as of {latest_date.strftime('%Y-%m-%d')}:")
    
    for ticker in sorted(adtech_tickers, key=lambda t: adtech_ticker_caps.loc[latest_date, t] if t in adtech_ticker_caps.columns and latest_date in adtech_ticker_caps.index else 0, reverse=True):
        if ticker in adtech_ticker_caps.columns and latest_date in adtech_ticker_caps.index:
            ticker_cap = convert_to_billions(adtech_ticker_caps.loc[latest_date, ticker])
            percentage = (ticker_cap / latest_sector_cap) * 100
            print(f"  {ticker}: ${ticker_cap:.2f}B ({percentage:.1f}%)")
    
    print(f"  Total: ${latest_sector_cap:.2f}B")
    
    # Display line chart of AdTech market cap over time
    print("\nHistorical AdTech Market Cap Trend:")
    print(f"  Start: ${convert_to_billions(adtech_sector_cap.iloc[0]):.2f}B")
    print(f"  End:   ${convert_to_billions(adtech_sector_cap.iloc[-1]):.2f}B")
    change = (adtech_sector_cap.iloc[-1] / adtech_sector_cap.iloc[0] - 1) * 100
    print(f"  Change: {change:.1f}%")

if __name__ == "__main__":
    display_adtech_market_cap_history()