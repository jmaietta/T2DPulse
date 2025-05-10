"""
Display AdTech sentiment alongside market cap data for the past 30 days.
"""
import os
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pytz
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Define directories and files
DATA_DIR = "data"
CACHE_DIR = os.path.join(DATA_DIR, "cache")
SECTOR_HISTORY_FILE = os.path.join(DATA_DIR, "sector_sentiment_history.json")
AUTHENTIC_HISTORY_FILE = os.path.join(DATA_DIR, "authentic_sector_history.json")

def load_market_cap_data():
    """Load historical market cap data for each ticker"""
    shares_cache = os.path.join(CACHE_DIR, "shares_outstanding.json")
    price_cache = os.path.join(CACHE_DIR, "historical_prices.pkl")
    
    if not os.path.exists(shares_cache) or not os.path.exists(price_cache):
        logging.error("Missing required cache files")
        return None
    
    # Load shares outstanding
    with open(shares_cache, 'r') as f:
        shares_dict = json.load(f)
    
    # Load price data
    price_dict = pd.read_pickle(price_cache)
    price_df = pd.DataFrame(price_dict)
    price_df.index = pd.to_datetime(price_df.index)
    
    # Calculate market caps
    market_caps = pd.DataFrame(index=price_df.index)
    
    # Define AdTech tickers
    adtech_tickers = ["GOOGL", "META", "APP", "TTD", "DV", "MGNI", "CRTO", "PUBM", "APPS"]
    
    for ticker in adtech_tickers:
        if ticker in shares_dict and ticker in price_df.columns:
            market_caps[ticker] = price_df[ticker] * shares_dict[ticker]
    
    # Calculate total AdTech market cap
    market_caps['Total'] = market_caps.sum(axis=1)
    
    # Filter to business days in last 30 days
    end_date = market_caps.index.max()
    start_date = end_date - timedelta(days=30)
    market_caps = market_caps[(market_caps.index >= start_date) & (market_caps.index.dayofweek < 5)]
    
    return market_caps

def load_sector_sentiment_history():
    """Load historical sector sentiment data"""
    if os.path.exists(SECTOR_HISTORY_FILE):
        try:
            with open(SECTOR_HISTORY_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Error loading sector sentiment history: {e}")
            return None
    else:
        logging.error(f"Sector sentiment history file not found: {SECTOR_HISTORY_FILE}")
        return None

def load_macro_indicators():
    """Load macro economic indicators"""
    # These are the indicators we need for AdTech sentiment calculation
    indicators = [
        "10Y_Treasury_Yield_%", "VIX", "Fed_Funds_Rate_%", "CPI_YoY_%", 
        "PCEPI_YoY_%", "Real_GDP_Growth_%_SAAR", "Real_PCE_YoY_%", 
        "Unemployment_%", "Software_Dev_Job_Postings_YoY_%",
        "PPI_Data_Processing_YoY_%", "PPI_Software_Publishers_YoY_%",
        "Consumer_Sentiment"
    ]
    
    # Get latest values for these indicators
    # For now, use fixed values for demonstration
    macro_values = {
        "10Y_Treasury_Yield_%": 3.95,
        "VIX": 24.76,
        "Fed_Funds_Rate_%": 4.33,
        "CPI_YoY_%": 3.5,
        "PCEPI_YoY_%": 2.7,
        "Real_GDP_Growth_%_SAAR": 2.1,
        "Real_PCE_YoY_%": 2.2,
        "Unemployment_%": 4.2,
        "Software_Dev_Job_Postings_YoY_%": -5.3,
        "PPI_Data_Processing_YoY_%": 1.8,
        "PPI_Software_Publishers_YoY_%": 2.5,
        "Consumer_Sentiment": 61.33
    }
    
    return macro_values

def display_adtech_sentiment_with_marketcap():
    """
    Display AdTech sentiment scores alongside market cap data
    """
    # Load market cap data
    market_caps = load_market_cap_data()
    
    if market_caps is None:
        logging.error("Could not load market cap data")
        return
    
    # Load sector sentiment history
    sector_history = load_sector_sentiment_history()
    
    if sector_history is None:
        logging.error("Could not load sector sentiment history")
        return
    
    # Create combined table
    print("\nAdTech Sector Sentiment and Market Cap History:\n")
    print("{:<12} {:<15} {:<15}".format(
        "Date", "Market Cap", "Sentiment"
    ))
    print("-" * 45)
    
    # Sort dates in reverse chronological order
    dates = sorted(market_caps.index, reverse=True)
    
    for date in dates:
        date_str = date.strftime("%Y-%m-%d")
        market_cap = market_caps.loc[date, 'Total'] / 1_000_000_000_000  # Convert to trillions
        
        # Get sentiment if available
        sentiment = None
        if date_str in sector_history:
            if 'AdTech' in sector_history[date_str]:
                sentiment = sector_history[date_str]['AdTech']
        
        # Print row
        print("{:<12} ${:<14.3f}T {:<15}".format(
            date_str, 
            market_cap, 
            f"{sentiment:.1f}" if sentiment is not None else "N/A"
        ))
    
    # Print relationship analysis
    print("\nRelationship between AdTech Market Cap and Sentiment:")
    
    # Create DataFrames for analysis
    combined_data = []
    
    for date in market_caps.index:
        date_str = date.strftime("%Y-%m-%d")
        market_cap = market_caps.loc[date, 'Total'] / 1_000_000_000_000
        
        sentiment = None
        if date_str in sector_history and 'AdTech' in sector_history[date_str]:
            sentiment = sector_history[date_str]['AdTech']
        
        if sentiment is not None:
            combined_data.append({
                'date': date,
                'market_cap': market_cap,
                'sentiment': sentiment
            })
    
    if combined_data:
        df = pd.DataFrame(combined_data)
        
        # Check if there's a correlation between market cap and sentiment
        correlation = df['market_cap'].corr(df['sentiment'])
        print(f"Correlation between Market Cap and Sentiment: {correlation:.3f}")
        
        # Find periods of highest/lowest market cap
        highest_cap = df.loc[df['market_cap'].idxmax()]
        lowest_cap = df.loc[df['market_cap'].idxmin()]
        
        print(f"Highest Market Cap: ${highest_cap['market_cap']:.3f}T on {highest_cap['date'].strftime('%Y-%m-%d')} with sentiment {highest_cap['sentiment']:.1f}")
        print(f"Lowest Market Cap: ${lowest_cap['market_cap']:.3f}T on {lowest_cap['date'].strftime('%Y-%m-%d')} with sentiment {lowest_cap['sentiment']:.1f}")
    else:
        print("Insufficient data for relationship analysis")

if __name__ == "__main__":
    display_adtech_sentiment_with_marketcap()