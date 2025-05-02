"""Sector indices using NASDAQ Data Link (Quandl) API for T2D Pulse dashboard.

This module creates sector-specific market indices using EOD data from NASDAQ Data Link.
It handles rate limits better and provides daily updates automatically without manual CSV uploads.
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import time
import nasdaqdatalink as ndl
import pytz

# Get API key from environment
NASDAQ_DATA_LINK_API_KEY = os.environ.get('NASDAQ_DATA_LINK_API_KEY')
if NASDAQ_DATA_LINK_API_KEY:
    ndl.ApiConfig.api_key = NASDAQ_DATA_LINK_API_KEY

# Data directory for caching index data
DATA_DIR = 'data'
os.makedirs(os.path.join(DATA_DIR, 'sector_indices'), exist_ok=True)

# Define ticker lists for each sector (same as sector_tickers.py but we keep a copy here)
SECTOR_TICKERS = {
    # AdTech companies
    "AdTech": [
        "GOOGL",  # Alphabet (Google)
        "META",   # Meta Platforms
        "TTD",    # The Trade Desk
        "MGNI",   # Magnite
        "APPS",   # Digital Turbine
        "CRTO"    # Criteo
    ],
    
    # Cloud companies
    "Cloud": [
        "MSFT",   # Microsoft (Azure)
        "AMZN",   # Amazon (AWS)
        "GOOGL",  # Alphabet (Google Cloud)
        "CRM",    # Salesforce
        "NET",    # Cloudflare
        "DDOG",   # Datadog
        "SNOW",   # Snowflake
        "ZS"      # Zscaler
    ],
    
    # Fintech companies
    "Fintech": [
        "PYPL",   # PayPal
        "COIN",   # Coinbase
        "AFRM",   # Affirm
        "FIS"     # Fidelity National Information
    ],
    
    # eCommerce companies
    "eCommerce": [
        "AMZN",   # Amazon
        "SHOP",   # Shopify
        "MELI",   # MercadoLibre
        "CPNG",   # Coupang
        "SE",     # Sea Limited
        "W",      # Wayfair
        "ETSY"    # Etsy
    ],
    
    # Cybersecurity companies
    "Cybersecurity": [
        "PANW",   # Palo Alto Networks
        "CRWD",   # CrowdStrike
        "FTNT",   # Fortinet
        "ZS",     # Zscaler
        "OKTA",   # Okta
        "S",      # SentinelOne
        "CYBR"    # CyberArk
    ],
    
    # Developer Tools / Analytics companies
    "Dev Tools / Analytics": [
        "MSFT",   # Microsoft (GitHub)
        "TEAM",   # Atlassian
        "DDOG",   # Datadog
        "NET",    # Cloudflare
        "GTLB",   # GitLab
        "ESTC",   # Elastic
        "MDB"     # MongoDB
    ],
    
    # Semiconductors companies
    "Semiconductors": [
        "NVDA",   # NVIDIA
        "AMD",    # Advanced Micro Devices
        "INTC",   # Intel
        "TSM",    # Taiwan Semiconductor
        "AVGO",   # Broadcom
        "QCOM",   # Qualcomm
        "ARM",    # ARM Holdings
        "AMAT"    # Applied Materials
    ],
    
    # AI Infrastructure companies
    "AI Infrastructure": [
        "NVDA",   # NVIDIA
        "AMD",    # Advanced Micro Devices
        "GOOGL",  # Alphabet (Google)
        "MSFT",   # Microsoft
        "AMZN",   # Amazon (AWS)
        "IBM",    # IBM
        "TSM",    # Taiwan Semiconductor
        "MU"      # Micron Technology
    ],
    
    # Enterprise SaaS companies
    "Enterprise SaaS": [
        "CRM",    # Salesforce
        "MSFT",   # Microsoft
        "WDAY",   # Workday
        "NOW",    # ServiceNow
        "ZM",     # Zoom
        "TEAM",   # Atlassian
        "DOCU",   # DocuSign
        "ZS"      # Zscaler
    ],
    
    # Vertical SaaS companies
    "Vertical SaaS": [
        "HUBS",   # HubSpot
        "VEEV",   # Veeva Systems
        "PCTY",   # Paylocity
        "ASAN",   # Asana
        "FRSH"    # Freshworks
    ],
    
    # Consumer Internet companies
    "Consumer Internet": [
        "GOOGL",  # Alphabet (Google)
        "META",   # Meta Platforms
        "NFLX",   # Netflix
        "UBER",   # Uber
        "ABNB",   # Airbnb
        "SNAP",   # Snap
        "PINS",   # Pinterest
        "SPOT"    # Spotify
    ],
    
    # Gaming companies
    "Gaming": [
        "EA",     # Electronic Arts
        "TTWO",   # Take-Two Interactive
        "RBLX",   # Roblox
        "U",      # Unity Software
        "CRSR",   # Corsair Gaming
        "MTCH"    # Match Group
    ],
    
    # IT Services / Legacy Tech companies
    "IT Services / Legacy Tech": [
        "IBM",    # IBM
        "ACN",    # Accenture
        "CTSH",   # Cognizant
        "ORCL",   # Oracle
        "XRX",    # Xerox
        "HPQ"     # HP
    ],
    
    # Hardware / Devices companies
    "Hardware / Devices": [
        "AAPL",   # Apple
        "HPQ",    # HP
        "LOGI",   # Logitech
        "SONO"    # Sonos
    ]
}

# Mapping from sector names to T2D Pulse sector names
SECTOR_MAPPING = {
    "SMB SaaS": "Vertical SaaS",  # Map SMB SaaS to Vertical SaaS tickers
    "Enterprise SaaS": "Enterprise SaaS",
    "Cloud Infrastructure": "Cloud",
    "AdTech": "AdTech",
    "Fintech": "Fintech",
    "Consumer Internet": "Consumer Internet",
    "eCommerce": "eCommerce",
    "Cybersecurity": "Cybersecurity",
    "Dev Tools / Analytics": "Dev Tools / Analytics",
    "Semiconductors": "Semiconductors",
    "AI Infrastructure": "AI Infrastructure",
    "Vertical SaaS": "Vertical SaaS",
    "IT Services / Legacy Tech": "IT Services / Legacy Tech",
    "Hardware / Devices": "Hardware / Devices"
}

def is_trading_day(date):
    """Check if a given date is a trading day (weekday, not a holiday).
    
    This is a simplified check that only excludes weekends, not holidays.
    
    Args:
        date (datetime): Date to check
        
    Returns:
        bool: True if the date is a trading day, False otherwise
    """
    # Check if it's a weekday (Monday=0, Sunday=6)
    return date.weekday() < 5  # 0-4 are weekdays

def get_latest_trading_day():
    """Get the latest trading day (most recent weekday).
    
    Returns:
        datetime: Date representing the latest trading day
    """
    today = datetime.now(pytz.timezone('US/Eastern'))
    
    # If today is a weekend, adjust to the most recent Friday
    if today.weekday() > 4:  # Saturday or Sunday
        days_to_subtract = today.weekday() - 4  # Adjust to Friday
        today = today - timedelta(days=days_to_subtract)
        
    return today.replace(hour=0, minute=0, second=0, microsecond=0)

def get_historical_stock_data(ticker, days=60):
    """Get historical stock price data for a ticker using NASDAQ Data Link.
    
    Args:
        ticker (str): Stock ticker symbol
        days (int): Number of days of historical data to fetch
        
    Returns:
        pd.DataFrame: DataFrame with date and price data
    """
    # Calculate start and end dates
    end_date = get_latest_trading_day()
    start_date = end_date - timedelta(days=days)
    
    try:
        # Create the database code for the stock (EOD database)
        database_code = f"EOD/{ticker}"
        
        # Fetch the data using NASDAQ Data Link API
        data = ndl.get(database_code, start_date=start_date, end_date=end_date)
        
        # Reset index to get date as a column
        data = data.reset_index()
        
        # Keep only the relevant columns (date and adjusted close)
        if 'Adj_Close' in data.columns:
            data = data[['Date', 'Adj_Close']]
            data.columns = ['date', 'price']
        else:
            return pd.DataFrame(columns=['date', 'price'])
        
        return data
    except Exception as e:
        print(f"Error fetching data for {ticker}: {str(e)}")
        return pd.DataFrame(columns=['date', 'price'])

def get_sector_index_data(sector_name, days=60, cache=True):
    """Get market index data for a sector using NASDAQ Data Link.
    
    Args:
        sector_name (str): Name of the sector
        days (int): Number of days of historical data
        cache (bool): Whether to use cached data if available
        
    Returns:
        pd.DataFrame: DataFrame with date, value, ema, and gap_pct columns
    """
    cache_file = os.path.join(DATA_DIR, 'sector_indices', f"{sector_name.replace('/', '_')}_index.csv")
    
    # Check if cache file exists and is recent
    if cache and os.path.exists(cache_file):
        try:
            cached_data = pd.read_csv(cache_file)
            cached_data['date'] = pd.to_datetime(cached_data['date'])
            
            # Check if the data is recent enough (within the last day)
            last_date = cached_data['date'].max()
            if datetime.now() - pd.to_datetime(last_date) < timedelta(days=1):
                print(f"Using cached index data for {sector_name}")
                return cached_data
            else:
                print(f"Cached data for {sector_name} is outdated, fetching fresh data")
        except Exception as e:
            print(f"Error reading cached index data: {str(e)}")
    
    # Get the tickers for this sector
    if sector_name not in SECTOR_TICKERS:
        print(f"Sector {sector_name} not found in sector tickers")
        return pd.DataFrame()
    
    tickers = SECTOR_TICKERS[sector_name]
    if not tickers:
        print(f"No tickers defined for sector {sector_name}")
        return pd.DataFrame()
    
    # Fetch data for each ticker
    ticker_data = {}
    for ticker in tickers:
        # Add a short delay to avoid hitting rate limits
        time.sleep(0.2)
        
        df = get_historical_stock_data(ticker, days)
        if not df.empty:
            ticker_data[ticker] = df
    
    if not ticker_data:
        print(f"No data retrieved for any tickers in {sector_name}")
        return pd.DataFrame()
    
    # For this implementation, we'll use equal weighting for simplicity
    # Create a common date index across all ticker data
    all_dates = set()
    for df in ticker_data.values():
        all_dates.update(df['date'])
    
    all_dates = sorted(all_dates)
    
    # Create the sector index by averaging prices across tickers for each date
    index_data = []
    for date in all_dates:
        values = []
        for ticker, df in ticker_data.items():
            # Find the price for this date if it exists
            price_row = df[df['date'] == date]
            if not price_row.empty:
                values.append(price_row['price'].iloc[0])
        
        if values:  # Only add if we have at least one value for this date
            index_value = np.mean(values)  # Simple average for now
            index_data.append({
                'date': date,
                'value': index_value
            })
    
    # Convert to DataFrame
    index_df = pd.DataFrame(index_data)
    
    if index_df.empty:
        print(f"No valid index data could be created for {sector_name}")
        return pd.DataFrame()
    
    # Sort by date
    index_df = index_df.sort_values('date')
    
    # Calculate 20-day EMA and gap percentage
    if len(index_df) >= 20:
        # Calculate EMA
        index_df['ema'] = index_df['value'].ewm(span=20, adjust=False).mean()
        
        # Calculate gap percentage
        index_df['gap_pct'] = ((index_df['value'] - index_df['ema']) / index_df['ema']) * 100
    else:
        # Not enough data for EMA, use value as EMA and zero gap
        index_df['ema'] = index_df['value']
        index_df['gap_pct'] = 0.0
    
    # Cache the data
    try:
        index_df.to_csv(cache_file, index=False)
        print(f"Cached index data for {sector_name} with {len(index_df)} rows")
    except Exception as e:
        print(f"Error caching index data: {str(e)}")
    
    return index_df

def get_sector_momentum(sector_name, cache=True):
    """Get the momentum (gap between current value and EMA) for a sector.
    
    Args:
        sector_name (str): Name of the sector
        cache (bool): Whether to use cached data if available
        
    Returns:
        float: Gap percentage (momentum indicator)
    """
    index_df = get_sector_index_data(sector_name, cache=cache)
    
    if index_df.empty or 'gap_pct' not in index_df.columns:
        print(f"No valid momentum data for {sector_name}")
        return 0.0
    
    latest_gap = index_df['gap_pct'].iloc[-1]
    print(f"{sector_name} momentum: {latest_gap:.2f}%")
    
    return latest_gap

def get_all_sector_momentums(cache=True):
    """Get momentum indicators for all sectors.
    
    Args:
        cache (bool): Whether to use cached data if available
        
    Returns:
        dict: Dictionary mapping sector names to their momentum values
    """
    momentums = {}
    for sector_name in SECTOR_TICKERS.keys():
        momentum = get_sector_momentum(sector_name, cache=cache)
        momentums[sector_name] = momentum
    
    print(f"Retrieved sector momentums for {len(momentums)} sectors")
    return momentums

def integrate_momentum_with_sentiment(sector_name, base_score, momentum, weight=0.4):
    """Integrate sector momentum into the sentiment score.
    
    Args:
        sector_name (str): Name of the sector
        base_score (float): Original sentiment score (0-100)
        momentum (float): Momentum value (gap percentage)
        weight (float): Weight to give the momentum in the final score (0-1)
        
    Returns:
        float: Adjusted sentiment score (0-100)
    """
    # Scale momentum to a 0-100 range for integration
    # Typical EMA gap percentages range from -5% to +5%, so scale accordingly
    # Values beyond this range will be capped
    scaled_momentum = max(0, min(100, (momentum + 5) * 10))
    
    # Blend the base score with the momentum score
    adjusted_score = base_score * (1 - weight) + scaled_momentum * weight
    
    return adjusted_score

# Function to check if we have a valid NASDAQ Data Link API key
def check_api_key():
    """Check if we have a valid NASDAQ Data Link API key configured.
    
    Returns:
        bool: True if API key is valid, False otherwise
    """
    if not NASDAQ_DATA_LINK_API_KEY:
        print("No NASDAQ Data Link API key found in environment")
        return False
    
    try:
        # Try a simple API call to verify the key
        ndl.get('EOD/AAPL', rows=1)
        return True
    except Exception as e:
        print(f"Error validating NASDAQ Data Link API key: {str(e)}")
        return False

# Main function to test the module
if __name__ == "__main__":
    # Check if we have a valid API key
    if not check_api_key():
        print("Please set the NASDAQ_DATA_LINK_API_KEY environment variable")
        exit(1)
    
    # Test with a sample sector
    sector_name = "Cloud"
    print(f"Testing with {sector_name} sector")
    
    # Get the sector index data
    index_df = get_sector_index_data(sector_name, cache=False)
    
    # Print the latest values
    if not index_df.empty:
        latest = index_df.iloc[-1]
        print(f"\nLatest index value: {latest['value']:.2f}")
        print(f"Latest EMA (20-day): {latest['ema']:.2f}")
        print(f"Latest momentum (gap %): {latest['gap_pct']:.2f}%")
        
        # Test integration with sentiment
        base_score = 60.0  # Example base sentiment score
        momentum = latest['gap_pct']
        adjusted_score = integrate_momentum_with_sentiment(sector_name, base_score, momentum)
        print(f"\nBase sentiment score: {base_score:.1f}")
        print(f"Momentum-adjusted score: {adjusted_score:.1f}")
    else:
        print("\nNo index data available")
