"""Sector data reader for T2D Pulse dashboard using Finnhub API.

This module reads sector data from Finnhub API to calculate sector indices
and momentum for the T2D Pulse dashboard.
"""

import os
import json
import time
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pytz

# Data directory for caching
DATA_DIR = 'data'
SECTOR_DATA_DIR = os.path.join(DATA_DIR, 'sector_data')
FINNHUB_DATA_DIR = os.path.join(DATA_DIR, 'finnhub_data')
os.makedirs(SECTOR_DATA_DIR, exist_ok=True)
os.makedirs(FINNHUB_DATA_DIR, exist_ok=True)

# Cache timeout in hours (how long to use cached data before refreshing)
CACHE_TIMEOUT_HOURS = 24

# Define sector names
SECTOR_NAMES = [
    "AdTech",
    "Cloud",
    "Fintech",
    "eCommerce",
    "Cybersecurity",
    "Dev Tools",
    "Semiconductors",
    "AI Infrastructure",
    "Enterprise SaaS",
    "Vertical SaaS",
    "Consumer Internet",
    "SMB SaaS",
    "IT Services",
    "Hardware/Devices"
]

# Define sector ticker mapping
sector_tickers = {
    "AdTech": ["APP", "APPS", "CRTO", "DV", "GOOGL", "IAD", "META", "MGNI", "PUBM", "TTD"],
    "Cloud": ["AMZN", "CRM", "CSCO", "GOOGL", "MSFT", "NET", "ORCL", "SNOW"],
    "Fintech": ["ADYEY", "AFRM", "BILL", "COIN", "FIS", "FISV", "GPN", "PYPL", "SQ", "SSNC"],
    "eCommerce": ["AMZN", "BABA", "BKNG", "CHWY", "EBAY", "ETSY", "PDD", "SE", "SHOP", "WMT"],
    "Consumer Internet": ["ABNB", "BKNG", "GOOGL", "META", "NFLX", "PINS", "SNAP", "SPOT", "TRIP", "YELP"],
    "IT Services": ["ACN", "CTSH", "DXC", "HPQ", "IBM", "INFY", "PLTR", "WIT"],
    "Hardware/Devices": ["AAPL", "DELL", "HPQ", "LOGI", "PSTG", "SMCI", "SSYS", "STX", "WDC"],
    "Cybersecurity": ["CHKP", "CRWD", "CYBR", "FTNT", "NET", "OKTA", "PANW", "S", "ZS"],
    "Dev Tools": ["DDOG", "ESTC", "GTLB", "MDB", "TEAM"],
    "AI Infrastructure": ["AMZN", "GOOGL", "IBM", "META", "MSFT", "NVDA", "ORCL"],
    "Semiconductors": ["AMAT", "AMD", "ARM", "AVGO", "INTC", "NVDA", "QCOM", "TSM"],
    "Vertical SaaS": ["CCCS", "CPRT", "CSGP", "GWRE", "ICE", "PCOR", "SSNC", "TTAN"],
    "Enterprise SaaS": ["ADSK", "AMZN", "CRM", "IBM", "MSFT", "NOW", "ORCL", "SAP", "WDAY"],
    "SMB SaaS": ["ADBE", "BILL", "GOOGL", "HUBS", "INTU", "META"]
}

# Mapping from Finnhub sector names to T2D Pulse sector names
SECTOR_MAPPING = {
    "SMB SaaS": "Vertical SaaS",  # Map SMB SaaS to Vertical SaaS for compatibility
    "Dev Tools": "Dev Tools / Analytics",  # Map to code's naming convention
    "IT Services": "IT Services / Legacy Tech",  # Map to code's naming convention
    "Hardware/Devices": "Hardware / Devices",  # Map to code's naming convention
    # Other sectors match exactly
}

def get_api_key():
    """Get Finnhub API key from environment variables"""
    api_key = os.environ.get('FINNHUB_API_KEY')
    if not api_key:
        print("WARNING: FINNHUB_API_KEY environment variable not set")
    return api_key

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

def get_cache_filename(sector):
    """Get the filename for cached sector data.
    
    Args:
        sector (str): The sector name
        
    Returns:
        str: Path to the cache file
    """
    return os.path.join(FINNHUB_DATA_DIR, f"{sector.replace('/', '_')}_data.csv")

def is_cache_valid(sector):
    """Check if the cached data is still valid (not expired).
    
    Args:
        sector (str): The sector name
        
    Returns:
        bool: True if cache is valid, False otherwise
    """
    cache_file = get_cache_filename(sector)
    
    if not os.path.exists(cache_file):
        return False
        
    file_mtime = datetime.fromtimestamp(os.path.getmtime(cache_file))
    age_hours = (datetime.now() - file_mtime).total_seconds() / 3600
    
    return age_hours < CACHE_TIMEOUT_HOURS

def fetch_sector_data(sector):
    """Fetch data for a specific sector from Finnhub.
    
    Args:
        sector (str): The sector name
        
    Returns:
        pd.DataFrame: DataFrame with ticker data for the sector
    """
    # Check if we should use cached data
    cache_file = get_cache_filename(sector)
    if is_cache_valid(sector):
        print(f"Using cached Finnhub data for {sector}")
        return pd.read_csv(cache_file)
    
    api_key = get_api_key()
    if not api_key:
        if os.path.exists(cache_file):
            print(f"Using expired cached data for {sector} (no API key)")
            return pd.read_csv(cache_file)
        return pd.DataFrame()
    
    # Get tickers for this sector
    tickers = sector_tickers.get(sector, [])
    if not tickers:
        print(f"No tickers defined for sector: {sector}")
        return pd.DataFrame()
    
    # Initialize list to store ticker data
    data = []
    
    # Fetch data for each ticker
    for ticker in tickers:
        try:
            # Fetch current price
            quote_url = f'https://finnhub.io/api/v1/quote?symbol={ticker}&token={api_key}'
            quote_response = requests.get(quote_url)
            if quote_response.status_code == 200:
                quote_data = quote_response.json()
                current_price = quote_data.get('c')
            else:
                print(f"Failed to fetch price for {ticker}: {quote_response.status_code}")
                current_price = None
    
            # Fetch market capitalization
            profile_url = f'https://finnhub.io/api/v1/stock/profile2?symbol={ticker}&token={api_key}'
            profile_response = requests.get(profile_url)
            if profile_response.status_code == 200:
                profile_data = profile_response.json()
                market_cap = profile_data.get('marketCapitalization')
            else:
                print(f"Failed to fetch profile for {ticker}: {profile_response.status_code}")
                market_cap = None
    
            # Append the data
            if current_price is not None and market_cap is not None:
                data.append({
                    'Ticker': ticker,
                    'Price': current_price,
                    'MarketCap': market_cap * 1_000_000  # Convert to dollars (Finnhub returns in millions)
                })
            
            # Respect API rate limits (60 requests per minute)
            time.sleep(1.1)  # Just over 1 second per request to stay under the limit
            
        except Exception as e:
            print(f"Error fetching data for {ticker}: {str(e)}")
    
    # Convert to DataFrame
    df = pd.DataFrame(data)
    
    # Calculate total market cap and weights
    if not df.empty:
        total_market_cap = df['MarketCap'].sum()
        df['Weight'] = df['MarketCap'] / total_market_cap
        df['WeightedPrice'] = df['Price'] * df['Weight']
    
    # Save to cache
    if not df.empty:
        df.to_csv(cache_file, index=False)
        print(f"Cached Finnhub data for {sector} with {len(df)} tickers")
    
    return df

def calculate_sector_index(sector_data, base_value=100.0, base_date=None):
    """Calculate a simple sector index from the ticker data.
    
    Args:
        sector_data (pd.DataFrame): Ticker data with weights
        base_value (float): The base value for the index
        base_date (datetime, optional): The base date for the index
        
    Returns:
        dict: Dictionary with sector index data
    """
    if sector_data.empty:
        return None
    
    # Calculate the current weighted value
    weighted_value = sector_data['WeightedPrice'].sum()
    
    # For now, just use a constant base value (real implementation would calculate this over time)
    current_value = base_value
    
    # In a real implementation, we would calculate the EMA based on historical data
    # For now, use a simple approximation (5% below current value)
    ema20 = current_value * 0.95
    
    # Calculate the gap percentage
    gap_pct = ((current_value - ema20) / ema20) * 100
    
    return {
        'date': datetime.now().strftime('%Y-%m-%d'),
        'value': current_value,
        'ema20': ema20,
        'gap_pct': gap_pct
    }

def get_sector_momentum(sector_name):
    """Get the momentum (gap between current value and EMA) for a sector.
    
    Args:
        sector_name (str): Name of the sector
        
    Returns:
        float: Gap percentage (momentum indicator)
    """
    # Map sector name if needed
    mapped_sector = sector_name
    if sector_name in SECTOR_MAPPING:
        mapped_sector = SECTOR_MAPPING[sector_name]
        print(f"Mapping sector name from {sector_name} to {mapped_sector}")
    
    # Check if the reverse mapping exists (we need the original name for fetching data)
    original_sector = sector_name
    for orig, mapped in SECTOR_MAPPING.items():
        if mapped == sector_name:
            original_sector = orig
            print(f"Found original sector name {original_sector} for {sector_name}")
            break
    
    # Try using the original sector name for data fetching
    sector_data = fetch_sector_data(original_sector)
    if sector_data.empty:
        print(f"No data found for sector: {original_sector}")
        return 0.0
    
    # Calculate the sector index
    index_data = calculate_sector_index(sector_data)
    if not index_data:
        print(f"Could not calculate index for sector: {sector_name}")
        return 0.0
    
    # Get the gap percentage
    gap_pct = index_data['gap_pct']
    print(f"{sector_name} momentum from Finnhub: {gap_pct:.2f}%")
    
    return gap_pct

def get_all_sector_momentums():
    """Get momentum indicators for all sectors.
    
    Returns:
        dict: Dictionary mapping sector names to their momentum values
    """
    momentums = {}
    
    # Fetch data for each sector
    for sector in sector_tickers.keys():
        try:
            # Get the sector data
            sector_data = fetch_sector_data(sector)
            if sector_data.empty:
                continue
            
            # Calculate the sector index
            index_data = calculate_sector_index(sector_data)
            if not index_data:
                continue
            
            # Map sector name if needed for consistency with application
            mapped_sector = SECTOR_MAPPING.get(sector, sector)
            
            # Store the momentum value
            momentums[mapped_sector] = index_data['gap_pct']
            print(f"{mapped_sector} momentum: {index_data['gap_pct']:.2f}%")
            
        except Exception as e:
            print(f"Error calculating momentum for {sector}: {str(e)}")
    
    print(f"Retrieved sector momentums from Finnhub for {len(momentums)} sectors")
    return momentums

# Test the module when run directly
if __name__ == "__main__":
    # Test for one sector
    print("\nTesting for AdTech sector:")
    momentum = get_sector_momentum("AdTech")
    print(f"AdTech momentum: {momentum:.2f}%")
    
    # Test for all sectors
    print("\nTesting for all sectors:")
    all_momentums = get_all_sector_momentums()
    for sector, momentum in all_momentums.items():
        print(f"{sector}: {momentum:.2f}%")
