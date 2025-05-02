"""Sector market indices calculator using Yahoo Finance data.

This module calculates sector indices and momentum indicators from Yahoo Finance
using the provided ticker list for each technology sector.
"""

import os
import json
import time
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pytz
import yfinance as yf
import random

# Data directory for caching sector data
DATA_DIR = 'data'
SECTOR_DATA_DIR = os.path.join(DATA_DIR, 'sector_data')
TICKER_DATA_DIR = os.path.join(DATA_DIR, 'ticker_data')
os.makedirs(SECTOR_DATA_DIR, exist_ok=True)
os.makedirs(TICKER_DATA_DIR, exist_ok=True)

# Cache timeout (24 hours in seconds)
CACHE_TIMEOUT_SECONDS = 86400

# Path to the ticker list CSV file
TICKER_LIST_CSV = 'attached_assets/Formatted_Sector_Ticker_List.csv'

# Mapping for sector names (if needed)
SECTOR_MAPPING = {
    "SMB SaaS": "Vertical SaaS",  # Map SMB SaaS to Vertical SaaS for compatibility
    "Dev Tools": "Dev Tools / Analytics",  # Map to code's naming convention
    "IT Services": "IT Services / Legacy Tech",  # Map to code's naming convention
    "Hardware/Devices": "Hardware / Devices",  # Map to code's naming convention
    # Other sectors match exactly
}

def load_ticker_list():
    """Load sector tickers from the CSV file.
    
    Returns:
        dict: Dictionary mapping sector names to lists of ticker symbols
    """
    try:
        # Check if CSV file exists
        if not os.path.exists(TICKER_LIST_CSV):
            print(f"ERROR: Ticker list CSV file not found at {TICKER_LIST_CSV}")
            return {}
            
        # Read the CSV file
        df = pd.read_csv(TICKER_LIST_CSV)
        
        # Group by sector and create a dictionary
        sector_tickers = {}
        for sector, group in df.groupby('Sector'):
            sector_tickers[sector] = group['Ticker'].tolist()
            
        print(f"Loaded {len(sector_tickers)} sectors with {sum(len(tickers) for tickers in sector_tickers.values())} tickers")
        return sector_tickers
    
    except Exception as e:
        print(f"Error loading ticker list: {str(e)}")
        return {}

def get_cache_filename(ticker):
    """Get the filename for cached ticker data.
    
    Args:
        ticker (str): The ticker symbol
        
    Returns:
        str: Path to the cache file
    """
    return os.path.join(TICKER_DATA_DIR, f"{ticker.replace('/', '_')}_data.json")

def get_sector_cache_filename(sector):
    """Get the filename for cached sector index data.
    
    Args:
        sector (str): The sector name
        
    Returns:
        str: Path to the cache file
    """
    return os.path.join(SECTOR_DATA_DIR, f"{sector.replace('/', '_')}_index.json")

def is_cache_valid(filename):
    """Check if cached data is still valid (not expired).
    
    Args:
        filename (str): Path to the cache file
        
    Returns:
        bool: True if cache is valid, False otherwise
    """
    if not os.path.exists(filename):
        return False
        
    # Get file modification time
    mod_time = os.path.getmtime(filename)
    age_seconds = time.time() - mod_time
    
    # Valid if less than 24 hours old
    return age_seconds < CACHE_TIMEOUT_SECONDS

def fetch_ticker_data(ticker, period='30d'):
    """Fetch historical data for a ticker with caching.
    
    Args:
        ticker (str): The ticker symbol
        period (str, optional): Period of historical data to fetch
        
    Returns:
        dict: Dictionary with ticker data
    """
    # Check cache first
    cache_file = get_cache_filename(ticker)
    if is_cache_valid(cache_file):
        try:
            with open(cache_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error reading cache for {ticker}: {str(e)}")
            # Continue with fetching fresh data
    
    try:
        # Random delay between 0.1 and 1.0 seconds to avoid rate limiting
        time.sleep(random.uniform(0.1, 1.0))
        
        # Fetch historical data from Yahoo Finance
        ticker_obj = yf.Ticker(ticker)
        hist = ticker_obj.history(period=period)
        
        if hist.empty:
            print(f"No historical data found for {ticker}")
            return None
            
        # Get market cap (for weighting)
        try:
            market_cap = ticker_obj.info.get('marketCap')
            if not market_cap or market_cap <= 0:
                market_cap = None
        except Exception as e:
            print(f"Error fetching market cap for {ticker}: {str(e)}")
            market_cap = None
            
        # Prepare result dictionary
        result = {
            'ticker': ticker,
            'market_cap': market_cap,
            'last_price': float(hist['Close'].iloc[-1]),
            'prices': hist['Close'].tolist(),
            'dates': [d.strftime('%Y-%m-%d') for d in hist.index.tolist()],
            'fetch_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # Cache the result
        try:
            with open(cache_file, 'w') as f:
                json.dump(result, f)
        except Exception as e:
            print(f"Error saving cache for {ticker}: {str(e)}")
            
        return result
        
    except Exception as e:
        print(f"Error fetching data for {ticker}: {str(e)}")
        return None

def calculate_ema(prices, span=20):
    """Calculate Exponential Moving Average for a price series.
    
    Args:
        prices (list): List of price values
        span (int, optional): EMA period/span
        
    Returns:
        list: List of EMA values
    """
    if not prices or len(prices) < span:
        return []
        
    # Convert to numpy array
    prices_array = np.array(prices)
    
    # Calculate alpha (smoothing factor)
    alpha = 2 / (span + 1)
    
    # Calculate EMA
    ema = np.zeros_like(prices_array)
    ema[0] = prices_array[0]  # Initialize with first price
    
    for i in range(1, len(prices_array)):
        ema[i] = alpha * prices_array[i] + (1 - alpha) * ema[i-1]
    
    return ema.tolist()

def calculate_sector_index(sector, use_cache=True):
    """Calculate market-cap weighted index for a sector.
    
    Args:
        sector (str): The sector name
        use_cache (bool, optional): Whether to use cached data
        
    Returns:
        dict: Dictionary with sector index data
    """
    print(f"Calculating sector index for {sector}")
    
    # Check sector cache if requested
    cache_file = get_sector_cache_filename(sector)
    if use_cache and is_cache_valid(cache_file):
        try:
            with open(cache_file, 'r') as f:
                data = json.load(f)
                print(f"Using cached sector data for {sector}")
                return data
        except Exception as e:
            print(f"Error reading sector cache for {sector}: {str(e)}")
            # Continue with calculation
    
    # Load sector tickers
    sector_tickers = load_ticker_list().get(sector, [])
    if not sector_tickers:
        print(f"No tickers found for sector: {sector}")
        return None
    
    print(f"Fetching data for {len(sector_tickers)} tickers in {sector}...")
    
    # Fetch data for all tickers in the sector
    ticker_data = []
    for ticker in sector_tickers:
        data = fetch_ticker_data(ticker)
        if data and data.get('market_cap'):
            ticker_data.append(data)
        else:
            print(f"Skipping {ticker} due to missing data or market cap")
    
    if not ticker_data:
        print(f"No valid ticker data for sector: {sector}")
        return None
    
    # Calculate market cap weights
    total_market_cap = sum(d['market_cap'] for d in ticker_data if d.get('market_cap'))
    for data in ticker_data:
        if data.get('market_cap'):
            data['weight'] = data['market_cap'] / total_market_cap
        else:
            data['weight'] = 0
    
    # Save weights to a separate file for reference
    weights_file = os.path.join(SECTOR_DATA_DIR, f"{sector.replace('/', '_')}_weights.json")
    with open(weights_file, 'w') as f:
        weights_data = {
            'sector': sector,
            'tickers': [{"ticker": d["ticker"], "weight": d["weight"]} for d in ticker_data]
        }
        json.dump(weights_data, f, indent=2)
    print(f"Saved weights for {sector} to {weights_file}")
    
    # Get a list of all dates across all tickers
    all_dates = set()
    for data in ticker_data:
        all_dates.update(data.get('dates', []))
    
    # Sort dates chronologically
    sorted_dates = sorted(all_dates)
    
    # For each date, calculate the weighted index value
    index_values = []
    for date in sorted_dates:
        value = 0
        total_weight_for_date = 0
        
        for data in ticker_data:
            if date in data.get('dates', []):
                idx = data['dates'].index(date)
                price = data['prices'][idx]
                weight = data.get('weight', 0)
                value += price * weight
                total_weight_for_date += weight
        
        # Normalize by dividing by total weight for this date
        if total_weight_for_date > 0:
            value = value / total_weight_for_date
            index_values.append({
                'date': date,
                'value': value
            })
    
    # Calculate 20-day EMA for the index
    if len(index_values) >= 20:
        prices = [item['value'] for item in index_values]
        ema_values = calculate_ema(prices, span=20)
        
        # Add EMA to index values
        for i in range(len(index_values)):
            if i < len(ema_values):
                index_values[i]['ema20'] = ema_values[i]
    
    # Calculate gap percentage for the most recent value
    if len(index_values) > 0 and 'ema20' in index_values[-1]:
        latest = index_values[-1]
        current_value = latest['value']
        ema_value = latest['ema20']
        gap_pct = ((current_value - ema_value) / ema_value) * 100
    else:
        gap_pct = 0
    
    # Prepare sector index data
    result = {
        'sector': sector,
        'values': index_values,
        'last_value': index_values[-1]['value'] if index_values else 0,
        'last_ema': index_values[-1].get('ema20', 0) if index_values else 0,
        'gap_pct': gap_pct,
        'num_tickers': len(ticker_data),
        'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    # Cache the result
    with open(cache_file, 'w') as f:
        json.dump(result, f)
    print(f"Cached index data for {sector} with {len(index_values)} data points")
    
    return result

def get_sector_momentum(sector_name, use_cache=True):
    """Get the momentum (gap between current value and EMA) for a sector.
    
    Args:
        sector_name (str): Name of the sector
        use_cache (bool, optional): Whether to use cached data
        
    Returns:
        float: Gap percentage (momentum indicator)
    """
    # Map sector name if needed
    original_sector = sector_name
    for orig, mapped in SECTOR_MAPPING.items():
        if mapped == sector_name:
            original_sector = orig
            print(f"Using original sector name {original_sector} for {sector_name}")
            break
    
    # Calculate or retrieve the sector index
    index_data = calculate_sector_index(original_sector, use_cache=use_cache)
    if not index_data:
        print(f"No index data available for sector: {original_sector}")
        return 0.0
    
    # Get the gap percentage
    gap_pct = index_data.get('gap_pct', 0.0)
    print(f"{sector_name} momentum: {gap_pct:.2f}%")
    
    return gap_pct

def get_all_sector_momentums(use_cache=True):
    """Get momentum indicators for all sectors.
    
    Args:
        use_cache (bool, optional): Whether to use cached data
        
    Returns:
        dict: Dictionary mapping sector names to their momentum values
    """
    # Load ticker list to get all sector names
    sector_tickers = load_ticker_list()
    if not sector_tickers:
        print("No sectors found in ticker list")
        return {}
    
    momentums = {}
    
    # Process each sector
    for sector in sector_tickers.keys():
        try:
            # Calculate sector momentum
            momentum = get_sector_momentum(sector, use_cache=use_cache)
            
            # Map sector name if needed for consistency with application
            mapped_sector = SECTOR_MAPPING.get(sector, sector)
            
            # Store momentum
            momentums[mapped_sector] = momentum
            
        except Exception as e:
            print(f"Error processing {sector}: {str(e)}")
    
    print(f"Retrieved sector momentums for {len(momentums)} sectors")
    return momentums

def get_sector_index_dataframe(sector_name, use_cache=True):
    """Get sector index data as a pandas DataFrame.
    
    Args:
        sector_name (str): Name of the sector
        use_cache (bool, optional): Whether to use cached data
        
    Returns:
        pd.DataFrame: DataFrame with date, value, ema20, and gap_pct columns
    """
    # Calculate or retrieve the sector index
    index_data = calculate_sector_index(sector_name, use_cache=use_cache)
    if not index_data or 'values' not in index_data:
        print(f"No index data available for sector: {sector_name}")
        return pd.DataFrame()
    
    # Extract values
    values = index_data['values']
    
    # Create DataFrame
    rows = []
    for item in values:
        row = {
            'date': pd.to_datetime(item['date']),
            'value': item['value'],
            'ema20': item.get('ema20', None)
        }
        
        # Calculate gap percentage if EMA is available
        if 'ema20' in item and item['ema20'] > 0:
            row['gap_pct'] = ((item['value'] - item['ema20']) / item['ema20']) * 100
        else:
            row['gap_pct'] = None
            
        rows.append(row)
    
    df = pd.DataFrame(rows)
    
    # Forward fill gaps in ema20 and gap_pct
    if not df.empty:
        df['ema20'] = df['ema20'].ffill()
        df['gap_pct'] = df['gap_pct'].ffill()
    
    return df

# Test the module when run directly
if __name__ == "__main__":
    # Load all sectors and tickers
    sectors = load_ticker_list()
    print(f"Found {len(sectors)} sectors: {', '.join(sectors.keys())}")
    
    # Test calculating sector momentum for one sector
    test_sector = list(sectors.keys())[0] if sectors else "AdTech"
    print(f"\nCalculating momentum for {test_sector}:")
    momentum = get_sector_momentum(test_sector, use_cache=False)
    print(f"{test_sector} momentum: {momentum:.2f}%")
    
    # Uncomment to test calculating momentum for all sectors
    # print("\nCalculating momentum for all sectors:")
    # all_momentums = get_all_sector_momentums(use_cache=False)
    # for sector, momentum in all_momentums.items():
    #     print(f"{sector}: {momentum:.2f}%")
