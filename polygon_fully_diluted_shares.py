"""
Polygon Fully Diluted Shares

This module provides functions to fetch and use fully diluted share counts 
from Polygon.io API for accurate market cap calculations.

Business Rule: Always use fully diluted shares for all market cap calculations.
"""
import os
import sys
import json
import logging
import requests
import pandas as pd
import time
from datetime import datetime, timedelta
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('polygon_fully_diluted.log')
    ]
)

# Define directories
DATA_DIR = "data"
CACHE_DIR = os.path.join(DATA_DIR, "cache")
SHARES_CACHE_FILE = os.path.join(CACHE_DIR, "shares_outstanding.json")

# Ensure directories exist
Path(DATA_DIR).mkdir(exist_ok=True)
Path(CACHE_DIR).mkdir(exist_ok=True)

# Define manual overrides for companies with known discrepancies
# Use these values when API data is unavailable or incorrect
SHARE_COUNT_OVERRIDES = {
    # Use the most authoritative source - company SEC filings
    "GOOGL": 12_291_000_000,  # Alphabet Inc.
    "META": 2_590_000_000,    # Meta Platforms Inc.
    # Add any others that need manual verification
}

def get_api_key():
    """Get Polygon API key from environment variable"""
    api_key = os.environ.get("POLYGON_API_KEY")
    if not api_key:
        logging.error("POLYGON_API_KEY environment variable not set")
        return None
    return api_key

def fetch_fully_diluted_shares(ticker, api_key):
    """
    Fetch fully diluted shares outstanding for a ticker from Polygon API
    
    Args:
        ticker (str): The ticker symbol
        api_key (str): The Polygon API key
        
    Returns:
        tuple: (fully_diluted_shares, ticker_name, market_cap)
    """
    # Check for manual override
    if ticker in SHARE_COUNT_OVERRIDES:
        logging.info(f"Using manual override for {ticker}: {SHARE_COUNT_OVERRIDES[ticker]:,} shares")
        return SHARE_COUNT_OVERRIDES[ticker], f"{ticker} (Manual Override)", None
    
    # Use Polygon v3 Tickers API endpoint
    url = f"https://api.polygon.io/v3/reference/tickers/{ticker}"
    headers = {"Authorization": f"Bearer {api_key}"}
    
    try:
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            results = data.get("results", {})
            
            # Get weighted shares outstanding (fully diluted)
            fully_diluted_shares = results.get("weighted_shares_outstanding")
            ticker_name = results.get("name", ticker)
            market_cap = results.get("market_cap")
            
            if fully_diluted_shares:
                logging.info(f"Retrieved fully diluted shares for {ticker}: {fully_diluted_shares:,}")
                return fully_diluted_shares, ticker_name, market_cap
            else:
                logging.warning(f"No fully diluted shares data available for {ticker}")
                
                # Fall back to share_class_shares_outstanding if available
                class_shares = results.get("share_class_shares_outstanding")
                if class_shares:
                    logging.warning(f"Using share class shares for {ticker}: {class_shares:,} (not fully diluted)")
                    return class_shares, ticker_name, market_cap
                
                return None, ticker_name, market_cap
        elif response.status_code == 429:
            # Rate limited - wait and retry
            logging.warning(f"Rate limited by Polygon API. Waiting 60 seconds...")
            time.sleep(60)
            return fetch_fully_diluted_shares(ticker, api_key)
        else:
            logging.error(f"Error {response.status_code} for {ticker}: {response.text}")
            return None, None, None
    except Exception as e:
        logging.error(f"Request error for {ticker}: {e}")
        return None, None, None

def load_shares_cache():
    """Load the shares outstanding cache"""
    if os.path.exists(SHARES_CACHE_FILE):
        try:
            with open(SHARES_CACHE_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Error loading shares cache: {e}")
    
    # Create cache directory if it doesn't exist
    Path(CACHE_DIR).mkdir(exist_ok=True)
    return {}

def save_shares_cache(cache):
    """Save the shares outstanding cache"""
    try:
        with open(SHARES_CACHE_FILE, 'w') as f:
            json.dump(cache, f, indent=2)
        logging.info(f"Saved shares cache to {SHARES_CACHE_FILE}")
    except Exception as e:
        logging.error(f"Error saving shares cache: {e}")

def get_fully_diluted_share_count(ticker, use_cache=True, update_cache=True):
    """
    Get fully diluted share count for a ticker
    
    Args:
        ticker (str): The ticker symbol
        use_cache (bool): Whether to use cached data if available
        update_cache (bool): Whether to update the cache with new data
        
    Returns:
        int or None: The fully diluted shares outstanding, or None if unavailable
    """
    # Check for manual override first
    if ticker in SHARE_COUNT_OVERRIDES:
        return SHARE_COUNT_OVERRIDES[ticker]
    
    # Check cache if enabled
    if use_cache:
        cache = load_shares_cache()
        if ticker in cache:
            cache_entry = cache[ticker]
            # Handle both dictionary and integer formats in cache
            if isinstance(cache_entry, dict):
                # Dictionary format (new style)
                # Check if cache entry is recent (less than 7 days old)
                cached_date = datetime.fromisoformat(cache_entry.get("date", "2000-01-01"))
                if datetime.now() - cached_date < timedelta(days=7):
                    logging.info(f"Using cached share count for {ticker}: {cache_entry.get('shares'):,}")
                    return cache_entry.get("shares")
            elif isinstance(cache_entry, (int, float)) and cache_entry > 0:
                # Integer format (old style) - still a valid share count
                logging.info(f"Using cached share count for {ticker}: {cache_entry:,}")
                return cache_entry
    
    # Fetch fresh data
    api_key = get_api_key()
    if not api_key:
        return None
    
    shares, name, market_cap = fetch_fully_diluted_shares(ticker, api_key)
    
    # Update cache if enabled and we got data
    if update_cache and shares is not None:
        cache = load_shares_cache()
        cache[ticker] = {
            "shares": shares,
            "name": name,
            "date": datetime.now().isoformat(),
            "market_cap": market_cap
        }
        save_shares_cache(cache)
    
    return shares

def ensure_fully_diluted_shares():
    """
    Ensure that fully diluted shares are being used for all market cap calculations
    by updating the cache with the latest data
    """
    logging.info("Ensuring fully diluted shares for all tickers")
    
    # Load or create the cache
    cache = load_shares_cache()
    
    # Check if we have the override tickers in the cache
    for ticker, shares in SHARE_COUNT_OVERRIDES.items():
        if ticker not in cache or cache[ticker].get("shares") != shares:
            logging.info(f"Updating cache with override for {ticker}: {shares:,}")
            cache[ticker] = {
                "shares": shares,
                "name": f"{ticker} (Manual Override)",
                "date": datetime.now().isoformat(),
                "market_cap": None
            }
    
    # Save the updated cache
    save_shares_cache(cache)
    
    logging.info("Fully diluted shares enforcement completed")
    return True

def update_share_counts():
    """
    Update share counts for all tickers in the system 
    to ensure fully diluted shares are used
    """
    logging.info("Starting update of all share counts with fully diluted values")
    
    try:
        # Load the list of tickers from the coverage file
        coverage_file = "T2D_Pulse_93_tickers_coverage.csv"
        if not os.path.exists(coverage_file):
            logging.error(f"Coverage file not found: {coverage_file}")
            return False
            
        # Load the CSV, skipping the header rows
        df = pd.read_csv(coverage_file, skiprows=7)
        
        # Get unique tickers
        tickers = df['Ticker'].unique()
        logging.info(f"Found {len(tickers)} unique tickers in coverage file")
        
        # Get API key
        api_key = get_api_key()
        if not api_key:
            logging.error("POLYGON_API_KEY not set in environment. Cannot fetch share data.")
            return False
            
        # Initialize cache if it doesn't exist
        if not os.path.exists(SHARES_CACHE_FILE):
            cache = {}
            Path(CACHE_DIR).mkdir(exist_ok=True)
            with open(SHARES_CACHE_FILE, 'w') as f:
                json.dump(cache, f)
        else:
            # Load the cache
            with open(SHARES_CACHE_FILE, 'r') as f:
                try:
                    cache = json.load(f)
                except json.JSONDecodeError:
                    logging.error("Invalid JSON in cache file. Creating new cache.")
                    cache = {}
        
        # First, ensure all override tickers are in the cache
        for ticker, shares in SHARE_COUNT_OVERRIDES.items():
            if ticker not in cache or cache[ticker].get('shares') != shares:
                logging.info(f"Adding override for {ticker}: {shares:,} shares")
                cache[ticker] = {
                    "shares": shares,
                    "name": f"{ticker} (Manual Override)",
                    "date": datetime.now().isoformat(),
                    "market_cap": None
                }
        
        # Save the updated cache with overrides
        with open(SHARES_CACHE_FILE, 'w') as f:
            json.dump(cache, f, indent=2)
        
        # Update each ticker
        success_count = 0
        for ticker in tickers:
            try:
                # Check if it's an override ticker
                if ticker in SHARE_COUNT_OVERRIDES:
                    success_count += 1
                    continue
                    
                # Check if ticker is already in cache and recent
                if ticker in cache:
                    entry = cache[ticker]
                    if "date" in entry and "shares" in entry:
                        cache_date = datetime.fromisoformat(entry["date"])
                        if datetime.now() - cache_date < timedelta(days=7):
                            # Cache is recent, no need to update
                            success_count += 1
                            continue
                
                # Fetch the fully diluted share count
                shares, name, market_cap = fetch_fully_diluted_shares(ticker, api_key)
                
                if shares is not None:
                    # Update the cache
                    cache[ticker] = {
                        "shares": shares,
                        "name": name,
                        "date": datetime.now().isoformat(),
                        "market_cap": market_cap
                    }
                    success_count += 1
                    logging.info(f"Updated {ticker} with {shares:,} shares")
                else:
                    logging.warning(f"Could not get shares for {ticker}")
            except Exception as e:
                logging.error(f"Error processing {ticker}: {e}")
                
        # Save the final updated cache
        with open(SHARES_CACHE_FILE, 'w') as f:
            json.dump(cache, f, indent=2)
                
        logging.info(f"Successfully updated {success_count}/{len(tickers)} tickers with fully diluted shares")
        return success_count == len(tickers)
        
    except Exception as e:
        logging.error(f"Error updating share counts: {e}")
        return False
    
def get_all_share_counts():
    """
    Get all share counts as a dictionary mapping ticker to share count
    
    Returns:
        dict: Dictionary mapping ticker to share count
    """
    # Load the cache
    cache = load_shares_cache()
    
    # Extract share counts
    share_counts = {}
    for ticker, entry in cache.items():
        share_counts[ticker] = entry.get("shares")
    
    # Add any missing overrides
    for ticker, shares in SHARE_COUNT_OVERRIDES.items():
        if ticker not in share_counts:
            share_counts[ticker] = shares
    
    return share_counts

if __name__ == "__main__":
    print("Polygon Fully Diluted Shares Utility")
    print("------------------------------------")
    
    if len(sys.argv) > 1:
        ticker = sys.argv[1].upper()
        shares = get_fully_diluted_share_count(ticker, use_cache=False, update_cache=True)
        if shares:
            print(f"{ticker}: {shares:,} shares (fully diluted)")
        else:
            print(f"No share data available for {ticker}")
    else:
        print("Updating all share counts with fully diluted values...")
        if update_share_counts():
            print("Successfully updated all share counts")
        else:
            print("Failed to update some share counts")