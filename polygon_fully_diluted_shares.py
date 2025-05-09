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
            return {}
    else:
        return {}

def save_shares_cache(shares_dict):
    """Save the shares outstanding cache"""
    try:
        # Create backup of existing file
        if os.path.exists(SHARES_CACHE_FILE):
            backup_file = f"{SHARES_CACHE_FILE}.bak"
            with open(backup_file, 'w') as f:
                json.dump(shares_dict, f)
            logging.info(f"Created backup of shares cache at {backup_file}")
        
        # Save updated file
        with open(SHARES_CACHE_FILE, 'w') as f:
            json.dump(shares_dict, f)
        return True
    except Exception as e:
        logging.error(f"Error saving shares cache: {e}")
        return False

def update_ticker_share_count(ticker, api_key=None):
    """
    Update share count for a single ticker
    
    Args:
        ticker (str): The ticker symbol
        api_key (str, optional): The Polygon API key
        
    Returns:
        dict: Dictionary with old and new share counts
    """
    # Get API key if not provided
    if not api_key:
        api_key = get_api_key()
        if not api_key:
            return None
    
    # Load current share counts
    shares_dict = load_shares_cache()
    
    # Get current value
    current_value = shares_dict.get(ticker)
    
    # Get fully diluted shares from Polygon
    fully_diluted_shares, name, market_cap = fetch_fully_diluted_shares(ticker, api_key)
    
    if fully_diluted_shares:
        # Record change if there is one
        if current_value != fully_diluted_shares:
            old_value = current_value
            shares_dict[ticker] = fully_diluted_shares
            
            # Save updated shares
            save_shares_cache(shares_dict)
            
            # Calculate percent change
            if old_value:
                percent_change = (fully_diluted_shares / old_value - 1) * 100
                change_str = f"{percent_change:.2f}%"
            else:
                change_str = "New"
            
            logging.info(f"Updated {ticker} share count: {old_value or 'None'} -> {fully_diluted_shares} ({change_str})")
            
            return {
                "ticker": ticker,
                "name": name,
                "old_value": old_value,
                "new_value": fully_diluted_shares,
                "percent_change": change_str
            }
        else:
            logging.info(f"No change for {ticker}: {current_value}")
            return None
    else:
        logging.warning(f"Failed to get share count data for {ticker}")
        return None

def update_all_ticker_share_counts(tickers):
    """
    Update share counts for multiple tickers
    
    Args:
        tickers (list): List of ticker symbols
        
    Returns:
        dict: Dictionary of tickers with their old and new share counts
    """
    # Get API key
    api_key = get_api_key()
    if not api_key:
        return {}
    
    changes = {}
    
    # Process each ticker
    for ticker in tickers:
        change = update_ticker_share_count(ticker, api_key)
        if change:
            changes[ticker] = change
        # Sleep briefly to avoid rate limits
        time.sleep(0.1)
    
    return changes

def get_sector_tickers():
    """Get tickers for each sector"""
    return {
        "AdTech": ["APP", "APPS", "CRTO", "DV", "GOOGL", "META", "MGNI", "PUBM", "TTD"],
        "Cloud Infrastructure": ["AMZN", "CRM", "CSCO", "GOOGL", "MSFT", "NET", "ORCL", "SNOW"],
        "Fintech": ["AFRM", "BILL", "COIN", "FIS", "FI", "GPN", "PYPL", "SSNC"],
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

def get_all_unique_tickers():
    """Get a list of all unique tickers"""
    sectors = get_sector_tickers()
    all_tickers = set()
    for tickers in sectors.values():
        all_tickers.update(tickers)
    return sorted(list(all_tickers))

def update_all_shares_data():
    """Update share count data for all tickers"""
    # Get all unique tickers
    all_tickers = get_all_unique_tickers()
    logging.info(f"Updating share counts for {len(all_tickers)} unique tickers")
    
    # Update share counts
    changes = update_all_ticker_share_counts(all_tickers)
    
    # Print summary
    if changes:
        print(f"\nUpdated share counts for {len(changes)} tickers:")
        for ticker, change in changes.items():
            print(f"  {ticker} ({change['name']}): {change['old_value'] or 'None'} -> {change['new_value']} ({change['percent_change']})")
    else:
        print("No share count changes made")
    
    return len(changes) > 0

def calculate_market_caps():
    """
    Calculate market caps for all tickers and sectors
    
    Returns:
        tuple: (ticker_market_caps, sector_market_caps)
    """
    # Load share counts
    shares_dict = load_shares_cache()
    if not shares_dict:
        logging.error("No share count data available")
        return None, None
    
    # Load price data from cache
    price_cache = os.path.join(CACHE_DIR, "historical_prices.pkl")
    if not os.path.exists(price_cache):
        logging.error(f"Price cache file not found: {price_cache}")
        return None, None
    
    try:
        # Load price data
        price_dict = pd.read_pickle(price_cache)
        
        # Create price DataFrame
        price_df = pd.DataFrame(price_dict)
        price_df.index = pd.to_datetime(price_df.index)
        price_df = price_df.sort_index()
        
        # Calculate market caps for each ticker
        market_caps = pd.DataFrame(index=price_df.index)
        for ticker in price_df.columns:
            if ticker in shares_dict:
                market_caps[ticker] = price_df[ticker] * shares_dict[ticker]
        
        # Calculate sector market caps
        sector_tickers = get_sector_tickers()
        sector_caps = pd.DataFrame(index=market_caps.index)
        total_market_cap = pd.Series(0, index=market_caps.index)
        
        for sector, tickers in sector_tickers.items():
            available_tickers = [t for t in tickers if t in market_caps.columns]
            if available_tickers:
                sector_caps[sector] = market_caps[available_tickers].sum(axis=1)
                total_market_cap += sector_caps[sector]
            else:
                sector_caps[sector] = pd.Series(0, index=market_caps.index)
        
        # Add total
        sector_caps["Total"] = total_market_cap
        
        # Calculate weight percentages
        for sector in sector_tickers.keys():
            weight_col = f"{sector}_weight_pct"
            sector_caps[weight_col] = (sector_caps[sector] / sector_caps["Total"]) * 100
        
        return market_caps, sector_caps
    
    except Exception as e:
        logging.error(f"Error calculating market caps: {e}")
        return None, None

def save_market_cap_data(ticker_caps, sector_caps):
    """
    Save market cap data to files
    
    Args:
        ticker_caps (DataFrame): Market caps for each ticker
        sector_caps (DataFrame): Market caps for each sector
    """
    try:
        # Save ticker market caps
        ticker_file = os.path.join(DATA_DIR, "ticker_market_caps.parquet")
        ticker_caps.to_parquet(ticker_file)
        logging.info(f"Saved ticker market caps to {ticker_file}")
        
        # Save sector market caps
        sector_file = os.path.join(DATA_DIR, "sector_market_caps.parquet")
        sector_csv = os.path.join(DATA_DIR, "sector_market_caps.csv")
        
        # Create backups if files exist
        if os.path.exists(sector_file):
            backup_file = f"{sector_file}.bak"
            sector_caps.to_parquet(backup_file)
            logging.info(f"Created backup of sector market caps at {backup_file}")
        
        # Save new files
        sector_caps.to_parquet(sector_file)
        sector_caps.to_csv(sector_csv)
        logging.info(f"Saved sector market caps to {sector_file} and {sector_csv}")
        
        # Print latest sector stats
        latest_date = sector_caps.index.max()
        print(f"\nSector Market Caps as of {latest_date.strftime('%Y-%m-%d')}:")
        
        # Convert to billions for display
        latest_sectors = sector_caps.loc[latest_date].copy()
        sector_names = [s for s in sector_caps.columns if "_weight_pct" not in s and s != "Total"]
        
        for sector in sector_names:
            cap_billions = latest_sectors[sector] / 1_000_000_000
            weight_pct = latest_sectors.get(f"{sector}_weight_pct", 0)
            print(f"  {sector}: ${cap_billions:.2f} billion ({weight_pct:.1f}%)")
        
        # Print total
        total_billions = latest_sectors["Total"] / 1_000_000_000
        print(f"  Total: ${total_billions:.2f} billion")
        
        return True
    
    except Exception as e:
        logging.error(f"Error saving market cap data: {e}")
        return False

def print_detailed_sector_report(sector_name, ticker_caps):
    """
    Print detailed report for a specific sector
    
    Args:
        sector_name (str): The sector name
        ticker_caps (DataFrame): Market caps for each ticker
    """
    print(f"\nDetailed report for {sector_name} sector:")
    
    # Get tickers for this sector
    sector_tickers = get_sector_tickers().get(sector_name, [])
    if not sector_tickers:
        print(f"  No tickers defined for {sector_name} sector")
        return
    
    # Get latest date
    latest_date = ticker_caps.index.max()
    
    # Print header
    print(f"  Market caps as of {latest_date.strftime('%Y-%m-%d')}:")
    
    # Calculate total sector market cap
    available_tickers = [t for t in sector_tickers if t in ticker_caps.columns]
    total_sector_cap = ticker_caps.loc[latest_date, available_tickers].sum()
    
    # Sort tickers by market cap
    ticker_values = []
    for ticker in available_tickers:
        if ticker in ticker_caps.columns:
            market_cap = ticker_caps.loc[latest_date, ticker]
            ticker_values.append((ticker, market_cap))
    
    # Sort by market cap (descending)
    ticker_values.sort(key=lambda x: x[1], reverse=True)
    
    # Print each ticker
    for ticker, market_cap in ticker_values:
        cap_billions = market_cap / 1_000_000_000
        percentage = (market_cap / total_sector_cap) * 100
        print(f"    {ticker}: ${cap_billions:.2f} billion ({percentage:.1f}% of sector)")
    
    # Print total
    total_billions = total_sector_cap / 1_000_000_000
    print(f"    Total: ${total_billions:.2f} billion")

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Update fully diluted share counts and calculate market caps")
    parser.add_argument("--update-shares", action="store_true", help="Update share counts for all tickers")
    parser.add_argument("--calculate-caps", action="store_true", help="Calculate market caps using current share counts")
    parser.add_argument("--ticker", type=str, help="Update share count for a specific ticker")
    parser.add_argument("--sector", type=str, help="Print detailed report for a specific sector")
    parser.add_argument("--all", action="store_true", help="Update shares and calculate market caps")
    args = parser.parse_args()
    
    # Default to --all if no arguments provided
    if not (args.update_shares or args.calculate_caps or args.ticker or args.sector or args.all):
        args.all = True
    
    # Update share for a single ticker
    if args.ticker:
        change = update_ticker_share_count(args.ticker)
        if change:
            print(f"Updated {args.ticker} share count: {change['old_value'] or 'None'} -> {change['new_value']} ({change['percent_change']})")
        else:
            print(f"No change for {args.ticker}")
    
    # Update all share counts
    if args.update_shares or args.all:
        updated = update_all_shares_data()
        
        # Only calculate market caps if shares were updated or explicitly requested
        if updated or args.calculate_caps or args.all:
            ticker_caps, sector_caps = calculate_market_caps()
            if ticker_caps is not None and sector_caps is not None:
                save_market_cap_data(ticker_caps, sector_caps)
                
                # Print detailed sector report if requested
                if args.sector and ticker_caps is not None:
                    print_detailed_sector_report(args.sector, ticker_caps)
    
    # Calculate market caps without updating shares
    elif args.calculate_caps:
        ticker_caps, sector_caps = calculate_market_caps()
        if ticker_caps is not None and sector_caps is not None:
            save_market_cap_data(ticker_caps, sector_caps)
            
            # Print detailed sector report if requested
            if args.sector and ticker_caps is not None:
                print_detailed_sector_report(args.sector, ticker_caps)
    
    # Print detailed sector report only
    elif args.sector:
        ticker_caps, _ = calculate_market_caps()
        if ticker_caps is not None:
            print_detailed_sector_report(args.sector, ticker_caps)

if __name__ == "__main__":
    main()