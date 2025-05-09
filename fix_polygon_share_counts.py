"""
Fix Polygon Share Counts

This script fixes the Polygon share count collection logic to use weighted_shares_outstanding
instead of share_class_shares_outstanding for a more accurate representation,
especially for companies with multiple share classes.
"""
import os
import json
import logging
import requests
import argparse
from pathlib import Path

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Define directory for data and cache
DATA_DIR = "data"
CACHE_DIR = os.path.join(DATA_DIR, "cache")
SHARES_CACHE_FILE = os.path.join(CACHE_DIR, "shares_outstanding.json")

def get_polygon_shares_outstanding(ticker, api_key):
    """
    Get the weighted shares outstanding for a ticker from Polygon API
    
    Args:
        ticker (str): The ticker symbol
        api_key (str): The Polygon API key
        
    Returns:
        tuple: (weighted_shares, share_class_shares, ticker_name)
    """
    url = f"https://api.polygon.io/v3/reference/tickers/{ticker}"
    headers = {"Authorization": f"Bearer {api_key}"}
    
    try:
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            results = data.get("results", {})
            
            # Extract both share count metrics
            weighted_shares = results.get("weighted_shares_outstanding")
            share_class_shares = results.get("share_class_shares_outstanding")
            ticker_name = results.get("name", "")
            
            return weighted_shares, share_class_shares, ticker_name
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
        Path(CACHE_DIR).mkdir(parents=True, exist_ok=True)
        with open(SHARES_CACHE_FILE, 'w') as f:
            json.dump(shares_dict, f)
        return True
    except Exception as e:
        logging.error(f"Error saving shares cache: {e}")
        return False

def update_share_counts_for_tickers(tickers):
    """
    Update share counts for a list of tickers
    
    Args:
        tickers (list): List of ticker symbols to update
        
    Returns:
        dict: Dictionary of tickers with their old and new share counts
    """
    # Get API key
    api_key = os.environ.get("POLYGON_API_KEY")
    if not api_key:
        logging.error("POLYGON_API_KEY environment variable not set")
        return None
    
    # Load current share counts
    shares_dict = load_shares_cache()
    
    # Create backup
    if shares_dict:
        backup_file = f"{SHARES_CACHE_FILE}.bak"
        with open(backup_file, 'w') as f:
            json.dump(shares_dict, f)
        logging.info(f"Created backup of shares outstanding data at {backup_file}")
    
    # Track changes
    changes = {}
    
    # Process each ticker
    for ticker in tickers:
        # Get current value
        current_value = shares_dict.get(ticker, "Not in cache")
        
        # Get both share counts from Polygon
        weighted_shares, share_class_shares, name = get_polygon_shares_outstanding(ticker, api_key)
        
        if weighted_shares is not None and share_class_shares is not None:
            # Determine which value to use - prefer weighted_shares_outstanding if available
            if weighted_shares is not None and weighted_shares > 0:
                new_value = weighted_shares
                source = "weighted_shares_outstanding"
            elif share_class_shares is not None and share_class_shares > 0:
                new_value = share_class_shares
                source = "share_class_shares_outstanding"
            else:
                new_value = None
                source = "none available"
            
            # Record change if there is one
            if new_value != current_value and new_value is not None:
                shares_dict[ticker] = new_value
                changes[ticker] = {
                    "name": name,
                    "old_value": current_value,
                    "new_value": new_value,
                    "percent_change": "N/A" if current_value == "Not in cache" else f"{(new_value / int(current_value) - 1) * 100:.2f}%",
                    "source": source
                }
                logging.info(f"Updated {ticker} share count: {current_value} -> {new_value} ({source})")
            else:
                logging.info(f"No change for {ticker}: {current_value}")
        else:
            logging.warning(f"Failed to get share count data for {ticker}")
    
    # Save updated share counts
    if changes:
        save_shares_cache(shares_dict)
    
    return changes

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="Update stock share counts from Polygon API")
    parser.add_argument("--tickers", type=str, help="Comma-separated list of tickers to update")
    parser.add_argument("--all", action="store_true", help="Update all tickers in the cache")
    parser.add_argument("--key-stocks", action="store_true", help="Update only important stocks (GOOGL, META, etc.)")
    args = parser.parse_args()
    
    # Determine which tickers to update
    if args.tickers:
        tickers = [t.strip() for t in args.tickers.split(",")]
    elif args.key_stocks:
        tickers = ["GOOGL", "META", "AAPL", "MSFT", "AMZN", "NVDA"]
    elif args.all:
        # Load current share counts to get all tickers
        shares_dict = load_shares_cache()
        tickers = list(shares_dict.keys())
    else:
        tickers = ["GOOGL", "META"]  # Default to key stocks
    
    logging.info(f"Updating share counts for {len(tickers)} tickers: {', '.join(tickers)}")
    
    # Update share counts
    changes = update_share_counts_for_tickers(tickers)
    
    # Print summary of changes
    if changes:
        print(f"\nUpdated share counts for {len(changes)} tickers:")
        for ticker, change in changes.items():
            print(f"  {ticker} ({change['name']}): {change['old_value']} -> {change['new_value']} ({change['percent_change']}) [Source: {change['source']}]")
    else:
        print("No share count changes made")

if __name__ == "__main__":
    main()