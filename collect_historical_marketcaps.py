#!/usr/bin/env python3
"""
Collect Historical Market Cap Data
Specifically designed to load historical prices for business days
to ensure proper market cap calculation with daily changes
"""
import os
import json
import logging
import requests
import time
import sqlite3
import pandas as pd
from datetime import datetime, date, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("collect_historical_marketcaps.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY")
DB_PATH = "data/t2d_pulse.db"
SECTOR_CSV = "data/sector_market_caps.csv"

def load_sectors():
    """Load sectors from file"""
    try:
        with open('data/sectors.json', 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load sectors: {e}")
        return {}

def get_business_days(days=30):
    """Get a list of business days going back the specified number of days"""
    result = []
    current = date.today()
    count = 0
    
    while count < days:
        current = current - timedelta(days=1)
        # Skip weekends (0=Monday, 6=Sunday)
        if current.weekday() < 5:
            result.append(current.isoformat())
            count += 1
    
    return result

def get_polygon_fully_diluted_shares(ticker):
    """Get fully diluted shares outstanding from Polygon API"""
    if not POLYGON_API_KEY:
        logger.error("POLYGON_API_KEY environment variable not set")
        return None
    
    url = f"https://api.polygon.io/v3/reference/tickers/{ticker}?apiKey={POLYGON_API_KEY}"
    
    try:
        response = requests.get(url)
        if response.status_code != 200:
            logger.warning(f"Failed to get data for {ticker}: {response.status_code}")
            return None
        
        data = response.json()
        
        # Extract the share count from the response
        if 'results' in data and data['results']:
            results = data['results']
            
            # Try weighted shares outstanding first
            if 'weighted_shares_outstanding' in results and results['weighted_shares_outstanding']:
                return int(results['weighted_shares_outstanding'])
            
            # Fall back to shares outstanding
            if 'shares_outstanding' in results and results['shares_outstanding']:
                return int(results['shares_outstanding'])
        
        logger.warning(f"No share count data available for {ticker}")
        return None
    
    except Exception as e:
        logger.error(f"Error fetching fully diluted shares for {ticker}: {e}")
        return None

def get_polygon_price(ticker, date_str):
    """Get closing price from Polygon API for a specific date"""
    if not POLYGON_API_KEY:
        logger.error("POLYGON_API_KEY environment variable not set")
        return None
    
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{date_str}/{date_str}?apiKey={POLYGON_API_KEY}"
    
    try:
        response = requests.get(url)
        if response.status_code != 200:
            logger.warning(f"Failed to get price for {ticker} on {date_str}: {response.status_code}")
            return None
        
        data = response.json()
        
        # Extract the closing price from the response
        if 'results' in data and data['results']:
            return data['results'][0]['c']  # Closing price
        
        logger.warning(f"No price data available for {ticker} on {date_str}")
        return None
    
    except Exception as e:
        logger.error(f"Error fetching price for {ticker} on {date_str}: {e}")
        return None

def ensure_db():
    """Ensure database exists and create tables if needed"""
    os.makedirs("data", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Create tables if they don't exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS share_counts (
            ticker TEXT PRIMARY KEY,
            count INTEGER,
            updated_at TEXT
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ticker_prices (
            ticker TEXT,
            date TEXT,
            price REAL,
            PRIMARY KEY (ticker, date)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ticker_market_caps (
            ticker TEXT,
            date TEXT,
            market_cap REAL,
            PRIMARY KEY (ticker, date)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sector_market_caps (
            sector TEXT,
            date TEXT,
            market_cap REAL,
            PRIMARY KEY (sector, date)
        )
    """)
    
    conn.commit()
    conn.close()

def get_share_count(ticker):
    """Get the share count for a ticker, fetching from API if not in database"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Check if we already have the share count
    cursor.execute("SELECT count FROM share_counts WHERE ticker = ?", (ticker,))
    result = cursor.fetchone()
    
    if result and result[0] > 0:
        conn.close()
        return result[0]
    
    # If not, fetch from API
    shares = get_polygon_fully_diluted_shares(ticker)
    
    if shares and shares > 0:
        # Save to database
        cursor.execute(
            "INSERT OR REPLACE INTO share_counts (ticker, count, updated_at) VALUES (?, ?, ?)",
            (ticker, shares, datetime.now().isoformat())
        )
        conn.commit()
        conn.close()
        
        return shares
    
    conn.close()
    logger.warning(f"Could not get share count for {ticker}")
    return None

def collect_historical_prices(tickers, dates):
    """Collect historical prices for multiple tickers and dates"""
    # Initialize counters
    total_requests = len(tickers) * len(dates)
    completed = 0
    successful = 0
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    logger.info(f"Collecting historical prices for {len(tickers)} tickers across {len(dates)} dates ({total_requests} total requests)")
    
    start_time = time.time()
    
    for ticker in tickers:
        for date_str in dates:
            # Skip if we already have this data
            cursor.execute("SELECT price FROM ticker_prices WHERE ticker = ? AND date = ?", (ticker, date_str))
            if cursor.fetchone():
                completed += 1
                successful += 1
                continue
            
            # Get price from API
            price = get_polygon_price(ticker, date_str)
            
            if price:
                cursor.execute(
                    "INSERT OR REPLACE INTO ticker_prices (ticker, date, price) VALUES (?, ?, ?)",
                    (ticker, date_str, price)
                )
                successful += 1
                logger.info(f"Got price for {ticker} on {date_str}: ${price:.2f}")
            else:
                logger.warning(f"No price data available for {ticker} on {date_str}")
            
            completed += 1
            
            # Report progress periodically
            if completed % 10 == 0:
                elapsed = time.time() - start_time
                remaining = (elapsed / completed) * (total_requests - completed) if completed > 0 else 0
                
                logger.info(f"Progress: {completed}/{total_requests} requests ({completed/total_requests*100:.1f}%)")
                logger.info(f"Success rate: {successful}/{completed} ({successful/completed*100:.1f}%)")
                logger.info(f"Estimated time remaining: {remaining/60:.1f} minutes")
            
            # Limit API requests to avoid rate limiting
            time.sleep(0.2)
    
    conn.commit()
    conn.close()
    
    logger.info(f"Completed price collection: {successful}/{total_requests} successful requests")
    return successful

def calculate_market_caps(dates):
    """Calculate market caps for all tickers and dates"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Get all tickers with prices
    cursor.execute("SELECT DISTINCT ticker FROM ticker_prices")
    tickers = [row[0] for row in cursor.fetchall()]
    
    logger.info(f"Calculating market caps for {len(tickers)} tickers across {len(dates)} dates")
    
    for ticker in tickers:
        # Get share count
        cursor.execute("SELECT count FROM share_counts WHERE ticker = ?", (ticker,))
        result = cursor.fetchone()
        
        if not result or not result[0]:
            logger.warning(f"Skipping {ticker} - no share count available")
            continue
        
        share_count = result[0]
        
        for date_str in dates:
            # Get price
            cursor.execute("SELECT price FROM ticker_prices WHERE ticker = ? AND date = ?", (ticker, date_str))
            result = cursor.fetchone()
            
            if not result:
                continue
            
            price = result[0]
            
            # Calculate market cap
            market_cap = price * share_count
            
            # Save to database
            cursor.execute(
                "INSERT OR REPLACE INTO ticker_market_caps (ticker, date, market_cap) VALUES (?, ?, ?)",
                (ticker, date_str, market_cap)
            )
            
            logger.info(f"Calculated market cap for {ticker} on {date_str}: ${market_cap/1e9:.2f}B")
    
    conn.commit()
    conn.close()
    
    logger.info("Completed market cap calculations")

def calculate_sector_market_caps(dates):
    """Calculate sector market caps for all dates"""
    sectors = load_sectors()
    if not sectors:
        logger.error("No sectors found")
        return
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    sector_data = {}
    
    for date_str in dates:
        sector_data[date_str] = {}
        
        for sector, tickers in sectors.items():
            # Sum market caps for all tickers in the sector
            market_cap_sum = 0
            
            for ticker in tickers:
                cursor.execute(
                    "SELECT market_cap FROM ticker_market_caps WHERE ticker = ? AND date = ?",
                    (ticker, date_str)
                )
                
                result = cursor.fetchone()
                if result:
                    market_cap_sum += result[0]
            
            # Save to database
            cursor.execute(
                "INSERT OR REPLACE INTO sector_market_caps (sector, date, market_cap) VALUES (?, ?, ?)",
                (sector, date_str, market_cap_sum)
            )
            
            sector_data[date_str][sector] = market_cap_sum
            
            logger.info(f"Sector {sector} on {date_str}: ${market_cap_sum/1e12:.2f}T")
    
    conn.commit()
    conn.close()
    
    logger.info("Completed sector market cap calculations")
    
    # Convert to DataFrame and save to CSV
    df = pd.DataFrame.from_dict(sector_data, orient='index')
    df.index.name = 'Date'
    df.to_csv(SECTOR_CSV)
    
    logger.info(f"Saved sector market caps to {SECTOR_CSV}")
    
    return df

def main():
    """Main function to collect historical market cap data"""
    logger.info("Starting historical market cap collection")
    
    ensure_db()
    
    sectors = load_sectors()
    if not sectors:
        logger.error("No sectors found")
        return
    
    # Get all unique tickers
    all_tickers = set()
    for tickers in sectors.values():
        all_tickers.update(tickers)
    
    logger.info(f"Found {len(all_tickers)} unique tickers across {len(sectors)} sectors")
    
    # Get share counts
    for ticker in all_tickers:
        # Add some rate limiting
        time.sleep(0.2)
        
        share_count = get_share_count(ticker)
        if share_count:
            logger.info(f"Share count for {ticker}: {share_count:,}")
    
    # Get business days for past 30 days
    dates = get_business_days(days=30)
    
    # Collect historical prices
    collect_historical_prices(all_tickers, dates)
    
    # Calculate market caps
    calculate_market_caps(dates)
    
    # Calculate sector market caps
    calculate_sector_market_caps(dates)
    
    logger.info("Historical market cap collection completed successfully")

if __name__ == "__main__":
    main()