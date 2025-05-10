#!/usr/bin/env python3
"""
Fix Market Cap Collection
Generate proper market cap data for all sectors with daily changes
and 30-day history
"""
import os
import csv
import json
import time
import logging
import requests
from io import StringIO
from datetime import datetime, date, timedelta
from threading import Thread
from sqlalchemy import create_engine, text

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("fix_market_caps.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY")
HIST_CSV = "data/market_caps.csv"
SECTOR_CSV = "data/sector_market_caps.csv"
DB_URL = "sqlite:///data/t2d_pulse.db"

# Helper functions
def load_sectors():
    """Load sectors from config.py or JSON file"""
    try:
        # Try to import from config.py first
        from config import SECTORS
        return SECTORS
    except (ImportError, AttributeError):
        # Fall back to JSON file
        try:
            with open('data/sectors.json', 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load sectors: {e}")
            return {}

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

def get_business_days(start_date, end_date):
    """Get a list of business days between start_date and end_date (inclusive)"""
    days = []
    current = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    
    while current <= end:
        # Skip weekends (0=Monday, 6=Sunday)
        if current.weekday() < 5:
            days.append(current.strftime('%Y-%m-%d'))
        current += timedelta(days=1)
    
    return days

def ensure_db():
    """Ensure database exists and has required tables"""
    os.makedirs("data", exist_ok=True)
    engine = create_engine(DB_URL)
    with engine.begin() as conn:
        # Table for ticker market caps
        conn.execute(text(
            "CREATE TABLE IF NOT EXISTS ticker_market_caps ("
            "ticker TEXT, date TEXT, market_cap REAL, "
            "PRIMARY KEY(ticker, date))"
        ))
        
        # Table for sector market caps
        conn.execute(text(
            "CREATE TABLE IF NOT EXISTS sector_market_caps ("
            "sector TEXT, date TEXT, market_cap REAL, "
            "PRIMARY KEY(sector, date))"
        ))
        
        # Table for T2D Pulse values
        conn.execute(text(
            "CREATE TABLE IF NOT EXISTS pulse_values ("
            "date TEXT PRIMARY KEY, score REAL)"
        ))
        
        # Table for share counts
        conn.execute(text(
            "CREATE TABLE IF NOT EXISTS share_counts ("
            "ticker TEXT PRIMARY KEY, count INTEGER, updated_at TEXT)"
        ))
    
    return engine

def get_share_count(ticker):
    """Get share count for a ticker, fetching from API if not in database"""
    engine = ensure_db()
    
    # Check if we already have the share count in the database
    with engine.connect() as conn:
        result = conn.execute(text(
            "SELECT count FROM share_counts WHERE ticker = :ticker"
        ), {"ticker": ticker}).fetchone()
        
        if result and result[0] > 0:
            return result[0]
    
    # If not in database, fetch from API
    share_count = get_polygon_fully_diluted_shares(ticker)
    
    if share_count and share_count > 0:
        # Save to database
        with engine.begin() as conn:
            conn.execute(text(
                "INSERT OR REPLACE INTO share_counts (ticker, count, updated_at) "
                "VALUES (:ticker, :count, :updated_at)"
            ), {"ticker": ticker, "count": share_count, "updated_at": datetime.now().isoformat()})
        
        return share_count
    
    logger.warning(f"Could not get share count for {ticker}")
    return None

def collect_market_caps(date_str=None):
    """Collect market cap data for a specific date"""
    if date_str is None:
        date_str = date.today().isoformat()
    
    logger.info(f"Collecting market cap data for {date_str}")
    
    sectors = load_sectors()
    if not sectors:
        logger.error("No sectors found")
        return False
    
    engine = ensure_db()
    
    # Get all unique tickers across all sectors
    all_tickers = set()
    for tickers in sectors.values():
        all_tickers.update(tickers)
    
    # Collect market cap data for each ticker
    ticker_data = []
    for ticker in all_tickers:
        # Add rate limiting to avoid API throttling
        time.sleep(0.2)
        
        # Get share count
        share_count = get_share_count(ticker)
        if not share_count or share_count <= 0:
            logger.warning(f"Skipping {ticker} - no share count available")
            continue
        
        # Get price for the date
        price = get_polygon_price(ticker, date_str)
        if not price:
            logger.warning(f"Skipping {ticker} - no price available for {date_str}")
            continue
        
        # Calculate market cap
        market_cap = price * share_count
        
        ticker_data.append({
            "ticker": ticker,
            "date": date_str,
            "price": price,
            "share_count": share_count,
            "market_cap": market_cap
        })
        
        # Store in database
        with engine.begin() as conn:
            conn.execute(text(
                "INSERT OR REPLACE INTO ticker_market_caps (ticker, date, market_cap) "
                "VALUES (:ticker, :date, :market_cap)"
            ), {"ticker": ticker, "date": date_str, "market_cap": market_cap})
        
        logger.info(f"Processed {ticker}: ${market_cap/1e9:.2f}B market cap on {date_str}")
    
    # Calculate sector market caps
    sector_data = []
    for sector, tickers in sectors.items():
        # Sum market caps for all tickers in the sector
        sector_market_cap = 0
        for ticker_info in ticker_data:
            if ticker_info["ticker"] in tickers:
                sector_market_cap += ticker_info["market_cap"]
        
        sector_data.append({
            "sector": sector,
            "date": date_str,
            "market_cap": sector_market_cap
        })
        
        # Store in database
        with engine.begin() as conn:
            conn.execute(text(
                "INSERT OR REPLACE INTO sector_market_caps (sector, date, market_cap) "
                "VALUES (:sector, :date, :market_cap)"
            ), {"sector": sector, "date": date_str, "market_cap": sector_market_cap})
        
        logger.info(f"Sector {sector}: ${sector_market_cap/1e12:.2f}T market cap on {date_str}")
    
    # Calculate T2D Pulse (sum of all sector market caps)
    total_market_cap = sum(sector["market_cap"] for sector in sector_data)
    
    # Store T2D Pulse in database
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT OR REPLACE INTO pulse_values (date, score) "
            "VALUES (:date, :score)"
        ), {"date": date_str, "score": total_market_cap})
    
    logger.info(f"T2D Pulse: ${total_market_cap/1e12:.2f}T total market cap on {date_str}")
    
    # Also store all data in CSV for backup and historical tracking
    csv_exists = os.path.exists(HIST_CSV)
    with open(HIST_CSV, "a", newline="") as f:
        fieldnames = ["date", "ticker", "price", "share_count", "market_cap"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        
        if not csv_exists:
            writer.writeheader()
        
        for data in ticker_data:
            # Remove any fields not in fieldnames
            row = {k: data[k] for k in fieldnames if k in data}
            writer.writerow(row)
    
    # Update the sector market cap CSV for dashboard compatibility
    update_sector_csv()
    
    return True

def backfill_market_caps(days=30):
    """Backfill market cap data for the specified number of days"""
    end_date = date.today().isoformat()
    start_date = (date.today() - timedelta(days=days)).isoformat()
    
    logger.info(f"Backfilling market cap data from {start_date} to {end_date}")
    
    # Get business days in the range
    business_days = get_business_days(start_date, end_date)
    
    # Process each date
    for date_str in reversed(business_days):
        logger.info(f"Processing {date_str}")
        collect_market_caps(date_str)
        # Add a small delay between dates
        time.sleep(1)
    
    logger.info(f"Backfill complete for {len(business_days)} business days")
    return True

def update_sector_csv():
    """Update the sector market cap CSV file for dashboard compatibility"""
    engine = ensure_db()
    
    # Get all sector market cap data
    with engine.connect() as conn:
        results = conn.execute(text(
            "SELECT sector, date, market_cap FROM sector_market_caps "
            "ORDER BY date, sector"
        )).fetchall()
    
    if not results:
        logger.warning("No sector market cap data available")
        return
    
    # Group by date
    data_by_date = {}
    for row in results:
        sector, date_str, market_cap = row
        if date_str not in data_by_date:
            data_by_date[date_str] = {}
        data_by_date[date_str][sector] = market_cap
    
    # Convert to DataFrame
    import pandas as pd
    df = pd.DataFrame.from_dict(data_by_date, orient='index')
    
    # Save to CSV
    df.to_csv(SECTOR_CSV)
    logger.info(f"Updated sector market cap CSV at {SECTOR_CSV}")

def schedule_daily(hour=17, minute=0):
    """Schedule daily collection at market close (5:00 PM ET)"""
    def job():
        while True:
            now = datetime.now()
            run_at = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
            wait = (run_at - now).total_seconds()
            if wait < 0:
                wait += 86400  # Wait until tomorrow
            logger.info(f"Scheduled collection in {wait/3600:.1f} hours")
            time.sleep(wait)
            collect_market_caps()
    
    # Start in a background thread
    Thread(target=job, daemon=True).start()
    logger.info(f"Scheduled daily collection at {hour}:{minute:02d}")

def main():
    """Main function to fix market cap data"""
    logger.info("Starting market cap data fix")
    
    # Ensure database structure
    ensure_db()
    
    # Backfill 30 days of market cap data
    backfill_market_caps(days=30)
    
    # Schedule daily updates at market close
    schedule_daily(hour=17, minute=0)
    
    logger.info("Market cap collection initialized. Press Ctrl+C to exit.")
    
    # Keep the script running
    while True:
        time.sleep(3600)  # Sleep for an hour

if __name__ == "__main__":
    main()