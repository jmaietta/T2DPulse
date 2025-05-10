#!/usr/bin/env python3
"""
Market Cap Ingest System
Collects, processes and stores historical market cap data for all sectors
using a database-driven approach for reliability.
"""
import os
import json
import sqlite3
import logging
import requests
import time
from datetime import date, datetime, timedelta
from threading import Thread
from typing import Dict, List, Tuple, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("market_cap_ingest.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- Configuration ---
DB_PATH = os.path.join(os.path.dirname(__file__), "data", "t2d_pulse.db")
POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY")
SHARE_COUNT_CACHE = os.path.join(os.path.dirname(__file__), "data", "share_count_cache.json")

# Ensure data directory exists
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# --- DB Helpers ---
def get_db():
    """Get a database connection with Row factory"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def migrate():
    """Set up the database schema if it doesn't exist"""
    logger.info("Running database migration")
    db = get_db()
    c = db.cursor()
    
    # Table for ticker prices
    c.execute("""
      CREATE TABLE IF NOT EXISTS ticker_prices (
        ticker TEXT,
        date   TEXT,
        price  REAL,
        PRIMARY KEY (ticker, date)
      )
    """)
    
    # Table for share counts
    c.execute("""
      CREATE TABLE IF NOT EXISTS share_counts (
        ticker      TEXT PRIMARY KEY,
        count       INTEGER,
        updated_at  TEXT
      )
    """)
    
    # Table for market caps by ticker
    c.execute("""
      CREATE TABLE IF NOT EXISTS ticker_market_caps (
        ticker      TEXT,
        date        TEXT,
        market_cap  REAL,
        PRIMARY KEY (ticker, date)
      )
    """)
    
    # Table for sector market caps
    c.execute("""
      CREATE TABLE IF NOT EXISTS sector_market_caps (
        sector      TEXT,
        date        TEXT,
        market_cap  REAL,
        PRIMARY KEY (sector, date)
      )
    """)
    
    # Table for sectors and their tickers
    c.execute("""
      CREATE TABLE IF NOT EXISTS sector_tickers (
        sector  TEXT,
        ticker  TEXT,
        PRIMARY KEY (sector, ticker)
      )
    """)
    
    # Table for data quality metrics
    c.execute("""
      CREATE TABLE IF NOT EXISTS data_quality (
        date            TEXT PRIMARY KEY,
        total_tickers   INTEGER,
        covered_tickers INTEGER,
        coverage_pct    REAL,
        status          TEXT,
        message         TEXT
      )
    """)
    
    db.commit()
    db.close()
    logger.info("Database migration completed")

def upsert(table, columns, values):
    """Insert or replace a record in the database"""
    db = get_db()
    cols = ",".join(columns)
    placeholders = ",".join("?" for _ in values)
    sql = f"INSERT OR REPLACE INTO {table} ({cols}) VALUES ({placeholders})"
    db.execute(sql, values)
    db.commit()
    db.close()

def upsert_many(table, columns, values_list):
    """Insert or replace multiple records in the database"""
    db = get_db()
    cols = ",".join(columns)
    placeholders = ",".join("?" for _ in columns)
    sql = f"INSERT OR REPLACE INTO {table} ({cols}) VALUES ({placeholders})"
    db.executemany(sql, values_list)
    db.commit()
    db.close()

def query(sql, params=()):
    """Execute a SQL query and return all results"""
    db = get_db()
    cursor = db.cursor()
    cursor.execute(sql, params)
    results = cursor.fetchall()
    db.close()
    return results

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

# --- Sector and Ticker Management ---
def load_sectors():
    """Load sector to tickers mapping from database or initialize if empty"""
    sectors = {}
    results = query("SELECT sector, ticker FROM sector_tickers")
    
    for row in results:
        sector = row['sector']
        ticker = row['ticker']
        if sector not in sectors:
            sectors[sector] = []
        sectors[sector].append(ticker)
    
    # If no sectors in database, try to load from file
    if not sectors:
        logger.info("No sectors found in database, loading from sector_tickers.csv")
        try:
            import pandas as pd
            df = pd.read_csv('data/sector_tickers.csv')
            for _, row in df.iterrows():
                sector = row['Sector']
                ticker = row['Ticker']
                
                if sector not in sectors:
                    sectors[sector] = []
                sectors[sector].append(ticker)
                
                # Add to database
                upsert("sector_tickers", ["sector", "ticker"], [sector, ticker])
            
            logger.info(f"Loaded {len(sectors)} sectors with {sum(len(tickers) for tickers in sectors.values())} tickers")
        except Exception as e:
            logger.error(f"Failed to load sectors from CSV: {e}")
            
            # Fallback to sectors.json
            try:
                with open('data/sectors.json', 'r') as f:
                    sectors = json.load(f)
                
                # Add to database
                for sector, tickers in sectors.items():
                    for ticker in tickers:
                        upsert("sector_tickers", ["sector", "ticker"], [sector, ticker])
                
                logger.info(f"Loaded {len(sectors)} sectors with {sum(len(tickers) for tickers in sectors.values())} tickers from JSON")
            except Exception as e:
                logger.error(f"Failed to load sectors from JSON: {e}")
    
    return sectors

def save_sectors(sectors):
    """Save sector to tickers mapping to database"""
    # Clear existing mappings
    db = get_db()
    db.execute("DELETE FROM sector_tickers")
    db.commit()
    db.close()
    
    # Insert new mappings
    values_list = []
    for sector, tickers in sectors.items():
        for ticker in tickers:
            values_list.append((sector, ticker))
    
    upsert_many("sector_tickers", ["sector", "ticker"], values_list)
    logger.info(f"Saved {len(sectors)} sectors with {len(values_list)} ticker mappings")

# --- Share Count Management ---
def load_share_counts():
    """Load share counts from database"""
    share_counts = {}
    results = query("SELECT ticker, count, updated_at FROM share_counts")
    
    for row in results:
        share_counts[row['ticker']] = row['count']
    
    # If no share counts in database, try to load from cache file
    if not share_counts:
        logger.info("No share counts found in database, loading from cache file")
        try:
            with open(SHARE_COUNT_CACHE, 'r') as f:
                data = json.load(f)
            
            # Process and normalize the data
            for ticker, share_count in data.items():
                # Handle different formats
                if isinstance(share_count, dict) and 'value' in share_count:
                    count = int(share_count['value'])
                elif isinstance(share_count, (int, float)):
                    count = int(share_count)
                else:
                    logger.warning(f"Invalid share count format for {ticker}: {share_count}")
                    continue
                
                share_counts[ticker] = count
                
                # Add to database
                now = datetime.now().isoformat()
                upsert("share_counts", ["ticker", "count", "updated_at"], [ticker, count, now])
            
            logger.info(f"Loaded {len(share_counts)} share counts from cache file")
        except Exception as e:
            logger.error(f"Failed to load share counts from cache file: {e}")
    
    return share_counts

def update_share_count(ticker, count):
    """Update share count for a ticker in the database"""
    now = datetime.now().isoformat()
    upsert("share_counts", ["ticker", "count", "updated_at"], [ticker, count, now])

# --- Data Collection ---
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

def ensure_share_counts(tickers, share_counts):
    """Ensure we have share counts for all tickers, fetching as needed"""
    missing_tickers = [t for t in tickers if t not in share_counts or share_counts[t] <= 0]
    
    if missing_tickers:
        logger.info(f"Fetching share counts for {len(missing_tickers)} tickers")
        
        for ticker in missing_tickers:
            # Add rate limiting to avoid API throttling
            time.sleep(0.2)
            
            count = get_polygon_fully_diluted_shares(ticker)
            if count is not None and count > 0:
                share_counts[ticker] = count
                update_share_count(ticker, count)
                logger.info(f"Updated share count for {ticker}: {count:,}")
            else:
                logger.warning(f"Failed to get share count for {ticker}")
    
    return share_counts

def collect_prices(tickers, date_str):
    """Collect prices for all tickers on a specific date"""
    logger.info(f"Collecting prices for {len(tickers)} tickers on {date_str}")
    
    # Check which tickers we already have prices for
    existing = query(
        "SELECT ticker FROM ticker_prices WHERE date = ?",
        (date_str,)
    )
    existing_tickers = set(row['ticker'] for row in existing)
    
    # Only fetch prices for tickers we don't have yet
    missing_tickers = [t for t in tickers if t not in existing_tickers]
    
    if not missing_tickers:
        logger.info(f"Already have all prices for {date_str}")
        return
    
    logger.info(f"Fetching prices for {len(missing_tickers)} tickers")
    
    values_list = []
    for ticker in missing_tickers:
        # Add rate limiting to avoid API throttling
        time.sleep(0.2)
        
        price = get_polygon_price(ticker, date_str)
        if price is not None:
            values_list.append((ticker, date_str, price))
    
    # Batch insert all prices
    if values_list:
        upsert_many("ticker_prices", ["ticker", "date", "price"], values_list)
        logger.info(f"Added {len(values_list)} prices for {date_str}")

def calculate_market_caps(date_str):
    """Calculate market caps for all tickers on a specific date"""
    logger.info(f"Calculating market caps for {date_str}")
    
    # Get all prices for the date
    prices = query(
        "SELECT ticker, price FROM ticker_prices WHERE date = ?",
        (date_str,)
    )
    
    if not prices:
        logger.warning(f"No prices available for {date_str}")
        return
    
    # Get all share counts
    share_counts = load_share_counts()
    
    # Calculate market caps
    values_list = []
    for row in prices:
        ticker = row['ticker']
        price = row['price']
        
        if ticker in share_counts and share_counts[ticker] > 0:
            market_cap = price * share_counts[ticker]
            values_list.append((ticker, date_str, market_cap))
    
    # Batch insert all market caps
    if values_list:
        upsert_many("ticker_market_caps", ["ticker", "date", "market_cap"], values_list)
        logger.info(f"Calculated {len(values_list)} market caps for {date_str}")

def calculate_sector_market_caps(date_str):
    """Calculate market caps for all sectors on a specific date"""
    logger.info(f"Calculating sector market caps for {date_str}")
    
    # Get sector to tickers mapping
    sectors = load_sectors()
    
    # Get all market caps for the date
    market_caps = query(
        "SELECT ticker, market_cap FROM ticker_market_caps WHERE date = ?",
        (date_str,)
    )
    
    if not market_caps:
        logger.warning(f"No market caps available for {date_str}")
        return
    
    # Convert to dict for easier lookup
    ticker_market_caps = {row['ticker']: row['market_cap'] for row in market_caps}
    
    # Calculate sector market caps
    values_list = []
    coverage_stats = {
        "total_tickers": 0,
        "covered_tickers": 0
    }
    
    for sector, tickers in sectors.items():
        coverage_stats["total_tickers"] += len(tickers)
        
        # Sum market caps for all tickers in the sector
        sector_tickers = [t for t in tickers if t in ticker_market_caps]
        coverage_stats["covered_tickers"] += len(sector_tickers)
        
        sector_market_cap = sum(ticker_market_caps.get(t, 0) for t in tickers)
        values_list.append((sector, date_str, sector_market_cap))
        
        # Log coverage for the sector
        coverage_pct = len(sector_tickers) / len(tickers) * 100 if tickers else 0
        logger.info(f"Sector {sector}: {len(sector_tickers)}/{len(tickers)} tickers ({coverage_pct:.1f}%), Market Cap: ${sector_market_cap/1e12:.2f}T")
    
    # Batch insert all sector market caps
    if values_list:
        upsert_many("sector_market_caps", ["sector", "date", "market_cap"], values_list)
        logger.info(f"Calculated {len(values_list)} sector market caps for {date_str}")
    
    # Record data quality metrics
    coverage_pct = coverage_stats["covered_tickers"] / coverage_stats["total_tickers"] * 100 if coverage_stats["total_tickers"] > 0 else 0
    status = "OK" if coverage_pct >= 95 else "WARNING" if coverage_pct >= 80 else "ERROR"
    message = f"Coverage: {coverage_pct:.1f}% ({coverage_stats['covered_tickers']}/{coverage_stats['total_tickers']} tickers)"
    
    upsert("data_quality", 
           ["date", "total_tickers", "covered_tickers", "coverage_pct", "status", "message"],
           [date_str, coverage_stats["total_tickers"], coverage_stats["covered_tickers"], coverage_pct, status, message])

def export_to_csv():
    """Export sector market caps to CSV for compatibility with the dashboard"""
    logger.info("Exporting sector market caps to CSV")
    
    # Get all sector market caps
    results = query(
        "SELECT sector, date, market_cap FROM sector_market_caps ORDER BY date"
    )
    
    if not results:
        logger.warning("No sector market caps available to export")
        return
    
    # Group by date
    data = {}
    for row in results:
        date_str = row['date']
        sector = row['sector']
        market_cap = row['market_cap']
        
        if date_str not in data:
            data[date_str] = {}
        
        data[date_str][sector] = market_cap
    
    # Convert to DataFrame
    import pandas as pd
    df = pd.DataFrame.from_dict(data, orient='index')
    
    # Save to CSV
    csv_path = 'data/sector_market_caps.csv'
    df.to_csv(csv_path)
    logger.info(f"Exported sector market caps to {csv_path}")

# --- Main Collection Function ---
def collect_market_data(date_str=None):
    """Collect and process market data for a specific date"""
    if date_str is None:
        date_str = date.today().isoformat()
    
    logger.info(f"Collecting market data for {date_str}")
    
    # Load sectors and get all unique tickers
    sectors = load_sectors()
    all_tickers = set()
    for tickers in sectors.values():
        all_tickers.update(tickers)
    
    # Load and ensure share counts
    share_counts = load_share_counts()
    share_counts = ensure_share_counts(all_tickers, share_counts)
    
    # Collect prices
    collect_prices(all_tickers, date_str)
    
    # Calculate market caps
    calculate_market_caps(date_str)
    
    # Calculate sector market caps
    calculate_sector_market_caps(date_str)
    
    # Export to CSV for dashboard compatibility
    export_to_csv()
    
    logger.info(f"Market data collection completed for {date_str}")

def backfill_historical_data(days=30):
    """Backfill historical market data for a specified number of days"""
    end_date = date.today().isoformat()
    start_date = (date.today() - timedelta(days=days)).isoformat()
    
    logger.info(f"Backfilling historical market data from {start_date} to {end_date}")
    
    # Get business days in the range
    business_days = get_business_days(start_date, end_date)
    
    # Process each date
    for date_str in business_days:
        collect_market_data(date_str)
        # Add a small delay between dates
        time.sleep(1)
    
    logger.info(f"Historical backfill completed for {len(business_days)} days")

# --- Scheduling ---
def schedule_daily(hour=17, minute=0):
    """Schedule daily collection at specified time (default: 5:00 PM)"""
    def runner():
        logger.info(f"Scheduled daily collection at {hour}:{minute:02d}")
        while True:
            now = datetime.now()
            # Schedule for market close (5:00 PM Eastern Time)
            run_at = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
            # If we already passed today's time, schedule for tomorrow
            if now > run_at:
                run_at = run_at + timedelta(days=1)
            
            # Calculate seconds to wait
            wait_seconds = (run_at - now).total_seconds()
            logger.info(f"Next collection scheduled at {run_at.isoformat()} (in {wait_seconds/3600:.1f} hours)")
            
            # Wait until the scheduled time
            time.sleep(wait_seconds)
            
            # Run collection
            try:
                today = date.today().isoformat()
                logger.info(f"Running scheduled collection for {today}")
                collect_market_data(today)
            except Exception as e:
                logger.error(f"Error in scheduled collection: {e}")
    
    # Start the scheduler in a background thread
    Thread(target=runner, daemon=True).start()

# --- Main Function ---
def main():
    """Main function to set up database and start collection process"""
    # Set up database schema
    migrate()
    
    # Collect today's data
    today = date.today().isoformat()
    collect_market_data(today)
    
    # Backfill data if needed
    backfill_historical_data(days=30)
    
    # Schedule daily collection at market close
    schedule_daily(hour=17, minute=0)
    
    logger.info("Market cap ingest system initialized successfully")

if __name__ == "__main__":
    main()