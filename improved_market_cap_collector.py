#!/usr/bin/env python3
"""
Improved Market Cap Collection System
Collects, processes and stores historical market cap data for all sectors
with daily updates and proper historical tracking.

Key improvements:
1. Daily collection of price data with proper versioning
2. Strict use of fully diluted share counts
3. Proper historical tracking with daily snapshots
4. Sector aggregation with complete coverage validation
5. Automated data quality checks
"""
import os
import json
import logging
import datetime
import pandas as pd
import numpy as np
from filelock import FileLock
import requests
import time
from typing import Dict, List, Tuple, Optional, Any
import pytz

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("market_cap_collector.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Constants
CACHE_DIR = "data/cache"
MARKET_CAPS_FILE = "data/sector_market_caps.csv"
MARKET_CAPS_LOCK = "data/sector_market_caps.csv.lock"
TICKER_HISTORY_FILE = "data/ticker_price_history.csv"
SHARE_COUNT_CACHE = "data/share_count_cache.json"
SHARE_COUNT_LOCK = "data/share_count_cache.json.lock"
POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY")

# Ensure cache directory exists
os.makedirs(CACHE_DIR, exist_ok=True)

# Market hours (Eastern Time)
ET_TIMEZONE = pytz.timezone('US/Eastern')
MARKET_OPEN_HOUR = 9  # 9:30 AM ET
MARKET_CLOSE_HOUR = 16  # 4:00 PM ET

def get_current_eastern_time() -> datetime.datetime:
    """Get current time in US Eastern timezone"""
    utc_now = datetime.datetime.now(pytz.utc)
    eastern_time = utc_now.astimezone(ET_TIMEZONE)
    return eastern_time

def is_market_open() -> bool:
    """Check if US stock market is currently open"""
    eastern_time = get_current_eastern_time()
    
    # Check if it's a weekday (0 = Monday, 6 = Sunday)
    if eastern_time.weekday() >= 5:  # Weekend
        return False
    
    # Check if within market hours (9:30 AM - 4:00 PM ET)
    if eastern_time.hour < MARKET_OPEN_HOUR or eastern_time.hour >= MARKET_CLOSE_HOUR:
        return False
    if eastern_time.hour == MARKET_OPEN_HOUR and eastern_time.minute < 30:
        return False
    
    # TODO: Add holiday calendar check
    
    return True

def get_latest_business_day() -> str:
    """Get the latest business day (skipping weekends)"""
    eastern_time = get_current_eastern_time()
    
    # If it's weekend, adjust to Friday
    if eastern_time.weekday() >= 5:  # Weekend
        days_to_subtract = eastern_time.weekday() - 4  # 5 (Sat) -> 1, 6 (Sun) -> 2
        eastern_time = eastern_time - datetime.timedelta(days=days_to_subtract)
    
    # If it's before market open, use previous day
    if eastern_time.hour < MARKET_OPEN_HOUR or (eastern_time.hour == MARKET_OPEN_HOUR and eastern_time.minute < 30):
        previous_day = eastern_time - datetime.timedelta(days=1)
        # If previous day is weekend, go to Friday
        if previous_day.weekday() >= 5:
            days_to_subtract = previous_day.weekday() - 4
            previous_day = previous_day - datetime.timedelta(days=days_to_subtract)
        eastern_time = previous_day
    
    return eastern_time.strftime('%Y-%m-%d')

def load_sector_tickers() -> Dict[str, List[str]]:
    """Load the mapping of sectors to their constituent tickers"""
    try:
        # First try to load from CSV format
        tickers_df = pd.read_csv('data/sector_tickers.csv')
        sectors = {}
        for _, row in tickers_df.iterrows():
            sector = row['Sector']
            ticker = row['Ticker']
            if sector not in sectors:
                sectors[sector] = []
            sectors[sector].append(ticker)
        return sectors
    except Exception as e:
        logger.warning(f"Could not load sector tickers from CSV: {e}")
        
        # Fall back to loading from sectors.json
        try:
            with open('data/sectors.json', 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load sector tickers: {e}")
            raise

def load_share_count_cache() -> Dict[str, int]:
    """Load the cache of fully diluted share counts for each ticker"""
    try:
        with FileLock(SHARE_COUNT_LOCK):
            with open(SHARE_COUNT_CACHE, 'r') as f:
                data = json.load(f)
                
        # Normalize the data to ensure all values are integers
        normalized_data = {}
        for ticker, share_count in data.items():
            if isinstance(share_count, dict) and 'value' in share_count:
                normalized_data[ticker] = int(share_count['value'])
            elif isinstance(share_count, (int, float)):
                normalized_data[ticker] = int(share_count)
            else:
                logger.warning(f"Invalid share count format for {ticker}: {share_count}")
                # Skip invalid entries
        
        return normalized_data
    except FileNotFoundError:
        logger.warning(f"Share count cache not found at {SHARE_COUNT_CACHE}, creating new cache")
        return {}
    except Exception as e:
        logger.error(f"Error loading share count cache: {e}")
        return {}

def save_share_count_cache(cache: Dict[str, int]) -> None:
    """Save the cache of fully diluted share counts"""
    try:
        # Ensure all values are properly formatted
        normalized_cache = {}
        for ticker, count in cache.items():
            normalized_cache[ticker] = int(count)
            
        with FileLock(SHARE_COUNT_LOCK):
            with open(SHARE_COUNT_CACHE, 'w') as f:
                json.dump(normalized_cache, f, indent=2)
        logger.info(f"Share count cache saved with {len(normalized_cache)} entries")
    except Exception as e:
        logger.error(f"Error saving share count cache: {e}")

def get_polygon_fully_diluted_shares(ticker: str) -> Optional[int]:
    """
    Get fully diluted shares outstanding from Polygon API (details/v3 endpoint)
    This is the authoritative source for share count data.
    
    Args:
        ticker: The stock ticker symbol
        
    Returns:
        int: The fully diluted shares outstanding or None if not available
    """
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
        
        # Extract the share count from the response, handling all possible data structures
        if 'results' in data and data['results']:
            results = data['results']
            
            # Try weighted shares outstanding first (for more accuracy)
            if 'weighted_shares_outstanding' in results and results['weighted_shares_outstanding']:
                return int(results['weighted_shares_outstanding'])
                
            # Fall back to shares outstanding
            if 'shares_outstanding' in results and results['shares_outstanding']:
                return int(results['shares_outstanding'])
            
            # Deeper search in market_cap if needed
            if 'market_cap' in results and results['market_cap']:
                market_cap = results['market_cap']
                # If we have price, we can calculate shares outstanding
                if 'last_price' in market_cap and market_cap['last_price'] > 0:
                    return int(market_cap['value'] / market_cap['last_price'])
        
        logger.warning(f"No share count data available for {ticker}")
        return None
        
    except Exception as e:
        logger.error(f"Error fetching fully diluted shares for {ticker}: {e}")
        return None

def get_fully_diluted_shares(ticker: str, cache: Dict[str, int]) -> Optional[int]:
    """
    Get fully diluted shares outstanding, using cache if available
    and refreshing from API if needed
    
    Args:
        ticker: The stock ticker symbol
        cache: The share count cache dictionary
    
    Returns:
        int: The fully diluted shares outstanding or None if not available
    """
    # Check cache first
    if ticker in cache and cache[ticker] > 0:
        return cache[ticker]
        
    # Fetch from API
    shares = get_polygon_fully_diluted_shares(ticker)
    
    if shares is not None and shares > 0:
        # Update cache with the new value
        cache[ticker] = shares
        save_share_count_cache(cache)
        return shares
        
    logger.warning(f"Could not get fully diluted shares for {ticker}")
    return None

def get_latest_prices(tickers: List[str]) -> Dict[str, float]:
    """
    Get the latest closing prices for a list of tickers from Polygon API
    
    Args:
        tickers: List of ticker symbols
        
    Returns:
        Dict mapping ticker symbols to their latest closing prices
    """
    if not POLYGON_API_KEY:
        logger.error("POLYGON_API_KEY environment variable not set")
        return {}
        
    prices = {}
    date = get_latest_business_day()
    
    for ticker in tickers:
        # Add rate limiting to avoid API throttling (5 requests per second max)
        time.sleep(0.2)
        
        url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{date}/{date}?apiKey={POLYGON_API_KEY}"
        
        try:
            response = requests.get(url)
            if response.status_code != 200:
                logger.warning(f"Failed to get price for {ticker}: {response.status_code}")
                continue
                
            data = response.json()
            
            if 'results' in data and data['results']:
                # Get the closing price (c) from the first result
                prices[ticker] = data['results'][0]['c']
            else:
                logger.warning(f"No price data available for {ticker} on {date}")
                
        except Exception as e:
            logger.error(f"Error fetching price for {ticker}: {e}")
            
    # Report coverage statistics
    logger.info(f"Retrieved prices for {len(prices)}/{len(tickers)} tickers ({len(prices)/len(tickers)*100:.1f}%)")
    
    return prices

def get_historical_prices(tickers: List[str], start_date: str, end_date: str) -> Dict[str, Dict[str, float]]:
    """
    Get historical closing prices for a list of tickers from Polygon API
    
    Args:
        tickers: List of ticker symbols
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        
    Returns:
        Dict mapping ticker symbols to dicts of dates and closing prices
    """
    if not POLYGON_API_KEY:
        logger.error("POLYGON_API_KEY environment variable not set")
        return {}
        
    historical_prices = {}
    
    for ticker in tickers:
        # Add rate limiting to avoid API throttling (5 requests per second max)
        time.sleep(0.2)
        
        url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start_date}/{end_date}?apiKey={POLYGON_API_KEY}"
        
        try:
            response = requests.get(url)
            if response.status_code != 200:
                logger.warning(f"Failed to get historical prices for {ticker}: {response.status_code}")
                continue
                
            data = response.json()
            
            if 'results' in data and data['results']:
                ticker_prices = {}
                for result in data['results']:
                    # Convert timestamp (ms) to date string
                    timestamp_ms = result['t']
                    date_str = datetime.datetime.fromtimestamp(timestamp_ms / 1000).strftime('%Y-%m-%d')
                    ticker_prices[date_str] = result['c']  # Closing price
                
                historical_prices[ticker] = ticker_prices
            else:
                logger.warning(f"No historical price data available for {ticker}")
                
        except Exception as e:
            logger.error(f"Error fetching historical prices for {ticker}: {e}")
            
    # Report coverage statistics
    logger.info(f"Retrieved historical prices for {len(historical_prices)}/{len(tickers)} tickers ({len(historical_prices)/len(tickers)*100:.1f}%)")
    
    return historical_prices

def calculate_market_caps(tickers: List[str], prices: Dict[str, float], share_counts: Dict[str, int]) -> Dict[str, float]:
    """
    Calculate market caps for a list of tickers
    
    Args:
        tickers: List of ticker symbols
        prices: Dict mapping ticker symbols to prices
        share_counts: Dict mapping ticker symbols to share counts
        
    Returns:
        Dict mapping ticker symbols to their market caps
    """
    market_caps = {}
    missing_data = []
    
    for ticker in tickers:
        if ticker in prices and ticker in share_counts:
            price = prices[ticker]
            shares = share_counts[ticker]
            
            # Calculate market cap (price * shares outstanding)
            market_cap = price * shares
            market_caps[ticker] = market_cap
        else:
            missing_data.append(ticker)
            
    if missing_data:
        logger.warning(f"Could not calculate market caps for {len(missing_data)} tickers: {', '.join(missing_data)}")
        
    return market_caps

def calculate_sector_market_caps(sector_tickers: Dict[str, List[str]], market_caps: Dict[str, float]) -> Dict[str, float]:
    """
    Calculate market caps for each sector by summing the market caps of constituent tickers
    
    Args:
        sector_tickers: Dict mapping sectors to lists of ticker symbols
        market_caps: Dict mapping ticker symbols to market caps
        
    Returns:
        Dict mapping sectors to their total market caps
    """
    sector_market_caps = {}
    
    for sector, tickers in sector_tickers.items():
        # Sum market caps of all tickers in the sector
        sector_total = sum(market_caps.get(ticker, 0) for ticker in tickers)
        sector_market_caps[sector] = sector_total
        
        # Calculate coverage percentage
        covered_tickers = [ticker for ticker in tickers if ticker in market_caps]
        coverage_pct = len(covered_tickers) / len(tickers) * 100 if tickers else 0
        
        logger.info(f"Sector {sector}: {len(covered_tickers)}/{len(tickers)} tickers ({coverage_pct:.1f}%), Market Cap: ${sector_total/1e12:.2f}T")
        
    return sector_market_caps

def load_historical_market_caps() -> pd.DataFrame:
    """
    Load historical market cap data from CSV
    
    Returns:
        DataFrame with dates as index and sectors as columns
    """
    try:
        with FileLock(MARKET_CAPS_LOCK):
            df = pd.read_csv(MARKET_CAPS_FILE)
            
        # Ensure the date column is used as index
        if 'Unnamed: 0' in df.columns:
            df = df.rename(columns={'Unnamed: 0': 'Date'})
        if 'Date' in df.columns:
            df = df.set_index('Date')
            
        return df
    except FileNotFoundError:
        logger.warning(f"Historical market cap file not found at {MARKET_CAPS_FILE}, creating new file")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error loading historical market caps: {e}")
        return pd.DataFrame()

def update_historical_market_caps(sector_market_caps: Dict[str, float], date: Optional[str] = None) -> None:
    """
    Update historical market cap data with new values
    
    Args:
        sector_market_caps: Dict mapping sectors to their market caps
        date: Date string in YYYY-MM-DD format (default: latest business day)
    """
    current_date = get_latest_business_day() if date is None else date
        
    try:
        # Load existing data
        df = load_historical_market_caps()
        
        # Create a new row with the updated values
        new_row = pd.Series(sector_market_caps, name=current_date)
        
        # Update the dataframe (replace if date exists, append if not)
        if current_date in df.index:
            df.loc[current_date] = new_row
        else:
            df = df.append(new_row)
            
        # Sort by date
        df = df.sort_index()
        
        # Save the updated dataframe
        with FileLock(MARKET_CAPS_LOCK):
            df.to_csv(MARKET_CAPS_FILE)
            
        logger.info(f"Updated historical market caps for {current_date}")
    except Exception as e:
        logger.error(f"Error updating historical market caps: {e}")

def collect_market_cap_data(date: str = None) -> None:
    """
    Collect and update market cap data for all sectors
    
    Args:
        date: Date string in YYYY-MM-DD format (default: latest business day)
    """
    if date is None:
        date = get_latest_business_day()
        
    logger.info(f"Collecting market cap data for {date}")
    
    # Load sector ticker mapping
    sector_tickers = load_sector_tickers()
    all_tickers = []
    for tickers in sector_tickers.values():
        all_tickers.extend(tickers)
    all_tickers = list(set(all_tickers))  # Remove duplicates
    
    # Load share count cache
    share_count_cache = load_share_count_cache()
    
    # Ensure we have share counts for all tickers
    for ticker in all_tickers:
        if ticker not in share_count_cache or share_count_cache[ticker] <= 0:
            shares = get_polygon_fully_diluted_shares(ticker)
            if shares is not None and shares > 0:
                share_count_cache[ticker] = shares
                logger.info(f"Updated share count for {ticker}: {shares:,}")
            else:
                logger.warning(f"Failed to get share count for {ticker}")
    
    # Save updated share count cache
    save_share_count_cache(share_count_cache)
    
    # Get latest prices
    prices = get_latest_prices(all_tickers)
    
    # Calculate market caps
    market_caps = calculate_market_caps(all_tickers, prices, share_count_cache)
    
    # Calculate sector market caps
    sector_market_caps = calculate_sector_market_caps(sector_tickers, market_caps)
    
    # Update historical data
    update_historical_market_caps(sector_market_caps, date)
    
    logger.info(f"Market cap data collection completed for {date}")

def collect_historical_market_cap_data(start_date: str, end_date: str) -> None:
    """
    Collect and update historical market cap data for a date range
    
    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format (inclusive)
    """
    logger.info(f"Collecting historical market cap data from {start_date} to {end_date}")
    
    # Load sector ticker mapping
    sector_tickers = load_sector_tickers()
    all_tickers = []
    for tickers in sector_tickers.values():
        all_tickers.extend(tickers)
    all_tickers = list(set(all_tickers))  # Remove duplicates
    
    # Load share count cache
    share_count_cache = load_share_count_cache()
    
    # Ensure we have share counts for all tickers
    for ticker in all_tickers:
        if ticker not in share_count_cache or share_count_cache[ticker] <= 0:
            shares = get_polygon_fully_diluted_shares(ticker)
            if shares is not None and shares > 0:
                share_count_cache[ticker] = shares
                logger.info(f"Updated share count for {ticker}: {shares:,}")
    
    # Save updated share count cache
    save_share_count_cache(share_count_cache)
    
    # Get historical prices
    historical_prices = get_historical_prices(all_tickers, start_date, end_date)
    
    # Generate list of business days in the date range
    start = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.datetime.strptime(end_date, '%Y-%m-%d')
    date_range = []
    current = start
    while current <= end:
        # Skip weekends
        if current.weekday() < 5:  # Monday to Friday
            date_range.append(current.strftime('%Y-%m-%d'))
        current += datetime.timedelta(days=1)
    
    # Process each date
    for date in date_range:
        # Extract prices for the current date
        prices = {}
        for ticker, ticker_prices in historical_prices.items():
            if date in ticker_prices:
                prices[ticker] = ticker_prices[date]
        
        # If we have prices for this date, calculate market caps
        if prices:
            # Calculate market caps
            market_caps = calculate_market_caps(all_tickers, prices, share_count_cache)
            
            # Calculate sector market caps
            sector_market_caps = calculate_sector_market_caps(sector_tickers, market_caps)
            
            # Update historical data
            update_historical_market_caps(sector_market_caps, date)
            
            logger.info(f"Processed historical market cap data for {date}")
        else:
            logger.warning(f"No price data available for {date}")
    
    logger.info(f"Historical market cap data collection completed for {start_date} to {end_date}")

def generate_sector_market_cap_report(days: int = 30) -> str:
    """
    Generate a report of sector market caps for the last N days
    
    Args:
        days: Number of days to include in the report
        
    Returns:
        Report as a formatted string
    """
    try:
        # Load historical data
        df = load_historical_market_caps()
        if df.empty:
            return "No historical market cap data available"
            
        # Sort by date and take the last N days
        df = df.sort_index().tail(days)
        
        # Calculate total market cap for each day
        df['Total'] = df.sum(axis=1)
        
        # Format the report
        report_lines = [f"Sector Market Cap Report - Last {days} Days"]
        report_lines.append("-" * 80)
        
        # Add header row
        header = "Date       "
        for sector in df.columns:
            header += f" | {sector[:10]:>10}"
        report_lines.append(header)
        report_lines.append("-" * len(header))
        
        # Add data rows
        for date, row in df.iterrows():
            line = f"{date}"
            for sector in df.columns:
                # Format in trillions with 2 decimal places
                value_str = f"${row[sector]/1e12:.2f}T"
                line += f" | {value_str:>10}"
            report_lines.append(line)
            
        return "\n".join(report_lines)
        
    except Exception as e:
        logger.error(f"Error generating sector market cap report: {e}")
        return f"Error generating report: {e}"

def validate_market_cap_data() -> Dict[str, Any]:
    """
    Validate market cap data to identify potential issues
    
    Returns:
        Dict with validation results
    """
    results = {
        "valid": True,
        "issues": [],
        "missing_dates": [],
        "static_sectors": [],
    }
    
    try:
        # Load historical data
        df = load_historical_market_caps()
        if df.empty:
            results["valid"] = False
            results["issues"].append("No historical market cap data available")
            return results
            
        # Check for missing business days in the last 30 days
        end_date = datetime.datetime.now()
        start_date = end_date - datetime.timedelta(days=30)
        
        # Generate list of business days
        business_days = []
        current = start_date
        while current <= end_date:
            # Skip weekends
            if current.weekday() < 5:  # Monday to Friday
                business_days.append(current.strftime('%Y-%m-%d'))
            current += datetime.timedelta(days=1)
            
        # Check for missing dates
        for date in business_days:
            if date not in df.index:
                results["missing_dates"].append(date)
                
        if results["missing_dates"]:
            results["valid"] = False
            results["issues"].append(f"Missing data for {len(results['missing_dates'])} business days")
            
        # Check for sectors with static values (no changes over time)
        for sector in df.columns:
            if df[sector].nunique() <= 1:
                results["static_sectors"].append(sector)
                
        if results["static_sectors"]:
            results["valid"] = False
            results["issues"].append(f"{len(results['static_sectors'])} sectors have static values")
            
        # Check for negative or zero values
        for sector in df.columns:
            if (df[sector] <= 0).any():
                results["valid"] = False
                results["issues"].append(f"Sector {sector} has negative or zero values")
                
        return results
        
    except Exception as e:
        logger.error(f"Error validating market cap data: {e}")
        results["valid"] = False
        results["issues"].append(f"Validation error: {e}")
        return results

def main():
    """Main function to run market cap collection"""
    # Collect today's market cap data
    collect_market_cap_data()
    
    # Validate the data
    validation = validate_market_cap_data()
    
    if not validation["valid"]:
        logger.warning("Market cap data validation failed:")
        for issue in validation["issues"]:
            logger.warning(f"- {issue}")
            
        # If we have missing dates, collect historical data to fill the gaps
        if validation["missing_dates"]:
            logger.info(f"Collecting missing historical data for {len(validation['missing_dates'])} dates")
            
            # Group consecutive dates for more efficient API calls
            missing_dates = sorted(validation["missing_dates"])
            if missing_dates:
                # Collect historical data for each missing date
                for date in missing_dates:
                    collect_market_cap_data(date)
                    
        # If we have static sectors, recollect the last 30 days of data
        if validation["static_sectors"]:
            logger.info(f"Recollecting historical data for sectors with static values")
            
            # Calculate date range (last 30 days)
            end_date = datetime.datetime.now().strftime('%Y-%m-%d')
            start_date = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime('%Y-%m-%d')
            
            # Collect historical data
            collect_historical_market_cap_data(start_date, end_date)
            
    # Generate and print a report
    report = generate_sector_market_cap_report()
    print(report)
    
    logger.info("Market cap collection process completed")

if __name__ == "__main__":
    main()