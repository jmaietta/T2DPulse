#!/usr/bin/env python3
"""
Rebuild Historical Market Caps

This script rebuilds the entire 30-day historical market cap data using the Polygon API.
It ensures that the historical data is consistent with the current data displayed in the dashboard.
"""

import os
import sys
import csv
import json
import time
import datetime
import requests
import pandas as pd
from datetime import datetime, timedelta
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("rebuild_historical_marketcaps.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

# Polygon API key
POLYGON_API_KEY = os.environ.get('POLYGON_API_KEY')
if not POLYGON_API_KEY:
    logging.error("POLYGON_API_KEY environment variable is not set")
    sys.exit(1)

# Ticker to sector mapping
def load_sector_tickers():
    """Load the mapping of sectors to their constituent tickers"""
    try:
        df = pd.read_csv('T2D_Pulse_93_tickers_coverage.csv', skiprows=7)  # Skip the header rows
        # Create a mapping of tickers to sectors
        ticker_to_sector = {}
        for _, row in df.iterrows():
            ticker = row['Ticker']
            sector = row['Sector']
            ticker_to_sector[ticker] = sector
        
        # Create a mapping of sectors to tickers
        sector_to_tickers = {}
        for ticker, sector in ticker_to_sector.items():
            if sector not in sector_to_tickers:
                sector_to_tickers[sector] = []
            sector_to_tickers[sector].append(ticker)
        
        return ticker_to_sector, sector_to_tickers
    except Exception as e:
        logging.error(f"Error loading sector tickers: {e}")
        # Fall back to loading from sector_market_caps.py if available
        try:
            from sector_market_cap import SECTOR_TICKERS
            ticker_to_sector = {}
            for sector, tickers in SECTOR_TICKERS.items():
                for ticker in tickers:
                    ticker_to_sector[ticker] = sector
            return ticker_to_sector, SECTOR_TICKERS
        except ImportError:
            logging.error("Could not load sector tickers from sector_market_cap.py")
            sys.exit(1)

def get_business_days(start_date, end_date):
    """Get a list of business days between start_date and end_date (inclusive)"""
    business_days = []
    current_date = start_date
    while current_date <= end_date:
        # Monday = 0, Sunday = 6
        if current_date.weekday() < 5:  # Weekday
            business_days.append(current_date)
        current_date += timedelta(days=1)
    return business_days

def fetch_polygon_market_cap(ticker, date):
    """Fetch market cap data from Polygon API for a specific ticker and date"""
    date_str = date.strftime('%Y-%m-%d')
    url = f"https://api.polygon.io/v3/reference/tickers/{ticker}?date={date_str}&apiKey={POLYGON_API_KEY}"
    
    try:
        logging.info(f"Fetching market cap for {ticker} on {date_str}...")
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.json()
            results = data.get('results', {})
            
            # Extract market cap
            market_cap = results.get('market_cap')
            if market_cap:
                logging.info(f"  ✓ Polygon: ${market_cap/1e9:.2f}B")
                return market_cap
            else:
                logging.warning(f"  ✗ No market cap data for {ticker} on {date_str}")
                return None
        else:
            logging.warning(f"  ✗ Polygon API error: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        logging.error(f"  ✗ Error fetching data for {ticker}: {e}")
        return None
    
    # Don't hit the API too hard
    time.sleep(0.2)  # 200ms delay between requests

def calculate_sector_market_caps(ticker_data, ticker_to_sector, date):
    """Calculate market caps for each sector based on ticker data"""
    sector_market_caps = {}
    missing_tickers = {}
    
    for ticker, market_cap in ticker_data.items():
        sector = ticker_to_sector.get(ticker)
        if not sector:
            logging.warning(f"Unknown sector for ticker {ticker}")
            continue
            
        if sector not in sector_market_caps:
            sector_market_caps[sector] = 0
            missing_tickers[sector] = []
            
        if market_cap:
            sector_market_caps[sector] += market_cap
        else:
            missing_tickers[sector].append(ticker)
    
    # Log missing tickers by sector
    for sector, tickers in missing_tickers.items():
        if tickers:
            logging.warning(f"Missing market cap data for {len(tickers)} tickers in {sector} sector on {date}")
    
    return sector_market_caps, missing_tickers

def rebuild_historical_data():
    """Rebuild the entire 30-day historical market cap data"""
    # Load ticker to sector mapping
    ticker_to_sector, sector_to_tickers = load_sector_tickers()
    all_tickers = list(ticker_to_sector.keys())
    all_sectors = list(sector_to_tickers.keys())
    
    logging.info(f"Loaded {len(all_tickers)} tickers across {len(all_sectors)} sectors")
    
    # Define date range (30 days)
    end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = end_date - timedelta(days=30)
    business_days = get_business_days(start_date, end_date)
    
    logging.info(f"Rebuilding market cap data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')} ({len(business_days)} business days)")
    
    # Prepare output file
    output_file = 'rebuilt_sector_market_caps.csv'
    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['date', 'sector', 'market_cap', 'missing_tickers'])
    
    # Process each business day
    for day in business_days:
        day_str = day.strftime('%Y-%m-%d')
        logging.info(f"Processing {day_str}...")
        
        # Fetch market cap data for all tickers on this day
        ticker_data = {}
        for ticker in all_tickers:
            market_cap = fetch_polygon_market_cap(ticker, day)
            ticker_data[ticker] = market_cap
            # Save progress after each ticker in case we need to resume
            with open(f'rebuild_progress_{day_str}.json', 'w') as f:
                json.dump(ticker_data, f)
        
        # Calculate sector market caps
        sector_market_caps, missing_tickers = calculate_sector_market_caps(ticker_data, ticker_to_sector, day)
        
        # Save sector market caps for this day
        with open(output_file, 'a', newline='') as f:
            writer = csv.writer(f)
            for sector, market_cap in sector_market_caps.items():
                missing = ','.join(missing_tickers.get(sector, []))
                writer.writerow([day_str, sector, market_cap, missing])
        
        logging.info(f"Completed processing for {day_str}")
    
    logging.info(f"Historical market cap data rebuilt and saved to {output_file}")
    
    # Replace the original file with the rebuilt one
    os.replace(output_file, 'historical_sector_market_caps.csv')
    logging.info("Replaced historical_sector_market_caps.csv with rebuilt data")
    
    # Backup the original sector_market_caps.csv file
    os.rename('sector_market_caps.csv', 'sector_market_caps.csv.bak')
    # Also replace the sector_market_caps.csv file
    os.replace(output_file, 'sector_market_caps.csv')
    logging.info("Replaced sector_market_caps.csv with rebuilt data")
    
    return True

if __name__ == "__main__":
    logging.info("Starting historical market cap data rebuild...")
    success = rebuild_historical_data()
    if success:
        logging.info("Historical market cap data rebuilt successfully!")
    else:
        logging.error("Failed to rebuild historical market cap data")