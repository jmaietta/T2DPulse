#!/usr/bin/env python3
"""
Rebuild Historical Market Caps - One Day at a Time

This script rebuilds the historical market cap data for a single specified date using the Polygon API.
This approach avoids overwhelming the API by allowing controlled rebuilding of the data day by day.
"""

import os
import sys
import csv
import json
import time
import argparse
import requests
import pandas as pd
from datetime import datetime, timedelta
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("rebuild_marketcaps_daily.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

# Polygon API key
POLYGON_API_KEY = os.environ.get('POLYGON_API_KEY')
if not POLYGON_API_KEY:
    logging.error("POLYGON_API_KEY environment variable is not set")
    sys.exit(1)

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
        # Fall back to loading from sector_market_cap.py if available
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

def rebuild_data_for_date(date_str):
    """Rebuild market cap data for a specific date"""
    try:
        date = datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        logging.error(f"Invalid date format: {date_str}. Please use YYYY-MM-DD format.")
        return False
    
    logging.info(f"Rebuilding market cap data for {date_str}")
    
    # Load ticker to sector mapping
    ticker_to_sector, sector_to_tickers = load_sector_tickers()
    all_tickers = list(ticker_to_sector.keys())
    all_sectors = list(sector_to_tickers.keys())
    
    logging.info(f"Loaded {len(all_tickers)} tickers across {len(all_sectors)} sectors")
    
    # Fetch market cap data for all tickers on this day
    ticker_data = {}
    for ticker in all_tickers:
        market_cap = fetch_polygon_market_cap(ticker, date)
        ticker_data[ticker] = market_cap
        # Save progress after each ticker in case we need to resume
        with open(f'rebuild_progress_{date_str}.json', 'w') as f:
            json.dump(ticker_data, f)
    
    # Calculate sector market caps
    sector_market_caps, missing_tickers = calculate_sector_market_caps(ticker_data, ticker_to_sector, date)
    
    # Check if historical_sector_market_caps.csv exists
    if os.path.exists('historical_sector_market_caps.csv'):
        # Load the existing file
        df = pd.read_csv('historical_sector_market_caps.csv')
        
        # Remove any existing entries for this date
        df = df[df['date'] != date_str]
        
        # Append new data for this date
        new_rows = []
        for sector, market_cap in sector_market_caps.items():
            missing = ','.join(missing_tickers.get(sector, []))
            new_rows.append({'date': date_str, 'sector': sector, 'market_cap': market_cap, 'missing_tickers': missing})
        
        new_df = pd.DataFrame(new_rows)
        df = pd.concat([df, new_df], ignore_index=True)
        
        # Save the updated file
        df.to_csv('historical_sector_market_caps.csv', index=False)
    else:
        # Create a new file
        with open('historical_sector_market_caps.csv', 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['date', 'sector', 'market_cap', 'missing_tickers'])
            
            for sector, market_cap in sector_market_caps.items():
                missing = ','.join(missing_tickers.get(sector, []))
                writer.writerow([date_str, sector, market_cap, missing])
    
    logging.info(f"Successfully rebuilt market cap data for {date_str}")
    return True

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Rebuild historical market cap data for a specific date')
    parser.add_argument('date', help='Date in YYYY-MM-DD format')
    return parser.parse_args()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python rebuild_marketcaps_daily.py YYYY-MM-DD")
        sys.exit(1)
    
    date_str = sys.argv[1]
    logging.info(f"Starting market cap data rebuild for {date_str}...")
    
    success = rebuild_data_for_date(date_str)
    
    if success:
        logging.info(f"Market cap data for {date_str} rebuilt successfully!")
    else:
        logging.error(f"Failed to rebuild market cap data for {date_str}")