#!/usr/bin/env python3
"""
Fetch Polygon 30-Day Ticker History

This script fetches 30 days of historical market cap data for all 93 tickers from the Polygon API,
then aggregates the data into sector totals. This provides authentic market cap history for all sectors.
"""

import os
import sys
import json
import time
import pandas as pd
import requests
import logging
from datetime import datetime, timedelta

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("polygon_30day_history.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

# Polygon API key
POLYGON_API_KEY = os.environ.get('POLYGON_API_KEY')
if not POLYGON_API_KEY:
    logging.error("POLYGON_API_KEY environment variable is not set")
    sys.exit(1)

def load_ticker_list():
    """Load the list of all 93 tickers from the coverage file"""
    try:
        df = pd.read_csv('T2D_Pulse_93_tickers_coverage.csv', skiprows=7)  # Skip the header rows
        tickers = df['Ticker'].tolist()
        sectors = df['Sector'].tolist()
        
        ticker_to_sector = dict(zip(tickers, sectors))
        
        # Group tickers by sector
        sector_to_tickers = {}
        for ticker, sector in ticker_to_sector.items():
            if sector not in sector_to_tickers:
                sector_to_tickers[sector] = []
            sector_to_tickers[sector].append(ticker)
        
        logging.info(f"Loaded {len(tickers)} tickers across {len(sector_to_tickers)} sectors")
        return tickers, ticker_to_sector, sector_to_tickers
    
    except Exception as e:
        logging.error(f"Error loading ticker list: {e}")
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

def fetch_polygon_ticker_history(ticker, days=30):
    """Fetch historical market cap data for a ticker over the specified number of days"""
    logging.info(f"Fetching {days}-day market cap history for {ticker}...")
    
    # Calculate date range
    end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = end_date - timedelta(days=days)
    
    # Get list of business days
    business_days = get_business_days(start_date, end_date)
    
    # Fetch data for each day
    ticker_history = []
    for day in business_days:
        day_str = day.strftime('%Y-%m-%d')
        url = f"https://api.polygon.io/v3/reference/tickers/{ticker}?date={day_str}&apiKey={POLYGON_API_KEY}"
        
        try:
            response = requests.get(url)
            
            if response.status_code == 200:
                data = response.json()
                results = data.get('results', {})
                
                # Extract market cap
                market_cap = results.get('market_cap')
                if market_cap:
                    ticker_history.append({
                        'date': day_str,
                        'ticker': ticker,
                        'market_cap': market_cap
                    })
                    logging.info(f"  ✓ {day_str}: ${market_cap/1e9:.2f}B")
                else:
                    logging.warning(f"  ✗ No market cap data for {ticker} on {day_str}")
            else:
                logging.warning(f"  ✗ Polygon API error for {ticker} on {day_str}: {response.status_code}")
        
        except Exception as e:
            logging.error(f"  ✗ Error fetching data for {ticker} on {day_str}: {e}")
        
        # Don't hit the API too hard
        time.sleep(0.2)  # 200ms delay between requests
    
    logging.info(f"Fetched {len(ticker_history)} days of market cap data for {ticker}")
    return ticker_history

def fetch_all_ticker_histories(tickers, days=30):
    """Fetch historical market cap data for all tickers"""
    all_ticker_history = []
    total_tickers = len(tickers)
    
    for i, ticker in enumerate(tickers):
        logging.info(f"Processing ticker {i+1}/{total_tickers}: {ticker}")
        
        ticker_history = fetch_polygon_ticker_history(ticker, days)
        all_ticker_history.extend(ticker_history)
        
        # Save progress after each ticker
        ticker_df = pd.DataFrame(all_ticker_history)
        ticker_df.to_csv('T2D_Pulse_Full_Ticker_History.csv', index=False)
        logging.info(f"Saved progress: {len(all_ticker_history)} data points across {i+1} tickers")
        
        # Short pause between tickers to avoid overwhelming the API
        if i < total_tickers - 1:
            time.sleep(1)
    
    return all_ticker_history

def aggregate_by_sector(ticker_history, ticker_to_sector):
    """Aggregate ticker market cap data into sector totals"""
    # Convert to DataFrame for easier manipulation
    df = pd.DataFrame(ticker_history)
    
    # Add sector column
    df['sector'] = df['ticker'].map(ticker_to_sector)
    
    # Group by date and sector, summing market caps
    sector_history = df.groupby(['date', 'sector'])['market_cap'].sum().reset_index()
    
    # Add missing_tickers column (empty for now)
    sector_history['missing_tickers'] = ''
    
    return sector_history

def main():
    """Main function to fetch and process 30-day market cap history"""
    # Load ticker list
    tickers, ticker_to_sector, sector_to_tickers = load_ticker_list()
    
    # Check if we already have some ticker history to avoid duplicate work
    if os.path.exists('T2D_Pulse_Full_Ticker_History.csv'):
        logging.info("Existing ticker history found. Checking for missing data...")
        existing_df = pd.read_csv('T2D_Pulse_Full_Ticker_History.csv')
        
        # Calculate which tickers need data
        existing_tickers = existing_df['ticker'].unique()
        missing_tickers = [t for t in tickers if t not in existing_tickers]
        
        if missing_tickers:
            logging.info(f"Found {len(missing_tickers)} tickers missing from history. Will fetch their data.")
            new_history = fetch_all_ticker_histories(missing_tickers)
            
            # Combine with existing data
            all_ticker_history = existing_df.to_dict('records') + new_history
        else:
            logging.info("All tickers already have some history. Using existing data.")
            all_ticker_history = existing_df.to_dict('records')
    else:
        # No existing data, fetch all ticker histories
        logging.info(f"No existing ticker history found. Fetching data for all {len(tickers)} tickers...")
        all_ticker_history = fetch_all_ticker_histories(tickers)
    
    # Aggregate by sector
    sector_history = aggregate_by_sector(all_ticker_history, ticker_to_sector)
    
    # Save sector history
    sector_history.to_csv('historical_sector_market_caps.csv', index=False)
    logging.info(f"Saved historical sector market caps with {len(sector_history)} data points")
    
    # Also replace sector_market_caps.csv to ensure dashboard uses the same data
    sector_history.to_csv('sector_market_caps.csv', index=False)
    logging.info("Replaced sector_market_caps.csv with authentic historical data")
    
    # Create a backup of the original file
    os.rename('T2D_Pulse_Full_Ticker_History.csv', 'T2D_Pulse_Full_Ticker_History.csv.bak')
    
    # Save full ticker history in a more organized format
    ticker_df = pd.DataFrame(all_ticker_history)
    ticker_df.to_csv('T2D_Pulse_Full_Ticker_History.csv', index=False)
    logging.info(f"Saved full ticker history with {len(all_ticker_history)} data points")
    
    return True

if __name__ == "__main__":
    logging.info("Starting Polygon 30-day market cap history fetch...")
    success = main()
    if success:
        logging.info("Successfully fetched and processed 30-day market cap history!")
    else:
        logging.error("Failed to fetch and process 30-day market cap history")