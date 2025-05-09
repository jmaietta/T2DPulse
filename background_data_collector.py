#!/usr/bin/env python3
# background_data_collector.py
# -----------------------------------------------------------
# Background process to continuously collect ticker data
# Runs in the background and progressively improves data coverage

import os
import sys
import time
import pandas as pd
import datetime
import pytz
import logging
from config import SECTORS
from process_sector_tickers import process_sector
from ensure_complete_data import get_sector_coverage

# Path to the official ticker list
OFFICIAL_TICKERS_FILE = "official_tickers.csv"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('background_data_collector.log'),
        logging.StreamHandler()
    ]
)

def get_eastern_time():
    """Get current time in US Eastern timezone"""
    eastern = pytz.timezone('US/Eastern')
    return datetime.datetime.now(eastern)

def run_continuous_collection(max_tickers_per_sector=3, sleep_between_sectors=30):
    """
    Run a continuous background process to collect ticker data
    
    Args:
        max_tickers_per_sector (int): Maximum number of tickers to process per sector per iteration
        sleep_between_sectors (int): Seconds to sleep between processing sectors
    """
    logging.info("Starting background ticker data collection process...")
    
    # Main loop - run continuously
    while True:
        try:
            eastern_time = get_eastern_time()
            current_hour = eastern_time.hour
            
            # Only run between 9 AM and 6 PM Eastern time on weekdays
            if eastern_time.weekday() < 5 and 9 <= current_hour < 18:
                logging.info(f"Starting collection cycle at {eastern_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
                
                # Get current coverage
                coverage = get_sector_coverage()
                if not coverage:
                    logging.error("Failed to get sector coverage. Will retry later.")
                    time.sleep(300)  # Sleep for 5 minutes before retrying
                    continue
                
                # Calculate overall coverage based on unique official tickers
                # Get the official list from file
                official_tickers = []
                try:
                    with open(OFFICIAL_TICKERS_FILE, 'r') as f:
                        for line in f:
                            ticker = line.strip()
                            if ticker:
                                official_tickers.append(ticker)
                except Exception as e:
                    logging.error(f"Error reading official tickers file: {e}")
                    official_tickers = []
                
                if not official_tickers:
                    # Fallback to sum of sectors if official file not available
                    total_tickers = sum(data["total_tickers"] for _, data in coverage.items())
                    total_covered = sum(data["price_coverage"] for _, data in coverage.items())
                    logging.warning("Using sector sum for coverage (not unique tickers)")
                else:
                    # Use official list for proper unique ticker counting
                    total_tickers = len(official_tickers)
                    
                    # Load data files to check actual coverage
                    try:
                        price_df = pd.read_csv('data/historical_ticker_prices.csv', index_col=0)
                        mcap_df = pd.read_csv('data/historical_ticker_marketcap.csv', index_col=0)
                        latest_date = price_df.index[-1]
                        
                        # Count tickers with complete data
                        total_covered = 0
                        for ticker in official_tickers:
                            price_complete = ticker in price_df.columns and pd.notna(price_df.loc[latest_date, ticker])
                            mcap_complete = ticker in mcap_df.columns and pd.notna(mcap_df.loc[latest_date, ticker])
                            if price_complete and mcap_complete:
                                total_covered += 1
                    except Exception as e:
                        logging.error(f"Error calculating official coverage: {e}")
                        # Fallback to sector sum
                        total_tickers = sum(data["total_tickers"] for _, data in coverage.items())
                        total_covered = sum(data["price_coverage"] for _, data in coverage.items())
                        logging.warning("Using sector sum for coverage due to error")
                
                coverage_pct = total_covered / total_tickers * 100
                
                logging.info(f"Current coverage: {total_covered}/{total_tickers} ({coverage_pct:.1f}%)")
                
                # If we have 100% coverage, we can sleep longer
                if coverage_pct >= 100:
                    logging.info("Already at 100% coverage. Will check again in 30 minutes.")
                    time.sleep(1800)  # Sleep for 30 minutes
                    continue
                
                # Sort sectors by coverage (lowest first)
                sorted_sectors = sorted(
                    coverage.items(), 
                    key=lambda x: (x[1]["price_pct"] + x[1]["marketcap_pct"]) / 2
                )
                
                # Process each sector, starting with the lowest coverage
                for sector_name, sector_data in sorted_sectors:
                    if len(sector_data["missing_tickers"]) == 0:
                        logging.info(f"Sector {sector_name} already has 100% coverage. Skipping.")
                        continue
                    
                    logging.info(f"Processing sector: {sector_name} ({sector_data['price_pct']:.1f}% coverage)")
                    logging.info(f"Missing tickers: {len(sector_data['missing_tickers'])}")
                    
                    # Process this sector with the maximum tickers limit
                    process_sector(sector_name, max_tickers=max_tickers_per_sector)
                    
                    # Sleep between sectors to avoid API rate limits
                    logging.info(f"Sleeping for {sleep_between_sectors} seconds to avoid API rate limits")
                    time.sleep(sleep_between_sectors)
                
                # After processing all sectors, sleep for 10 minutes before the next cycle
                logging.info("Completed collection cycle. Sleeping for 10 minutes before next cycle.")
                time.sleep(600)
            else:
                # Outside of market hours, sleep for 30 minutes
                if eastern_time.weekday() >= 5:
                    logging.info("Weekend detected. Sleeping for 1 hour.")
                    time.sleep(3600)  # 1 hour on weekends
                else:
                    logging.info("Outside market hours. Sleeping for 30 minutes.")
                    time.sleep(1800)  # 30 minutes on weekdays outside market hours
        
        except Exception as e:
            logging.error(f"Error in background collection process: {e}")
            logging.exception("Exception details:")
            logging.info("Sleeping for 5 minutes before retrying.")
            time.sleep(300)  # Sleep for 5 minutes before retrying

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Background process to continuously collect ticker data')
    parser.add_argument('--max', type=int, default=3, help='Maximum tickers to process per sector per iteration')
    parser.add_argument('--sleep', type=int, default=30, help='Seconds to sleep between processing sectors')
    args = parser.parse_args()
    
    logging.info(f"Starting background collector with max={args.max}, sleep={args.sleep}")
    run_continuous_collection(max_tickers_per_sector=args.max, sleep_between_sectors=args.sleep)