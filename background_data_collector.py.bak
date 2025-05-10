#!/usr/bin/env python3
# background_data_collector.py
# -----------------------------------------------------------
# Background process to continuously collect ticker data
# Runs in the background and maintains market data coverage
# Uses batch_ticker_collector for more efficient API usage

import os
import sys
import time
import pandas as pd
import numpy as np
import datetime
import pytz
import logging
import batch_ticker_collector
from config import SECTORS

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

def check_ticker_coverage():
    """
    Check the current coverage of ticker data to determine if an update is needed
    
    Returns:
        dict: Coverage statistics with the following keys:
            - total_tickers: Total number of tickers being tracked
            - covered_tickers: Number of tickers with current data
            - coverage_pct: Percentage of tickers with current data
            - latest_date: Latest date in the data
            - days_behind: Number of trading days behind current date
    """
    try:
        # Use batch_ticker_collector's function to get latest date
        latest_date = batch_ticker_collector.get_latest_date()
        
        # Get all tickers being tracked
        all_tickers = batch_ticker_collector.load_sector_tickers()
        total_tickers = len(all_tickers)
        
        # Get current date
        current_date = datetime.datetime.now().date()
        
        # Calculate days behind
        days_behind = (current_date - latest_date).days
        
        # Check for weekend (don't count weekends as "behind")
        current_weekday = current_date.weekday()
        if current_weekday < 5:  # Weekday (Monday = 0, Friday = 4)
            # If we're on a weekday, calculate business days behind
            days_behind = max(0, days_behind)
        else:
            # If we're on weekend, calculate from Friday
            days_to_friday = current_weekday - 4
            adjusted_days_behind = days_behind - days_to_friday
            days_behind = max(0, adjusted_days_behind)
        
        return {
            "total_tickers": total_tickers,
            "latest_date": latest_date,
            "days_behind": days_behind
        }
    except Exception as e:
        logging.error(f"Error checking ticker coverage: {e}")
        logging.exception("Exception details:")
        return None

def run_continuous_collection(check_interval=30, update_interval=60):
    """
    Run a continuous background process to collect ticker data
    
    Args:
        check_interval (int): Minutes between coverage checks
        update_interval (int): Minutes between forced updates during market hours
    """
    logging.info("Starting background ticker data collection process...")
    
    # Variables to track last update
    last_update_time = None
    
    # Main loop - run continuously
    while True:
        try:
            eastern_time = get_eastern_time()
            current_hour = eastern_time.hour
            
            # Check if we need to run an update
            run_update = False
            
            # Check if we've never updated
            if last_update_time is None:
                run_update = True
                reason = "Initial run"
            
            # Check if we're during market hours (9 AM to 6 PM Eastern on weekdays)
            is_market_hours = eastern_time.weekday() < 5 and 9 <= current_hour < 18
            
            # Check if we need to force an update during market hours
            if is_market_hours and last_update_time is not None:
                minutes_since_update = (eastern_time - last_update_time).total_seconds() / 60
                if minutes_since_update >= update_interval:
                    run_update = True
                    reason = f"Scheduled update during market hours (every {update_interval} minutes)"
            
            # Check current coverage status
            coverage = check_ticker_coverage()
            if coverage:
                if coverage["days_behind"] > 0:
                    run_update = True
                    reason = f"Data is {coverage['days_behind']} trading days behind"
            
            # Run the batch update if needed
            if run_update:
                logging.info(f"Running batch ticker collection. Reason: {reason}")
                success = batch_ticker_collector.run_batch_collection()
                
                if success:
                    logging.info("Batch collection completed successfully")
                    last_update_time = eastern_time
                else:
                    logging.error("Batch collection failed")
                
                # Log coverage stats after update
                new_coverage = check_ticker_coverage()
                if new_coverage:
                    logging.info(f"Latest data: {new_coverage['latest_date']}")
                    logging.info(f"Days behind: {new_coverage['days_behind']}")
                    logging.info(f"Total tickers: {new_coverage['total_tickers']}")
            
            # Sleep before next check
            sleep_time = check_interval * 60  # Convert minutes to seconds
            
            # Sleep longer outside market hours
            if not is_market_hours:
                if eastern_time.weekday() >= 5:  # Weekend
                    sleep_time = 60 * 60  # 1 hour on weekends
                else:
                    sleep_time = 30 * 60  # 30 minutes on weekdays outside market hours
            
            logging.info(f"Sleeping for {sleep_time/60:.1f} minutes before next check")
            time.sleep(sleep_time)
        
        except Exception as e:
            logging.error(f"Error in background collection process: {e}")
            logging.exception("Exception details:")
            logging.info("Sleeping for 5 minutes before retrying")
            time.sleep(300)  # Sleep for 5 minutes before retrying

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Background process to continuously collect ticker data')
    parser.add_argument('--check', type=int, default=30, help='Minutes between coverage checks')
    parser.add_argument('--update', type=int, default=60, help='Minutes between forced updates during market hours')
    args = parser.parse_args()
    
    logging.info(f"Starting background collector with check_interval={args.check}, update_interval={args.update}")
    run_continuous_collection(check_interval=args.check, update_interval=args.update)