#!/usr/bin/env python3
"""
Rebuild Historical Market Caps - Scheduler

This script schedules the rebuilding of historical market cap data one day at a time,
pacing the requests to avoid overwhelming the Polygon API. It will rebuild the entire
30-day history over time.
"""

import os
import sys
import time
import logging
import subprocess
from datetime import datetime, timedelta

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("rebuild_marketcaps_scheduler.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

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

def rebuild_day(date_str):
    """Rebuild market cap data for a specific date"""
    logging.info(f"Starting rebuild for {date_str}")
    
    try:
        # Execute the daily rebuild script for this date
        command = ["python", "rebuild_marketcaps_daily.py", date_str]
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        
        # Log progress updates every 15 seconds
        while process.poll() is None:
            time.sleep(15)
            logging.info(f"Still processing {date_str}...")
        
        # Get the output
        stdout, stderr = process.communicate()
        
        if process.returncode == 0:
            logging.info(f"Successfully rebuilt market cap data for {date_str}")
            return True
        else:
            logging.error(f"Failed to rebuild market cap data for {date_str}")
            logging.error(f"Error: {stderr}")
            return False
    except Exception as e:
        logging.error(f"Exception while rebuilding data for {date_str}: {e}")
        return False

def schedule_rebuild():
    """Schedule the rebuilding of historical market cap data"""
    # Define date range (30 days)
    end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = end_date - timedelta(days=30)
    
    business_days = get_business_days(start_date, end_date)
    total_days = len(business_days)
    
    logging.info(f"Planning to rebuild market cap data for {total_days} business days, from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    
    # Process each business day with a delay to avoid overwhelming the API
    for i, day in enumerate(business_days):
        day_str = day.strftime('%Y-%m-%d')
        logging.info(f"Processing day {i+1} of {total_days}: {day_str}")
        
        # Rebuild data for this day
        success = rebuild_day(day_str)
        
        if success:
            logging.info(f"Completed day {i+1}/{total_days}: {day_str}")
        else:
            logging.error(f"Failed to process day {i+1}/{total_days}: {day_str}")
        
        # Sleep for 1 minute between days to avoid overwhelming the API
        if i < total_days - 1:
            logging.info(f"Waiting 60 seconds before processing the next day...")
            time.sleep(60)
    
    logging.info("Completed rebuilding historical market cap data")
    return True

if __name__ == "__main__":
    logging.info("Starting historical market cap data rebuild scheduler...")
    success = schedule_rebuild()
    if success:
        logging.info("Historical market cap data rebuild scheduler completed successfully!")
    else:
        logging.error("Failed to complete historical market cap data rebuild")