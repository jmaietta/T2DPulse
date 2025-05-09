"""
Authentic Market Cap Updater

This script fetches and updates authentic market cap data from Polygon.io API.
It's designed to be run daily after market close (e.g., 6pm ET) to ensure
accurate sector weighting data is available for the T2D Pulse index.

Use Cases:
1. Run this script manually to refresh market cap data
2. Schedule this script to run daily via cron or background task

Requirements:
- POLYGON_API_KEY environment variable must be set
- polygon_sector_caps.py must be available in the same directory
"""

import os
import sys
import logging
import argparse
import time
from datetime import datetime, timedelta
import pytz
import json
import subprocess
import pandas as pd

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('authentic_marketcap_updater.log')
    ]
)

# Define market hours (US Eastern Time)
MARKET_OPEN_HOUR = 9  # 9:00 AM ET
MARKET_CLOSE_HOUR = 16  # 4:00 PM ET
DEFAULT_UPDATE_HOUR = 18  # 6:00 PM ET (2 hours after market close)

def is_market_day(date):
    """Check if a given date is a market trading day (not weekend)"""
    return date.weekday() < 5  # 0-4 are Monday to Friday

def get_current_eastern_time():
    """Get current time in US Eastern time zone"""
    et_zone = pytz.timezone('US/Eastern')
    return datetime.now(pytz.utc).astimezone(et_zone)

def is_market_hours():
    """Check if it's currently during market hours"""
    now = get_current_eastern_time()
    
    # Skip weekends
    if not is_market_day(now):
        return False
    
    # Check if we're between market open and close
    return MARKET_OPEN_HOUR <= now.hour < MARKET_CLOSE_HOUR

def run_polygon_collector(days=5, verbose=True, output_dir="data"):
    """
    Run the polygon_sector_caps.py script to fetch authentic market cap data
    
    Args:
        days (int): Number of days of history to fetch
        verbose (bool): Whether to print verbose output
        output_dir (str): Output directory
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Ensure POLYGON_API_KEY is set
        polygon_api_key = os.environ.get("POLYGON_API_KEY")
        if not polygon_api_key:
            logging.error("POLYGON_API_KEY environment variable not set. Cannot fetch market cap data.")
            return False
        
        # Set up command arguments
        cmd = [
            sys.executable,  # Python executable
            "polygon_sector_caps.py",
            f"--days={days}"
        ]
        
        if verbose:
            cmd.append("--verbose")
            
        if output_dir != "data":
            cmd.append(f"--output-dir={output_dir}")
        
        # Run the command
        logging.info(f"Running polygon_sector_caps.py with args: {' '.join(cmd[1:])}")
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        # Check for errors
        if result.returncode != 0:
            logging.error(f"polygon_sector_caps.py failed with return code {result.returncode}")
            logging.error(f"Error output: {result.stderr}")
            return False
        
        # Log output
        logging.info(f"polygon_sector_caps.py output: {result.stdout}")
        
        # Verify that the output files were created
        parquet_file = os.path.join(output_dir, "sector_market_caps.parquet")
        csv_file = os.path.join(output_dir, "sector_market_caps.csv")
        
        if os.path.exists(parquet_file) and os.path.getsize(parquet_file) > 0:
            logging.info(f"Successfully created {parquet_file}")
            return True
        elif os.path.exists(csv_file) and os.path.getsize(csv_file) > 0:
            logging.info(f"Successfully created {csv_file}")
            return True
        else:
            logging.error(f"Failed to create output files: {parquet_file} or {csv_file}")
            return False
        
    except Exception as e:
        logging.error(f"Error running polygon_sector_caps.py: {e}")
        return False

def get_last_update_time():
    """Get the timestamp of the last successful update"""
    try:
        if os.path.exists("data/market_cap_last_update.txt"):
            with open("data/market_cap_last_update.txt", "r") as f:
                timestamp_str = f.read().strip()
                return datetime.fromisoformat(timestamp_str)
        return None
    except Exception as e:
        logging.error(f"Error reading last update timestamp: {e}")
        return None

def set_last_update_time():
    """Set the timestamp of the current successful update"""
    try:
        # Ensure directory exists
        os.makedirs("data", exist_ok=True)
        
        # Write timestamp
        with open("data/market_cap_last_update.txt", "w") as f:
            f.write(datetime.now().isoformat())
        
        logging.info(f"Updated last update timestamp: {datetime.now().isoformat()}")
    except Exception as e:
        logging.error(f"Error writing last update timestamp: {e}")

def should_update():
    """
    Determine if we should update the market cap data
    
    Returns:
        bool: True if we should update, False otherwise
    """
    # Get last update time
    last_update = get_last_update_time()
    
    # If we've never updated, we should update now
    if last_update is None:
        logging.info("No previous update found, will update now")
        return True
    
    # Get current time
    now = get_current_eastern_time()
    
    # If it's after market close today and we haven't updated yet today
    if (now.hour >= MARKET_CLOSE_HOUR and 
        (last_update.date() < now.date() or 
         (last_update.date() == now.date() and last_update.hour < MARKET_CLOSE_HOUR))):
        logging.info(f"Market closed for today and last update was {last_update}, will update now")
        return True
    
    # If the market is closed today (weekend) and we haven't updated since before market close on Friday
    if not is_market_day(now):
        last_market_day = now.date() - timedelta(days=1 if now.weekday() == 5 else 2)  # Friday
        if last_update.date() < last_market_day:
            logging.info(f"Weekend and last update was before Friday close ({last_update}), will update now")
            return True
    
    # For manual runs and other cases, check if it's been more than 4 hours since the last update
    if (now - last_update) > timedelta(hours=4):
        logging.info(f"Last update was more than 4 hours ago ({last_update}), will update now")
        return True
    
    logging.info(f"Last update was recent ({last_update}), no need to update now")
    return False

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Update authentic market cap data")
    parser.add_argument("--force", action="store_true", help="Force update even if not needed")
    parser.add_argument("--days", type=int, default=30, help="Number of days of history to fetch")
    parser.add_argument("--silent", action="store_true", help="Suppress verbose output")
    parser.add_argument("--output-dir", type=str, default="data", help="Output directory")
    return parser.parse_args()

def generate_market_cap_report():
    """
    Generate a human-readable report of the latest market cap data
    
    Returns:
        str: Formatted report text
    """
    try:
        # Import this after we've created the file
        from authentic_marketcap_reader import generate_market_cap_report as get_report
        return get_report()
    except ImportError:
        logging.error("Could not import authentic_marketcap_reader.py")
        return "Error generating market cap report: authentic_marketcap_reader.py not found"

def main():
    """Main function"""
    start_time = time.time()
    
    # Parse arguments
    args = parse_args()
    
    # Check if we should update
    if args.force or should_update():
        # Run the polygon collector
        success = run_polygon_collector(
            days=args.days,
            verbose=not args.silent,
            output_dir=args.output_dir
        )
        
        if success:
            # Set last update time
            set_last_update_time()
            
            # Generate and print report
            if not args.silent:
                print("\n" + generate_market_cap_report())
                
            logging.info(f"Market cap update completed successfully in {time.time() - start_time:.1f} seconds")
        else:
            logging.error("Market cap update failed")
    else:
        logging.info("No update needed at this time. Use --force to override.")
    
if __name__ == "__main__":
    main()