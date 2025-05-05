#!/usr/bin/env python3
# run_daily.py
# -----------------------------------------------------------
# Daily script to collect sector market capitalization values and momentum (EMA gap)

import os
import time
import datetime
import pytz
from config import SECTORS, FINNHUB_API_KEY
from finnhub_data_collector import collect_daily_sector_data
from update_sector_history import main as update_sector_history_main

def get_eastern_time():
    """Get current time in US Eastern timezone"""
    eastern = pytz.timezone('US/Eastern')
    return datetime.datetime.now(eastern)

def main():
    """
    Main function to collect sector values, momentum, and save them
    """
    if not FINNHUB_API_KEY or FINNHUB_API_KEY == "":
        print("Error: Please set a valid Finnhub API key in config.py")
        return False
    
    eastern_time = get_eastern_time()
    print(f"Starting daily sector data collection at {eastern_time.strftime('%Y-%m-%d %H:%M:%S %Z')}...")
    
    # Use the new comprehensive Finnhub data collector
    success = collect_daily_sector_data()
    
    if success:
        print(f"Successfully ran daily sector data collection for {eastern_time.strftime('%Y-%m-%d')}")
        
        # Now update the authentic sector history with the new data
        print("Updating authentic sector history with new Finnhub data...")
        history_success = update_sector_history_main()
        
        if history_success:
            print("Successfully updated authentic sector history with new Finnhub data")
        else:
            print("Failed to update authentic sector history with new Finnhub data")
            success = False
    else:
        print(f"Daily sector data collection failed for {eastern_time.strftime('%Y-%m-%d')}")
        
    return success

if __name__ == "__main__":
    main()