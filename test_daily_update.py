#!/usr/bin/env python3
# test_daily_update.py
# -----------------------------------------------------------
# Script to test the daily update process for sector sentiment data and T2D Pulse score
# This script simulates the 5:00pm ET daily update process but runs immediately

import os
import datetime
import pytz
import time
import traceback

def get_eastern_time():
    """Get current time in US Eastern timezone"""
    eastern = pytz.timezone('US/Eastern')
    return datetime.datetime.now(eastern)

def main():
    """Test the daily update process by simulating the 5:00pm ET update"""
    eastern_time = get_eastern_time()
    today_date = eastern_time.strftime('%Y-%m-%d')
    print(f"TEST: Simulating 5:00pm ET update for {today_date}...")
    
    try:
        # Step 1: Run daily sector data collection using Finnhub API
        print(f"TEST: Running daily sector data collection...")
        from run_daily import main as run_daily_collection
        daily_collection_success = run_daily_collection()
        
        if daily_collection_success:
            print(f"TEST: Successfully collected fresh sector data on {today_date}")
            
            # Step 2: Calculate the authentic T2D Pulse score
            print(f"TEST: Calculating authentic T2D Pulse score...")
            from calculate_authentic_pulse import calculate_pulse_scores_from_sectors, save_authentic_current_score
            pulse_df = calculate_pulse_scores_from_sectors()
            if pulse_df is not None:
                latest_score = save_authentic_current_score()
                print(f"TEST: Updated authentic T2D Pulse score to {latest_score} on {today_date}")
                return True
            else:
                print(f"TEST: Failed to calculate authentic T2D Pulse score")
                return False
        else:
            print(f"TEST: Failed to collect fresh sector data on {today_date}")
            return False
    except Exception as e:
        print(f"TEST: Error in simulated update process: {str(e)}")
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    if success:
        print("✅ TEST: Successfully simulated 5:00pm ET update process")
    else:
        print("❌ TEST: Failed to simulate 5:00pm ET update process")