#!/usr/bin/env python3
# improved_run_daily.py
# -----------------------------------------------------------
# Daily script to collect sector market capitalization values and momentum (EMA gap)
# Uses the improved data collector with historical data persistence

import os
import time
import datetime
import pytz
from config import SECTORS, FINNHUB_API_KEY
from improved_finnhub_data_collector import collect_daily_sector_data
from update_sector_history import main as update_sector_history_main

def get_eastern_time():
    """Get current time in US Eastern timezone"""
    eastern = pytz.timezone('US/Eastern')
    return datetime.datetime.now(eastern)

def main():
    """
    Main function to collect sector values, momentum, update sector history
    and recalculate the T2D Pulse score
    
    Uses the improved data collector that:
    - Stores historical price data
    - Only fetches new data points
    - Uses multiple sources to prevent zeros
    - Efficiently handles duplicate tickers across sectors
    """
    if not FINNHUB_API_KEY or FINNHUB_API_KEY == "":
        print("Error: Please set a valid Finnhub API key in config.py")
        return False
    
    eastern_time = get_eastern_time()
    today_date = eastern_time.strftime('%Y-%m-%d')
    print(f"Starting improved daily sector data collection at {eastern_time.strftime('%Y-%m-%d %H:%M:%S %Z')}...")
    
    # Use the improved comprehensive Finnhub data collector
    success = collect_daily_sector_data()
    
    if success:
        print(f"Improved data collection completed for {today_date}")
        
        # Now update the authentic sector history with the new data
        print("Updating authentic sector history with new Finnhub data...")
        history_success = update_sector_history_main()
        
        if history_success:
            print("Successfully updated authentic sector history with new Finnhub data")
            
            # Calculate authentic T2D Pulse score from sector data
            try:
                print("Calculating authentic T2D Pulse score...")
                
                # Import the calculation functions
                import sys
                try:
                    # Try to import directly
                    from calculate_authentic_pulse import calculate_pulse_scores_from_sectors, save_authentic_current_score
                except ImportError:
                    # If that fails, try to add the current directory to the path
                    print("Adjusting path to import calculate_authentic_pulse module...")
                    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
                    from calculate_authentic_pulse import calculate_pulse_scores_from_sectors, save_authentic_current_score
                
                # Calculate the scores
                pulse_df = calculate_pulse_scores_from_sectors()
                
                if pulse_df is not None:
                    # Save the current authentic score
                    latest_score = save_authentic_current_score()
                    print(f"Updated authentic T2D Pulse score to {latest_score} for {today_date}")
                else:
                    print("Failed to calculate pulse scores")
            except Exception as e:
                print(f"Error calculating authentic T2D Pulse score: {e}")
                import traceback
                traceback.print_exc()
        else:
            print("Failed to update authentic sector history with new Finnhub data")
            success = False
    else:
        print(f"Improved daily sector data collection failed for {today_date}")
        print("No data available to update sector history")
        
    return success

if __name__ == "__main__":
    main()