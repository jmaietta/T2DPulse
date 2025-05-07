#!/usr/bin/env python3
# force_update_today.py
# -----------------------------------------------------------
# Script to force an update of sector sentiment data and T2D Pulse score for today

import os
import time
import datetime
import pytz
import pandas as pd
import numpy as np
import sys

def get_eastern_time():
    """Get current time in US Eastern timezone"""
    eastern = pytz.timezone('US/Eastern')
    return datetime.datetime.now(eastern)

def main():
    """Force an update of sector data and T2D Pulse score for today"""
    eastern_time = get_eastern_time()
    today_date = eastern_time.strftime('%Y-%m-%d')
    print(f"Forcing update of sector sentiment data for {today_date}...")
    
    # First check if we have sector values in sector_values.csv for today
    have_raw_data = False
    try:
        if os.path.exists("data/sector_values.csv"):
            df = pd.read_csv("data/sector_values.csv")
            if today_date in df['Date'].values:
                print(f"Found raw sector data for {today_date} in sector_values.csv")
                have_raw_data = True
            else:
                print(f"No raw sector data for {today_date} in sector_values.csv")
                print("Will try to collect data using Finnhub API")
        else:
            print("sector_values.csv file not found")
    except Exception as e:
        print(f"Error checking sector_values.csv: {e}")
    
    # If we don't have raw data, collect it
    if not have_raw_data:
        try:
            print("Running daily collection with Finnhub API...")
            from run_daily import main as run_daily_main
            success = run_daily_main()
            if not success:
                print("Failed to collect raw sector data")
                return False
        except Exception as e:
            print(f"Error collecting raw sector data: {e}")
            return False
    
    # Now update the sector history with force_date parameter to ensure we use today's date
    try:
        print(f"Forcing update of authentic sector history for {today_date}...")
        
        # First, get the sector sentiment scores
        from update_sector_history import load_sector_values, convert_to_sentiment_scores, update_authentic_sector_history
        
        # Load the sector values from Finnhub API
        print("Loading sector values from Finnhub API...")
        sector_values_df = load_sector_values()
        if sector_values_df is None:
            print("Failed to load sector values")
            return False
        
        print(f"Loaded sector values for {len(sector_values_df)} dates")
        
        # Convert to sentiment scores
        print("Converting to sentiment scores...")
        scores = convert_to_sentiment_scores(sector_values_df)
        if scores is None:
            print("Failed to convert to sentiment scores")
            return False
        
        print(f"Generated {len(scores)} sector sentiment scores")
        
        # Update the authentic sector history with forced date
        print(f"Updating authentic sector history with force_date={today_date}...")
        success = update_authentic_sector_history(scores, force_date=today_date)
        if not success:
            print("Failed to update authentic sector history")
            return False
        
        print(f"Successfully updated authentic sector history for {today_date}")
    except Exception as e:
        print(f"Error updating sector history: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # Finally, calculate the authentic T2D Pulse score
    try:
        print("Calculating authentic T2D Pulse score...")
        
        # Import the calculation functions
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
            return True
        else:
            print("Failed to calculate pulse scores")
            return False
    except Exception as e:
        print(f"Error calculating authentic T2D Pulse score: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    if success:
        print("✅ Successfully forced update for today")
    else:
        print("❌ Failed to force update for today")