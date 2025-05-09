#!/usr/bin/env python3
# run_daily.py
# -----------------------------------------------------------
# Daily script to collect sector market capitalization values and momentum (EMA gap)
# Enhanced with multi-source approach (Finnhub, Yahoo Finance, AlphaVantage)

import os
import time
import datetime
import pytz
import pandas as pd
from config import SECTORS, FINNHUB_API_KEY
# Using improved data collector with historical persistence and fallback mechanisms
from improved_finnhub_data_collector import collect_daily_sector_data
from update_sector_history import main as update_sector_history_main

def get_eastern_time():
    """Get current time in US Eastern timezone"""
    eastern = pytz.timezone('US/Eastern')
    return datetime.datetime.now(eastern)

def get_previous_sector_scores():
    """
    Get previous sector scores from authentic sector history
    Returns a dictionary of sector names to scores
    """
    authentic_history_file = os.path.join('data', 'authentic_sector_history.csv')
    previous_sector_scores = {}
    
    if os.path.exists(authentic_history_file):
        try:
            # Load the authentic sector history to get previous day's scores
            authentic_df = pd.read_csv(authentic_history_file)
            # Find the most recent date
            if not authentic_df.empty and 'Date' in authentic_df.columns:
                authentic_df['Date'] = pd.to_datetime(authentic_df['Date'])
                authentic_df = authentic_df.sort_values('Date', ascending=False)
                
                if not authentic_df.empty:
                    latest_row = authentic_df.iloc[0]
                    for sector in SECTORS:
                        if sector in latest_row:
                            previous_sector_scores[sector] = latest_row[sector]
                    print(f"Loaded previous sector scores from {authentic_history_file}")
        except Exception as e:
            print(f"Error loading authentic sector history: {e}")
    
    return previous_sector_scores

def check_api_keys():
    """Check if we have the necessary API keys for market data"""
    missing_keys = []
    
    if not FINNHUB_API_KEY or FINNHUB_API_KEY == "":
        missing_keys.append("FINNHUB_API_KEY")
    
    # Check for AlphaVantage API key in environment
    if not os.environ.get("ALPHAVANTAGE_API_KEY"):
        missing_keys.append("ALPHAVANTAGE_API_KEY")
    
    if missing_keys:
        print(f"Warning: Missing API keys: {', '.join(missing_keys)}")
        print("The system will use available sources and fallbacks")
        return False
    
    return True

def main():
    """
    Main function to collect sector values, momentum, update sector history
    and recalculate the T2D Pulse score using multi-source approach
    """
    # Check API keys but continue even if some are missing
    check_api_keys()
    
    eastern_time = get_eastern_time()
    today_date = eastern_time.strftime('%Y-%m-%d')
    print(f"Starting multi-source sector data collection at {eastern_time.strftime('%Y-%m-%d %H:%M:%S %Z')}...")
    
    # Get previous sector scores for fallback
    previous_scores = get_previous_sector_scores()
    print(f"Loaded {len(previous_scores)} previous sector scores for fallback")
    
    # Use the enhanced multi-source data collector
    success = collect_daily_sector_data()
    
    # Check if we're getting 403 errors in the collection process
    # If so, we might have hit the API rate limit, but we still want to update the sector history
    data_collection_issues = False
    if not success:
        print(f"Multi-source sector data collection had issues for {today_date}")
        # Check if we at least have some data to work with (partial success)
        if os.path.exists("data/sector_values.csv"):
            try:
                with open("data/sector_values.csv", "r") as f:
                    lines = f.readlines()
                    if len(lines) > 1 and today_date in lines[-1]:
                        print("We have partial data - will continue with history update")
                        data_collection_issues = True
                        success = True
            except Exception as e:
                print(f"Error checking sector_values.csv: {e}")
    
    if success:
        print(f"Data collection completed for {today_date}")
        
        # Now update the authentic sector history with the new data
        print("Updating authentic sector history with collected market data...")
        history_success = update_sector_history_main()
        
        if history_success:
            print("Successfully updated authentic sector history")
            
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
            print("Failed to update authentic sector history with new market data")
            # Only mark as failed if we didn't have data collection issues (partial success)
            if not data_collection_issues:
                success = False
    else:
        print(f"All data collection sources failed for {today_date}")
        print("No data available to update sector history")
        
    return success

if __name__ == "__main__":
    main()