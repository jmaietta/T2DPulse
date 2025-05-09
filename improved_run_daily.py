#!/usr/bin/env python3
# improved_run_daily.py
# -----------------------------------------------------------
# Improved daily update process with enhanced market data persistence and validation

import os
import time
import datetime
import pytz
import pandas as pd
import config
from improved_finnhub_data_collector import collect_daily_sector_data
from historical_data_manager import (
    update_historical_data,
    verify_historical_data,
    get_missing_data_by_sector,
    log_data_update
)
from notification_utils import send_market_cap_alert, send_data_integrity_alert

def get_eastern_time():
    """Get current time in US Eastern timezone"""
    eastern = pytz.timezone('US/Eastern')
    return datetime.datetime.now(eastern)

def is_market_day(date):
    """Check if a date is a weekday (not Saturday or Sunday)"""
    return date.weekday() < 5  # 0-4 are Mon-Fri

def write_authentic_pulse_score(score):
    """Write authentic T2D Pulse score to file"""
    data_dir = 'data'
    os.makedirs(data_dir, exist_ok=True)
    
    with open(os.path.join(data_dir, 'current_pulse_score.txt'), 'w') as f:
        f.write(str(score))
    print(f"Authentic T2D Pulse score {score} written to file")

def update_sector_history(sector_scores, date_str):
    """Update authentic sector history with new scores"""
    data_dir = 'data'
    os.makedirs(data_dir, exist_ok=True)
    
    # Create daily file with sector scores
    daily_file = os.path.join(data_dir, f'authentic_sector_history_{date_str}.csv')
    
    # Prepare the data for saving
    data = {'Date': date_str}
    for sector_data in sector_scores:
        data[sector_data['sector']] = sector_data['score']
    
    # Create DataFrame and save to CSV
    df = pd.DataFrame([data])
    df.to_csv(daily_file, index=False)
    print(f"Daily sector scores saved to {daily_file}")
    
    # Update the combined history file
    history_file = os.path.join(data_dir, 'authentic_sector_history.csv')
    
    if os.path.exists(history_file):
        try:
            # Load existing history
            history_df = pd.read_csv(history_file)
            
            # Check if this date already exists
            if date_str in history_df['Date'].values:
                # Update the existing entry
                history_df.loc[history_df['Date'] == date_str] = data
            else:
                # Append new entry
                history_df = pd.concat([history_df, pd.DataFrame([data])], ignore_index=True)
            
            # Sort by date (newest first)
            history_df['Date'] = pd.to_datetime(history_df['Date'])
            history_df = history_df.sort_values('Date', ascending=False)
            history_df['Date'] = history_df['Date'].dt.strftime('%Y-%m-%d')
            
            # Save updated history
            history_df.to_csv(history_file, index=False)
            print(f"Combined sector history updated in {history_file}")
        except Exception as e:
            print(f"Error updating sector history: {e}")
            # Create a new history file if there was an error
            df.to_csv(history_file, index=False)
            print(f"Created new sector history file {history_file}")
    else:
        # Create new history file if it doesn't exist
        df.to_csv(history_file, index=False)
        print(f"Created new sector history file {history_file}")

def calculate_pulse_score(sector_scores, sector_weights=None):
    """Calculate T2D Pulse score as a weighted average of sector scores"""
    if not sector_scores:
        print("No sector scores available for T2D Pulse calculation")
        return None
    
    # Default to equal weights if none provided
    if not sector_weights:
        num_sectors = len(sector_scores)
        sector_weights = {score['sector']: 100.0 / num_sectors for score in sector_scores}
    
    # Calculate weighted sum
    weighted_sum = 0.0
    total_weight = 0.0
    
    for sector_data in sector_scores:
        sector = sector_data['sector']
        score = sector_data['score']
        
        # Convert score from -1 to 1 scale to 0 to 100 scale
        score_0_100 = (score + 1) * 50
        
        # Apply weight
        weight = sector_weights.get(sector, 0)
        weighted_sum += score_0_100 * weight
        total_weight += weight
    
    # Return weighted average rounded to 1 decimal place
    if total_weight > 0:
        return round(weighted_sum / total_weight, 1)
    else:
        print("Error: Total weight is zero in T2D Pulse calculation")
        return None

def run_daily_update():
    """Run the daily update process with enhanced error handling and data validation"""
    try:
        now = get_eastern_time()
        date_str = now.strftime('%Y-%m-%d')
        print(f"=== Starting daily update process for {date_str} ===")
        
        # Only run on weekdays
        if not is_market_day(now):
            print(f"{date_str} is not a market day (weekend). Skipping update.")
            return False
        
        # Step 1: Update historical ticker price and market cap data
        print("Step 1: Updating historical ticker data...")
        price_df, marketcap_df, historical_update_success = update_historical_data()
        if not historical_update_success:
            print("Warning: Some historical ticker data could not be updated")
            # Continue anyway - we'll still try to calculate what we can
        
        # Step 2: Verify historical data integrity
        print("Step 2: Verifying historical data integrity...")
        is_valid, issues = verify_historical_data()
        if not is_valid:
            print("Warning: Historical data validation found issues:")
            for issue in issues:
                print(f"  - {issue}")
            # Continue anyway - we can still calculate with what we have
        
        # Step 3: Collect daily sector data using multi-source approach
        print("Step 3: Collecting daily sector data...")
        sector_data = collect_daily_sector_data()
        if not sector_data:
            print("Error: Failed to collect daily sector data")
            # Send alert about data collection failure
            missing_sectors = get_missing_data_by_sector()
            send_data_integrity_alert(date_str, issues, missing_sectors)
            return False
        
        # Step 4: Calculate sector sentiment scores
        print("Step 4: Calculating sector sentiment scores...")
        import sentiment_engine
        
        # Get macro indicators (use real production data)
        # This comes from our main app.py and is designed to use authentic market data
        from app import calculate_sentiment_index
        macros = calculate_sentiment_index()
        
        # Check if we have macro data
        if not macros:
            print("Error: No macro indicator data available for sentiment calculation")
            return False
        
        # Calculate sector scores
        try:
            sector_scores = sentiment_engine.score_sectors(macros, sector_data=sector_data)
            print(f"Successfully calculated scores for {len(sector_scores)} sectors")
        except Exception as e:
            print(f"Error calculating sector scores: {e}")
            return False
        
        # Step 5: Calculate T2D Pulse score
        print("Step 5: Calculating T2D Pulse score...")
        pulse_score = calculate_pulse_score(sector_scores)
        if pulse_score is None:
            print("Error: Failed to calculate T2D Pulse score")
            return False
        
        print(f"T2D Pulse score for {date_str}: {pulse_score}")
        
        # Step 6: Save authentic sector scores and T2D Pulse score
        print("Step 6: Saving authentic sector and T2D Pulse scores...")
        update_sector_history(sector_scores, date_str)
        write_authentic_pulse_score(pulse_score)
        
        # Step 7: Log completion of daily update
        log_data_update("Daily update completed", {
            "date": date_str,
            "t2d_pulse_score": pulse_score,
            "sectors_calculated": len(sector_scores)
        })
        
        print(f"=== Daily update completed successfully for {date_str} ===")
        return True
    
    except Exception as e:
        print(f"Error in daily update process: {e}")
        # Log the error
        log_data_update("Daily update failed", {"error": str(e)})
        return False

if __name__ == "__main__":
    # Run the daily update process
    success = run_daily_update()
    print(f"Daily update {'succeeded' if success else 'failed'}")