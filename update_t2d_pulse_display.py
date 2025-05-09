#!/usr/bin/env python3
# update_t2d_pulse_display.py
# ------------------------------------------------------------
# Update T2D Pulse score from authentic sector scores for dashboard display

import os
import datetime
import pandas as pd
import pytz
import subprocess

def get_eastern_date():
    """Get the current date in US Eastern Time"""
    eastern = pytz.timezone('US/Eastern')
    return datetime.datetime.now(eastern)

def write_authentic_pulse_score(score):
    """Write authentic T2D Pulse score to file"""
    data_dir = 'data'
    os.makedirs(data_dir, exist_ok=True)
    
    with open(os.path.join(data_dir, 'current_pulse_score.txt'), 'w') as f:
        f.write(str(score))
    print(f"Authentic T2D Pulse score {score} written to file")

def calculate_pulse_score_from_file(date_str=None):
    """Calculate T2D Pulse score from the most recent authentic sector scores"""
    data_dir = 'data'
    if date_str is None:
        date_str = get_eastern_date().strftime('%Y-%m-%d')
    
    file_path = os.path.join(data_dir, f'authentic_sector_history_{date_str}.csv')
    
    if not os.path.exists(file_path):
        print(f"Error: Sector history file {file_path} not found")
        return None
    
    try:
        # Load the sector scores
        df = pd.read_csv(file_path)
        if df.empty:
            print(f"Error: Empty sector history file {file_path}")
            return None
        
        # The first column should be the date, all others are sectors
        sector_columns = [col for col in df.columns if col != 'Date']
        
        # Calculate the weighted average (for now, equal weights)
        sector_scores_raw = df[sector_columns].iloc[0].tolist()
        
        # Convert from -1 to +1 scale to 0 to 100 scale
        sector_scores_0_100 = [(score + 1) * 50 for score in sector_scores_raw]
        
        # Equal weights for all sectors
        weights = [100.0 / len(sector_columns)] * len(sector_columns)
        
        # Calculate weighted average
        weighted_sum = sum(score * weight for score, weight in zip(sector_scores_0_100, weights))
        weighted_avg = weighted_sum / sum(weights)
        
        # Round to 1 decimal place
        pulse_score = round(weighted_avg, 1)
        
        print(f"Calculated T2D Pulse score for {date_str}: {pulse_score}")
        return pulse_score
    
    except Exception as e:
        print(f"Error calculating Pulse score: {e}")
        return None

def update_pulse_display():
    """Update the T2D Pulse score display and export sector history"""
    date_str = get_eastern_date().strftime('%Y-%m-%d')
    print(f"Updating T2D Pulse display for {date_str}...")
    
    # Calculate the T2D Pulse score
    pulse_score = calculate_pulse_score_from_file(date_str)
    if pulse_score is None:
        print("Error: Failed to calculate T2D Pulse score")
        return False
    
    # Save the authentic T2D Pulse score
    write_authentic_pulse_score(pulse_score)
    
    # Export the sector history for dashboard download
    try:
        subprocess.check_call(['python', 'export_updated_sector_history.py', date_str])
        print(f"Successfully exported sector history for {date_str}")
    except subprocess.CalledProcessError as e:
        print(f"Error exporting sector history: {e}")
        return False
    
    print(f"T2D Pulse display updated successfully for {date_str}")
    return True

if __name__ == '__main__':
    success = update_pulse_display()
    if success:
        print("\nSUCCESS: T2D Pulse display updated with today's authentic data")
    else:
        print("\nWARNING: There were errors updating the T2D Pulse display")