#!/usr/bin/env python3
# recalculate_pulse_score.py
# -----------------------------------------------------------
# Recalculate the T2D Pulse score based on the current sector scores

import os
import pandas as pd
import pytz
from datetime import datetime

def recalculate_pulse_score():
    """Recalculate the T2D Pulse score based on current sector scores"""
    # Get the current date in Eastern time format
    eastern = pytz.timezone('US/Eastern')
    today = datetime.now(eastern).strftime('%Y-%m-%d')
    
    # File paths
    today_specific_file = f"data/authentic_sector_history_{today}.csv"
    pulse_score_file = "data/current_pulse_score.txt"
    
    # Check if the file exists
    if not os.path.exists(today_specific_file):
        print(f"Error: {today_specific_file} not found")
        return False
    
    # Load the sector scores
    try:
        sector_df = pd.read_csv(today_specific_file)
        
        if sector_df.empty:
            print(f"Error: {today_specific_file} is empty")
            return False
        
        print(f"Loaded {today_specific_file} with {len(sector_df)} rows")
        
        # Get the row with today's data (should be the only row)
        today_row = sector_df.iloc[0]
        
        # Get all sector columns (excluding 'date')
        sector_columns = [col for col in sector_df.columns if col != 'date']
        
        if not sector_columns:
            print("Error: No sector columns found")
            return False
        
        # Calculate equal weights for each sector
        equal_weight = 100.0 / len(sector_columns)
        
        # Calculate weighted average
        weighted_sum = 0.0
        total_weight = 0.0
        
        for sector in sector_columns:
            score = today_row[sector]
            weight = equal_weight
            
            weighted_sum += score * weight
            total_weight += weight
            
            print(f"Sector: {sector}, Score: {score:.2f}, Weight: {weight:.2f}")
        
        # Calculate weighted average
        pulse_score = weighted_sum / total_weight
        
        print(f"Recalculated T2D Pulse score: {pulse_score:.1f}")
        
        # Save the new pulse score
        with open(pulse_score_file, 'w') as f:
            f.write(f"{pulse_score:.1f}")
        
        print(f"Updated {pulse_score_file} with new score: {pulse_score:.1f}")
        return True
    
    except Exception as e:
        print(f"Error recalculating pulse score: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("Recalculating T2D Pulse score...")
    success = recalculate_pulse_score()
    print(f"Recalculation {'succeeded' if success else 'failed'}")