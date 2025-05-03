#!/usr/bin/env python3
# calculate_authentic_pulse.py
# --------------------------------------------------------------
# Calculate authentic T2D Pulse scores directly from sector data

import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta

# Path to sector scores file
SECTOR_HISTORY_PATH = "data/authentic_sector_history.csv"

def calculate_pulse_scores_from_sectors():
    """Calculate T2D Pulse scores as the average of all sector scores for each day"""
    print("\nCalculating T2D Pulse scores from authentic sector data...")
    
    try:
        # Load the sector history
        sector_df = pd.read_csv(SECTOR_HISTORY_PATH)
        
        # Convert date column to datetime
        sector_df['date'] = pd.to_datetime(sector_df['date'])
        
        # Create a dataframe for the pulse scores
        pulse_scores = []
        
        # For each date, calculate the average score
        for date, data in sector_df.groupby('date'):
            # Get all sector scores for this date (all columns except the date)
            sector_scores = data.iloc[0, 1:].values
            
            # Calculate the average of all sector scores
            pulse_score = np.mean(sector_scores)
            
            pulse_scores.append({
                'date': date,
                'T2D Pulse Score': round(pulse_score, 1)
            })
        
        # Create a DataFrame from the pulse scores
        pulse_df = pd.DataFrame(pulse_scores)
        
        # Sort by date
        pulse_df = pulse_df.sort_values('date')
        
        # Save to CSV
        pulse_df.to_csv("data/t2d_pulse_history.csv", index=False)
        print(f"Saved T2D Pulse scores to data/t2d_pulse_history.csv")
        
        # Print the latest scores
        latest_dates = pulse_df.tail(3)
        print("\nLatest T2D Pulse Scores:")
        for _, row in latest_dates.iterrows():
            print(f"{row['date'].strftime('%Y-%m-%d')}: {row['T2D Pulse Score']}")
        
        return pulse_df
    
    except Exception as e:
        print(f"Error calculating pulse scores: {e}")
        import traceback
        traceback.print_exc()
        return None

def compare_with_provided_scores():
    """Compare our calculated scores with the provided scores"""
    try:
        # Check if the provided file exists
        provided_path = "attached_assets/T2D Pulse Scores JM (05032025).csv"
        if not os.path.exists(provided_path):
            print(f"Provided scores file not found: {provided_path}")
            return
        
        # Load the provided scores
        provided_df = pd.read_csv(provided_path)
        provided_df['date'] = pd.to_datetime(provided_df['date'])
        
        # Load our calculated scores
        our_df = pd.read_csv("data/t2d_pulse_history.csv")
        our_df['date'] = pd.to_datetime(our_df['date'])
        
        # Join the two dataframes
        comparison = pd.merge(our_df, provided_df, on='date', how='inner', suffixes=('_calc', '_provided'))
        
        # Print comparison for the last few days
        print("\nComparison with provided scores:")
        last_days = comparison.tail(5)
        for _, row in last_days.iterrows():
            calc = row['T2D Pulse Score_calc']
            prov = row['T2D Pulse Score_provided'] if not pd.isna(row['T2D Pulse Score_provided']) else "N/A"
            date = row['date'].strftime('%Y-%m-%d')
            diff = calc - prov if not pd.isna(row['T2D Pulse Score_provided']) else "N/A"
            print(f"{date}: Calculated={calc}, Provided={prov}, Difference={diff}")
        
    except Exception as e:
        print(f"Error comparing with provided scores: {e}")
        import traceback
        traceback.print_exc()

def save_authentic_current_score():
    """Save the most recent authentic T2D Pulse score to a file"""
    try:
        # Load the pulse scores
        pulse_df = pd.read_csv("data/t2d_pulse_history.csv")
        pulse_df['date'] = pd.to_datetime(pulse_df['date'])
        
        # Get the latest score
        latest_score = pulse_df.iloc[-1]['T2D Pulse Score']
        latest_date = pulse_df.iloc[-1]['date'].strftime('%Y-%m-%d')
        
        # Save to file
        with open("data/current_pulse_score.txt", "w") as f:
            f.write(str(latest_score))
        
        print(f"\nSaved current authentic T2D Pulse score ({latest_score}) for {latest_date} to data/current_pulse_score.txt")
        
        return latest_score
        
    except Exception as e:
        print(f"Error saving authentic current score: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    # Calculate authentic T2D Pulse scores from sector data
    pulse_df = calculate_pulse_scores_from_sectors()
    
    # Compare with provided scores
    compare_with_provided_scores()
    
    # Save the current score
    save_authentic_current_score()
    
    print("\nTo use this authentic score in the dashboard, add the following code to app.py:")
    print("# At the top of the file, add this function:")
    print("def get_authentic_pulse_score():")
    print("    \"\"\"Get the most recent authentic T2D Pulse score calculated from sector data\"\"\"")
    print("    try:")
    print("        with open(\"data/current_pulse_score.txt\", \"r\") as f:")
    print("            return float(f.read().strip())")
    print("    except Exception as e:")
    print("        print(f\"Error reading authentic pulse score: {e}\")")
    print("        return None")
    print("")
    print("# Then modify the beginning of the update_t2d_pulse_score function to use this score:")
    print("def update_t2d_pulse_score(weights_json):")
    print("    \"\"\"Update the T2D Pulse score when weights change\"\"\"")
    print("    # Use the authentic score first if available")
    print("    authentic_score = get_authentic_pulse_score()")
    print("    if authentic_score is not None:")
    print("        print(f\"Using authentic T2D Pulse score: {authentic_score}\")")
    print("        return authentic_score")
    print("    print(\"Authentic score not available, using calculated score\")")
    print("    # ... rest of the original function")
    
    print("\nOr you can restart the dashboard to see the changes immediately.")
