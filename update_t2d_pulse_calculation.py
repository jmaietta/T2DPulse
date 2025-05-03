#!/usr/bin/env python3
# update_t2d_pulse_calculation.py
# --------------------------------------------------------------
# Update T2D Pulse calculation to consistently use actual sector scores

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

def update_app_calculation_code():
    """Update the app.py file to always use the authentic sector data for pulse calculation"""
    try:
        # Load the history for the most recent date
        pulse_df = pd.read_csv("data/t2d_pulse_history.csv")
        pulse_df['date'] = pd.to_datetime(pulse_df['date'])
        
        # Get the most recent data point
        most_recent = pulse_df.iloc[-1]
        date = most_recent['date'].strftime('%Y-%m-%d')
        score = most_recent['T2D Pulse Score']
        
        print(f"\nUpdating T2D Pulse dashboard to use actual score of {score} for {date}")
        
        # Create the file that the app will read for the current score
        with open("data/current_pulse_score.txt", "w") as f:
            f.write(str(score))
        
        print(f"Saved current authentic T2D Pulse score to data/current_pulse_score.txt")
        
        # Now let's make sure the app.py reads this file
        update_needed = False
        with open("app.py", "r") as f:
            app_content = f.read()
            
        if "current_pulse_score.txt" not in app_content:
            print("Need to update app.py to read the authentic current score")
            update_needed = True
        
        if update_needed:
            # Add the code to read the current_pulse_score.txt file
            update_code = """
# Function to get the authentic current T2D Pulse score from disk
def get_authentic_pulse_score():
    """Get the most recent authentic T2D Pulse score calculated from sector data"""
    try:
        with open("data/current_pulse_score.txt", "r") as f:
            return float(f.read().strip())
    except Exception as e:
        print(f"Error reading authentic pulse score: {e}")
        return None
            """
            
            # Search for the calculate_t2d_pulse_from_sectors function
            if "def calculate_t2d_pulse_from_sectors" in app_content:
                # Add our update_code before that function
                app_content = app_content.replace(
                    "def calculate_t2d_pulse_from_sectors", 
                    update_code + "def calculate_t2d_pulse_from_sectors"
                )
                
                print("Added authentic score reading function to app.py")
                
                # Now we need to modify the update_t2d_pulse_score function
                if "def update_t2d_pulse_score" in app_content:
                    # Find the start of the function
                    start_idx = app_content.find("def update_t2d_pulse_score")
                    
                    # Find the part where it calculates the score
                    score_calc_part = app_content.find("Calculated T2D Pulse Score", start_idx)
                    
                    if score_calc_part > 0:
                        # Find the end of the line
                        end_line = app_content.find("\n", score_calc_part)
                        
                        # Get the authentic score
                        auth_score = get_authentic_pulse_score()
                        if auth_score is not None:
                            authentic_code = f"\n    # Get the authentic T2D Pulse score\n    authentic_score = get_authentic_pulse_score()\n    if authentic_score is not None:\n        print(f\"Using authentic T2D Pulse score: {authentic_score}\")\n        return authentic_score\n    print(\"Authentic score not available, using calculated score\")\n"
                            
                            # Insert our code right after the docstring
                            docstring_end = app_content.find('"""", start_idx) + 4
                            app_content = app_content[:docstring_end] + authentic_code + app_content[docstring_end:]
                            
                            print("Modified update_t2d_pulse_score to use authentic score")
                        else:
                            print("Could not get authentic score, not modifying the function")
                    else:
                        print("Could not find score calculation part in update_t2d_pulse_score")
                else:
                    print("Could not find update_t2d_pulse_score function")
            else:
                print("Could not find calculate_t2d_pulse_from_sectors function")
            
            # Write the updated content back to the file
            with open("app.py", "w") as f:
                f.write(app_content)
            
            print("Updated app.py to use authentic T2D Pulse score")
        else:
            print("app.py already set up to use authentic T2D Pulse score")
        
    except Exception as e:
        print(f"Error updating app calculation code: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # Calculate the T2D Pulse scores from the sector data
    pulse_scores = calculate_pulse_scores_from_sectors()
    
    # Compare with the provided scores
    compare_with_provided_scores()
    
    # Update the app.py code to use authentic scores
    update_app_calculation_code()
