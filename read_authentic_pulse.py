#!/usr/bin/env python3
"""
Helper module to read the authentic T2D Pulse score from market cap data.
"""

import pandas as pd
import numpy as np
import os
from datetime import datetime
import pytz

def read_pulse_score(filename="data/authentic_pulse_history.csv"):
    """
    Read the authentic T2D Pulse score from the CSV file.
    
    Args:
        filename (str): Path to the authentic pulse history CSV file
        
    Returns:
        float: The most recent T2D Pulse score from the data
    """
    try:
        if not os.path.exists(filename):
            print(f"Warning: Authentic pulse history file '{filename}' not found")
            return 50.0  # Default score
            
        df = pd.read_csv(filename)
        
        # Check if empty
        if df.empty:
            print(f"Warning: Authentic pulse history file '{filename}' is empty")
            return 50.0  # Default score
            
        # Convert date to datetime if needed
        if 'date' in df.columns and not pd.api.types.is_datetime64_any_dtype(df['date']):
            df['date'] = pd.to_datetime(df['date'])
            
        # Sort by date and get most recent score
        df = df.sort_values('date', ascending=False)
        
        # Get the latest score
        latest_score = df['score'].iloc[0]
        
        # Sanity check
        if not np.isfinite(latest_score):
            print(f"Warning: Latest score is not a valid number: {latest_score}")
            return 50.0  # Default score
            
        return float(latest_score)
        
    except Exception as e:
        print(f"Error reading authentic pulse score: {e}")
        return 50.0  # Default score

def calculate_pulse_from_sectors(sector_data):
    """
    Calculate the T2D Pulse score from sector data.
    
    Args:
        sector_data (DataFrame): Dataframe with sector scores
        
    Returns:
        float: The calculated T2D Pulse score
    """
    try:
        # If no data, return default
        if sector_data is None or len(sector_data) == 0:
            return 50.0
            
        # Calculate average of all sector scores
        # Exclude date column if present
        score_columns = [col for col in sector_data.columns if col != 'date']
        
        # If no score columns, return default
        if not score_columns:
            return 50.0
            
        # Get the latest row of data
        latest_data = sector_data.iloc[-1]
        
        # Calculate the average of all sector scores
        sector_values = [latest_data[col] for col in score_columns if pd.notna(latest_data[col])]
        
        # If no valid scores, return default
        if not sector_values:
            return 50.0
            
        # Calculate average
        average_score = sum(sector_values) / len(sector_values)
        
        return float(average_score)
        
    except Exception as e:
        print(f"Error calculating pulse from sectors: {e}")
        return 50.0  # Default score

def save_pulse_score(score, date=None):
    """
    Save the T2D Pulse score to the authentic pulse history file.
    
    Args:
        score (float): The T2D Pulse score to save
        date (datetime, optional): The date for the score, defaults to today
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        filename = "data/authentic_pulse_history.csv"
        
        # Get today's date in Eastern time if not provided
        if date is None:
            eastern = pytz.timezone('US/Eastern')
            date = datetime.now(eastern)
            
        date_str = date.strftime('%Y-%m-%d')
        
        # Create or load the dataframe
        if os.path.exists(filename):
            df = pd.read_csv(filename)
            
            # Convert date to datetime
            df['date'] = pd.to_datetime(df['date'])
        else:
            # Create new dataframe
            df = pd.DataFrame(columns=['date', 'score'])
            
        # Check if we already have an entry for this date
        if date_str in df['date'].dt.strftime('%Y-%m-%d').values:
            # Update existing row
            idx = df[df['date'].dt.strftime('%Y-%m-%d') == date_str].index[0]
            df.at[idx, 'score'] = score
        else:
            # Add new row
            new_row = pd.DataFrame({'date': [pd.Timestamp(date_str)], 'score': [score]})
            df = pd.concat([df, new_row], ignore_index=True)
            
        # Sort by date
        df = df.sort_values('date')
        
        # Save to CSV
        df.to_csv(filename, index=False)
        
        print(f"Saved authentic pulse score {score} for {date_str}")
        return True
        
    except Exception as e:
        print(f"Error saving authentic pulse score: {e}")
        return False

def create_pulse_from_sector_history():
    """
    Create the authentic pulse history from the sector history file.
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        sector_file = "data/authentic_sector_history.csv"
        
        if not os.path.exists(sector_file):
            print(f"Warning: Sector history file '{sector_file}' not found")
            return False
            
        # Load sector data
        sector_df = pd.read_csv(sector_file)
        
        # Convert date to datetime
        sector_df['date'] = pd.to_datetime(sector_df['date'])
        
        # Create a new dataframe for pulse history
        pulse_df = pd.DataFrame(columns=['date', 'score'])
        
        # Calculate pulse score for each date
        for _, row in sector_df.iterrows():
            date = row['date']
            
            # Get all sector scores for this date
            sector_scores = {col: row[col] for col in sector_df.columns if col != 'date'}
            
            # Calculate average score
            valid_scores = [score for score in sector_scores.values() if pd.notna(score)]
            
            if valid_scores:
                avg_score = sum(valid_scores) / len(valid_scores)
                
                # Add to pulse dataframe
                new_row = pd.DataFrame({'date': [date], 'score': [avg_score]})
                pulse_df = pd.concat([pulse_df, new_row], ignore_index=True)
        
        # Sort by date
        pulse_df = pulse_df.sort_values('date')
        
        # Save to CSV
        pulse_df.to_csv("data/authentic_pulse_history.csv", index=False)
        
        print(f"Created authentic pulse history with {len(pulse_df)} entries")
        return True
        
    except Exception as e:
        print(f"Error creating pulse from sector history: {e}")
        return False

if __name__ == "__main__":
    # First create the pulse history from sector data
    success = create_pulse_from_sector_history()
    
    if success:
        # Read the latest pulse score
        score = read_pulse_score()
        print(f"Latest T2D Pulse score: {score:.2f}")
    else:
        print("Failed to create authentic pulse history")