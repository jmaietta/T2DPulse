#!/usr/bin/env python3
# t2d_pulse_history.py
# -----------------------------------------------------------
# Manages historical T2D Pulse scores and provides data for the 30-day chart

import os
import pandas as pd
import pytz
from datetime import datetime, timedelta

def save_t2d_pulse_score(score, sector_scores=None):
    """
    Save T2D Pulse score to historical data file
    
    Args:
        score (float): T2D Pulse score to save
        sector_scores (dict, optional): Dictionary of sector scores used for this calculation
    
    Returns:
        bool: True if successful, False otherwise
    """
    # File path for historical T2D Pulse data
    csv_path = "data/t2d_pulse_history.csv"
    
    # Get current date in Eastern time
    eastern = pytz.timezone('US/Eastern')
    today = datetime.now(eastern)
    date_str = today.strftime('%Y-%m-%d')
    
    # Check if it's a weekend (Saturday=5, Sunday=6)
    is_weekend = today.weekday() >= 5
    
    # Create the data directory if it doesn't exist
    os.makedirs("data", exist_ok=True)
    
    try:
        if os.path.exists(csv_path):
            # Read existing data
            df = pd.read_csv(csv_path)
            
            # Check if we already have an entry for today
            if date_str in df['date'].values:
                # Update the existing entry
                df.loc[df['date'] == date_str, 'pulse_score'] = score
            else:
                # Add a new row for today
                new_row = pd.DataFrame({'date': [date_str], 'pulse_score': [score]})
                df = pd.concat([new_row, df], ignore_index=True)
        else:
            # Create a new DataFrame
            df = pd.DataFrame({'date': [date_str], 'pulse_score': [score]})
        
        # Sort by date descending (newest first)
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date', ascending=False).reset_index(drop=True)
        
        # Convert date back to string for CSV storage
        df['date'] = df['date'].dt.strftime('%Y-%m-%d')
        
        # Save to CSV
        df.to_csv(csv_path, index=False)
        
        # If weekend, also save to date-specific file
        if is_weekend:
            date_specific_file = f"data/t2d_pulse_score_{date_str}.csv"
            df_today = pd.DataFrame({'date': [date_str], 'pulse_score': [score]})
            df_today.to_csv(date_specific_file, index=False)
            
            # If sector scores were provided, save them too
            if sector_scores:
                sector_file = f"data/t2d_pulse_sectors_{date_str}.json"
                pd.DataFrame([sector_scores]).to_json(sector_file, orient='records')
        
        print(f"Successfully saved T2D Pulse score {score} for {date_str}")
        return True
    
    except Exception as e:
        print(f"Error saving T2D Pulse history: {e}")
        return False

def get_t2d_pulse_history(days=30):
    """
    Get T2D Pulse score history for the specified number of days
    
    Args:
        days (int): Number of days of history to retrieve
        
    Returns:
        DataFrame: DataFrame with date and pulse_score columns
    """
    csv_path = "data/t2d_pulse_history.csv"
    
    if not os.path.exists(csv_path):
        print(f"Warning: T2D Pulse history file not found at {csv_path}")
        return pd.DataFrame(columns=['date', 'pulse_score'])
    
    try:
        # Read the CSV file
        df = pd.read_csv(csv_path)
        
        # Convert date to datetime
        df['date'] = pd.to_datetime(df['date'])
        
        # Calculate cutoff date
        today = datetime.now()
        cutoff_date = today - timedelta(days=days)
        
        # Filter for the specified days and sort
        df = df[df['date'] >= cutoff_date]
        df = df.sort_values('date')
        
        # Filter out weekends if requested (Saturday=5, Sunday=6)
        # df = df[df['date'].dt.dayofweek < 5]
        
        return df
    
    except Exception as e:
        print(f"Error getting T2D Pulse history: {e}")
        return pd.DataFrame(columns=['date', 'pulse_score'])

def get_most_recent_t2d_pulse_score():
    """
    Get the most recent T2D Pulse score
    
    Returns:
        float: Most recent T2D Pulse score, or None if not available
    """
    try:
        history = get_t2d_pulse_history(days=30)
        if not history.empty:
            # The most recent score will be the last row
            return history.iloc[-1]['pulse_score']
    except Exception as e:
        print(f"Error getting most recent T2D Pulse score: {e}")
    
    # If we get here, return None
    return None
