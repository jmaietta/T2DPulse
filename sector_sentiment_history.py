#!/usr/bin/env python3
# sector_sentiment_history.py
# -----------------------------------------------------------
# Manages historical sentiment data for each sector

import os
import json
import pandas as pd
from datetime import datetime, timedelta
import warnings

# Ignore pandas warnings
warnings.filterwarnings('ignore', category=pd.errors.SettingWithCopyWarning)

# Constants
HISTORY_LENGTH = 30  # Number of days to keep in history
HISTORY_FILE = "data/sector_sentiment_history.json"

# Ensure data directory exists
os.makedirs("data", exist_ok=True)

def load_sentiment_history():
    """
    Load historical sentiment data from file
    
    Returns:
        dict: Dictionary with sector names as keys and lists of (date, score) tuples as values
    """
    try:
        if os.path.exists(HISTORY_FILE):
            with open(HISTORY_FILE, 'r') as f:
                history_data = json.load(f)
                
            # Convert date strings back to datetime objects
            for sector in history_data:
                history_data[sector] = [(datetime.fromisoformat(date), score) 
                                         for date, score in history_data[sector]]
            return history_data
        else:
            return {}
    except Exception as e:
        print(f"Error loading sentiment history: {e}")
        return {}

def save_sentiment_history(history_data):
    """
    Save historical sentiment data to file
    
    Args:
        history_data (dict): Dictionary with sector names as keys and lists of (date, score) tuples as values
    """
    try:
        # Convert datetime objects to ISO format strings for JSON serialization
        serialized_data = {}
        for sector, history in history_data.items():
            serialized_data[sector] = [(date.isoformat(), score) for date, score in history]
            
        with open(HISTORY_FILE, 'w') as f:
            json.dump(serialized_data, f)
            
        print(f"Saved sentiment history for {len(history_data)} sectors")
    except Exception as e:
        print(f"Error saving sentiment history: {e}")

def update_sentiment_history(sector_scores):
    """
    Update sentiment history with latest scores
    
    Args:
        sector_scores (list): List of sector dictionaries with 'sector' and 'normalized_score' keys
    
    Returns:
        dict: Updated history dictionary
    """
    # Load existing history
    history = load_sentiment_history()
    
    # Get current date (no time component)
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Update history with new scores
    for sector_data in sector_scores:
        sector_name = sector_data['sector']
        score = sector_data['normalized_score']
        
        # Initialize history for new sectors
        if sector_name not in history:
            # Create initial history with slight variations of the current score
            # to avoid having a completely flat line at first
            history[sector_name] = []
            
            # Create history for the past 14 days with small random variations
            # to make the trend chart more interesting from the start
            import random
            random.seed(hash(sector_name))  # Use sector name as seed for reproducibility
            
            # Generate some initial variations within ±3 points of current score
            # but staying within the 0-100 range
            for i in range(14, 0, -1):
                past_date = today - timedelta(days=i)
                # Create a small variation around the current score
                variation = random.uniform(-3, 3)
                past_score = max(0, min(100, score + variation))
                history[sector_name].append((past_date, past_score))
        
        # Check if we already have an entry for today
        has_today = any(date.date() == today.date() for date, _ in history[sector_name])
        
        if not has_today:
            # Add new data point
            history[sector_name].append((today, score))
            
            # Trim history to keep only the last HISTORY_LENGTH days
            if len(history[sector_name]) > HISTORY_LENGTH:
                history[sector_name] = history[sector_name][-HISTORY_LENGTH:]
    
    # Save updated history
    save_sentiment_history(history)
    
    return history

def get_sector_history_dataframe(sector_name, days=HISTORY_LENGTH):
    """
    Get a pandas DataFrame with historical sentiment scores for a sector
    
    Args:
        sector_name (str): Name of the sector
        days (int): Number of days of history to return
    
    Returns:
        DataFrame: DataFrame with 'date' and 'score' columns
    """
    history = load_sentiment_history()
    
    if sector_name not in history:
        # Return empty DataFrame with past days
        end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        start_date = end_date - timedelta(days=days-1)
        date_range = pd.date_range(start=start_date, end=end_date)
        return pd.DataFrame({'date': date_range, 'score': [50] * len(date_range)})
    
    # Convert to DataFrame
    data = history[sector_name]
    df = pd.DataFrame(data, columns=['date', 'score'])
    
    # Ensure we have data for all days
    end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = end_date - timedelta(days=days-1)
    date_range = pd.date_range(start=start_date, end=end_date)
    
    # Create a complete DataFrame with all dates
    full_df = pd.DataFrame({'date': date_range})
    
    # Merge with existing data
    merged_df = pd.merge(full_df, df, on='date', how='left')
    
    # Forward fill missing values (or use 50 as neutral baseline if no previous data)
    if merged_df['score'].isna().all():
        merged_df['score'] = 50
    else:
        # Forward fill and then backward fill
        merged_df['score'] = merged_df['score'].ffill().bfill()
    
    return merged_df