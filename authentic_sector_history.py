#!/usr/bin/env python3
# authentic_sector_history.py
# -----------------------------------------------------------
# Integrates authentic historical sector sentiment data with the dashboard

import os
import json
import pandas as pd
from datetime import datetime

def get_authentic_sector_history(sector_name=None):
    """
    Get authentic historical sector sentiment scores
    
    Args:
        sector_name (str, optional): Name of the sector to get history for
        
    Returns:
        dict or DataFrame: If sector_name is None, returns a dictionary with
        sector names as keys and DataFrames as values. If sector_name is provided,
        returns a DataFrame for just that sector.
    """
    csv_path = "data/authentic_sector_history.csv"
    
    if not os.path.exists(csv_path):
        print(f"Warning: Authentic sector history file not found at {csv_path}")
        return {} if sector_name is None else None
        
    try:
        # Read the CSV file
        df = pd.read_csv(csv_path)
        
        # Convert date to datetime
        df['date'] = pd.to_datetime(df['date'])
        
        if sector_name is None:
            # Dictionary to store results for all sectors
            sector_history = {}
            
            # Get all column names except 'date'
            sector_columns = [col for col in df.columns if col != 'date']
            
            # Create a DataFrame for each sector
            for sector in sector_columns:
                sector_df = df[['date', sector]].copy()
                sector_df.columns = ['date', 'value']  # Rename for consistency
                sector_df = sector_df.set_index('date')
                sector_history[sector] = sector_df
                
            return sector_history
            
        else:
            # Return DataFrame for just the requested sector
            if sector_name not in df.columns:
                print(f"Warning: Sector '{sector_name}' not found in authentic history")
                return None
                
            sector_df = df[['date', sector_name]].copy()
            sector_df.columns = ['date', 'value']  # Rename for consistency
            sector_df = sector_df.set_index('date')
            
            return sector_df
            
    except Exception as e:
        print(f"Error loading authentic sector history: {e}")
        return {} if sector_name is None else None

def update_authentic_history(sector_scores=None, force_update=False):
    """
    Update the authentic sector history with new scores or ensure it's loaded
    
    Args:
        sector_scores (list, optional): List of sector dictionaries with 'sector' and 'score' keys
        force_update (bool, optional): Whether to force update even if scores are None
        
    Returns:
        bool: True if successful, False otherwise
    """
    if sector_scores:
        # If we have new scores, save them
        return save_authentic_sector_history(sector_scores)
    elif force_update:
        # Load the history if needed, but don't change it
        history = get_authentic_sector_history()
        if history:
            print(f"Loaded authentic sector history for {len(history)} sectors")
        else:
            print("No authentic sector history found")
        return True
    return False

def save_authentic_sector_history(sector_scores):
    """
    Save sector scores to authentic history file
    
    Args:
        sector_scores (list): List of sector dictionaries with 'sector' and 'score' keys
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Load existing data
        csv_path = "data/authentic_sector_history.csv"
        
        if os.path.exists(csv_path):
            df = pd.read_csv(csv_path)
            df['date'] = pd.to_datetime(df['date'])
        else:
            # Create new DataFrame
            df = pd.DataFrame(columns=['date'])
        
        # Today's date
        today = datetime.now().strftime('%Y-%m-%d')
        
        # Check if we already have an entry for today
        if today in df['date'].dt.strftime('%Y-%m-%d').values:
            # Update existing row
            idx = df[df['date'].dt.strftime('%Y-%m-%d') == today].index[0]
        else:
            # Add new row
            df = df.append({'date': today}, ignore_index=True)
            idx = len(df) - 1
        
        # Update values
        for sector_data in sector_scores:
            sector_name = sector_data['sector']
            
            # Convert raw score from [-1,1] to [0-100] for display
            raw_score = sector_data['score']
            normalized_score = ((raw_score + 1.0) / 2.0) * 100
            
            # Add column if it doesn't exist
            if sector_name not in df.columns:
                df[sector_name] = None
            
            # Update value
            df.at[idx, sector_name] = normalized_score
        
        # Save to CSV
        df.to_csv(csv_path, index=False)
        
        # Also export as JSON for easier access
        json_path = "data/authentic_sector_history.json"
        
        # Convert to dictionary
        history_dict = {}
        for _, row in df.iterrows():
            date_str = row['date'].strftime('%Y-%m-%d')
            history_dict[date_str] = {sector: row[sector] for sector in df.columns if sector != 'date'}
        
        # Save to JSON
        with open(json_path, 'w') as f:
            json.dump(history_dict, f, indent=2)
        
        # Export today's data to date-specific CSV for direct download
        today_csv_path = f"data/authentic_sector_history_{today}.csv"
        df.to_csv(today_csv_path, index=False)
        
        print(f"Saved authentic sector history to {json_path}")
        print(f"Exported authentic sector history to {today_csv_path}")
        
        return True
        
    except Exception as e:
        print(f"Error saving authentic sector history: {e}")
        return False