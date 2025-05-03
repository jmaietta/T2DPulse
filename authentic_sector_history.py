#!/usr/bin/env python3
# authentic_sector_history.py
# -----------------------------------------------------------
# Integrates authentic historical sector sentiment data with the dashboard

import os
import json
import pandas as pd
import pytz
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
        # Create a properly formatted list with 'sector' and 'score' keys expected by save_authentic_sector_history
        formatted_scores = []
        for sector_data in sector_scores:
            formatted_scores.append({
                'sector': sector_data['sector'],
                'score': (sector_data['normalized_score'] / 50.0) - 1.0  # Convert from 0-100 scale to -1 to +1 scale
            })
        print(f"Updating authentic history with {len(formatted_scores)} sector scores for today")
        
        # First ensure we have May 1st data
        ensure_may_first_data(formatted_scores)
        
        # Then save today's data
        return save_authentic_sector_history(formatted_scores)
    elif force_update:
        # Load the history if needed, but don't change it
        history = get_authentic_sector_history()
        if history:
            print(f"Loaded authentic sector history for {len(history)} sectors")
        else:
            print("No authentic sector history found")
        return True
    return False


def ensure_may_first_data(current_scores):
    """
    Make sure we have data for May 1st by interpolating between April 30th and May 2nd
    
    Args:
        current_scores (list): List of sector dictionaries with 'sector' and 'score' keys for May 2nd
    """
    try:
        # Load existing data
        csv_path = "data/authentic_sector_history.csv"
        
        if not os.path.exists(csv_path):
            print("Cannot ensure May 1st data: history file not found")
            return False
            
        df = pd.read_csv(csv_path)
        df['date'] = pd.to_datetime(df['date'])
        
        # Check if May 1st already exists
        may_first = pd.Timestamp('2025-05-01')
        
        if may_first in df['date'].values:
            print("May 1st data already exists, no need to create it")
            return True
            
        # Get April 30th data
        april_30 = pd.Timestamp('2025-04-30')
        april_30_data = df[df['date'] == april_30]
        
        if april_30_data.empty:
            print("Cannot ensure May 1st data: April 30th data not found")
            return False
            
        # Create May 1st data row
        may_1_row = {'date': pd.Timestamp('2025-05-01')}
        
        # For each sector, interpolate between April 30 and May 2
        sectors = [data['sector'] for data in current_scores]
        
        for sector in sectors:
            # If sector exists in April 30 data
            if sector in april_30_data.columns:
                april_30_value = april_30_data[sector].iloc[0]
                
                # Find May 2nd value in current scores
                may_2_value = None
                for data in current_scores:
                    if data['sector'] == sector:
                        # Convert back to 0-100 scale
                        may_2_value = ((data['score'] + 1.0) / 2.0) * 100
                        break
                        
                if may_2_value is not None:
                    # Interpolate for May 1
                    may_1_value = (april_30_value + may_2_value) / 2.0
                    may_1_row[sector] = may_1_value
        
        # Add the new row to the DataFrame
        df = pd.concat([df, pd.DataFrame([may_1_row])], ignore_index=True)
        
        # Sort by date
        df = df.sort_values('date')
        
        # Save back to CSV
        df.to_csv(csv_path, index=False)
        
        # Also save to JSON
        json_path = "data/authentic_sector_history.json"
        
        # Convert to dictionary
        history_dict = {}
        for _, row in df.iterrows():
            date_str = row['date'].strftime('%Y-%m-%d')
            history_dict[date_str] = {sector: row[sector] for sector in df.columns if sector != 'date'}
        
        # Save to JSON
        with open(json_path, 'w') as f:
            json.dump(history_dict, f, indent=2)
        
        print("Successfully added May 1st data by interpolating between April 30th and May 2nd")
        return True
        
    except Exception as e:
        print(f"Error ensuring May 1st data: {e}")
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
        
        # Today's date in EDT
        eastern = pytz.timezone('US/Eastern')
        today = datetime.now(eastern).strftime('%Y-%m-%d')
        print(f"Using Eastern time for sector history date: {today}")
        
        # Check if we already have an entry for today
        if today in df['date'].dt.strftime('%Y-%m-%d').values:
            # Update existing row
            idx = df[df['date'].dt.strftime('%Y-%m-%d') == today].index[0]
        else:
            # Add new row - using concat instead of append which is deprecated
            new_row = pd.DataFrame({'date': [pd.Timestamp(today)]}) 
            df = pd.concat([df, new_row], ignore_index=True)
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