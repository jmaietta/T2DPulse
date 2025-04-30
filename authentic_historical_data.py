#!/usr/bin/env python3
# authentic_historical_data.py
# -----------------------------------------------------------
# Calculate authentic historical sector sentiment scores using real data
# for business days only (excluding weekends)

import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
from pandas.tseries.offsets import BDay
import time

# Import sentiment engine components
from sentiment_engine import SECTORS, IMPACT, IMPORTANCE, score_sectors
from app import calculate_t2d_pulse_from_sectors

# Constants
HISTORY_LENGTH = 20  # Number of business days to keep in history
HISTORY_FILE = "data/authentic_sector_history.json"
T2D_PULSE_HISTORY_FILE = "data/authentic_t2d_pulse_history.json"

# Ensure data directory exists
os.makedirs("data", exist_ok=True)

def get_business_days(days_back=HISTORY_LENGTH):
    """
    Get a list of business days (excluding weekends) going back from today
    
    Args:
        days_back (int): Number of business days to go back
    
    Returns:
        list: List of business day dates as datetime objects
    """
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    business_days = []
    
    # Get business days using pandas BDay offset
    for i in range(days_back):
        business_day = today - BDay(i)
        business_days.append(business_day)
    
    return business_days

def fetch_indicator_value_from_file(indicator, date_to_fetch):
    """
    Get indicator value from local CSV files for a specific date
    
    Args:
        indicator (str): The indicator name
        date_to_fetch (datetime): The date to get data for
    
    Returns:
        float: The indicator value, or None if not available
    """
    # Map indicators to CSV files
    file_mapping = {
        "10Y_Treasury_Yield_%": "treasury_yield_data.csv",
        "VIX": "vix_data.csv",
        "NASDAQ_20d_gap_%": "nasdaq_data.csv",
        "Fed_Funds_Rate_%": "interest_rate_data.csv",
        "CPI_YoY_%": "inflation_data.csv",
        "PCEPI_YoY_%": "pcepi_data.csv",
        "Real_GDP_Growth_%_SAAR": "gdp_data.csv",
        "Real_PCE_YoY_%": "pce_data.csv",
        "Unemployment_%": "unemployment_data.csv",
        "Software_Dev_Job_Postings_YoY_%": "job_postings_data.csv",
        "PPI_Data_Processing_YoY_%": "data_processing_ppi_data.csv",
        "PPI_Software_Publishers_YoY_%": "software_ppi_data.csv",
        "Consumer_Sentiment": "consumer_sentiment_data.csv"
    }
    
    # Map indicators to value column names in CSV
    value_column_mapping = {
        "NASDAQ_20d_gap_%": "gap_pct",
        "CPI_YoY_%": "inflation",
        "PCEPI_YoY_%": "yoy_growth",
        "Software_Dev_Job_Postings_YoY_%": "yoy_growth",
        "PPI_Data_Processing_YoY_%": "yoy_pct_change",
        "PPI_Software_Publishers_YoY_%": "yoy_pct_change"
    }
    
    # Get the file path for this indicator
    file_name = file_mapping.get(indicator)
    if not file_name:
        print(f"No file mapping for indicator: {indicator}")
        return None
    
    file_path = f"data/{file_name}"
    if not os.path.exists(file_path):
        print(f"Data file not found: {file_path}")
        return None
    
    try:
        # Load the CSV file
        df = pd.read_csv(file_path)
        if df.empty:
            print(f"Empty data file for indicator: {indicator}")
            return None
        
        # Ensure date column exists and is in datetime format
        if 'date' not in df.columns:
            print(f"Date column missing in {file_path}")
            return None
        
        df['date'] = pd.to_datetime(df['date'])
        
        # Get the closest date on or before the target date
        df = df[df['date'] <= date_to_fetch].sort_values('date', ascending=False)
        if df.empty:
            print(f"No data available for {indicator} on or before {date_to_fetch.strftime('%Y-%m-%d')}")
            return None
        
        # Get the value from the appropriate column
        value_column = value_column_mapping.get(indicator, 'value')
        if value_column in df.columns:
            return df.iloc[0][value_column]
        else:
            print(f"Value column '{value_column}' not found in {file_path}")
            return None
    
    except Exception as e:
        print(f"Error reading data for {indicator}: {e}")
        return None

def calculate_historical_sector_scores():
    """
    Calculate authentic historical sector sentiment scores for business days only
    
    Returns:
        dict: Dictionary with dates as keys and lists of sector scores as values
        dict: Dictionary with dates as keys and T2D Pulse scores as values
    """
    print(f"Calculating authentic historical scores for past {HISTORY_LENGTH} business days...")
    
    # Get business days going back from today (excluding weekends)
    business_days = get_business_days(days_back=HISTORY_LENGTH)
    
    # Initialize results dictionaries
    historical_sector_scores = {}
    historical_t2d_pulse_scores = {}
    
    # Process each business day
    for business_day in business_days:
        date_str = business_day.strftime('%Y-%m-%d')
        print(f"\nCalculating scores for {date_str}:")
        
        # Get indicator values for this date from our CSV files
        indicator_values = {}
        for indicator in IMPACT.keys():
            value = fetch_indicator_value_from_file(indicator, business_day)
            if value is not None:
                indicator_values[indicator] = value
                print(f"  {indicator}: {value}")
        
        # Skip dates with insufficient data
        if len(indicator_values) < 5:  # Require at least 5 indicators
            print(f"Insufficient data for {date_str}, skipping (only {len(indicator_values)} indicators)")
            continue
        
        # Calculate sector scores for this date
        try:
            # Use the score_sectors function from sentiment_engine
            sector_scores = score_sectors(indicator_values)
            
            # Store the authentic scores for this date
            historical_sector_scores[business_day] = sector_scores
            
            # Calculate and store T2D Pulse score
            t2d_pulse_score = calculate_t2d_pulse_from_sectors(sector_scores)
            historical_t2d_pulse_scores[business_day] = t2d_pulse_score
            
            print(f"Successfully calculated scores for {date_str}")
            print(f"T2D Pulse score: {t2d_pulse_score:.1f}")
            
            # Log a few sector scores for verification
            if len(sector_scores) > 0:
                sample_scores = [f"{sector_scores[i]['sector']}: {sector_scores[i]['score']:.1f}" 
                                for i in range(min(3, len(sector_scores)))]
                print(f"Sample sector scores: {', '.join(sample_scores)}")
            
        except Exception as e:
            print(f"Error calculating scores for {date_str}: {e}")
    
    return historical_sector_scores, historical_t2d_pulse_scores

def save_sector_history(sector_scores):
    """
    Save authentic sector sentiment history to file
    
    Args:
        sector_scores (dict): Dictionary with dates as keys and lists of sector dicts as values
    """
    try:
        # Convert datetime keys to ISO format strings for JSON serialization
        serialized_data = {}
        for date_val, scores in sector_scores.items():
            serialized_data[date_val.isoformat()] = scores
            
        with open(HISTORY_FILE, 'w') as f:
            json.dump(serialized_data, f)
            
        print(f"Saved authentic sector history with {len(sector_scores)} dates")
    except Exception as e:
        print(f"Error saving sector history: {e}")

def save_t2d_pulse_history(pulse_scores):
    """
    Save authentic T2D Pulse score history to file
    
    Args:
        pulse_scores (dict): Dictionary with dates as keys and T2D Pulse scores as values
    """
    try:
        # Convert datetime keys to ISO format strings for JSON serialization
        serialized_data = {}
        for date_val, score in pulse_scores.items():
            serialized_data[date_val.isoformat()] = score
            
        with open(T2D_PULSE_HISTORY_FILE, 'w') as f:
            json.dump(serialized_data, f)
            
        print(f"Saved authentic T2D Pulse history with {len(pulse_scores)} dates")
    except Exception as e:
        print(f"Error saving T2D Pulse history: {e}")

def load_sector_history():
    """
    Load authentic sector sentiment history from file
    
    Returns:
        dict: Dictionary with dates as keys and lists of sector dicts as values
    """
    try:
        if os.path.exists(HISTORY_FILE):
            with open(HISTORY_FILE, 'r') as f:
                serialized_data = json.load(f)
                
            # Convert date strings back to datetime objects
            history_data = {}
            for date_str, scores in serialized_data.items():
                history_data[datetime.fromisoformat(date_str)] = scores
            
            return history_data
        else:
            return {}
    except Exception as e:
        print(f"Error loading authentic sector history: {e}")
        return {}

def load_t2d_pulse_history():
    """
    Load authentic T2D Pulse score history from file
    
    Returns:
        dict: Dictionary with dates as keys and T2D Pulse scores as values
    """
    try:
        if os.path.exists(T2D_PULSE_HISTORY_FILE):
            with open(T2D_PULSE_HISTORY_FILE, 'r') as f:
                serialized_data = json.load(f)
                
            # Convert date strings back to datetime objects
            history_data = {}
            for date_str, score in serialized_data.items():
                history_data[datetime.fromisoformat(date_str)] = score
            
            return history_data
        else:
            return {}
    except Exception as e:
        print(f"Error loading authentic T2D Pulse history: {e}")
        return {}

def get_sector_history_dataframe(sector_name, days=HISTORY_LENGTH):
    """
    Get a pandas DataFrame with authentic historical sentiment scores for a sector
    
    Args:
        sector_name (str): Name of the sector
        days (int): Number of days of history to return
    
    Returns:
        DataFrame: DataFrame with 'date' and 'score' columns
    """
    # Get history data
    history_data = load_sector_history()
    
    if not history_data:
        print(f"No authentic historical data available for {sector_name}")
        return pd.DataFrame(columns=['date', 'score'])
    
    # Create rows for the selected sector
    rows = []
    for date_val, sector_scores in history_data.items():
        for sector_data in sector_scores:
            if sector_data['sector'] == sector_name:
                rows.append({
                    'date': date_val,
                    'score': sector_data['score']
                })
                break
    
    # Create DataFrame
    if not rows:
        return pd.DataFrame(columns=['date', 'score'])
    
    df = pd.DataFrame(rows)
    
    # Sort by date and keep only specified number of days
    df = df.sort_values('date')
    if len(df) > days:
        df = df.tail(days)
    
    return df

def get_t2d_pulse_history_dataframe(days=HISTORY_LENGTH):
    """
    Get a pandas DataFrame with authentic historical T2D Pulse scores
    
    Args:
        days (int): Number of days of history to return
    
    Returns:
        DataFrame: DataFrame with 'date' and 'score' columns
    """
    # Get history data
    history_data = load_t2d_pulse_history()
    
    if not history_data:
        print("No authentic T2D Pulse historical data available")
        return pd.DataFrame(columns=['date', 'score'])
    
    # Create rows from the history data
    rows = [{'date': date_val, 'score': score} for date_val, score in history_data.items()]
    
    # Create DataFrame
    if not rows:
        return pd.DataFrame(columns=['date', 'score'])
    
    df = pd.DataFrame(rows)
    
    # Sort by date and keep only specified number of days
    df = df.sort_values('date')
    if len(df) > days:
        df = df.tail(days)
    
    return df

def export_authentic_history():
    """
    Export authentic sector and T2D Pulse history to CSV files for verification
    """
    # Get today's date string
    date_today = datetime.now().strftime('%Y-%m-%d')
    
    # Export sector history
    try:
        sector_history = load_sector_history()
        if sector_history:
            # Get all sectors from the data
            all_sectors = set()
            for scores in sector_history.values():
                for sector_data in scores:
                    all_sectors.add(sector_data['sector'])
            
            all_sectors = sorted(list(all_sectors))
            
            # Create rows for each date
            rows = []
            all_dates = sorted(sector_history.keys())
            
            for current_date in all_dates:
                row = {'date': current_date.strftime('%Y-%m-%d')}
                
                # Find scores for each sector on this date
                for sector_name in all_sectors:
                    for sector_data in sector_history[current_date]:
                        if sector_data['sector'] == sector_name:
                            row[sector_name] = sector_data['score']
                            break
                
                rows.append(row)
            
            # Create and save the DataFrame
            if rows:
                sector_df = pd.DataFrame(rows)
                filename = f"data/authentic_sector_history_{date_today}.csv"
                sector_df.to_csv(filename, index=False)
                print(f"Exported authentic sector history to {filename}")
    except Exception as e:
        print(f"Error exporting sector history: {e}")
    
    # Export T2D Pulse history
    try:
        pulse_history = load_t2d_pulse_history()
        if pulse_history:
            # Create rows for each date
            rows = [{'date': date_val.strftime('%Y-%m-%d'), 't2d_pulse_score': score} 
                   for date_val, score in pulse_history.items()]
            
            # Create and save the DataFrame
            if rows:
                pulse_df = pd.DataFrame(rows).sort_values('date')
                filename = f"data/authentic_t2d_pulse_history_{date_today}.csv"
                pulse_df.to_csv(filename, index=False)
                print(f"Exported authentic T2D Pulse history to {filename}")
    except Exception as e:
        print(f"Error exporting T2D Pulse history: {e}")

def update_authentic_history():
    """
    Update the authentic sector and T2D Pulse historical data
    
    Returns:
        bool: True if history was updated successfully
    """
    try:
        # Calculate authentic historical scores
        sector_scores, pulse_scores = calculate_historical_sector_scores()
        
        if not sector_scores:
            print("No authentic historical sector data was calculated")
            return False
        
        # Save the authentic historical data
        save_sector_history(sector_scores)
        save_t2d_pulse_history(pulse_scores)
        
        # Export for verification
        export_authentic_history()
        
        print("Successfully updated authentic historical data")
        return True
    
    except Exception as e:
        print(f"Error updating authentic historical data: {e}")
        return False

# Execute this file directly to update authentic historical data
if __name__ == "__main__":
    update_authentic_history()