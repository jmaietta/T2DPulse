#!/usr/bin/env python3
# authentic_historical_data.py
# -----------------------------------------------------------
# Calculate authentic historical sector sentiment scores using direct API data
# This uses real API calls to get historical data for each date (business days only)

import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
import json
from pandas.tseries.offsets import BDay
import time

# Import API functions from app.py
from app import (
    fetch_fred_data, 
    fetch_bea_data, 
    fetch_treasury_yield_data,
    fetch_vix_from_yahoo,
    fetch_nasdaq_with_ema,
    fetch_consumer_sentiment_data,
    fetch_bls_data,
    calculate_sector_sentiment,
    calculate_t2d_pulse_from_sectors
)

# Import sentiment engine components
from sentiment_engine import SECTORS, IMPACT, IMPORTANCE, score_sectors

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

def fetch_indicator_data_for_date(indicator, date_to_fetch):
    """
    Fetch a specific indicator's data for a specific date from the appropriate API
    
    Args:
        indicator (str): The indicator to fetch (e.g., "10Y_Treasury_Yield_%")
        date_to_fetch (datetime): The date to fetch data for
    
    Returns:
        float: The value for the indicator on the specified date
    """
    # Format date for API calls
    date_str = date_to_fetch.strftime('%Y-%m-%d')
    
    # Determine which API to call based on the indicator
    try:
        if indicator == "10Y_Treasury_Yield_%":
            # For treasury yield, get data from Yahoo Finance via our function
            df = fetch_treasury_yield_data()
            if not df.empty and 'date' in df.columns and 'value' in df.columns:
                # Find the closest date before or on our target date
                df['date'] = pd.to_datetime(df['date'])
                closest = df[df['date'] <= date_to_fetch].sort_values('date', ascending=False)
                if not closest.empty:
                    return closest.iloc[0]['value']
                    
        elif indicator == "VIX":
            # For VIX, get data from Yahoo Finance via our function
            df = fetch_vix_from_yahoo()
            if not df.empty and 'date' in df.columns and 'value' in df.columns:
                # Find the closest date before or on our target date
                df['date'] = pd.to_datetime(df['date'])
                closest = df[df['date'] <= date_to_fetch].sort_values('date', ascending=False)
                if not closest.empty:
                    return closest.iloc[0]['value']
                    
        elif indicator == "NASDAQ_20d_gap_%":
            # For NASDAQ, get data from Yahoo Finance via our function including gap_pct
            df = fetch_nasdaq_with_ema()
            if not df.empty and 'date' in df.columns and 'gap_pct' in df.columns:
                # Find the closest date before or on our target date
                df['date'] = pd.to_datetime(df['date'])
                closest = df[df['date'] <= date_to_fetch].sort_values('date', ascending=False)
                if not closest.empty:
                    return closest.iloc[0]['gap_pct']
                    
        elif indicator == "Fed_Funds_Rate_%":
            # For Fed Funds Rate, use FRED API with series DFF (Daily Federal Funds Rate)
            start_date = (date_to_fetch - timedelta(days=10)).strftime('%Y-%m-%d')
            df = fetch_fred_data('DFF', start_date=start_date, end_date=date_str)
            if not df.empty and 'date' in df.columns and 'value' in df.columns:
                # Find the closest date before or on our target date
                df['date'] = pd.to_datetime(df['date'])
                closest = df[df['date'] <= date_to_fetch].sort_values('date', ascending=False)
                if not closest.empty:
                    return closest.iloc[0]['value']
                    
        elif indicator == "CPI_YoY_%":
            # For CPI YoY, use FRED API with series CPIAUCSL
            start_date = (date_to_fetch - timedelta(days=365)).strftime('%Y-%m-%d')
            df = fetch_fred_data('CPIAUCSL', start_date=start_date, end_date=date_str)
            if not df.empty and 'date' in df.columns and 'value' in df.columns:
                # Calculate YoY growth using oldest available value within last year
                df['date'] = pd.to_datetime(df['date'])
                recent = df[df['date'] <= date_to_fetch].sort_values('date', ascending=False)
                if not recent.empty:
                    recent_value = recent.iloc[0]['value']
                    # Get value from ~12 months ago
                    year_ago = df[df['date'] <= (date_to_fetch - timedelta(days=350))].sort_values('date', ascending=False)
                    if not year_ago.empty:
                        year_ago_value = year_ago.iloc[0]['value']
                        return ((recent_value / year_ago_value) - 1) * 100
                    
        elif indicator == "PCEPI_YoY_%":
            # For PCEPI YoY, use FRED API with series PCEPI
            start_date = (date_to_fetch - timedelta(days=365)).strftime('%Y-%m-%d')
            df = fetch_fred_data('PCEPI', start_date=start_date, end_date=date_str)
            if not df.empty and 'date' in df.columns and 'value' in df.columns:
                # Calculate YoY growth using oldest available value within last year
                df['date'] = pd.to_datetime(df['date'])
                recent = df[df['date'] <= date_to_fetch].sort_values('date', ascending=False)
                if not recent.empty:
                    recent_value = recent.iloc[0]['value']
                    # Get value from ~12 months ago
                    year_ago = df[df['date'] <= (date_to_fetch - timedelta(days=350))].sort_values('date', ascending=False)
                    if not year_ago.empty:
                        year_ago_value = year_ago.iloc[0]['value']
                        return ((recent_value / year_ago_value) - 1) * 100
                        
        elif indicator == "Real_GDP_Growth_%_SAAR":
            # For GDP, use FRED API with series GDPC1
            start_date = (date_to_fetch - timedelta(days=365)).strftime('%Y-%m-%d')
            df = fetch_fred_data('GDPC1', start_date=start_date, end_date=date_str)
            if not df.empty and 'date' in df.columns and 'value' in df.columns:
                # Use most recent GDP value
                df['date'] = pd.to_datetime(df['date'])
                recent = df[df['date'] <= date_to_fetch].sort_values('date', ascending=False)
                if not recent.empty:
                    return recent.iloc[0]['value']
                    
        elif indicator == "Real_PCE_YoY_%":
            # For Real PCE, use FRED API with series PCEC96
            start_date = (date_to_fetch - timedelta(days=365)).strftime('%Y-%m-%d')
            df = fetch_fred_data('PCEC96', start_date=start_date, end_date=date_str)
            if not df.empty and 'date' in df.columns and 'value' in df.columns:
                # Use most recent PCE value
                df['date'] = pd.to_datetime(df['date'])
                recent = df[df['date'] <= date_to_fetch].sort_values('date', ascending=False)
                if not recent.empty:
                    return recent.iloc[0]['value']
                    
        elif indicator == "Unemployment_%":
            # For Unemployment Rate, use FRED API with series UNRATE
            start_date = (date_to_fetch - timedelta(days=60)).strftime('%Y-%m-%d')
            df = fetch_fred_data('UNRATE', start_date=start_date, end_date=date_str)
            if not df.empty and 'date' in df.columns and 'value' in df.columns:
                # Use most recent unemployment rate
                df['date'] = pd.to_datetime(df['date'])
                recent = df[df['date'] <= date_to_fetch].sort_values('date', ascending=False)
                if not recent.empty:
                    return recent.iloc[0]['value']
                    
        elif indicator == "Software_Dev_Job_Postings_YoY_%":
            # For job postings, use FRED or historical CSV (may not be available via direct API)
            # Use our app.py function to get the most recent data
            df = None
            # First check if we have the data in our job_postings_data.csv
            if os.path.exists("data/job_postings_data.csv"):
                df = pd.read_csv("data/job_postings_data.csv")
                if 'date' in df.columns and 'yoy_growth' in df.columns:
                    df['date'] = pd.to_datetime(df['date'])
                    recent = df[df['date'] <= date_to_fetch].sort_values('date', ascending=False)
                    if not recent.empty:
                        return recent.iloc[0]['yoy_growth']
                
        elif indicator == "PPI_Data_Processing_YoY_%":
            # For PPI Data Processing, use FRED API with series PCU518210518210
            start_date = (date_to_fetch - timedelta(days=365)).strftime('%Y-%m-%d')
            df = fetch_fred_data('PCU518210518210', start_date=start_date, end_date=date_str)
            if not df.empty and 'date' in df.columns and 'value' in df.columns:
                # Calculate YoY growth
                df['date'] = pd.to_datetime(df['date'])
                recent = df[df['date'] <= date_to_fetch].sort_values('date', ascending=False)
                if not recent.empty:
                    recent_value = recent.iloc[0]['value']
                    # Get value from ~12 months ago
                    year_ago = df[df['date'] <= (date_to_fetch - timedelta(days=350))].sort_values('date', ascending=False)
                    if not year_ago.empty:
                        year_ago_value = year_ago.iloc[0]['value']
                        return ((recent_value / year_ago_value) - 1) * 100
                        
        elif indicator == "PPI_Software_Publishers_YoY_%":
            # For PPI Software Publishers, use FRED API with series PCU511210511210
            start_date = (date_to_fetch - timedelta(days=365)).strftime('%Y-%m-%d')
            df = fetch_fred_data('PCU511210511210', start_date=start_date, end_date=date_str)
            if not df.empty and 'date' in df.columns and 'value' in df.columns:
                # Calculate YoY growth
                df['date'] = pd.to_datetime(df['date'])
                recent = df[df['date'] <= date_to_fetch].sort_values('date', ascending=False)
                if not recent.empty:
                    recent_value = recent.iloc[0]['value']
                    # Get value from ~12 months ago
                    year_ago = df[df['date'] <= (date_to_fetch - timedelta(days=350))].sort_values('date', ascending=False)
                    if not year_ago.empty:
                        year_ago_value = year_ago.iloc[0]['value']
                        return ((recent_value / year_ago_value) - 1) * 100
                        
        elif indicator == "Consumer_Sentiment":
            # For Consumer Sentiment, use our function to get the data
            df = fetch_consumer_sentiment_data()
            if not df.empty and 'date' in df.columns and 'value' in df.columns:
                # Find the closest date before or on our target date
                df['date'] = pd.to_datetime(df['date'])
                closest = df[df['date'] <= date_to_fetch].sort_values('date', ascending=False)
                if not closest.empty:
                    return closest.iloc[0]['value']
    
    except Exception as e:
        print(f"Error fetching {indicator} data for {date_str}: {e}")
        
    # If we reach here, we couldn't get data for this indicator
    print(f"No data available for {indicator} on {date_str}")
    return None

def calculate_historical_sector_scores():
    """
    Calculate authentic historical sector sentiment scores for business days
    
    Returns:
        dict: Dictionary with dates as keys and lists of sector scores as values
        dict: Dictionary with dates as keys and T2D Pulse scores as values
    """
    print(f"Calculating authentic historical scores for the past {HISTORY_LENGTH} business days...")
    
    # Get business days going back from today
    business_days = get_business_days(days_back=HISTORY_LENGTH)
    
    # Initialize results dictionaries
    historical_sector_scores = {}
    historical_t2d_pulse_scores = {}
    
    # Process each business day
    for business_day in business_days:
        date_str = business_day.strftime('%Y-%m-%d')
        print(f"\nCalculating scores for {date_str}:")
        
        # Get indicator values for this date
        indicator_values = {}
        for indicator in IMPACT.keys():
            print(f"  Fetching {indicator}...")
            value = fetch_indicator_data_for_date(indicator, business_day)
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
            sample_sectors = [sector_scores[i]['sector'] for i in range(min(3, len(sector_scores)))]
            sample_scores = [f"{sector_scores[i]['sector']}: {sector_scores[i]['score']:.1f}" for i in range(min(3, len(sector_scores)))]
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
            # Convert each sector score entry to a dict if it's not already
            processed_scores = []
            for score_data in scores:
                if isinstance(score_data, dict):
                    processed_scores.append(score_data)
                else:
                    # Handle other formats if needed
                    processed_scores.append({"sector": score_data[0], "score": score_data[1]})
            
            serialized_data[date_val.isoformat()] = processed_scores
            
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