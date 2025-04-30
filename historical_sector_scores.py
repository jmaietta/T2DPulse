#!/usr/bin/env python3
# historical_sector_scores.py
# -----------------------------------------------------------
# Generate historical sector sentiment scores using past data

import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
import json
import time

# Import data helpers
from data_cache import get_data
from sentiment_engine import SECTORS, IMPACT, IMPORTANCE, score_sectors
import sector_sentiment_history

def get_indicator_value_for_date(indicator_name, target_date):
    """
    Get the closest available value for an economic indicator on a specific date
    
    Args:
        indicator_name (str): Name of the indicator (matching data file names)
        target_date (datetime): The date to get data for
    
    Returns:
        float: The indicator value, or None if not available
    """
    # Map indicator names to data file names
    indicator_files = {
        "10Y_Treasury_Yield_%": "treasury_yield",
        "VIX": "vix",
        "NASDAQ_20d_gap_%": "nasdaq",
        "Fed_Funds_Rate_%": "interest_rate",
        "CPI_YoY_%": "cpi",
        "PCEPI_YoY_%": "pcepi",
        "Real_GDP_Growth_%_SAAR": "gdp",
        "Real_PCE_YoY_%": "pce",
        "Unemployment_%": "unemployment",
        "Software_Dev_Job_Postings_YoY_%": "job_postings",
        "PPI_Data_Processing_YoY_%": "data_ppi",
        "PPI_Software_Publishers_YoY_%": "software_ppi",
        "Consumer_Sentiment": "consumer_sentiment"
    }
    
    # Get the data file for this indicator
    file_key = indicator_files.get(indicator_name)
    if not file_key:
        print(f"No data file mapping for indicator: {indicator_name}")
        return None
    
    # Get data for this indicator
    df = get_data(file_key)
    if df is None or df.empty:
        print(f"No data available for indicator: {indicator_name}")
        return None
    
    # Get the data point closest to target_date
    if 'date' not in df.columns:
        print(f"Date column missing for indicator: {indicator_name}")
        return None
    
    # Get timestamp for target date to make comparison easier
    target_timestamp = pd.Timestamp(target_date)
    
    # Convert dates to timestamps if needed
    if not pd.api.types.is_datetime64_any_dtype(df['date']):
        df['date'] = pd.to_datetime(df['date'])
    
    # Find the closest date to the target date
    df['date_diff'] = abs(df['date'] - target_timestamp)
    closest_row = df.loc[df['date_diff'].idxmin()]
    
    # Get the appropriate value column
    if indicator_name == "NASDAQ_20d_gap_%":
        if 'gap_pct' in df.columns:
            value = closest_row.get('gap_pct')
        else:
            value = None
    elif indicator_name == "10Y_Treasury_Yield_%":
        value = closest_row.get('value')
    elif indicator_name == "VIX":
        # If smoothed VIX (ema14) is available, use it
        if 'ema14' in df.columns:
            value = closest_row.get('ema14')
        else:
            value = closest_row.get('value')
    elif indicator_name in ["CPI_YoY_%", "PCEPI_YoY_%"]:
        # For inflation measures, use the year-over-year value if available
        if 'year_ago_value' in df.columns:
            value = ((closest_row.get('value') / closest_row.get('year_ago_value')) - 1) * 100
        else:
            value = closest_row.get('value')
    else:
        value = closest_row.get('value')
    
    # Print for debugging
    print(f"For {indicator_name} on {target_date.strftime('%Y-%m-%d')}, using value {value} from {closest_row['date'].strftime('%Y-%m-%d')}")
    
    return value

def calculate_historical_sector_scores(days_back=30):
    """
    Calculate historical sector sentiment scores for a given number of days
    
    Args:
        days_back (int): Number of past days to calculate scores for
    
    Returns:
        dict: Dictionary with dates as keys and normalized sector scores as values
    """
    print(f"Calculating historical sector scores for the past {days_back} days...")
    
    # Get today's date
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Initialize results dictionary
    historical_scores = {}
    
    # Generate a series of dates going back from today
    for i in range(days_back, 0, -1):
        target_date = today - timedelta(days=i)
        print(f"\nCalculating scores for {target_date.strftime('%Y-%m-%d')}:")
        
        # Get indicator values for this date
        indicator_values = {}
        for indicator in IMPACT.keys():
            value = get_indicator_value_for_date(indicator, target_date)
            if value is not None:
                indicator_values[indicator] = value
        
        # Skip dates with insufficient data
        if len(indicator_values) < 5:  # Require at least 5 indicators
            print(f"Insufficient data for {target_date.strftime('%Y-%m-%d')}, skipping")
            continue
        
        # Calculate raw scores for this date
        try:
            # Use the score_sectors function from sentiment_engine
            raw_scores = score_sectors(indicator_values)
            
            # The scores from score_sectors are already in the correct format
            historical_scores[target_date] = raw_scores
            print(f"Successfully calculated scores for {target_date.strftime('%Y-%m-%d')}")
        except Exception as e:
            print(f"Error calculating scores for {target_date.strftime('%Y-%m-%d')}: {e}")
    
    return historical_scores

def update_sector_history_with_historical_data():
    """
    Update the sector sentiment history with calculated historical values
    
    Returns:
        bool: True if history was updated successfully
    """
    # Calculate historical scores
    historical_data = calculate_historical_sector_scores(days_back=30)
    
    if not historical_data:
        print("No historical data was calculated")
        return False
    
    # Load existing history
    history = sector_sentiment_history.load_sentiment_history()
    
    # Process each day of historical data
    for history_date, scores in historical_data.items():
        for sector_data in scores:
            sector_name = sector_data['sector']
            score = sector_data['score']
            
            # Initialize history for new sectors
            if sector_name not in history:
                history[sector_name] = []
            
            # Check if we already have an entry for this date
            has_date = any(date.date() == history_date.date() for date, _ in history[sector_name])
            
            if not has_date:
                # Add historical data point
                history[sector_name].append((history_date, score))
    
    # Sort each sector's history by date
    for sector_name in history:
        history[sector_name] = sorted(history[sector_name], key=lambda x: x[0])
    
    # Save updated history
    sector_sentiment_history.save_sentiment_history(history)
    print(f"Updated sector history with calculated historical values")
    
    return True

if __name__ == "__main__":
    # Update sector history with historical data
    update_sector_history_with_historical_data()