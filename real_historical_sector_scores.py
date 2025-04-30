#!/usr/bin/env python3
# real_historical_sector_scores.py
# -----------------------------------------------------------
# Calculate historical sector sentiment scores using real API data
# This replaces the synthetic data approach with actual historical calculations

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
    
    # Load data from CSV
    data_file = f"data/{file_key}_data.csv"
    if not os.path.exists(data_file):
        print(f"Data file not found: {data_file}")
        return None
    
    try:
        df = pd.read_csv(data_file)
        if df is None or df.empty:
            print(f"No data available for indicator: {indicator_name}")
            return None
        
        # Check if date column exists
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
        elif indicator_name in ["CPI_YoY_%", "PCEPI_YoY_%"]:
            # For inflation measures, use the year-over-year value if available
            if 'year_ago_value' in df.columns and not pd.isna(closest_row.get('year_ago_value')):
                value = ((closest_row.get('value') / closest_row.get('year_ago_value')) - 1) * 100
            elif 'yoy_growth' in df.columns:
                value = closest_row.get('yoy_growth')
            elif 'inflation' in df.columns:
                value = closest_row.get('inflation') 
            else:
                value = closest_row.get('value')
        elif indicator_name in ["PPI_Data_Processing_YoY_%", "PPI_Software_Publishers_YoY_%"]:
            # For PPI measures, use yoy_pct_change if available
            if 'yoy_pct_change' in df.columns:
                value = closest_row.get('yoy_pct_change')
            elif 'year_ago_value' in df.columns and not pd.isna(closest_row.get('year_ago_value')):
                value = ((closest_row.get('value') / closest_row.get('year_ago_value')) - 1) * 100
            else:
                value = closest_row.get('value')
        elif indicator_name == "Software_Dev_Job_Postings_YoY_%":
            # For job postings, use yoy_growth if available
            if 'yoy_growth' in df.columns:
                value = closest_row.get('yoy_growth')
            else:
                value = closest_row.get('value')
        else:
            value = closest_row.get('value')
        
        date_diff = closest_row['date_diff'].days
        if date_diff > 10:  # If closest data point is more than 10 days away, warn
            print(f"Warning: For {indicator_name} on {target_date.strftime('%Y-%m-%d')}, closest data is {date_diff} days away: {closest_row['date'].strftime('%Y-%m-%d')}")
            
        return value
        
    except Exception as e:
        print(f"Error getting indicator value for {indicator_name} on {target_date}: {e}")
        return None

def calculate_historical_sector_scores(days_back=30):
    """
    Calculate historical sector sentiment scores for a given number of days
    using real data from our API sources
    
    Args:
        days_back (int): Number of past days to calculate scores for
    
    Returns:
        dict: Dictionary with dates as keys and list of sector dictionaries as values
    """
    print(f"Calculating historical sector scores for the past {days_back} days using real API data...")
    
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
            print(f"Insufficient data for {target_date.strftime('%Y-%m-%d')}, skipping (only {len(indicator_values)} indicators)")
            continue
        
        # Log the indicators we have data for
        print(f"Indicators with data ({len(indicator_values)}): {', '.join(indicator_values.keys())}")
        print(f"Indicator values: {indicator_values}")
        
        # Calculate raw scores for this date
        try:
            # Use the score_sectors function from sentiment_engine
            raw_scores = score_sectors(indicator_values)
            
            # The scores from score_sectors are already in the correct format
            historical_scores[target_date] = raw_scores
            print(f"Successfully calculated scores for {target_date.strftime('%Y-%m-%d')}")
            
            # Log a few scores for verification
            sample_sectors = [raw_scores[i]['sector'] for i in range(min(3, len(raw_scores)))]
            sample_scores = [f"{raw_scores[i]['sector']}: {raw_scores[i]['score']:.1f}" for i in range(min(3, len(raw_scores)))]
            print(f"Sample sector scores: {', '.join(sample_scores)}")
            
        except Exception as e:
            print(f"Error calculating scores for {target_date.strftime('%Y-%m-%d')}: {e}")
    
    return historical_scores

def update_sector_history_with_real_data():
    """
    Update the sector sentiment history with calculated historical values
    using real API data
    
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
    
    # Create mapping from dates to sector scores
    sector_scores_by_date = {}
    dates_processed = []
    
    # Process each day of historical data
    for history_date, scores in historical_data.items():
        dates_processed.append(history_date)
        
        for sector_data in scores:
            sector_name = sector_data['sector']
            # Use the regular score field (not normalized_score)
            score = sector_data['score']
            
            # Initialize history for new sectors
            if sector_name not in history:
                history[sector_name] = []
            
            # Check if we already have an entry for this date
            has_date = any(date.date() == history_date.date() for date, _ in history[sector_name])
            
            if not has_date:
                # Add historical data point
                history[sector_name].append((history_date, score))
                print(f"Added {history_date.strftime('%Y-%m-%d')} score for {sector_name}: {score:.1f}")
                
                # Keep track of sector scores by date for logging
                if history_date not in sector_scores_by_date:
                    sector_scores_by_date[history_date] = []
                sector_scores_by_date[history_date].append((sector_name, score))
    
    # Sort each sector's history by date
    for sector_name in history:
        history[sector_name] = sorted(history[sector_name], key=lambda x: x[0])
    
    # Save updated history
    sector_sentiment_history.save_sentiment_history(history)
    print(f"Updated sector history with calculated historical values for {len(dates_processed)} dates")
    
    # Log the dates we processed
    print(f"Processed dates: {', '.join([d.strftime('%Y-%m-%d') for d in sorted(dates_processed)])}")
    
    # Export to CSV for verification
    date_today = datetime.now().strftime('%Y-%m-%d')
    filename = f"data/real_sector_sentiment_history_{date_today}.csv"
    
    try:
        # Create a direct conversion from our history data
        all_sectors = list(history.keys())
        all_sectors.sort()  # Sort sector names alphabetically
        
        # Create a DataFrame with rows for each date
        rows = []
        all_dates = set()
        
        # Collect all unique dates
        for sector_name in all_sectors:
            for date_val, _ in history[sector_name]:
                all_dates.add(date_val.date())
        
        # Sort dates
        all_dates = sorted(all_dates)
        
        # Create rows for each date
        for current_date in all_dates:
            row = {'date': current_date.strftime('%Y-%m-%d')}
            
            # Add scores for each sector on this date
            for sector_name in all_sectors:
                # Find the score for this sector on this date
                for date_val, score in history[sector_name]:
                    if date_val.date() == current_date:
                        row[sector_name] = score
                        break
            
            rows.append(row)
        
        # Create DataFrame and export to CSV
        if rows:
            df = pd.DataFrame(rows)
            df.to_csv(filename, index=False)
            print(f"Exported real sector sentiment history to {filename}")
            
            # Print the first few rows for verification
            print("\nSample of real sector sentiment history CSV:")
            for i, row in df.head(3).iterrows():
                print(f"Date: {row['date']}, Sample sectors: "
                      f"{all_sectors[0]}: {row.get(all_sectors[0], 'N/A'):.1f}, "
                      f"{all_sectors[1]}: {row.get(all_sectors[1], 'N/A'):.1f}, "
                      f"{all_sectors[2]}: {row.get(all_sectors[2], 'N/A'):.1f}")
    except Exception as e:
        print(f"Error exporting sector sentiment history: {e}")
    
    return True

if __name__ == "__main__":
    # Update sector history with real historical data
    update_sector_history_with_real_data()