#!/usr/bin/env python3
# debug_historical_data.py
# --------------------------------------------------------------
# Script to debug why consecutive days in historical data have identical scores

import pandas as pd
import numpy as np
import os
from datetime import datetime
import traceback
import sys

# Path to JM's historical indicator data
HISTORICAL_DATA_PATH = "attached_assets/Historical Indicator Data JM.csv"

# Import necessary modules for processing
sys.path.append('.')
import sentiment_engine
from process_jm_historical_data import clean_percentage, clean_nasdaq_value, calculate_ema

def load_historical_data():
    """Load and preprocess the historical data"""
    try:
        # Read the CSV file
        df = pd.read_csv(HISTORICAL_DATA_PATH)
        
        # Convert date to datetime
        df['date'] = pd.to_datetime(df['date'])
        
        # Sort by date
        df = df.sort_values('date')
        
        # Clean percentage values
        for col in ['Real GDP % Change', 'PCE', 'Unemployment Rate', 'Software Job Postings', 
                    'Inflation (CPI)', 'PCEPI (YoY)', 'Fed Funds Rate', '10-Year Treasury Yield',
                    'PPI: Software Publishers', 'PPI: Data Processing Services']:
            df[col] = df[col].apply(clean_percentage)
        
        # Clean NASDAQ values
        df['NASDAQ Raw Value'] = df['NASDAQ Raw Value'].apply(clean_nasdaq_value)
        
        # Calculate 20-day EMA for NASDAQ
        df['NASDAQ EMA20'] = calculate_ema(df['NASDAQ Raw Value'], span=20)
        
        # Calculate NASDAQ gap percentage
        df['NASDAQ Gap %'] = ((df['NASDAQ Raw Value'] - df['NASDAQ EMA20']) / df['NASDAQ EMA20']) * 100
        
        # Calculate 14-day EMA for VIX
        df['VIX EMA14'] = calculate_ema(df['VIX Raw Value'], span=14)
        
        return df
    
    except Exception as e:
        print(f"Error loading historical data: {e}")
        traceback.print_exc()
        return None

def prepare_macro_dict(row):
    """Convert a row of historical data to the macro dictionary format expected by sentiment_engine"""
    macro_dict = {
        "Real_GDP_Growth_%_SAAR": row['Real GDP % Change'],
        "Real_PCE_YoY_%": row['PCE'],
        "Unemployment_%": row['Unemployment Rate'],
        "Software_Dev_Job_Postings_YoY_%": row['Software Job Postings'],
        "CPI_YoY_%": row['Inflation (CPI)'],
        "PCEPI_YoY_%": row['PCEPI (YoY)'],
        "Fed_Funds_Rate_%": row['Fed Funds Rate'],
        "NASDAQ_20d_gap_%": row['NASDAQ Gap %'],
        "PPI_Software_Publishers_YoY_%": row['PPI: Software Publishers'],
        "PPI_Data_Processing_YoY_%": row['PPI: Data Processing Services'],
        "10Y_Treasury_Yield_%": row['10-Year Treasury Yield'],
        "VIX": row['VIX EMA14'],
        "Consumer_Sentiment": row['Consumer Sentiment']
    }
    
    # Directly add an EMA factor based on NASDAQ gap
    nasdaq_gap = row['NASDAQ Gap %']
    macro_dict['Sector_EMA_Factor'] = max(-1.0, min(1.0, nasdaq_gap / 10.0))
    
    return macro_dict

def analyze_consecutive_days():
    """Analyze why consecutive days in historical data have identical scores"""
    print("\nDEBUG: Analyzing why consecutive days have identical scores")
    
    # Load the historical data
    df = load_historical_data()
    
    if df is None or df.empty:
        print("Error: Could not load historical data")
        return False
    
    # Find multiple consecutive days where we have observed identical scores
    problem_ranges = [
        ("2025-04-01", "2025-04-02"),  # Days 1-2
        ("2025-04-07", "2025-04-08"),  # Days 7-8
        ("2025-04-09", "2025-04-11"),  # Days 9-11
    ]
    
    for start_date, end_date in problem_ranges:
        print(f"\n--- Analyzing range {start_date} to {end_date} ---")
        
        # Filter to the dates we're interested in
        mask = (df['date'] >= start_date) & (df['date'] <= end_date)
        date_range_df = df[mask]
        
        if date_range_df.empty or len(date_range_df) < 2:
            print(f"Error: No data found for range {start_date} to {end_date}")
            continue
        
        # Print raw values for market indicators
        print("\nMarket indicator values:")
        for _, row in date_range_df.iterrows():
            date_str = row['date'].strftime('%Y-%m-%d')
            print(f"{date_str}: NASDAQ={row['NASDAQ Raw Value']:,.2f}, Gap={row['NASDAQ Gap %']:.2f}%, VIX={row['VIX Raw Value']:.2f}, EMA14={row['VIX EMA14']:.2f}, Treasury={row['10-Year Treasury Yield']:.2f}%")
        
        # Process each day and show why scores might be identical
        print("\nDetailed debug of score calculation:")
        for idx, row in date_range_df.iterrows():
            date_str = row['date'].strftime('%Y-%m-%d')
            print(f"\nProcessing {date_str}:")
            
            # Convert row to macro dictionary
            macro_dict = prepare_macro_dict(row)
            
            # Print the key elements that go into score calculation
            print("Key input factors for scoring:")
            for key in ['NASDAQ_20d_gap_%', 'VIX', '10Y_Treasury_Yield_%', 'Sector_EMA_Factor']:
                print(f"  {key}: {macro_dict.get(key)}")
            
            # Get the band information for these indicators
            print("\nBand calculations:")
            for key in ['NASDAQ_20d_gap_%', 'VIX', '10Y_Treasury_Yield_%', 'Sector_EMA_Factor']:
                direction, neutral_min, neutral_max = sentiment_engine.BANDS.get(key, ("higher", 0, 0))
                print(f"  {key}: {direction}, neutral range {neutral_min} to {neutral_max}")
                value = macro_dict.get(key)
                # Determine if positive, neutral, or negative
                if direction == "higher":
                    if value >= neutral_max:
                        signal = +1
                        status = "positive"
                    elif value <= neutral_min:
                        signal = -1
                        status = "negative"
                    else:
                        signal = 0
                        status = "neutral"
                else:  # "lower"
                    if value <= neutral_min:
                        signal = +1
                        status = "positive"
                    elif value >= neutral_max:
                        signal = -1
                        status = "negative"
                    else:
                        signal = 0
                        status = "neutral"
                print(f"    Value {value}: {status} (signal {signal})")
            
            # Calculate sector score for a representative sector
            print("\nScoring calculation for 'AdTech':")
            try:
                # Get impact weight for this sector
                sector_scores = sentiment_engine.score_sectors(macro_dict)
                for sector_data in sector_scores:
                    if sector_data['sector'] == 'AdTech':
                        score = sector_data['score']
                        normalized = ((score + 1.0) / 2.0) * 100
                        print(f"  Raw score: {score:.3f}, Normalized (0-100): {normalized:.1f}")
                        break
            except Exception as e:
                print(f"Error calculating scores: {e}")
                traceback.print_exc()
    
    return True

if __name__ == "__main__":
    print("Historical Data Debug Tool")
    print("=========================")
    
    analyze_consecutive_days()
