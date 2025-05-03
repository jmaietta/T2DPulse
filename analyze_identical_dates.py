#!/usr/bin/env python3
# analyze_identical_dates.py
# --------------------------------------------------------------
# Script to analyze why some dates with different indicator values still have identical scores

import pandas as pd
import numpy as np
import os
from datetime import datetime
import traceback
import sys

# Path to JM's historical indicator data
HISTORICAL_DATA_PATH = "attached_assets/Historical Indicator Data JM.csv"
AUTHENTIC_SECTOR_HISTORY = "data/authentic_sector_history.csv"

# Import necessary modules for processing
sys.path.append('.')
import sentiment_engine
from process_jm_historical_data import clean_percentage, clean_nasdaq_value, calculate_ema
from sentiment_engine import raw_signal, BANDS

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

def prepare_macro_dict(row, date_str):
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
    
    print(f"\nMacro Dictionary for {date_str}:")
    for key, value in macro_dict.items():
        direction, fav_hi, unfav_lo = BANDS.get(key, ("higher", 0, 0))
        signal = raw_signal(key, value)
        signal_str = "POSITIVE" if signal == 1 else "NEGATIVE" if signal == -1 else "NEUTRAL"
        band_info = f"{direction.upper()}, {fav_hi} to {unfav_lo}"
        print(f"  {key:<30}: {value:>8} | Signal: {signal_str:<8} | Band: {band_info}")
    
    return macro_dict

def analyze_problematic_dates():
    """Analyze why dates with different indicator values still have identical scores"""
    print("\nDEBUG: Analyzing why dates with different raw data have identical scores")
    
    # Load the historical data
    df = load_historical_data()
    
    if df is None or df.empty:
        print("Error: Could not load historical data")
        return False
    
    # Load the processed sector scores
    try:
        sector_scores_df = pd.read_csv(AUTHENTIC_SECTOR_HISTORY)
        sector_scores_df['date'] = pd.to_datetime(sector_scores_df['date'])
    except Exception as e:
        print(f"Error loading sector scores: {e}")
        return False
    
    # Find problematic date pairs where data is different but scores are identical
    problem_date_pairs = [
        ("2025-04-03", "2025-04-04"),
        ("2025-04-01", "2025-04-02")
    ]
    
    for date1, date2 in problem_date_pairs:
        date1_dt = datetime.strptime(date1, "%Y-%m-%d")
        date2_dt = datetime.strptime(date2, "%Y-%m-%d")
        
        print(f"\n============ Analyzing {date1} vs {date2} ============")
        
        # Get raw data rows
        date1_row = df[df['date'] == date1_dt].iloc[0] if not df[df['date'] == date1_dt].empty else None
        date2_row = df[df['date'] == date2_dt].iloc[0] if not df[df['date'] == date2_dt].empty else None
        
        if date1_row is None or date2_row is None:
            print(f"Error: Missing data for dates {date1} and/or {date2}")
            continue
        
        # Print raw data differences
        print(f"\nRaw data comparison:")
        print(f"{'Indicator':<25} | {date1:>12} | {date2:>12} | Different?")
        print(f"{'-'*25}-+-{'-'*12}-+-{'-'*12}-+-{'-'*10}")
        
        indicators = [
            ('NASDAQ Raw Value', 'NASDAQ'),
            ('NASDAQ Gap %', 'NASDAQ Gap %'),
            ('VIX Raw Value', 'VIX'),
            ('VIX EMA14', 'VIX EMA14'),
            ('10-Year Treasury Yield', 'Treasury')
        ]
        
        for col, label in indicators:
            val1 = date1_row[col]
            val2 = date2_row[col]
            diff = "YES" if abs(val1 - val2) > 0.001 else "NO"
            print(f"{label:<25} | {val1:>12.2f} | {val2:>12.2f} | {diff}")
        
        # Get sector scores
        scores1 = sector_scores_df[sector_scores_df['date'] == date1_dt]
        scores2 = sector_scores_df[sector_scores_df['date'] == date2_dt]
        
        if scores1.empty or scores2.empty:
            print(f"Error: Missing sector scores for dates {date1} and/or {date2}")
            continue
            
        # Compare sector scores
        print(f"\nSector score comparison:")
        print(f"{'Sector':<25} | {date1:>12} | {date2:>12} | Different?")
        print(f"{'-'*25}-+-{'-'*12}-+-{'-'*12}-+-{'-'*10}")
        
        # Skip date column
        for sector in scores1.columns[1:]:
            val1 = scores1[sector].iloc[0]
            val2 = scores2[sector].iloc[0]
            diff = "YES" if abs(val1 - val2) > 0.001 else "NO"
            print(f"{sector:<25} | {val1:>12.2f} | {val2:>12.2f} | {diff}")
        
        # Process raw market data through sentiment engine for both dates
        print(f"\nPerforming detailed signal analysis:")
        macro1 = prepare_macro_dict(date1_row, date1)
        macro2 = prepare_macro_dict(date2_row, date2)
        
        # Compare key signals
        print(f"\nKey signal comparison (focus on changing indicators):")
        for key in ['NASDAQ_20d_gap_%', 'VIX', '10Y_Treasury_Yield_%', 'Sector_EMA_Factor']:
            sig1 = raw_signal(key, macro1[key])
            sig2 = raw_signal(key, macro2[key])
            sig1_str = "positive" if sig1 == 1 else "negative" if sig1 == -1 else "neutral"
            sig2_str = "positive" if sig2 == 1 else "negative" if sig2 == -1 else "neutral"
            same = "SAME" if sig1 == sig2 else "DIFFERENT"
            print(f"{key:<25}: {sig1_str:<8} vs {sig2_str:<8} | {same}")
            
        # Process a sample sector to see contributors
        print(f"\nCalculating detailed score breakdown for AdTech sector:")
        scores1 = sentiment_engine.score_sectors(macro1)
        scores2 = sentiment_engine.score_sectors(macro2)
        
        # Find AdTech scores
        adtech1 = next((s for s in scores1 if s['sector'] == 'AdTech'), None)
        adtech2 = next((s for s in scores2 if s['sector'] == 'AdTech'), None)
        
        if adtech1 and adtech2:
            print(f"\nFinal normalized AdTech scores:")
            norm1 = ((adtech1['score'] + 1) / 2) * 100
            norm2 = ((adtech2['score'] + 1) / 2) * 100
            print(f"{date1}: Raw={adtech1['score']:.2f}, Normalized={norm1:.2f}")
            print(f"{date2}: Raw={adtech2['score']:.2f}, Normalized={norm2:.2f}")
    
    return True

if __name__ == "__main__":
    print("Problematic Date Analysis Tool")
    print("=============================")
    
    analyze_problematic_dates()
