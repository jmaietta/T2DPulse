#!/usr/bin/env python3
# fix_historical_data.py
# -----------------------------------------------------------
# Recalculate historical sector scores with proper daily variation

import pandas as pd
import numpy as np
import os
from datetime import datetime
import pytz
import time

# Import the modules we need for the recalculation
from process_jm_historical_data import load_historical_data, prepare_macro_dict, export_historical_scores
import sentiment_engine

def get_date_in_eastern():
    # Get current date in US Eastern timezone
    eastern = pytz.timezone('US/Eastern')
    return datetime.now(eastern)

def recalculate_historical_sector_scores():
    """Recalculate sector scores for the historical data with accurate daily variation"""
    print("\nStarting historical data fix...")
    print("Loading historical indicator data...")
    
    # Load the source data file
    df = load_historical_data()
    
    if df is None or df.empty:
        print("Error: Could not load historical indicator data")
        return False
    
    # Sort data by date to ensure proper processing
    df = df.sort_values('date')
    
    # Print a summary of the input data
    date_range = f"{df['date'].min().strftime('%Y-%m-%d')} to {df['date'].max().strftime('%Y-%m-%d')}"
    print(f"Loaded {len(df)} days of historical data from {date_range}")
    
    # Verify the data has NASDAQ and VIX columns which should vary daily
    print("\nVerifying NASDAQ and VIX daily variation in source data:")
    nasdaq_values = df['NASDAQ Raw Value'].astype(float)
    vix_values = df['VIX Raw Value'].astype(float)
    treasury_values = df['10-Year Treasury Yield'].astype(float)
    
    print(f"NASDAQ Raw Values: Min={nasdaq_values.min():.2f}, Max={nasdaq_values.max():.2f}, Variance={nasdaq_values.var():.2f}")
    print(f"VIX Raw Values: Min={vix_values.min():.2f}, Max={vix_values.max():.2f}, Variance={vix_values.var():.2f}")
    print(f"10-Year Treasury Yield: Min={treasury_values.min():.2f}%, Max={treasury_values.max():.2f}%, Variance={treasury_values.var():.4f}")
    
    # Calculate the percentage change day-over-day for NASDAQ
    df['NASDAQ % Change'] = nasdaq_values.pct_change() * 100
    
    # Show some sample day-to-day changes
    print("\nSample day-to-day NASDAQ changes:")
    for i in range(1, 5):
        date1 = df['date'].iloc[i-1].strftime('%Y-%m-%d')
        date2 = df['date'].iloc[i].strftime('%Y-%m-%d')
        pct_change = df['NASDAQ % Change'].iloc[i]
        print(f"{date1} to {date2}: {pct_change:.2f}% change in NASDAQ")
    
    # Create results structure
    results = {'date': []}
    
    # Initialize columns for each sector
    for sector in sentiment_engine.SECTORS:
        results[sector] = []
    
    # Process each date with our improved calculation method
    print("\nRecalculating sector sentiment scores with daily market data variation:")
    for idx, row in df.iterrows():
        date_str = row['date'].strftime('%Y-%m-%d')
        print(f"\nProcessing {date_str} with high-variance market data")
        
        # Prepare the macro dictionary with debug output
        macro_dict = prepare_macro_dict(row, date_str)
        
        # Add a synthetic EMA factor based directly on NASDAQ performance
        # This ensures each day's score is influenced by that day's market data
        if 'NASDAQ Gap %' not in row and idx > 19:  # Need at least 20 days for EMA calc
            # Calculate NASDAQ 20-day EMA manually if not in the data
            nasdaq_values_prior = df.iloc[idx-20:idx]['NASDAQ Raw Value'].astype(float)
            ema20 = nasdaq_values_prior.ewm(span=20, adjust=False).mean().iloc[-1]
            nasdaq_raw = float(row['NASDAQ Raw Value'])
            gap_pct = ((nasdaq_raw - ema20) / ema20) * 100
            macro_dict['NASDAQ_20d_gap_%'] = gap_pct
            print(f"Calculated NASDAQ Gap: {gap_pct:.2f}%")
        
        # Also add the EMA factor directly based on NASDAQ gap
        if 'NASDAQ_20d_gap_%' in macro_dict:
            gap_pct = macro_dict['NASDAQ_20d_gap_%']
            macro_dict['Sector_EMA_Factor'] = max(-0.7, min(0.7, gap_pct / 10.0))
            print(f"Using NASDAQ Gap {gap_pct:.2f}% to create EMA factor: {macro_dict['Sector_EMA_Factor']:.3f}")
        # If gap not available (first days), use VIX as proxy
        elif 'VIX' in macro_dict:
            vix = macro_dict['VIX']
            vix_factor = max(-0.4, min(0.2, (20.0 - vix) / 25.0))
            macro_dict['Sector_EMA_Factor'] = vix_factor
            print(f"Using VIX {vix:.2f} as proxy EMA factor: {vix_factor:.3f}")
        
        # Calculate sector scores with the updated macro dictionary
        try:
            sector_scores = sentiment_engine.score_sectors(macro_dict)
            
            # Add date to results
            results['date'].append(row['date'])
            
            # Process each sector score
            print(f"Sector scores for {date_str}:")
            for sector_data in sector_scores:
                sector_name = sector_data['sector']
                raw_score = sector_data['score']
                
                # Convert raw score from [-1,1] to [0-100] for display
                normalized_score = ((raw_score + 1.0) / 2.0) * 100
                
                # Add to results
                results[sector_name].append(normalized_score)
                
                # Print select sector scores to verify appropriate variance
                if sector_name in ['AdTech', 'SMB SaaS', 'Cybersecurity']:
                    print(f"  {sector_name}: {normalized_score:.1f}")
        
        except Exception as e:
            print(f"Error calculating scores for {date_str}: {e}")
            import traceback
            traceback.print_exc()
    
    # Convert results to DataFrame
    results_df = pd.DataFrame(results)
    
    # Analyze to confirm we have variability
    print("\nVerifying sector score variance in output data:")
    for sector in ['SMB SaaS', 'Cybersecurity', 'Cloud Infrastructure']:
        sector_values = results_df[sector]
        print(f"{sector}: Min={sector_values.min():.1f}, Max={sector_values.max():.1f}, Variance={sector_values.var():.2f}")
    
    # Calculate day-over-day changes to verify we have appropriate variability
    print("\nSample day-to-day sector score changes:")
    for i in range(1, 5):
        date1 = results_df['date'].iloc[i-1].strftime('%Y-%m-%d')
        date2 = results_df['date'].iloc[i].strftime('%Y-%m-%d')
        sector = 'SMB SaaS'  # Example sector
        score1 = results_df[sector].iloc[i-1]
        score2 = results_df[sector].iloc[i]
        diff = score2 - score1
        print(f"{date1} to {date2}: {sector} changed by {diff:.1f} points")
    
    # Export the recalculated scores
    print("\nExporting recalculated historical scores")
    export_historical_scores(results_df)
    
    # Also save a backup copy with the date and time
    timestamp = get_date_in_eastern().strftime("%Y%m%d_%H%M%S")
    backup_path = f"data/authentic_sector_history_backup_{timestamp}.csv"
    results_df.to_csv(backup_path, index=False)
    print(f"Created backup at {backup_path}")
    
    print("\nHistorical data fix complete!")
    return True

if __name__ == "__main__":
    print("Historical Sector Sentiment Data Fix Tool")
    print("=======================================")
    print("This script will recalculate all historical sector sentiment scores")
    print("to ensure proper daily variation based on market indicators.")
    print("\nNote: This will overwrite the current authentic_sector_history.csv file.")
    
    # In non-interactive environment, proceed automatically
    print("\nProceeding with historical data recalculation...")
    
    start_time = time.time()
    success = recalculate_historical_sector_scores()
    end_time = time.time()
    
    if success:
        print(f"\nProcessing completed in {end_time - start_time:.2f} seconds")
        print("You may now restart the dashboard to see the fixed historical data")
    else:
        print("\nError: Processing failed")
