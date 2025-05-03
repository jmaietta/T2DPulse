#!/usr/bin/env python3
# extend_historical_data.py
# --------------------------------------------------------------
# Script to add May 1-2 data to our historical indicators and recalculate sector scores

import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
import sys

# Import our data fetching tools
sys.path.append('.')
import app  # This has our data fetching functions
from process_jm_historical_data import main as process_historical_data

# Paths to historical indicators files
HISTORICAL_DATA_PATH = "data/Historical_Indicator_Data.csv"
JM_HISTORICAL_DATA_PATH = "attached_assets/Historical Indicator Data JM.csv"

def extend_historical_data():
    """Add May 1-2, 2025 data to our historical indicators file using real API data"""
    print("\nAdding May 1-2, 2025 data to historical indicators...")
    
    try:
        # Load the existing historical data
        hist_df = pd.read_csv(HISTORICAL_DATA_PATH)
        
        # Convert date column to datetime
        hist_df['date'] = pd.to_datetime(hist_df['date'])
        
        # Get the last date in our historical data
        latest_date = hist_df['date'].max()
        print(f"Latest date in historical data: {latest_date.strftime('%Y-%m-%d')}")
        
        # Check if we already have May 1-2 data
        if latest_date >= datetime(2025, 5, 2):
            print("May 1-2 data already exists in the historical file.")
            return True
        
        # Calculate the dates we need to add
        if latest_date == datetime(2025, 4, 30):
            # We need to add May 1-2
            dates_to_add = [datetime(2025, 5, 1), datetime(2025, 5, 2)]
        elif latest_date == datetime(2025, 5, 1):
            # We need to add just May 2
            dates_to_add = [datetime(2025, 5, 2)]
        else:
            # Something unexpected, need to investigate
            print(f"Unexpected latest date: {latest_date}. Can't determine what to add.")
            return False
        
        # Create a new DataFrame for the dates we're adding
        extension_data = []
        
        # Get the existing FRED API data
        print("Loading existing FRED data...")
        # These are quarterly/monthly and won't change for May 1-2
        gdp_data = pd.read_csv("data/gdp_data.csv")
        pce_data = pd.read_csv("data/pce_data.csv")
        unemployment_data = pd.read_csv("data/unemployment_data.csv")
        cpi_data = pd.read_csv("data/inflation_data.csv")
        pcepi_data = pd.read_csv("data/pcepi_data.csv")
        fedfunds_data = pd.read_csv("data/interest_rate_data.csv")
        job_postings_data = pd.read_csv("data/job_postings_data.csv")
        software_ppi_data = pd.read_csv("data/software_ppi_data.csv")
        data_ppi_data = pd.read_csv("data/data_processing_ppi_data.csv")
        consumer_sentiment_data = pd.read_csv("data/consumer_sentiment_data.csv")
        
        # Get the daily changing data
        print("Loading market data for Treasury, NASDAQ, and VIX...")
        treasury_data = pd.read_csv("data/treasury_yield_data.csv")
        treasury_data['date'] = pd.to_datetime(treasury_data['date'])
        
        nasdaq_data = pd.read_csv("data/nasdaq_data.csv")
        nasdaq_data['date'] = pd.to_datetime(nasdaq_data['date'])
        
        vix_data = pd.read_csv("data/vix_data.csv")
        vix_data['date'] = pd.to_datetime(vix_data['date'])
        
        # Helper to get the latest value before or on a specific date
        def get_latest_value(df, date_col, value_col, target_date):
            mask = df[date_col] <= target_date
            if not mask.any():
                return None
            return df[mask].iloc[-1][value_col]
        
        # Get the values that change monthly/quarterly
        latest_gdp = gdp_data.iloc[-1]['value']
        latest_pce = pce_data.iloc[-1]['value']
        latest_unemployment = unemployment_data.iloc[-1]['value']
        latest_cpi = cpi_data.iloc[-1]['inflation']
        latest_pcepi = pcepi_data.iloc[-1]['yoy_growth']
        latest_fedfunds = fedfunds_data.iloc[-1]['value']
        latest_job_postings = job_postings_data.iloc[-1]['yoy_growth']
        latest_software_ppi = software_ppi_data.iloc[-1]['yoy_pct_change']
        latest_data_ppi = data_ppi_data.iloc[-1]['yoy_pct_change']
        latest_consumer_sentiment = consumer_sentiment_data.iloc[-1]['value']
        
        # For each date we need to add
        for target_date in dates_to_add:
            # Format the date as string for the dataframe
            date_str = target_date.strftime("%Y-%m-%d")
            
            # Get the daily changing values for this specific date
            treasury_value = get_latest_value(treasury_data, 'date', 'value', target_date)
            nasdaq_value = get_latest_value(nasdaq_data, 'date', 'value', target_date)
            vix_value = get_latest_value(vix_data, 'date', 'value', target_date)
            
            # Ensure we have valid data
            if treasury_value is None or nasdaq_value is None or vix_value is None:
                print(f"Missing market data for {date_str}, skipping...")
                continue
            
            # Format the values as they appear in the historical data
            nasdaq_formatted = f'"{int(nasdaq_value):,}"'
            
            # Create the row for this date
            row = {
                'date': date_str,
                'Real GDP % Change': f"{latest_gdp}%",
                'PCE': f"{latest_pce}%",
                'Unemployment Rate': f"{latest_unemployment}%",
                'Software Job Postings': f"{latest_job_postings}%",
                'Inflation (CPI)': f"{latest_cpi}%",
                'PCEPI (YoY)': f"{latest_pcepi}%",
                'Fed Funds Rate': f"{latest_fedfunds}%",
                'NASDAQ Raw Value': nasdaq_formatted,
                'PPI: Software Publishers': f"{latest_software_ppi}%",
                'PPI: Data Processing Services': f"{latest_data_ppi}%",
                '10-Year Treasury Yield': f"{treasury_value}%",
                'VIX Raw Value': vix_value,
                'Consumer Sentiment': latest_consumer_sentiment
            }
            
            extension_data.append(row)
            print(f"Added data for {date_str}: NASDAQ={nasdaq_formatted}, VIX={vix_value}, Treasury={treasury_value}%")
        
        # If we have no data to add, exit
        if len(extension_data) == 0:
            print("No valid data found for May 1-2, 2025. Check the market data files.")
            return False
        
        # Create DataFrame from the extension data
        extension_df = pd.DataFrame(extension_data)
        
        # Combine with existing data
        combined_df = pd.concat([hist_df, extension_df], ignore_index=True)
        
        # Sort by date
        combined_df['date'] = pd.to_datetime(combined_df['date'])
        combined_df = combined_df.sort_values('date')
        
        # Save the updated data back to the file
        combined_df.to_csv(HISTORICAL_DATA_PATH, index=False)
        print(f"Successfully saved extended historical data through {combined_df['date'].max().strftime('%Y-%m-%d')}")
        
        return True
        
    except Exception as e:
        print(f"Error extending historical data: {e}")
        import traceback
        traceback.print_exc()
        return False

def check_historical_data_dates():
    """Check the date range available in the historical indicators file"""
    try:
        # Load the historical data
        hist_df = pd.read_csv(HISTORICAL_DATA_PATH)
        
        # Convert date column to datetime
        hist_df['date'] = pd.to_datetime(hist_df['date'])
        
        # Get the date range
        start_date = hist_df['date'].min()
        end_date = hist_df['date'].max()
        
        print(f"Historical data spans from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        return start_date, end_date
    except Exception as e:
        print(f"Error checking historical data dates: {e}")
        return None, None

def check_sector_history_dates():
    """Check the date range available in the sector sentiment history"""
    try:
        # Load the sector sentiment history
        sector_df = pd.read_csv("data/authentic_sector_history.csv")
        
        # Convert date column to datetime
        sector_df['date'] = pd.to_datetime(sector_df['date'])
        
        # Get the date range
        start_date = sector_df['date'].min()
        end_date = sector_df['date'].max()
        
        print(f"Sector sentiment data spans from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        return start_date, end_date
    except Exception as e:
        print(f"Error checking sector history dates: {e}")
        return None, None

def main():
    print("Checking historical data date ranges...")
    hist_start, hist_end = check_historical_data_dates()
    sector_start, sector_end = check_sector_history_dates()
    
    # Extend historical data to include May 1-2, 2025
    success = extend_historical_data()
    
    if success:
        # Reprocess the historical data to calculate sector scores for the new dates
        print("\nRecalculating sector sentiment scores including the new dates...")
        process_historical_data()
        
        # Check the updated sector history dates
        print("\nVerifying updated sector history date range:")
        new_sector_start, new_sector_end = check_sector_history_dates()
        
        if new_sector_end is not None and sector_end is not None and new_sector_end > sector_end:
            print(f"Successfully extended sector sentiment history from {sector_end.strftime('%Y-%m-%d')} to {new_sector_end.strftime('%Y-%m-%d')}")
        elif new_sector_end is not None:
            print(f"Successfully updated sector sentiment history through {new_sector_end.strftime('%Y-%m-%d')}")
        else:
            print("Warning: Sector sentiment history wasn't extended as expected. Check for errors in processing.")
    else:
        print("Failed to extend historical data. Fix the errors above and try again.")

if __name__ == "__main__":
    main()
