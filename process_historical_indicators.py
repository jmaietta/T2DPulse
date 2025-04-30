#!/usr/bin/env python3
# process_historical_indicators.py
# -----------------------------------------------------------
# Process historical indicator data from an Excel file and calculate sector sentiment scores

import pandas as pd
import numpy as np
import os
from datetime import datetime
import sentiment_engine

# Path to the uploaded historical indicator data
HISTORICAL_DATA_PATH = "data/Historical_Indicator_Data.xlsx" 

def calculate_ema(series, span=20):
    """Calculate Exponential Moving Average for a series"""
    return series.ewm(span=span, adjust=False).mean()

def calculate_nasdaq_gap(nasdaq_df):
    """
    Calculate the NASDAQ 20-day gap percentage (difference between price and 20-day EMA)
    
    Args:
        nasdaq_df: DataFrame with 'date' and 'value' columns for NASDAQ data
        
    Returns:
        DataFrame with 'date', 'value', 'ema20', and 'gap_pct' columns
    """
    # Ensure data is sorted by date
    nasdaq_df = nasdaq_df.sort_values('date')
    
    # Calculate 20-day EMA
    nasdaq_df['ema20'] = calculate_ema(nasdaq_df['value'], span=20)
    
    # Calculate percentage gap between price and EMA
    nasdaq_df['gap_pct'] = ((nasdaq_df['value'] - nasdaq_df['ema20']) / nasdaq_df['ema20']) * 100
    
    return nasdaq_df

def calculate_vix_ema(vix_df):
    """
    Calculate 14-day EMA for VIX data
    
    Args:
        vix_df: DataFrame with 'date' and 'value' columns for VIX data
        
    Returns:
        DataFrame with 'date', 'value', and 'ema14' columns
    """
    # Ensure data is sorted by date
    vix_df = vix_df.sort_values('date')
    
    # Calculate 14-day EMA
    vix_df['ema14'] = calculate_ema(vix_df['value'], span=14)
    
    return vix_df

def load_historical_indicators():
    """
    Load historical indicator data from the Excel file
    
    Returns:
        dict: Dictionary of DataFrames for each indicator
    """
    if not os.path.exists(HISTORICAL_DATA_PATH):
        print(f"Error: Historical data file not found at {HISTORICAL_DATA_PATH}")
        return None
    
    try:
        # Try to read the Excel file
        xls = pd.ExcelFile(HISTORICAL_DATA_PATH)
        
        # Dictionary to store DataFrames for each indicator
        indicators = {}
        
        # Parse each sheet
        for sheet_name in xls.sheet_names:
            # Skip any non-data sheets (like intro or instructions)
            if sheet_name.lower() in ['intro', 'instructions', 'readme']:
                continue
                
            # Read the sheet into a DataFrame
            df = pd.read_excel(xls, sheet_name)
            
            # Process specific indicators
            if "nasdaq" in sheet_name.lower():
                # Process NASDAQ data - calculate the gap percentage
                df = calculate_nasdaq_gap(df)
                indicators["NASDAQ"] = df
                
            elif "vix" in sheet_name.lower():
                # Process VIX data - calculate the EMA
                df = calculate_vix_ema(df)
                indicators["VIX"] = df
                
            else:
                # Store other indicators as is
                indicators[sheet_name] = df
                
        return indicators
        
    except Exception as e:
        print(f"Error loading historical indicators: {e}")
        return None

def map_indicators_to_macros(indicators, date):
    """
    Map indicators to the macro dictionary format expected by the sentiment engine
    
    Args:
        indicators (dict): Dictionary of indicator DataFrames
        date (datetime): The date to get values for
        
    Returns:
        dict: Macro dictionary for the sentiment engine
    """
    macro_dict = {}
    
    # Convert date to pandas Timestamp for comparison
    target_date = pd.Timestamp(date)
    
    # Map each indicator to the expected keys in the macro dictionary
    for name, df in indicators.items():
        try:
            # Ensure 'date' column is datetime
            if not pd.api.types.is_datetime64_any_dtype(df['date']):
                df['date'] = pd.to_datetime(df['date'])
            
            # Find the closest date to the target date
            df['date_diff'] = abs(df['date'] - target_date)
            closest_row = df.loc[df['date_diff'].idxmin()]
            
            # Map specific indicators to the expected keys
            if name == "NASDAQ":
                macro_dict["NASDAQ_20d_gap_%"] = closest_row.get('gap_pct')
            elif name == "VIX":
                # Use the EMA if available, otherwise use the raw value
                if 'ema14' in df.columns:
                    macro_dict["VIX"] = closest_row.get('ema14')
                else:
                    macro_dict["VIX"] = closest_row.get('value')
            elif name == "Treasury_Yield":
                macro_dict["10Y_Treasury_Yield_%"] = closest_row.get('value')
            elif name == "Fed_Funds_Rate":
                macro_dict["Fed_Funds_Rate_%"] = closest_row.get('value')
            elif name == "CPI":
                macro_dict["CPI_YoY_%"] = closest_row.get('value')
            elif name == "PCEPI":
                macro_dict["PCEPI_YoY_%"] = closest_row.get('value')
            elif name == "GDP":
                macro_dict["Real_GDP_Growth_%_SAAR"] = closest_row.get('value')
            elif name == "PCE":
                macro_dict["Real_PCE_YoY_%"] = closest_row.get('value')
            elif name == "Unemployment":
                macro_dict["Unemployment_%"] = closest_row.get('value')
            elif name == "Job_Postings":
                macro_dict["Software_Dev_Job_Postings_YoY_%"] = closest_row.get('value')
            elif name == "Data_PPI":
                macro_dict["PPI_Data_Processing_YoY_%"] = closest_row.get('value')
            elif name == "Software_PPI":
                macro_dict["PPI_Software_Publishers_YoY_%"] = closest_row.get('value')
            elif name == "Consumer_Sentiment":
                macro_dict["Consumer_Sentiment"] = closest_row.get('value')
            else:
                # Store any additional indicators with their original names
                macro_dict[name] = closest_row.get('value')
                
        except Exception as e:
            print(f"Error mapping indicator {name} for date {date}: {e}")
            
    return macro_dict

def calculate_historical_sector_scores(start_date, end_date):
    """
    Calculate historical sector sentiment scores for a date range
    
    Args:
        start_date (datetime): Start date
        end_date (datetime): End date
        
    Returns:
        DataFrame: DataFrame with historical sector scores
    """
    # Load the historical indicators
    indicators = load_historical_indicators()
    
    if not indicators:
        print("No indicator data available")
        return None
    
    # Create a date range (business days only)
    date_range = pd.bdate_range(start=start_date, end=end_date)
    
    # Dictionary to store results for each date
    results = {'date': []}
    
    # Initialize a column for each sector
    for sector in sentiment_engine.SECTORS:
        results[sector] = []
    
    # Process each date
    for date in date_range:
        # Convert to datetime.date for printing
        date_str = date.strftime('%Y-%m-%d')
        print(f"Processing scores for {date_str}")
        
        # Get the macro values for this date
        macro_dict = map_indicators_to_macros(indicators, date)
        
        # Skip dates with insufficient data
        if len(macro_dict) < 5:
            print(f"  Insufficient data for {date_str}, skipping")
            continue
        
        # Calculate sector scores using the sentiment engine
        try:
            sector_scores = sentiment_engine.score_sectors(macro_dict)
            
            # Add date to results
            results['date'].append(date)
            
            # Process each sector score
            for sector_data in sector_scores:
                sector_name = sector_data['sector']
                raw_score = sector_data['score']
                
                # Convert raw score from [-1,1] to [0,100] for display
                normalized_score = ((raw_score + 1.0) / 2.0) * 100
                
                # Add to results
                results[sector_name].append(normalized_score)
                
        except Exception as e:
            print(f"  Error calculating scores for {date_str}: {e}")
    
    # Convert results to DataFrame
    results_df = pd.DataFrame(results)
    
    return results_df

def export_historical_scores_to_csv(df, output_path="data/authentic_historical_scores.csv"):
    """
    Export historical scores to CSV
    
    Args:
        df (DataFrame): DataFrame with historical scores
        output_path (str): Path to save the CSV file
    """
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Save to CSV
    df.to_csv(output_path, index=False)
    print(f"Exported historical scores to {output_path}")

def export_historical_scores_to_excel(df, output_path="data/authentic_historical_scores.xlsx"):
    """
    Export historical scores to Excel
    
    Args:
        df (DataFrame): DataFrame with historical scores
        output_path (str): Path to save the Excel file
    """
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Create a Pandas Excel writer using XlsxWriter
    writer = pd.ExcelWriter(output_path, engine='openpyxl')
    
    # Write the DataFrame to Excel
    df.to_excel(writer, sheet_name='Historical Sector Scores', index=False)
    
    # Format dates
    if 'date' in df.columns:
        # Get the worksheet
        worksheet = writer.sheets['Historical Sector Scores']
        
        # Format the date column
        date_format = writer.book.add_format({'num_format': 'yyyy-mm-dd'})
        worksheet.set_column('A:A', 12, date_format)
        
        # Set column widths for other columns
        for i in range(1, len(df.columns)):
            worksheet.set_column(i, i, 15)
    
    # Close the Pandas Excel writer
    writer.close()
    print(f"Exported historical scores to {output_path}")

def main():
    """
    Main function to process historical indicator data and generate sector scores
    """
    # Specify date range (e.g., last 30 days)
    end_date = datetime.now()
    start_date = end_date - pd.DateOffset(days=30)
    
    # Calculate historical sector scores
    historical_scores = calculate_historical_sector_scores(start_date, end_date)
    
    if historical_scores is not None:
        # Export to CSV
        export_historical_scores_to_csv(historical_scores)
        
        # Export to Excel
        export_historical_scores_to_excel(historical_scores)
    
if __name__ == "__main__":
    main()