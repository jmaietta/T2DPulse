#!/usr/bin/env python3
# process_jm_historical_data.py
# -----------------------------------------------------------
# Process JM's historical indicator data and calculate authentic sector sentiment scores

import pandas as pd
import numpy as np
import os
from datetime import datetime
import sentiment_engine

# Path to the uploaded historical indicator data
HISTORICAL_DATA_PATH = "data/Historical_Indicator_Data.csv"
OUTPUT_CSV_PATH = "data/authentic_sector_history.csv"
OUTPUT_EXCEL_PATH = "data/sector_sentiment_history.xlsx" 

def calculate_ema(series, span):
    """Calculate Exponential Moving Average for a series"""
    return series.ewm(span=span, adjust=False).mean()

def clean_percentage(value):
    """Convert percentage string to float value"""
    if isinstance(value, str):
        # Remove the % symbol and any surrounding whitespace
        cleaned = value.strip().rstrip('%')
        return float(cleaned)
    return value

def clean_nasdaq_value(value):
    """Clean NASDAQ value by removing commas and quotes"""
    if isinstance(value, str):
        return float(value.replace(',', '').replace('"', ''))
    return value

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
    return macro_dict

def calculate_historical_scores():
    """Calculate historical sector scores using authentic data"""
    # Load the historical data
    df = load_historical_data()
    
    if df is None or df.empty:
        print("No historical data available")
        return None
    
    # DataFrame to store results
    results = {'date': []}
    
    # Initialize a column for each sector
    for sector in sentiment_engine.SECTORS:
        results[sector] = []
    
    # Process each date
    for idx, row in df.iterrows():
        date_str = row['date'].strftime('%Y-%m-%d')
        print(f"Processing scores for {date_str}")
        
        # Convert row to macro dictionary
        macro_dict = prepare_macro_dict(row)
        
        # Calculate sector scores
        try:
            sector_scores = sentiment_engine.score_sectors(macro_dict)
            
            # Add date to results
            results['date'].append(row['date'])
            
            # Process each sector score
            for sector_data in sector_scores:
                sector_name = sector_data['sector']
                raw_score = sector_data['score']
                
                # Convert raw score from [-1,1] to [0-100] for display
                normalized_score = ((raw_score + 1.0) / 2.0) * 100
                
                # Add to results
                results[sector_name].append(normalized_score)
                
                # Print debug info for the first few sectors
                if sector_name in ['AdTech', 'SMB SaaS', 'Enterprise SaaS']:
                    print(f"  {sector_name}: raw={raw_score:.2f}, normalized={normalized_score:.1f}")
                
        except Exception as e:
            print(f"Error calculating scores for {date_str}: {e}")
    
    # Convert results to DataFrame
    results_df = pd.DataFrame(results)
    
    return results_df

def export_historical_scores(df):
    """Export historical scores to CSV and Excel"""
    if df is None or df.empty:
        print("No data to export")
        return False
    
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(OUTPUT_CSV_PATH), exist_ok=True)
    
    # Format date column to string for better display
    df_export = df.copy()
    df_export['date'] = df_export['date'].dt.strftime('%Y-%m-%d')
    
    # Save to CSV
    df_export.to_csv(OUTPUT_CSV_PATH, index=False)
    print(f"Exported historical scores to {OUTPUT_CSV_PATH}")
    
    # Save to Excel
    try:
        # Create a Pandas Excel writer
        writer = pd.ExcelWriter(OUTPUT_EXCEL_PATH, engine='openpyxl')
        
        # Write the DataFrame to Excel
        df_export.to_excel(writer, sheet_name='Sector Sentiment History', index=False)
        
        # Close the Pandas Excel writer to save the file
        writer.close()
        print(f"Exported historical scores to {OUTPUT_EXCEL_PATH}")
        
        # Save the file that matches the expected filename format in the dashboard
        today = datetime.now().strftime("%Y-%m-%d")
        today_excel_path = f"data/sector_sentiment_history_{today}.xlsx"
        df_export.to_excel(today_excel_path, index=False)
        print(f"Also saved to {today_excel_path} for dashboard access")
        
        return True
    except Exception as e:
        print(f"Error saving to Excel: {e}")
        return False

def update_predefined_data_file():
    """Update the predefined_sector_history.csv file with the authentic historical scores"""
    try:
        # Check if authentic_sector_history.csv exists
        if not os.path.exists(OUTPUT_CSV_PATH):
            print(f"Error: {OUTPUT_CSV_PATH} doesn't exist yet")
            return False
        
        # Copy to predefined_sector_history.csv
        authentic_df = pd.read_csv(OUTPUT_CSV_PATH)
        authentic_df.to_csv("data/predefined_sector_history.csv", index=False)
        print("Updated predefined_sector_history.csv with authentic historical scores")
        return True
    except Exception as e:
        print(f"Error updating predefined data file: {e}")
        return False

def main():
    """Main function to process historical data and generate sector scores"""
    print("Processing JM's historical indicator data...")
    
    # Calculate historical sector scores
    historical_scores = calculate_historical_scores()
    
    if historical_scores is not None and not historical_scores.empty:
        # Export results
        export_historical_scores(historical_scores)
        
        # Update predefined data file
        update_predefined_data_file()
        
        print("Done! Authentic historical sector scores have been generated.")
    else:
        print("Failed to generate historical sector scores.")

if __name__ == "__main__":
    main()