#!/usr/bin/env python3
# export_sector_history.py
# -----------------------------------------------------------
# Exports sector sentiment history to Excel file for analysis

import pandas as pd
import sector_sentiment_history
import os
from datetime import datetime, timedelta

def main():
    """
    Export all sector sentiment history to an Excel file
    """
    # Create output directory if needed
    os.makedirs("data", exist_ok=True)
    
    # Get today's date for the filename
    today = datetime.now().strftime("%Y-%m-%d")
    output_file = f"data/sector_sentiment_history_{today}.xlsx"
    
    # Get history for all sectors
    history = sector_sentiment_history.load_sentiment_history()
    
    if not history:
        print("No sector history data available. Please run the dashboard first.")
        return
    
    # Create a DataFrame with all dates in the last 30 days
    end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = end_date - timedelta(days=sector_sentiment_history.HISTORY_LENGTH-1)
    date_range = pd.date_range(start=start_date, end=end_date)
    
    # Initialize DataFrame with dates as index
    result_df = pd.DataFrame(index=date_range)
    
    # Add each sector as a column
    for sector_name, data_points in history.items():
        # Convert to DataFrame
        sector_df = pd.DataFrame(data_points, columns=["date", sector_name])
        sector_df.set_index("date", inplace=True)
        
        # Merge with result DataFrame
        result_df = result_df.join(sector_df)
    
    # Reset index to make 'date' a column
    result_df.reset_index(inplace=True)
    result_df.rename(columns={"index": "date"}, inplace=True)
    
    # Format date column to string for better Excel display
    result_df['date'] = result_df['date'].dt.strftime('%Y-%m-%d')
    
    # Save to Excel
    result_df.to_excel(output_file, index=False)
    print(f"Exported sector sentiment history to {output_file}")
    
    # Additional summary data sheets
    with pd.ExcelWriter(output_file, engine='openpyxl', mode='a') as writer:
        # Create summary statistics sheet
        summary_df = result_df.describe().T
        summary_df.reset_index(inplace=True)
        summary_df.rename(columns={"index": "sector"}, inplace=True)
        summary_df.to_excel(writer, sheet_name='Summary Statistics', index=False)
        
        # Create volatility ranking sheet (based on standard deviation)
        volatility_df = summary_df[['sector', 'std']].sort_values('std', ascending=False)
        volatility_df.rename(columns={"std": "volatility"}, inplace=True)
        volatility_df.to_excel(writer, sheet_name='Volatility Ranking', index=False)
        
        # Create correlation matrix sheet
        corr_df = result_df.drop(columns=['date']).corr()
        corr_df.to_excel(writer, sheet_name='Correlation Matrix')
    
    print(f"Added summary sheets to {output_file}")

if __name__ == "__main__":
    main()