#!/usr/bin/env python3
# export_sector_history_csv.py
# -----------------------------------------------------------
# Exports sector sentiment history to CSV file for analysis

import pandas as pd
import sector_sentiment_history
import os
from datetime import datetime, timedelta

def main():
    """
    Export all sector sentiment history to a CSV file
    """
    # Create output directory if needed
    os.makedirs("data", exist_ok=True)
    
    # Get today's date for the filename
    today = datetime.now().strftime("%Y-%m-%d")
    output_file = f"data/sector_sentiment_history_{today}.csv"
    
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
    
    # Format date column to string for better display
    result_df['date'] = result_df['date'].dt.strftime('%Y-%m-%d')
    
    # Save to CSV
    result_df.to_csv(output_file, index=False)
    print(f"Exported sector sentiment history to {output_file}")
    
    # Also create a text file with basic statistics
    stats_file = f"data/sector_statistics_{today}.txt"
    with open(stats_file, 'w') as f:
        # Write summary statistics
        f.write("SECTOR SENTIMENT STATISTICS\n")
        f.write("===========================\n\n")
        f.write(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}\n\n")
        
        # Calculate and write sector statistics
        f.write("Sector Averages:\n")
        f.write("---------------\n")
        for column in result_df.columns:
            if column != 'date':
                avg = result_df[column].mean()
                min_val = result_df[column].min()
                max_val = result_df[column].max()
                std_val = result_df[column].std()
                f.write(f"{column}: Avg={avg:.2f}, Min={min_val:.2f}, Max={max_val:.2f}, StdDev={std_val:.2f}\n")
        
        # Calculate correlation matrix
        f.write("\nCorrelation Matrix:\n")
        f.write("-----------------\n")
        corr_matrix = result_df.drop(columns=['date']).corr().round(2)
        
        # Format correlation matrix for text display
        sector_names = corr_matrix.columns
        header = "Sector".ljust(25) + " | " + " | ".join([s[:10].ljust(10) for s in sector_names])
        f.write(header + "\n")
        f.write("-" * len(header) + "\n")
        
        for sector in sector_names:
            row = sector[:25].ljust(25) + " | "
            row += " | ".join([f"{corr_matrix.loc[sector, col]:.2f}".ljust(10) for col in sector_names])
            f.write(row + "\n")
    
    print(f"Exported sector statistics to {stats_file}")

if __name__ == "__main__":
    main()