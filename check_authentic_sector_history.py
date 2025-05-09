#!/usr/bin/env python3
# check_authentic_sector_history.py
# -----------------------------------------------------------
# Check the authentic sector history data to see which sectors and dates have data

import os
import pandas as pd
from datetime import datetime, timedelta
import config

def main():
    """Check authentic sector history data"""
    authentic_history_file = os.path.join('data', 'authentic_sector_history.csv')
    
    if not os.path.exists(authentic_history_file):
        print(f"Error: Authentic sector history file not found at {authentic_history_file}")
        return
    
    try:
        # Load authentic sector history
        authentic_df = pd.read_csv(authentic_history_file)
        print(f"Loaded authentic sector history with {len(authentic_df)} rows")
        
        # Check columns
        print(f"Columns: {authentic_df.columns.tolist()}")
        
        # Convert date column to datetime
        if 'Date' in authentic_df.columns:
            authentic_df['Date'] = pd.to_datetime(authentic_df['Date'])
            
            # Get date range
            min_date = authentic_df['Date'].min()
            max_date = authentic_df['Date'].max()
            print(f"Date range: {min_date.strftime('%Y-%m-%d')} to {max_date.strftime('%Y-%m-%d')}")
            
            # Check the most recent rows
            print("\nMost recent rows:")
            recent_df = authentic_df.sort_values('Date', ascending=False).head(5)
            print(recent_df)
            
            # Check if all sectors have data for the most recent date
            most_recent_date = authentic_df['Date'].max()
            most_recent_row = authentic_df[authentic_df['Date'] == most_recent_date].iloc[0]
            
            print(f"\nMost recent data ({most_recent_date.strftime('%Y-%m-%d')}):")
            
            all_sectors = config.SECTORS.keys()
            sectors_with_data = []
            sectors_without_data = []
            
            for sector in all_sectors:
                if sector in most_recent_row and not pd.isna(most_recent_row[sector]):
                    sectors_with_data.append((sector, most_recent_row[sector]))
                else:
                    sectors_without_data.append(sector)
            
            print(f"\nSectors with data ({len(sectors_with_data)}/{len(all_sectors)}):")
            for sector, score in sectors_with_data:
                print(f"  {sector}: {score}")
            
            if sectors_without_data:
                print(f"\nSectors without data ({len(sectors_without_data)}/{len(all_sectors)}):")
                for sector in sectors_without_data:
                    print(f"  {sector}")
                    
            # Check specifically for IT Services
            it_services_sector = "IT Services / Legacy Tech"
            if it_services_sector in most_recent_row and not pd.isna(most_recent_row[it_services_sector]):
                print(f"\nIT Services sector score for {most_recent_date.strftime('%Y-%m-%d')}: {most_recent_row[it_services_sector]}")
            else:
                print(f"\nIT Services sector has no data for {most_recent_date.strftime('%Y-%m-%d')}")
                
            # Check historical data for IT Services
            print("\nHistorical IT Services sector scores:")
            for i, row in authentic_df.sort_values('Date', ascending=False).head(10).iterrows():
                if it_services_sector in row and not pd.isna(row[it_services_sector]):
                    print(f"  {row['Date'].strftime('%Y-%m-%d')}: {row[it_services_sector]}")
                else:
                    print(f"  {row['Date'].strftime('%Y-%m-%d')}: No data")
                    
            # Check current_pulse_score.txt
            current_pulse_file = os.path.join('data', 'current_pulse_score.txt')
            if os.path.exists(current_pulse_file):
                with open(current_pulse_file, 'r') as f:
                    current_pulse = f.read().strip()
                print(f"\nCurrent pulse score from file: {current_pulse}")
            else:
                print("\nNo current_pulse_score.txt file found")
        else:
            print("Error: No 'Date' column found in authentic sector history")
            
    except Exception as e:
        print(f"Error processing authentic sector history: {e}")

if __name__ == "__main__":
    main()