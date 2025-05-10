#!/usr/bin/env python3
"""
Verify Market Cap Data
Check if the market cap data has proper daily changes over 30 days
"""
import os
import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
from datetime import datetime

# Configuration
SECTOR_CSV = "data/sector_market_caps.csv"
DB_PATH = "data/t2d_pulse.db"

def check_csv_data():
    """Check if sector market cap CSV file has proper data"""
    if not os.path.exists(SECTOR_CSV):
        print(f"CSV file not found: {SECTOR_CSV}")
        return False
    
    try:
        df = pd.read_csv(SECTOR_CSV)
        print(f"CSV data shape: {df.shape}")
        
        # Check if we have a date column
        if 'Unnamed: 0' in df.columns:
            df = df.rename(columns={'Unnamed: 0': 'Date'})
        
        # Set date as index if it's not already
        if 'Date' in df.columns:
            df = df.set_index('Date')
        
        # Print sample of the data
        print("\nSample of sector market cap data:")
        print(df.head())
        
        # Check number of unique dates
        num_dates = len(df.index.unique())
        print(f"\nNumber of unique dates: {num_dates}")
        
        # Check if values change over time
        print("\nChecking if values change over time:")
        static_sectors = []
        for column in df.columns:
            unique_values = df[column].nunique()
            if unique_values <= 1:
                static_sectors.append(column)
                print(f"  {column}: STATIC (only {unique_values} unique value)")
            else:
                print(f"  {column}: OK ({unique_values} unique values)")
        
        # Summary
        if static_sectors:
            print(f"\nWARNING: {len(static_sectors)} sectors have static values that do not change over time")
            return False
        else:
            print("\nAll sectors have changing values over time - GOOD!")
            return True
    
    except Exception as e:
        print(f"Error checking CSV data: {e}")
        return False

def check_db_data():
    """Check market cap data in SQLite database"""
    if not os.path.exists(DB_PATH):
        print(f"Database not found: {DB_PATH}")
        return False
    
    try:
        conn = sqlite3.connect(DB_PATH)
        
        # Check sector_market_caps table
        sector_data = pd.read_sql("SELECT * FROM sector_market_caps ORDER BY date", conn)
        print(f"\nSector market caps in database: {len(sector_data)} records")
        
        if not sector_data.empty:
            # Check date range
            min_date = sector_data['date'].min()
            max_date = sector_data['date'].max()
            print(f"Date range: {min_date} to {max_date}")
            
            # Check number of sectors
            num_sectors = len(sector_data['sector'].unique())
            print(f"Number of sectors: {num_sectors}")
            
            # Check if we have data for multiple dates
            num_dates = len(sector_data['date'].unique())
            print(f"Number of dates: {num_dates}")
            
            # Sample of the data
            print("\nSample of sector data in database:")
            sample = sector_data.head(10)
            print(sample)
        
        # Check ticker_market_caps table
        ticker_data = pd.read_sql("SELECT * FROM ticker_market_caps ORDER BY date LIMIT 10", conn)
        print(f"\nTicker market caps in database: {pd.read_sql('SELECT COUNT(*) FROM ticker_market_caps', conn).iloc[0, 0]} records")
        
        if not ticker_data.empty:
            print("\nSample of ticker data in database:")
            print(ticker_data)
        
        # Check pulse_values table
        pulse_data = pd.read_sql("SELECT * FROM pulse_values ORDER BY date", conn)
        print(f"\nPulse values in database: {len(pulse_data)} records")
        
        if not pulse_data.empty:
            print("\nSample of pulse data in database:")
            print(pulse_data.head())
        
        # Check share_counts table
        share_counts = pd.read_sql("SELECT * FROM share_counts LIMIT 10", conn)
        print(f"\nShare counts in database: {pd.read_sql('SELECT COUNT(*) FROM share_counts', conn).iloc[0, 0]} records")
        
        if not share_counts.empty:
            print("\nSample of share count data in database:")
            print(share_counts)
        
        conn.close()
        return True
    
    except Exception as e:
        print(f"Error checking database: {e}")
        return False

def plot_sector_trends():
    """Plot sector market cap trends"""
    try:
        if not os.path.exists(SECTOR_CSV):
            print(f"CSV file not found: {SECTOR_CSV}")
            return False
        
        df = pd.read_csv(SECTOR_CSV)
        
        # Check if we have a date column
        if 'Unnamed: 0' in df.columns:
            df = df.rename(columns={'Unnamed: 0': 'Date'})
        
        # Convert date to datetime
        df['Date'] = pd.to_datetime(df['Date'])
        
        # Set date as index
        df = df.set_index('Date')
        
        # Plot the data
        plt.figure(figsize=(12, 8))
        
        for column in df.columns:
            plt.plot(df.index, df[column]/1e12, label=column)
        
        plt.title('Sector Market Cap Trends ($ Trillions)')
        plt.xlabel('Date')
        plt.ylabel('Market Cap ($ Trillions)')
        plt.legend(loc='upper left', bbox_to_anchor=(1, 1))
        plt.grid(True)
        plt.tight_layout()
        
        # Save the plot
        plt.savefig('sector_market_cap_trends.png')
        print("\nSector market cap trend plot saved to sector_market_cap_trends.png")
        
        return True
    
    except Exception as e:
        print(f"Error plotting sector trends: {e}")
        return False

def main():
    """Main function to verify market cap data"""
    print("=== Checking Market Cap Data Quality ===\n")
    print(f"Current time: {datetime.now()}")
    
    print("\n=== Checking CSV Data ===")
    csv_ok = check_csv_data()
    
    print("\n=== Checking Database Data ===")
    db_ok = check_db_data()
    
    print("\n=== Plotting Sector Trends ===")
    plot_ok = plot_sector_trends()
    
    print("\n=== Summary ===")
    if csv_ok and db_ok and plot_ok:
        print("Market cap data looks good!")
    else:
        print("Issues found with market cap data.")
        if not csv_ok:
            print("- Problems with CSV data")
        if not db_ok:
            print("- Problems with database data")
        if not plot_ok:
            print("- Problems creating trend plot")

if __name__ == "__main__":
    main()