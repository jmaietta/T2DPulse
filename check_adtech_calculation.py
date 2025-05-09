"""
Check AdTech Market Cap Calculation

This script examines the raw data behind the AdTech market cap calculation
to determine why there's a discrepancy.
"""
import pandas as pd
import os
import json

def examine_adtech_calculation():
    """Examine the AdTech market cap calculation in detail"""
    # First check the raw parquet file
    print("Checking raw market cap data...")
    try:
        if os.path.exists("data/sector_market_caps.parquet"):
            df = pd.read_parquet("data/sector_market_caps.parquet")
            
            # Basic info
            print(f"Data shape: {df.shape}")
            print(f"Column names: {df.columns.tolist()}")
            print(f"Index type: {type(df.index)}")
            print(f"Date range: {df.index.min()} to {df.index.max()}")
            
            # Get AdTech data specifically
            if 'AdTech' in df.columns:
                print("\nAdTech market cap data:")
                adtech_series = df['AdTech']
                print(f"Latest date: {adtech_series.index[-1]}")
                print(f"Latest value: ${adtech_series.iloc[-1] / 1e9:.2f} billion")
                
                # Show last 5 days of AdTech data
                print("\nLast 5 days of AdTech data:")
                last_5_days = adtech_series.iloc[-5:] / 1e9
                for date, mcap in last_5_days.items():
                    print(f"{date}: ${mcap:.2f} billion")
            else:
                print("Error: 'AdTech' column not found in the DataFrame")

            # Check for any related "_weight_pct" columns
            weight_cols = [col for col in df.columns if '_weight_pct' in col]
            if weight_cols:
                print("\nWeight percentage columns found:")
                for col in weight_cols:
                    if col == 'AdTech_weight_pct':
                        print(f"{col}: latest value = {df[col].iloc[-1]}")
            
            # Get all columns with their latest values
            print("\nAll sectors' latest market caps:")
            latest_row = df.iloc[-1] / 1e9  # Convert to billions
            for col in df.columns:
                if not col.endswith('_weight_pct'):
                    print(f"{col}: ${latest_row[col]:.2f} billion")
            
        else:
            print("Error: Market cap parquet file not found")
    except Exception as e:
        print(f"Error reading parquet file: {e}")
    
    # Look for any sector mapping/configuration files
    print("\nChecking for sector configuration files...")
    potential_config_files = [
        "sector_config.json",
        "sector_mappings.json",
        "data/sector_config.json",
        "data/sector_mappings.json",
        "sector_tickers.json",
        "data/sector_tickers.json"
    ]
    
    for config_file in potential_config_files:
        if os.path.exists(config_file):
            print(f"Found config file: {config_file}")
            try:
                with open(config_file, 'r') as f:
                    config = json.load(f)
                if isinstance(config, dict) and 'AdTech' in config:
                    print(f"AdTech configuration: {config['AdTech']}")
            except Exception as e:
                print(f"Error reading {config_file}: {e}")
    
    # Check for recent polygon data collection logs
    print("\nChecking for recent market cap collection logs...")
    log_files = [f for f in os.listdir('.') if f.endswith('.log') and ('market' in f or 'polygon' in f)]
    for log_file in log_files:
        print(f"Found log file: {log_file}")
        try:
            with open(log_file, 'r') as f:
                # Read last 50 lines
                lines = f.readlines()[-50:]
                adtech_lines = [line for line in lines if 'AdTech' in line]
                if adtech_lines:
                    print(f"Recent AdTech log entries:")
                    for line in adtech_lines:
                        print(f"  {line.strip()}")
        except Exception as e:
            print(f"Error reading {log_file}: {e}")
    
    # Also check if there's a polygon_sector_caps.py file that might have the logic
    if os.path.exists("polygon_sector_caps.py"):
        print("\nFound polygon_sector_caps.py - examining logic...")
        try:
            with open("polygon_sector_caps.py", 'r') as f:
                content = f.read()
                # Extract AdTech related code snippets
                lines = content.split('\n')
                adtech_lines = [line.strip() for line in lines if 'AdTech' in line]
                if adtech_lines:
                    print("AdTech related code snippets:")
                    for line in adtech_lines:
                        print(f"  {line}")
        except Exception as e:
            print(f"Error reading polygon_sector_caps.py: {e}")

if __name__ == "__main__":
    print("Examining AdTech market cap calculation...")
    examine_adtech_calculation()
    print("\nAnalysis complete.")