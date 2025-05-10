"""
Create a 30-day market cap table for all sectors in the T2D Pulse dashboard.
"""

import os
import pandas as pd
import json
from datetime import datetime, timedelta
import numpy as np
import glob

def find_market_cap_files():
    """Find all market cap related files in the project"""
    market_cap_files = []
    
    # Look for CSV files with market cap data
    for file in glob.glob("**/*market*cap*.csv", recursive=True):
        market_cap_files.append(file)
    
    # Look for parquet files with market cap data
    for file in glob.glob("**/*market*cap*.parquet", recursive=True):
        market_cap_files.append(file)
    
    # Look for history files that might contain market cap data
    for file in glob.glob("**/*history*.csv", recursive=True):
        if "market" in file.lower() or "cap" in file.lower():
            market_cap_files.append(file)
    
    # Look for sector files that might contain market cap data
    for file in glob.glob("**/*sector*.csv", recursive=True):
        if "market" in file.lower() or "cap" in file.lower():
            market_cap_files.append(file)
    
    return market_cap_files

def extract_sector_market_caps():
    """Extract sector market cap data from all available files"""
    market_cap_files = find_market_cap_files()
    print(f"Found {len(market_cap_files)} potential market cap files")
    
    # Dictionary to store sector market caps by date
    sector_data = {}
    
    # Target sector names (standardized)
    target_sectors = [
        'SMB SaaS', 'Enterprise SaaS', 'Cloud Infrastructure', 'AdTech', 
        'Fintech', 'Consumer Internet', 'eCommerce', 'Cybersecurity', 
        'Dev Tools / Analytics', 'Semiconductors', 'AI Infrastructure', 
        'Vertical SaaS', 'IT Services / Legacy Tech', 'Hardware / Devices'
    ]
    
    # Mapping for sector name variations
    sector_mapping = {
        'IT Services': 'IT Services / Legacy Tech',
        'Hardware/Devices': 'Hardware / Devices',
        'Dev Tools': 'Dev Tools / Analytics',
        'Dev Tools___Analytics': 'Dev Tools / Analytics'
    }
    
    # Process each file
    for file_path in market_cap_files:
        try:
            # Load the file
            if file_path.endswith('.csv'):
                df = pd.read_csv(file_path)
            elif file_path.endswith('.parquet'):
                df = pd.read_parquet(file_path)
            else:
                continue
            
            print(f"Processing {file_path}, columns: {df.columns.tolist()}")
            
            # Look for date column or similar
            date_col = None
            if 'date' in df.columns:
                date_col = 'date'
            elif 'Date' in df.columns:
                date_col = 'Date'
            elif 'Unnamed: 0' in df.columns and pd.to_datetime(df['Unnamed: 0'], errors='coerce').notna().all():
                date_col = 'Unnamed: 0'
                
            # Skip if no date column
            if date_col is None:
                print(f"No date column found in {file_path}, skipping")
                continue
                
            # Standardize date format
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            # Skip if date parsing failed
            if df[date_col].isna().all():
                print(f"Failed to parse dates in {file_path}, skipping")
                continue
                
            df[date_col] = df[date_col].dt.strftime('%Y-%m-%d')
            
            # Format 1: sector/market_cap columns
            if 'sector' in df.columns and ('market_cap' in df.columns or 'marketcap' in df.columns):
                print(f"Found sector/market_cap format in {file_path}")
                
                # Standardize column names
                if 'marketcap' in df.columns and 'market_cap' not in df.columns:
                    df['market_cap'] = df['marketcap']
                
                # Process each sector in the file
                for sector_name, sector_group in df.groupby('sector'):
                    # Standardize sector name if needed
                    if sector_name in sector_mapping:
                        sector_name = sector_mapping[sector_name]
                        
                    if sector_name not in sector_data:
                        sector_data[sector_name] = {}
                    
                    # Add market cap for each date
                    for _, row in sector_group.iterrows():
                        date_str = row[date_col]
                        market_cap = row['market_cap']
                        sector_data[sector_name][date_str] = market_cap
            
            # Format 2: Sector names as columns (wide format)
            else:
                # Find sector columns
                sector_cols = []
                for col in df.columns:
                    # Skip date and metadata columns
                    if col == date_col or col in ['Day', 'Month', 'Year', 'Total']:
                        continue
                    
                    # Skip weight columns
                    if '_weight_' in col or '_pct' in col:
                        continue
                        
                    # Check if it's a sector name or variation
                    normalized_col = col
                    if col in sector_mapping:
                        normalized_col = sector_mapping[col]
                        
                    if normalized_col in target_sectors or any(s in col for s in ['SaaS', 'Tech', 'Security', 'Commerce', 'Internet', 'Infrastructure']):
                        sector_cols.append(col)
                
                if sector_cols:
                    print(f"Found {len(sector_cols)} sector columns in {file_path}")
                    
                    # Process each sector column
                    for sector_col in sector_cols:
                        # Get standardized sector name
                        sector_name = sector_col
                        if sector_col in sector_mapping:
                            sector_name = sector_mapping[sector_col]
                            
                        if sector_name not in sector_data:
                            sector_data[sector_name] = {}
                        
                        # Process rows
                        for _, row in df.iterrows():
                            date_str = row[date_col]
                            
                            # Skip if date is invalid
                            if not isinstance(date_str, str) or len(date_str) < 8:
                                continue
                                
                            # Skip if sector value is missing
                            if pd.isna(row[sector_col]):
                                continue
                                
                            # Get market cap value
                            market_cap = row[sector_col]
                            
                            # Handle string formats like '1.23T'
                            if isinstance(market_cap, str):
                                market_cap = market_cap.strip()
                                if market_cap.endswith('T'):
                                    try:
                                        market_cap = float(market_cap.replace('T', '')) * 1_000_000_000_000
                                    except ValueError:
                                        continue
                                elif market_cap.endswith('B'):
                                    try:
                                        market_cap = float(market_cap.replace('B', '')) * 1_000_000_000
                                    except ValueError:
                                        continue
                                elif market_cap.endswith('M'):
                                    try:
                                        market_cap = float(market_cap.replace('M', '')) * 1_000_000
                                    except ValueError:
                                        continue
                                else:
                                    try:
                                        market_cap = float(market_cap)
                                    except ValueError:
                                        continue
                            
                            # Store in sector data
                            sector_data[sector_name][date_str] = market_cap
        
        except Exception as e:
            print(f"Error processing {file_path}: {e}")
    
    return sector_data

def create_30day_table(sector_data):
    """Create a table with market cap data for the past 30 days"""
    if not sector_data:
        print("No sector market cap data found")
        return None
    
    # Get all unique dates
    all_dates = set()
    for sector in sector_data:
        all_dates.update(sector_data[sector].keys())
    
    # Sort dates and take the most recent 30
    all_dates = sorted(list(all_dates), reverse=True)
    if len(all_dates) > 30:
        all_dates = all_dates[:30]
    
    # Get all sectors
    all_sectors = sorted(list(sector_data.keys()))
    
    # Create DataFrame for the table
    # Start with date column
    data = {'Date': all_dates}
    
    # Add column for each sector
    for sector in all_sectors:
        sector_values = []
        for date in all_dates:
            if date in sector_data[sector]:
                # Format as trillions with 2 decimal places
                market_cap = sector_data[sector][date]
                market_cap_t = market_cap / 1_000_000_000_000  # Convert to trillions
                sector_values.append(f"{market_cap_t:.2f}T")
            else:
                sector_values.append("N/A")
        data[sector] = sector_values
    
    # Create DataFrame
    df = pd.DataFrame(data)
    
    return df

def save_market_cap_table(df):
    """Save market cap table to various formats"""
    if df is None:
        return
    
    # Ensure data directory exists
    os.makedirs('data', exist_ok=True)
    
    # Current date for filenames
    today = datetime.now().strftime('%Y-%m-%d')
    
    # Save to CSV
    csv_file = f'data/sector_marketcap_30day_table_{today}.csv'
    df.to_csv(csv_file, index=False)
    print(f"Saved market cap table to CSV: {csv_file}")
    
    # Save to standard CSV filename
    std_csv_file = 'data/sector_marketcap_30day_table.csv'
    df.to_csv(std_csv_file, index=False)
    print(f"Saved market cap table to CSV: {std_csv_file}")
    
    # Save to Excel
    excel_file = f'data/sector_marketcap_30day_table_{today}.xlsx'
    df.to_excel(excel_file, index=False)
    print(f"Saved market cap table to Excel: {excel_file}")
    
    # Save to standard Excel filename
    std_excel_file = 'data/sector_marketcap_30day_table.xlsx'
    df.to_excel(std_excel_file, index=False)
    print(f"Saved market cap table to Excel: {std_excel_file}")
    
    return {
        'csv': csv_file,
        'excel': excel_file,
        'std_csv': std_csv_file,
        'std_excel': std_excel_file
    }

def main():
    """Main function to create sector market cap table"""
    # Extract sector market cap data
    sector_data = extract_sector_market_caps()
    
    # Create 30-day table
    df = create_30day_table(sector_data)
    
    # Save table to various formats
    output_files = save_market_cap_table(df)
    
    if output_files:
        print(f"\nMarket cap table created successfully!")
        print(f"CSV file: {output_files['csv']}")
        print(f"Excel file: {output_files['excel']}")
        print()
        
        # Print the table to the console for immediate viewing
        if df is not None:
            print("\n30-Day Market Cap Table:")
            print(df.head(10).to_string(index=False))
            print("...")
            
            # Return the dataframe for use in other scripts
            return df
    else:
        print("Failed to create market cap table")
        return None

if __name__ == "__main__":
    main()