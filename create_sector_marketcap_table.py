#!/usr/bin/env python3
# create_sector_marketcap_table.py
# -----------------------------------------------------------
# Creates a table of sector market capitalization data by day

import pandas as pd
import os
import logging
from datetime import datetime
import numpy as np

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Define sector ticker mapping
SECTOR_TICKERS = {
    "SMB SaaS": ["BILL", "PAYC", "DDOG"],
    "Enterprise SaaS": ["CRM", "NOW", "ADBE"],
    "Cloud Infrastructure": ["AMZN", "MSFT", "GOOG"],
    "AdTech": ["TTD", "PUBM", "GOOGL"],
    "Fintech": ["SQ", "PYPL", "ADYEY"],
    "Consumer Internet": ["META", "GOOGL", "PINS"],
    "eCommerce": ["AMZN", "SHOP", "SE"],
    "Cybersecurity": ["PANW", "FTNT", "CRWD"],
    "Dev Tools / Analytics": ["SNOW", "DDOG", "ESTC"],
    "Semiconductors": ["NVDA", "AMD", "AVGO"],
    "AI Infrastructure": ["NVDA", "AMD", "SMCI"],
    "Vertical SaaS": ["VEEV", "TYL", "WDAY"],
    "IT Services / Legacy Tech": ["IBM", "ACN", "DXC"],
    "Hardware / Devices": ["AAPL", "DELL", "HPQ"]
}

def load_marketcap_data():
    """Load historical ticker market cap data"""
    try:
        marketcap_path = 'data/historical_ticker_marketcap.csv'
        if not os.path.exists(marketcap_path):
            logging.error(f"Market cap data file not found: {marketcap_path}")
            return None
            
        # Load data
        df = pd.read_csv(marketcap_path, index_col=0)
        
        # Ensure index is datetime
        df.index = pd.to_datetime(df.index)
        
        # Filter out weekends (0=Monday, 6=Sunday)
        df = df[df.index.dayofweek < 5]
        
        logging.info(f"Loaded market cap data with {len(df)} days and {len(df.columns)} tickers")
        return df
    except Exception as e:
        logging.error(f"Error loading market cap data: {e}")
        return None

def create_sector_marketcap_table():
    """Create a table with market cap by sector and date"""
    # Load market cap data
    marketcap_df = load_marketcap_data()
    if marketcap_df is None:
        return None
        
    # Create a new dataframe for sector market caps
    dates = marketcap_df.index
    sector_names = list(SECTOR_TICKERS.keys())
    
    # Initialize dataframe with zeros
    sector_marketcap = pd.DataFrame(0, index=dates, columns=sector_names)
    
    # For each sector, calculate total market cap of its tickers
    for sector, tickers in SECTOR_TICKERS.items():
        # Get available tickers (those that exist in our data)
        available_tickers = [ticker for ticker in tickers if ticker in marketcap_df.columns]
        
        if not available_tickers:
            logging.warning(f"No data available for {sector} tickers: {tickers}")
            continue
            
        # Sum market caps for each day
        sector_marketcap[sector] = marketcap_df[available_tickers].sum(axis=1)
    
    # Format market cap values to billions for readability
    formatted_df = sector_marketcap.copy()
    for col in formatted_df.columns:
        formatted_df[col] = formatted_df[col] / 1_000_000_000
        
    # Reset index to make date a column
    formatted_df = formatted_df.reset_index()
    formatted_df = formatted_df.rename(columns={"index": "Date"})
    
    # Format date column
    formatted_df["Date"] = formatted_df["Date"].dt.strftime('%Y-%m-%d')
    
    return formatted_df

def save_to_csv(df, filename="sector_marketcap_table.csv"):
    """Save dataframe to CSV file"""
    try:
        df.to_csv(filename, index=False)
        logging.info(f"Saved sector market cap table to {filename}")
        return True
    except Exception as e:
        logging.error(f"Error saving to CSV: {e}")
        return False

def save_to_excel(df, filename="sector_marketcap_table.xlsx"):
    """Save dataframe to Excel file with formatting"""
    try:
        # Create Excel writer object
        writer = pd.ExcelWriter(filename, engine='openpyxl')
        
        # Write dataframe to Excel
        df.to_excel(writer, index=False, sheet_name='Sector Market Cap (Billions)')
        
        # Get the workbook and the worksheet
        workbook = writer.book
        worksheet = writer.sheets['Sector Market Cap (Billions)']
        
        # Format the header row
        for col_num, value in enumerate(df.columns.values):
            worksheet.cell(row=1, column=col_num+1).font = workbook.add_format({'bold': True})
        
        # Set column widths
        worksheet.column_dimensions['A'].width = 12  # Date column
        for i, col in enumerate(df.columns[1:], start=1):
            worksheet.column_dimensions[chr(66+i-1)].width = 15  # B, C, D, etc.
        
        # Save the workbook
        writer.close()
        logging.info(f"Saved sector market cap table to {filename}")
        return True
    except Exception as e:
        logging.error(f"Error saving to Excel: {e}")
        return False

def print_to_console(df, max_rows=30):
    """Print a stylized version of the table to console"""
    # Get the first and last rows
    first_rows = min(max_rows // 2, len(df))
    last_rows = min(max_rows // 2, len(df))
    
    print("\n===== SECTOR MARKET CAP TABLE (BILLIONS USD) =====")
    
    # Create header
    header = "Date       "
    for sector in df.columns[1:]:
        header += f" | {sector[:10]:<10}"
    print(header)
    print("-" * len(header))
    
    # Print first rows
    for i in range(first_rows):
        row = df.iloc[i]
        row_str = f"{row['Date']} "
        for sector in df.columns[1:]:
            val = row[sector]
            row_str += f" | {val:,.1f}".ljust(12)
        print(row_str)
    
    # Print ellipsis if we're not showing all rows
    if len(df) > first_rows + last_rows:
        print("..." + " " * (len(header) - 3))
    
    # Print last rows
    for i in range(max(0, len(df) - last_rows), len(df)):
        row = df.iloc[i]
        row_str = f"{row['Date']} "
        for sector in df.columns[1:]:
            val = row[sector]
            row_str += f" | {val:,.1f}".ljust(12)
        print(row_str)
    
    print("=" * len(header))

def main():
    """Main function to create and save sector market cap table"""
    print("Creating sector market cap table...")
    df = create_sector_marketcap_table()
    
    if df is None:
        print("Failed to create sector market cap table")
        return
    
    # Save to CSV
    save_to_csv(df)
    
    # Try to save to Excel if it's installed
    try:
        save_to_excel(df)
    except Exception as e:
        print(f"Excel export failed: {e}, falling back to CSV only")
    
    # Print to console
    print_to_console(df)
    
    print(f"\nTotal rows: {len(df)}")
    print(f"Date range: {df['Date'].iloc[0]} to {df['Date'].iloc[-1]}")
    print(f"Files saved: sector_marketcap_table.csv and sector_marketcap_table.xlsx (if Excel support available)")

if __name__ == "__main__":
    main()