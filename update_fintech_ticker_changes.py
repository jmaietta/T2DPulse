#!/usr/bin/env python3
"""
Update FinTech sector tickers to reflect the correct ticker symbols from April 1st forward:
- FISV changed to FI (Fiserv)
- SQ changed to XYZ (Block, Inc.)

This script updates all relevant configuration files to ensure consistent ticker usage.
"""

import os
import json
import logging
import pandas as pd
from datetime import datetime
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)

# Define ticker changes
TICKER_CHANGES = {
    "FISV": "FI",    # Fiserv
    "SQ": "XYZ"      # Block, Inc
}

# Define cutoff date - all data from this date forward should use new tickers
CUTOFF_DATE = "2025-04-01"

# Define paths to files that need to be updated
POLYGON_SECTOR_FILE = "polygon_sector_caps.py"
SECTOR_MAPPING_FILE = "data/sector_ticker_mapping.json"
SECTOR_CONFIG_FILE = "config.py"
TICKER_HISTORY_FILE = "T2D_Pulse_93_tickers_coverage.csv"
SECTOR_TICKERS_FILE = "attached_assets/Sector Sentiment tickers.csv"

def update_polygon_sector_caps():
    """Update the SECTOR_TICKERS dictionary in polygon_sector_caps.py"""
    if not os.path.exists(POLYGON_SECTOR_FILE):
        logging.error(f"File not found: {POLYGON_SECTOR_FILE}")
        return False
    
    try:
        # Read file content
        with open(POLYGON_SECTOR_FILE, 'r') as f:
            content = f.read()
        
        # Create backup
        with open(f"{POLYGON_SECTOR_FILE}.bak", 'w') as f:
            f.write(content)
        
        # Check if Fintech sector includes old tickers
        for old_ticker, new_ticker in TICKER_CHANGES.items():
            # Replace old ticker with new ticker only if old ticker exists
            if f'"{old_ticker}"' in content:
                content = content.replace(f'"{old_ticker}"', f'"{new_ticker}"')
                logging.info(f"Replaced {old_ticker} with {new_ticker} in {POLYGON_SECTOR_FILE}")
        
        # Write updated content
        with open(POLYGON_SECTOR_FILE, 'w') as f:
            f.write(content)
        
        logging.info(f"Updated {POLYGON_SECTOR_FILE}")
        return True
    
    except Exception as e:
        logging.error(f"Error updating {POLYGON_SECTOR_FILE}: {e}")
        return False

def update_sector_mapping():
    """Update the sector ticker mapping file"""
    try:
        # Create data directory if it doesn't exist
        Path("data").mkdir(exist_ok=True)
        
        # Load existing mapping or create new if not exists
        if os.path.exists(SECTOR_MAPPING_FILE):
            with open(SECTOR_MAPPING_FILE, 'r') as f:
                sector_mapping = json.load(f)
        else:
            # Create default mapping - this should match your polygon_sector_caps.py
            sector_mapping = {
                "AdTech": ["APP", "APPS", "CRTO", "DV", "GOOGL", "META", "MGNI", "PUBM", "TTD"],
                "Cloud_Infrastructure": ["AMZN", "CRM", "CSCO", "GOOGL", "MSFT", "NET", "ORCL", "SNOW"],
                "Fintech": ["AFRM", "BILL", "COIN", "FIS", "FISV", "GPN", "PYPL", "SQ", "SSNC"],
                "eCommerce": ["AMZN", "BABA", "BKNG", "CHWY", "EBAY", "ETSY", "PDD", "SE", "SHOP", "WMT"],
                "Consumer_Internet": ["ABNB", "BKNG", "GOOGL", "META", "NFLX", "PINS", "SNAP", "SPOT", "TRIP", "YELP"],
                "IT_Services": ["ACN", "CTSH", "DXC", "HPQ", "IBM", "INFY", "PLTR", "WIT"],
                "Hardware_Devices": ["AAPL", "DELL", "HPQ", "LOGI", "PSTG", "SMCI", "SSYS", "STX", "WDC"],
                "Cybersecurity": ["CHKP", "CRWD", "CYBR", "FTNT", "NET", "OKTA", "PANW", "S", "ZS"],
                "Dev_Tools": ["DDOG", "ESTC", "GTLB", "MDB", "TEAM"],
                "AI_Infrastructure": ["AMZN", "GOOGL", "IBM", "META", "MSFT", "NVDA", "ORCL"],
                "Semiconductors": ["AMAT", "AMD", "ARM", "AVGO", "INTC", "NVDA", "QCOM", "TSM"],
                "Vertical_SaaS": ["CCCS", "CPRT", "CSGP", "GWRE", "ICE", "PCOR", "SSNC", "TTAN"],
                "Enterprise_SaaS": ["ADSK", "AMZN", "CRM", "IBM", "MSFT", "NOW", "ORCL", "SAP", "WDAY"],
                "SMB_SaaS": ["ADBE", "BILL", "GOOGL", "HUBS", "INTU", "META"]
            }
        
        # Update FinTech tickers
        fintech_tickers = sector_mapping.get("Fintech", [])
        new_fintech_tickers = []
        
        # Replace old tickers with new ones
        for ticker in fintech_tickers:
            if ticker in TICKER_CHANGES:
                new_fintech_tickers.append(TICKER_CHANGES[ticker])
                logging.info(f"Replaced {ticker} with {TICKER_CHANGES[ticker]} in sector mapping")
            else:
                new_fintech_tickers.append(ticker)
        
        # Update mapping
        sector_mapping["Fintech"] = new_fintech_tickers
        
        # Save updated mapping
        with open(SECTOR_MAPPING_FILE, 'w') as f:
            json.dump(sector_mapping, f, indent=2)
        
        logging.info(f"Updated {SECTOR_MAPPING_FILE}")
        return True
    
    except Exception as e:
        logging.error(f"Error updating sector mapping: {e}")
        return False

def update_config_file():
    """Update the sector configurations in config.py"""
    if not os.path.exists(SECTOR_CONFIG_FILE):
        logging.error(f"File not found: {SECTOR_CONFIG_FILE}")
        return False
    
    try:
        # Read file content
        with open(SECTOR_CONFIG_FILE, 'r') as f:
            content = f.read()
        
        # Create backup
        with open(f"{SECTOR_CONFIG_FILE}.bak", 'w') as f:
            f.write(content)
        
        # Check if Fintech sector includes old tickers
        for old_ticker, new_ticker in TICKER_CHANGES.items():
            # Replace old ticker with new ticker only if old ticker exists
            if f"'{old_ticker}'" in content:
                content = content.replace(f"'{old_ticker}'", f"'{new_ticker}'")
                logging.info(f"Replaced {old_ticker} with {new_ticker} in {SECTOR_CONFIG_FILE}")
        
        # Write updated content
        with open(SECTOR_CONFIG_FILE, 'w') as f:
            f.write(content)
        
        logging.info(f"Updated {SECTOR_CONFIG_FILE}")
        return True
    
    except Exception as e:
        logging.error(f"Error updating {SECTOR_CONFIG_FILE}: {e}")
        return False

def update_ticker_history():
    """Update the ticker history file with new ticker symbols"""
    if not os.path.exists(TICKER_HISTORY_FILE):
        logging.error(f"File not found: {TICKER_HISTORY_FILE}")
        return False
    
    try:
        # Read CSV file
        df = pd.read_csv(TICKER_HISTORY_FILE)
        
        # Create backup
        df.to_csv(f"{TICKER_HISTORY_FILE}.bak", index=False)
        
        # Find ticker column
        ticker_col = None
        date_col = None
        for col in df.columns:
            if 'ticker' in col.lower():
                ticker_col = col
            if 'date' in col.lower():
                date_col = col
        
        if not ticker_col or not date_col:
            logging.error(f"Could not find ticker or date column in {TICKER_HISTORY_FILE}")
            return False
        
        # Convert date column to datetime
        df[date_col] = pd.to_datetime(df[date_col])
        
        # Update tickers after cutoff date
        cutoff = pd.to_datetime(CUTOFF_DATE)
        
        # For each row after cutoff, replace old tickers with new ones
        for idx, row in df.iterrows():
            if row[date_col] >= cutoff and row[ticker_col] in TICKER_CHANGES:
                old_ticker = row[ticker_col]
                new_ticker = TICKER_CHANGES[old_ticker]
                df.at[idx, ticker_col] = new_ticker
                logging.info(f"Updated ticker {old_ticker} -> {new_ticker} at index {idx}")
        
        # Save updated file
        df.to_csv(TICKER_HISTORY_FILE, index=False)
        logging.info(f"Updated {TICKER_HISTORY_FILE}")
        return True
    
    except Exception as e:
        logging.error(f"Error updating ticker history: {e}")
        return False

def update_sector_tickers_file():
    """Update the sector tickers configuration file"""
    if not os.path.exists(SECTOR_TICKERS_FILE):
        logging.warning(f"File not found: {SECTOR_TICKERS_FILE} - skipping")
        return True
    
    try:
        # Read CSV file
        df = pd.read_csv(SECTOR_TICKERS_FILE)
        
        # Create backup
        df.to_csv(f"{SECTOR_TICKERS_FILE}.bak", index=False)
        
        # Find ticker column and sector column
        ticker_col = None
        sector_col = None
        for col in df.columns:
            if 'ticker' in col.lower():
                ticker_col = col
            if 'sector' in col.lower():
                sector_col = col
        
        if not ticker_col:
            logging.error(f"Could not find ticker column in {SECTOR_TICKERS_FILE}")
            return False
        
        # Update tickers
        for idx, row in df.iterrows():
            if sector_col is None or row[sector_col] == 'Fintech' or 'fintech' in str(row[sector_col]).lower():
                if row[ticker_col] in TICKER_CHANGES:
                    old_ticker = row[ticker_col]
                    new_ticker = TICKER_CHANGES[old_ticker]
                    df.at[idx, ticker_col] = new_ticker
                    logging.info(f"Updated ticker {old_ticker} -> {new_ticker} in sector tickers file")
        
        # Save updated file
        df.to_csv(SECTOR_TICKERS_FILE, index=False)
        logging.info(f"Updated {SECTOR_TICKERS_FILE}")
        return True
    
    except Exception as e:
        logging.error(f"Error updating sector tickers file: {e}")
        return False

def main():
    """Main function to update Fintech ticker symbols"""
    print(f"Updating FinTech ticker symbols (effective from {CUTOFF_DATE}):")
    for old_ticker, new_ticker in TICKER_CHANGES.items():
        print(f"  - {old_ticker} -> {new_ticker}")
    
    success = True
    
    # 1. Update polygon_sector_caps.py
    if not update_polygon_sector_caps():
        print(f"Failed to update {POLYGON_SECTOR_FILE}")
        success = False
    
    # 2. Update sector ticker mapping
    if not update_sector_mapping():
        print(f"Failed to update {SECTOR_MAPPING_FILE}")
        success = False
    
    # 3. Update config.py
    if not update_config_file():
        print(f"Failed to update {SECTOR_CONFIG_FILE}")
        success = False
    
    # 4. Update ticker history file
    if not update_ticker_history():
        print(f"Failed to update {TICKER_HISTORY_FILE}")
        success = False
    
    # 5. Update sector tickers file
    if not update_sector_tickers_file():
        print(f"Failed to update {SECTOR_TICKERS_FILE}")
        success = False
    
    if success:
        print("\nSuccessfully updated all Fintech ticker references!")
        print("Please restart the Economic Dashboard Server to apply these changes.")
    else:
        print("\nSome updates failed. Please check the logs for details.")
    
    return success

if __name__ == "__main__":
    main()