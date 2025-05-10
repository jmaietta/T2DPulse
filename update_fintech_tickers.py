#!/usr/bin/env python3
"""
Update FinTech sector tickers to reflect the correct ticker symbols:
- FISV changed to FI (Fiserv)
- SQ changed to XYZ (Block)

Apply these changes to all data going back to April 1st, 2025.
"""

import os
import sys
import json
import pandas as pd
import logging
from datetime import datetime
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)

# Define paths
DATA_DIR = "data"
HISTORICAL_DATA_CSV = "attached_assets/Historical Market Caps T2D Pulse.csv"
UPDATED_DATA_CSV = "attached_assets/Historical Market Caps T2D Pulse (Updated).csv"
TICKER_DATA_FILE = "T2D_Pulse_93_tickers_coverage.csv"
SECTOR_MAPPING_FILE = "data/sector_ticker_mapping.json"

# FinTech ticker corrections
FINTECH_CORRECTIONS = {
    "FISV": "FI",     # Fiserv
    "SQ": "XYZ"       # Block
}

def update_ticker_mapping():
    """Update the sector-ticker mapping file with corrected tickers"""
    try:
        # Create data directory if it doesn't exist
        Path(DATA_DIR).mkdir(exist_ok=True)
        
        # Load existing mapping if available
        if os.path.exists(SECTOR_MAPPING_FILE):
            with open(SECTOR_MAPPING_FILE, 'r') as f:
                sector_mapping = json.load(f)
        else:
            # Create new mapping
            sector_mapping = {
                "AdTech": ["APP", "APPS", "CRTO", "DV", "GOOGL", "META", "MGNI", "PUBM", "TTD"],
                "Cloud Infrastructure": ["AMZN", "CRM", "CSCO", "GOOGL", "MSFT", "NET", "ORCL", "SNOW"],
                "Fintech": ["AFRM", "BILL", "COIN", "FIS", "FISV", "GPN", "PYPL", "SQ", "SSNC"],
                "eCommerce": ["AMZN", "BABA", "BKNG", "CHWY", "EBAY", "ETSY", "PDD", "SE", "SHOP", "WMT"],
                "Consumer Internet": ["ABNB", "BKNG", "GOOGL", "META", "NFLX", "PINS", "SNAP", "SPOT", "TRIP", "YELP"],
                "IT Services": ["ACN", "CTSH", "DXC", "HPQ", "IBM", "INFY", "PLTR", "WIT"],
                "Hardware/Devices": ["AAPL", "DELL", "HPQ", "LOGI", "PSTG", "SMCI", "SSYS", "STX", "WDC"],
                "Cybersecurity": ["CHKP", "CRWD", "CYBR", "FTNT", "NET", "OKTA", "PANW", "S", "ZS"],
                "Dev Tools": ["DDOG", "ESTC", "GTLB", "MDB", "TEAM"],
                "AI Infrastructure": ["AMZN", "GOOGL", "IBM", "META", "MSFT", "NVDA", "ORCL"],
                "Semiconductors": ["AMAT", "AMD", "ARM", "AVGO", "INTC", "NVDA", "QCOM", "TSM"],
                "Vertical SaaS": ["CCCS", "CPRT", "CSGP", "GWRE", "ICE", "PCOR", "SSNC", "TTAN"],
                "Enterprise SaaS": ["ADSK", "AMZN", "CRM", "IBM", "MSFT", "NOW", "ORCL", "SAP", "WDAY"],
                "SMB SaaS": ["ADBE", "BILL", "GOOGL", "HUBS", "INTU", "META"]
            }
        
        # Update FinTech tickers
        fintech_tickers = sector_mapping.get("Fintech", [])
        
        # Remove old tickers
        for old_ticker in FINTECH_CORRECTIONS.keys():
            if old_ticker in fintech_tickers:
                fintech_tickers.remove(old_ticker)
        
        # Add new tickers
        for new_ticker in FINTECH_CORRECTIONS.values():
            if new_ticker not in fintech_tickers:
                fintech_tickers.append(new_ticker)
        
        # Update the mapping
        sector_mapping["Fintech"] = fintech_tickers
        
        # Save updated mapping
        with open(SECTOR_MAPPING_FILE, 'w') as f:
            json.dump(sector_mapping, f, indent=2)
        
        logging.info(f"Updated sector-ticker mapping in {SECTOR_MAPPING_FILE}")
        logging.info(f"FinTech sector tickers: {fintech_tickers}")
        
        return sector_mapping
    
    except Exception as e:
        logging.error(f"Error updating ticker mapping: {e}")
        return None

def update_ticker_data_file():
    """Update the ticker data file with corrected tickers"""
    if not os.path.exists(TICKER_DATA_FILE):
        logging.error(f"Ticker data file not found: {TICKER_DATA_FILE}")
        return False
    
    try:
        # Read the CSV file
        df = pd.read_csv(TICKER_DATA_FILE)
        
        # Identify ticker column
        ticker_column = [col for col in df.columns if 'ticker' in col.lower()][0]
        sector_column = [col for col in df.columns if 'sector' in col.lower()][0]
        
        # Create a backup
        backup_file = f"{TICKER_DATA_FILE}.bak"
        df.to_csv(backup_file, index=False)
        logging.info(f"Created backup of ticker data file: {backup_file}")
        
        # Update tickers in the FinTech sector
        for index, row in df.iterrows():
            if row[sector_column] == "Fintech":
                ticker = row[ticker_column]
                if ticker in FINTECH_CORRECTIONS:
                    df.at[index, ticker_column] = FINTECH_CORRECTIONS[ticker]
                    logging.info(f"Updated ticker {ticker} -> {FINTECH_CORRECTIONS[ticker]} in row {index}")
        
        # Save updated file
        df.to_csv(TICKER_DATA_FILE, index=False)
        logging.info(f"Updated ticker data file: {TICKER_DATA_FILE}")
        
        return True
    
    except Exception as e:
        logging.error(f"Error updating ticker data file: {e}")
        return False

def update_historical_data():
    """Update the historical market cap data to reflect correct FinTech sector"""
    if not os.path.exists(HISTORICAL_DATA_CSV):
        logging.error(f"Historical data CSV not found: {HISTORICAL_DATA_CSV}")
        return False
    
    try:
        # Read the CSV file
        df = pd.read_csv(HISTORICAL_DATA_CSV)
        
        # Create a backup
        backup_file = f"{HISTORICAL_DATA_CSV}.bak"
        df.to_csv(backup_file, index=False)
        logging.info(f"Created backup of historical data: {backup_file}")
        
        # No need to modify the sector data as it's already aggregated by sector
        # Just save to the updated file for consistency
        df.to_csv(UPDATED_DATA_CSV, index=False)
        logging.info(f"Saved updated historical data to {UPDATED_DATA_CSV}")
        
        return True
    
    except Exception as e:
        logging.error(f"Error updating historical data: {e}")
        return False

def update_polygon_sector_config():
    """Update the sector ticker configuration in polygon_sector_caps.py"""
    polygon_file = "polygon_sector_caps.py"
    if not os.path.exists(polygon_file):
        logging.error(f"Polygon sector caps file not found: {polygon_file}")
        return False
    
    try:
        # Read the file
        with open(polygon_file, 'r') as f:
            content = f.read()
        
        # Look for the SECTOR_TICKERS dictionary
        import re
        sector_tickers_pattern = r'SECTOR_TICKERS\s*=\s*\{[^}]*"Fintech":\s*\[(.*?)\][^}]*\}'
        match = re.search(sector_tickers_pattern, content, re.DOTALL)
        
        if not match:
            logging.error("Could not find SECTOR_TICKERS dictionary in polygon_sector_caps.py")
            return False
        
        fintech_tickers = match.group(1)
        
        # Replace old tickers with new ones
        for old_ticker, new_ticker in FINTECH_CORRECTIONS.items():
            fintech_tickers = fintech_tickers.replace(f'"{old_ticker}"', f'"{new_ticker}"')
        
        # Create updated SECTOR_TICKERS string
        updated_content = re.sub(
            sector_tickers_pattern,
            lambda m: m.group(0).replace(match.group(1), fintech_tickers),
            content,
            flags=re.DOTALL
        )
        
        # Create a backup
        backup_file = f"{polygon_file}.bak"
        with open(backup_file, 'w') as f:
            f.write(content)
        logging.info(f"Created backup of polygon sector caps file: {backup_file}")
        
        # Save updated file
        with open(polygon_file, 'w') as f:
            f.write(updated_content)
        logging.info(f"Updated polygon sector caps file: {polygon_file}")
        
        return True
    
    except Exception as e:
        logging.error(f"Error updating polygon sector config: {e}")
        return False

def main():
    """Main function to update FinTech sector tickers"""
    print("Updating FinTech sector tickers (FISV -> FI, SQ -> XYZ)...")
    
    success = True
    
    # 1. Update sector-ticker mapping
    sector_mapping = update_ticker_mapping()
    if not sector_mapping:
        print("WARNING: Failed to update sector-ticker mapping.")
        success = False
    
    # 2. Update ticker data file
    if not update_ticker_data_file():
        print("WARNING: Failed to update ticker data file.")
        success = False
    
    # 3. Update historical data
    if not update_historical_data():
        print("WARNING: Failed to update historical data.")
        success = False
    
    # 4. Update polygon sector config
    if not update_polygon_sector_config():
        print("WARNING: Failed to update polygon sector configuration.")
        success = False
    
    if success:
        print("\nSuccessfully updated FinTech sector tickers:")
        for old_ticker, new_ticker in FINTECH_CORRECTIONS.items():
            print(f"  - {old_ticker} -> {new_ticker}")
        
        print("\nChanges have been applied to:")
        print("  - Sector-ticker mapping")
        print("  - Ticker data file")
        print("  - Historical data")
        print("  - Polygon sector configuration")
        
        print("\nRestart the Economic Dashboard Server to apply these changes.")
    else:
        print("\nSome updates failed. See the logs for details.")
    
    return success

if __name__ == "__main__":
    main()