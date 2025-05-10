#!/usr/bin/env python3
"""
Update historical market cap data with fully diluted share counts.

This script applies the fully diluted share counts retroactively to all historical
market cap data going back to April 1, 2025, ensuring consistent market cap 
calculations throughout the entire dataset.
"""

import os
import sys
import json
import pandas as pd
import logging
import pytz
from datetime import datetime, timedelta
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
CORRECTED_HISTORICAL_DATA_CSV = "data/historical_market_caps_corrected.csv"
TICKER_DATA_FILE = "T2D_Pulse_93_tickers_coverage.csv"
SHARE_COUNT_OVERRIDES = {
    "GOOGL": 12_291_000_000,  # Alphabet Inc. (fully diluted)
    "META": 2_590_000_000,    # Meta Platforms Inc. (fully diluted)
    # Add others as needed
}

# Sector mappings from ticker to sectors
# This needs to include all sector assignments for each ticker
SECTOR_TICKERS = {
    "AdTech": ["APP", "APPS", "CRTO", "DV", "GOOGL", "META", "MGNI", "PUBM", "TTD"],
    "Cloud Infrastructure": ["AMZN", "CRM", "CSCO", "GOOGL", "MSFT", "NET", "ORCL", "SNOW"],
    "Fintech": ["AFRM", "BILL", "COIN", "FIS", "FI", "GPN", "PYPL", "SSNC"],  # Removed problematic tickers: ADYEY, SQ
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

def load_historical_data():
    """Load historical market cap data from CSV"""
    if not os.path.exists(HISTORICAL_DATA_CSV):
        logging.error(f"Historical data CSV not found: {HISTORICAL_DATA_CSV}")
        return None
    
    try:
        # Read the CSV file, skipping any empty rows at the end
        df = pd.read_csv(HISTORICAL_DATA_CSV)
        
        # Filter out any rows where Date is NaN
        df = df.dropna(subset=['Date'])
        
        # Convert dates to datetime
        df['Date'] = pd.to_datetime(df['Date'])
        
        # Sort by date
        df = df.sort_values('Date')
        
        return df
    except Exception as e:
        logging.error(f"Error loading historical data: {e}")
        return None

def load_ticker_price_history():
    """Load ticker price history from CSV"""
    if not os.path.exists(TICKER_DATA_FILE):
        logging.error(f"Ticker data file not found: {TICKER_DATA_FILE}")
        return None
    
    try:
        df = pd.read_csv(TICKER_DATA_FILE)
        
        # Convert date column to datetime
        date_column = [col for col in df.columns if 'date' in col.lower()][0]
        df[date_column] = pd.to_datetime(df[date_column])
        
        return df
    except Exception as e:
        logging.error(f"Error loading ticker data: {e}")
        return None

def get_price_adjustment_factors():
    """
    Calculate adjustment factors for each ticker based on the difference between
    original share counts and fully diluted share counts.
    
    Returns a dictionary mapping ticker symbols to their adjustment factors.
    """
    # Load the ticker data
    ticker_data = load_ticker_price_history()
    if ticker_data is None:
        return {}
    
    # Create a dictionary of adjustment factors
    adjustment_factors = {}
    
    # Identify relevant columns
    ticker_column = [col for col in ticker_data.columns if 'ticker' in col.lower()][0]
    shares_column = [col for col in ticker_data.columns if 'shares' in col.lower() or 'outstanding' in col.lower()][0]
    
    # Calculate adjustment factors for each ticker
    for index, row in ticker_data.iterrows():
        ticker = row[ticker_column]
        if ticker in SHARE_COUNT_OVERRIDES:
            original_shares = row[shares_column]
            corrected_shares = SHARE_COUNT_OVERRIDES[ticker]
            
            if original_shares > 0:  # Avoid division by zero
                adjustment_factor = corrected_shares / original_shares
                adjustment_factors[ticker] = adjustment_factor
                logging.info(f"Adjustment factor for {ticker}: {adjustment_factor:.4f} " +
                             f"({original_shares:,.0f} -> {corrected_shares:,.0f} shares)")
    
    return adjustment_factors

def get_sectors_for_ticker(ticker):
    """Get all sectors a ticker belongs to"""
    sectors = []
    for sector, tickers in SECTOR_TICKERS.items():
        if ticker in tickers:
            sectors.append(sector)
    return sectors

def calculate_sector_adjustment_factors(adjustment_factors):
    """
    Calculate the adjustment factor for each sector based on the tickers
    in that sector and their respective adjustment factors.
    
    Returns a dictionary mapping sector names to their adjustment factors.
    """
    sector_adjustments = {}
    
    # Collect all unique tickers across all sectors
    all_tickers = set()
    for tickers in SECTOR_TICKERS.values():
        all_tickers.update(tickers)
    
    # Calculate the total market cap adjustment for each sector
    for sector, tickers in SECTOR_TICKERS.items():
        # Only consider tickers with adjustment factors
        adjustable_tickers = [t for t in tickers if t in adjustment_factors]
        
        if not adjustable_tickers:
            sector_adjustments[sector] = 1.0  # No adjustment needed
            continue
        
        # Estimate sector adjustment based on market cap weighted adjustments
        # Since we don't have actual market caps, use a simple average for now
        sector_adjustment = sum(adjustment_factors.get(ticker, 1.0) for ticker in adjustable_tickers) / len(adjustable_tickers)
        
        # Adjust based on known dominance of certain stocks in sectors
        # For example, GOOGL and META dominate AdTech
        if sector == "AdTech":
            # GOOGL and META make up about 95% of AdTech market cap
            googl_adj = adjustment_factors.get("GOOGL", 1.0)
            meta_adj = adjustment_factors.get("META", 1.0)
            sector_adjustment = (googl_adj * 0.53) + (meta_adj * 0.42) + (sector_adjustment * 0.05)
        
        elif sector == "AI Infrastructure":
            # GOOGL, META, MSFT, NVDA dominate AI Infrastructure
            googl_adj = adjustment_factors.get("GOOGL", 1.0)
            meta_adj = adjustment_factors.get("META", 1.0)
            sector_adjustment = (googl_adj * 0.45) + (meta_adj * 0.35) + (sector_adjustment * 0.20)
        
        elif sector == "Cloud Infrastructure":
            # MSFT, AMZN, GOOGL dominate Cloud Infrastructure
            googl_adj = adjustment_factors.get("GOOGL", 1.0)
            sector_adjustment = (googl_adj * 0.40) + (sector_adjustment * 0.60)
        
        sector_adjustments[sector] = sector_adjustment
        logging.info(f"Adjustment factor for {sector}: {sector_adjustment:.4f}")
    
    return sector_adjustments

def update_historical_market_caps(historical_data, sector_adjustments):
    """
    Apply sector adjustment factors to historical market cap data.
    
    Args:
        historical_data (DataFrame): Historical market cap data
        sector_adjustments (dict): Sector adjustment factors
        
    Returns:
        DataFrame: Updated historical market cap data
    """
    # Make a copy of the data
    updated_data = historical_data.copy()
    
    # Find the cutoff date (April 29th, 2025 is already adjusted)
    cutoff_date = pd.Timestamp('2025-04-29')
    
    # Find dates to adjust (before April 29th, 2025)
    dates_to_adjust = updated_data[updated_data['Date'] < cutoff_date]['Date'].unique()
    
    if len(dates_to_adjust) == 0:
        logging.info("No dates to adjust (all dates are on or after April 29th, 2025)")
        return updated_data
    
    logging.info(f"Adjusting market caps for {len(dates_to_adjust)} dates before April 29th, 2025")
    
    # Adjust each sector column for the relevant dates
    for sector, adjustment_factor in sector_adjustments.items():
        # Find the sector column name
        sector_column = None
        for col in updated_data.columns:
            if col.strip() == sector:
                sector_column = col
                break
        
        if sector_column is None:
            logging.warning(f"Could not find column for sector: {sector}")
            continue
        
        # Apply the adjustment factor for dates before April 29th, 2025
        mask = updated_data['Date'] < cutoff_date
        
        # Convert market cap string to numeric value
        updated_data.loc[mask, sector_column] = updated_data.loc[mask, sector_column].apply(
            lambda x: str(float(x.replace('$', '').replace('T', '')) * adjustment_factor) + 'T' 
            if isinstance(x, str) and 'T' in x else x
        )
    
    return updated_data

def save_corrected_data(updated_data):
    """Save the corrected historical market cap data"""
    try:
        # Create the data directory if it doesn't exist
        Path(DATA_DIR).mkdir(exist_ok=True)
        
        # Save the corrected data
        updated_data.to_csv(CORRECTED_HISTORICAL_DATA_CSV, index=False)
        logging.info(f"Saved corrected historical data to {CORRECTED_HISTORICAL_DATA_CSV}")
        
        # Also save a copy in the attached_assets directory for user reference
        updated_data.to_csv("attached_assets/Historical Market Caps T2D Pulse (Corrected).csv", index=False)
        logging.info("Saved a copy of corrected data to attached_assets directory")
        
        return True
    except Exception as e:
        logging.error(f"Error saving corrected data: {e}")
        return False

def main():
    """Main function to update historical market cap data"""
    print("Updating historical market cap data with fully diluted share counts...")
    
    # 1. Load historical data
    historical_data = load_historical_data()
    if historical_data is None:
        print("Failed to load historical data. Exiting.")
        return False
    
    print(f"Loaded historical market cap data with {len(historical_data)} rows")
    
    # 2. Calculate ticker adjustment factors
    adjustment_factors = get_price_adjustment_factors()
    if not adjustment_factors:
        logging.warning("No adjustment factors found. Will use sector estimates.")
    
    # 3. Calculate sector adjustment factors
    sector_adjustments = calculate_sector_adjustment_factors(adjustment_factors)
    
    # 4. Update historical market caps
    updated_data = update_historical_market_caps(historical_data, sector_adjustments)
    
    # 5. Save the corrected data
    if not save_corrected_data(updated_data):
        print("Failed to save corrected data.")
        return False
    
    print("\nSuccessfully updated historical market cap data with fully diluted share counts!")
    print(f"Corrected data saved to: {CORRECTED_HISTORICAL_DATA_CSV}")
    print("Also saved a copy to: attached_assets/Historical Market Caps T2D Pulse (Corrected).csv")
    
    return True

if __name__ == "__main__":
    main()