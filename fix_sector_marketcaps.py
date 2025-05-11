#!/usr/bin/env python3
"""
Fix sector market cap calculations to ensure full market cap of companies is 
counted in every sector they belong to.

This script:
1. Uses the correct ticker-sector mappings from the authentic source file
2. Fetches market cap data from the Polygon API for each ticker
3. Calculates sector market caps where each company's full market cap is 
   counted in every sector it belongs to
4. Creates a new CSVs with the corrected data that can be used by the app

IMPORTANT: This script ONLY uses authentic data from Polygon API.
"""

import os
import sys
import time
import json
import logging
import requests
import pandas as pd
from pathlib import Path
from typing import Dict, List, Tuple, Set, Optional
from datetime import datetime, date, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("fix_sector_marketcaps.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# API keys
POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY")
if not POLYGON_API_KEY:
    logger.error("POLYGON_API_KEY environment variable is not set")
    sys.exit(1)

# Source file for ticker-sector mappings
TICKER_SECTOR_FILE = "attached_assets/Pasted-AdTech-APP-AdTech-APPS-AdTech-CRTO-AdTech-DV-AdTech-GOOGL-AdTech-META-AdTech-MGNI-AdTech-PUBM-1746992995643.txt"

# Output files
TICKER_MARKET_CAPS_FILE = "authentic_ticker_market_caps.csv"
SECTOR_MARKET_CAPS_FILE = "authentic_sector_market_caps.csv"
SECTOR_TICKER_MAPPINGS_FILE = "authentic_sector_ticker_mappings.csv"

def load_sector_tickers():
    """
    Load ticker-sector mappings from the authentic file source
    Returns:
        Tuple of (sectors_dict, ticker_sectors_dict, unique_tickers)
    """
    sectors = {}  # sector -> list of tickers
    ticker_sectors = {}  # ticker -> list of sectors
    unique_tickers = set()
    
    try:
        with open(TICKER_SECTOR_FILE, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                    
                parts = line.split('\t')
                if len(parts) != 2:
                    continue
                    
                sector, ticker = parts[0].strip(), parts[1].strip()
                
                if sector not in sectors:
                    sectors[sector] = []
                    
                if ticker not in sectors[sector]:
                    sectors[sector].append(ticker)
                
                unique_tickers.add(ticker)
                
                if ticker not in ticker_sectors:
                    ticker_sectors[ticker] = []
                    
                if sector not in ticker_sectors[ticker]:
                    ticker_sectors[ticker].append(sector)
                    
        # Log some stats
        logger.info(f"Loaded {len(sectors)} sectors with {len(unique_tickers)} unique tickers")
        multi_sector_tickers = [t for t, s in ticker_sectors.items() if len(s) > 1]
        logger.info(f"Found {len(multi_sector_tickers)} tickers in multiple sectors:")
        for ticker in multi_sector_tickers:
            logger.info(f"  {ticker}: {', '.join(ticker_sectors[ticker])}")
            
        return sectors, ticker_sectors, unique_tickers
    except Exception as e:
        logger.error(f"Error loading sector-ticker mappings: {e}")
        return {}, {}, set()

def get_polygon_ticker_details(ticker):
    """
    Get ticker details from Polygon API, including market cap
    """
    url = f"https://api.polygon.io/v3/reference/tickers/{ticker}?apiKey={POLYGON_API_KEY}"
    try:
        response = requests.get(url, timeout=10)
        if response.status_code != 200:
            logger.warning(f"Error fetching details for {ticker}: HTTP {response.status_code}")
            return None
            
        data = response.json()
        if 'results' not in data:
            logger.warning(f"Invalid response format for {ticker}")
            return None
            
        return data['results']
    except Exception as e:
        logger.error(f"Error fetching details for {ticker}: {e}")
        return None

def get_polygon_ticker_price(ticker):
    """
    Get latest ticker price from Polygon API
    """
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/prev?apiKey={POLYGON_API_KEY}"
    try:
        response = requests.get(url, timeout=10)
        if response.status_code != 200:
            logger.warning(f"Error fetching price for {ticker}: HTTP {response.status_code}")
            return None
            
        data = response.json()
        if 'results' not in data or not data['results']:
            logger.warning(f"Invalid price response format for {ticker}")
            return None
            
        return data['results'][0].get('c')  # Closing price
    except Exception as e:
        logger.error(f"Error fetching price for {ticker}: {e}")
        return None

def calculate_ticker_market_caps(tickers):
    """
    Calculate market caps for each ticker using Polygon API
    """
    ticker_data = {}
    
    for ticker in tickers:
        logger.info(f"Fetching data for {ticker}")
        
        # Get ticker details
        details = get_polygon_ticker_details(ticker)
        if not details:
            logger.warning(f"Could not get details for {ticker}")
            continue
            
        # Get market cap directly if available
        market_cap = details.get('market_cap')
        shares = details.get('weighted_shares_outstanding') or details.get('share_class_shares_outstanding')
        
        # If market cap not available, calculate from price and shares
        if not market_cap and shares:
            price = get_polygon_ticker_price(ticker)
            if price:
                market_cap = price * shares
                
        if not market_cap:
            logger.warning(f"Could not determine market cap for {ticker}")
            continue
            
        # Store ticker data
        ticker_data[ticker] = {
            'symbol': ticker,
            'name': details.get('name', ''),
            'market_cap': market_cap,
            'price': get_polygon_ticker_price(ticker),
            'shares': shares
        }
        
        logger.info(f"Calculated market cap for {ticker}: ${market_cap/1e9:.2f}B")
        
        # Rate limiting
        time.sleep(0.5)
        
    return ticker_data

def calculate_sector_market_caps(sectors, ticker_data):
    """
    Calculate market caps for each sector based on constituent tickers.
    Each company's full market cap is counted in every sector it belongs to.
    """
    sector_market_caps = {}
    today = date.today().strftime('%Y-%m-%d')
    
    for sector, tickers in sectors.items():
        total_market_cap = 0
        ticker_count = 0
        missing_tickers = []
        
        for ticker in tickers:
            if ticker not in ticker_data:
                missing_tickers.append(ticker)
                continue
                
            # Add the full market cap to the sector total
            market_cap = ticker_data[ticker]['market_cap']
            total_market_cap += market_cap
            ticker_count += 1
            
        # Store sector data
        sector_market_caps[sector] = {
            'sector': sector,
            'date': today,
            'market_cap': total_market_cap,
            'ticker_count': ticker_count,
            'missing_tickers': missing_tickers
        }
        
        logger.info(f"Calculated market cap for {sector}: ${total_market_cap/1e9:.2f}B (from {ticker_count} tickers)")
        if missing_tickers:
            logger.warning(f"Missing tickers for {sector}: {', '.join(missing_tickers)}")
            
    return sector_market_caps

def save_to_csv(ticker_data, sector_market_caps, ticker_sectors):
    """
    Save ticker and sector data to CSV files
    """
    today = date.today().strftime('%Y-%m-%d')
    
    # Save ticker market caps
    ticker_rows = []
    for ticker, data in ticker_data.items():
        ticker_rows.append({
            'date': today,
            'ticker': ticker,
            'name': data.get('name', ''),
            'market_cap': data.get('market_cap', 0),
            'price': data.get('price', 0),
            'shares': data.get('shares', 0)
        })
    
    ticker_df = pd.DataFrame(ticker_rows)
    ticker_df.to_csv(TICKER_MARKET_CAPS_FILE, index=False)
    logger.info(f"Saved ticker market caps to {TICKER_MARKET_CAPS_FILE}")
    
    # Save sector market caps
    sector_rows = []
    for sector, data in sector_market_caps.items():
        sector_rows.append({
            'date': data['date'],
            'sector': sector,
            'market_cap': data['market_cap'],
            'ticker_count': data['ticker_count'],
            'missing_count': len(data['missing_tickers'])
        })
    
    sector_df = pd.DataFrame(sector_rows)
    sector_df.to_csv(SECTOR_MARKET_CAPS_FILE, index=False)
    logger.info(f"Saved sector market caps to {SECTOR_MARKET_CAPS_FILE}")
    
    # Save ticker-sector mappings
    mapping_rows = []
    for ticker, sectors in ticker_sectors.items():
        for sector in sectors:
            mapping_rows.append({
                'ticker': ticker,
                'sector': sector
            })
    
    mapping_df = pd.DataFrame(mapping_rows)
    mapping_df.to_csv(SECTOR_TICKER_MAPPINGS_FILE, index=False)
    logger.info(f"Saved ticker-sector mappings to {SECTOR_TICKER_MAPPINGS_FILE}")
    
    return ticker_df, sector_df, mapping_df

def verify_market_caps(sector_market_caps):
    """
    Verify the calculated market caps by checking for any known issues
    """
    # Known large sectors that should have significant market caps
    large_sectors = [
        "AI Infrastructure", 
        "Cloud Infrastructure", 
        "Enterprise SaaS",
        "Consumer Internet"
    ]
    
    # Check if the large sectors have significant market caps
    for sector in large_sectors:
        if sector in sector_market_caps:
            market_cap = sector_market_caps[sector]['market_cap']
            if market_cap < 1e12:  # Less than $1 trillion
                logger.warning(f"Potentially low market cap for {sector}: ${market_cap/1e9:.2f}B")
    
    # Create comparison table for all sectors
    market_cap_table = [(sector, data['market_cap']/1e9) for sector, data in sector_market_caps.items()]
    market_cap_table.sort(key=lambda x: x[1], reverse=True)
    
    logger.info("Sector market cap comparison:")
    logger.info(f"{'Sector':<25} {'Market Cap ($B)':<15}")
    logger.info("-" * 40)
    for sector, cap in market_cap_table:
        logger.info(f"{sector:<25} {cap:<15.2f}")
    
    # Calculate total market cap (note: this will double-count companies in multiple sectors)
    total_market_cap = sum(data['market_cap'] for data in sector_market_caps.values())
    logger.info(f"Total sector market cap (with double-counting): ${total_market_cap/1e9:.2f}B")
    
    # Calculate a rough estimate of "unique" market cap (this is not accurate due to overlaps)
    # Just for reference purposes
    ticker_market_caps = {}
    for sector, data in sector_market_caps.items():
        ticker_market_caps[sector] = data['market_cap']
    
    return True

def main():
    """
    Main function to fix sector market cap calculations
    """
    logger.info("Starting sector market cap fixing script")
    
    # Load sector-ticker mappings
    sectors, ticker_sectors, unique_tickers = load_sector_tickers()
    if not sectors:
        logger.error("Failed to load sector-ticker mappings")
        return False
    
    # Calculate market caps for each ticker
    ticker_data = calculate_ticker_market_caps(unique_tickers)
    if not ticker_data:
        logger.error("Failed to calculate ticker market caps")
        return False
    
    # Calculate market caps for each sector
    sector_market_caps = calculate_sector_market_caps(sectors, ticker_data)
    if not sector_market_caps:
        logger.error("Failed to calculate sector market caps")
        return False
    
    # Save data to CSV files
    ticker_df, sector_df, mapping_df = save_to_csv(ticker_data, sector_market_caps, ticker_sectors)
    
    # Verify the calculated market caps
    verify_market_caps(sector_market_caps)
    
    logger.info("Successfully fixed sector market cap calculations")
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)