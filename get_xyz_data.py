#!/usr/bin/env python3
# get_xyz_data.py
# -----------------------------------------------------------
# Script to specifically fetch XYZ ticker data with rate limiting
# Replaces SQ ticker with XYZ in all data files

import pandas as pd
import numpy as np
import yfinance as yf
import time
import os
import sys
import logging
from datetime import datetime, timedelta

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def fetch_xyz_data(retry_count=5, backoff_factor=2.0, initial_delay=10):
    """
    Fetch XYZ ticker data with exponential backoff for rate limiting
    
    Args:
        retry_count (int): Number of retries
        backoff_factor (float): Multiplier for backoff
        initial_delay (int): Initial delay in seconds
        
    Returns:
        tuple: (price, market_cap) or (None, None) if failed
    """
    logger.info(f"Attempting to fetch XYZ ticker data with {retry_count} retries...")
    
    price = None
    market_cap = None
    
    for attempt in range(retry_count):
        delay = initial_delay * (backoff_factor ** attempt)
        logger.info(f"Attempt {attempt+1}/{retry_count} (waiting {delay:.1f}s before request)")
        time.sleep(delay)  # Wait before making request
        
        try:
            # Get price data
            logger.info("Fetching price data for XYZ...")
            data = yf.Ticker("XYZ").history(period='1d')
            if not data.empty:
                price = data['Close'].iloc[-1]
                logger.info(f"Successfully fetched price: {price}")
            else:
                logger.warning("Received empty price data")
                
            # Add additional delay before market cap request
            time.sleep(5)
            
            # Get market cap data
            logger.info("Fetching market cap data for XYZ...")
            info = yf.Ticker("XYZ").info
            market_cap = info.get('marketCap')
            if market_cap:
                logger.info(f"Successfully fetched market cap: {market_cap:,}")
                break  # Success, exit retry loop
            else:
                logger.warning("Failed to get market cap data")
                
        except Exception as e:
            logger.error(f"Error fetching data: {e}")
        
        logger.warning(f"Retrying after error or incomplete data...")
    
    return price, market_cap

def update_historical_data(ticker='XYZ', old_ticker='SQ'):
    """
    Update the historical data files to replace old ticker with new ticker
    
    Args:
        ticker (str): New ticker symbol
        old_ticker (str): Old ticker symbol to replace
    """
    # Files to update
    price_file = 'data/historical_ticker_prices.csv'
    mcap_file = 'data/historical_ticker_marketcap.csv'
    
    logger.info(f"Updating {price_file} and {mcap_file} to replace {old_ticker} with {ticker}")
    
    try:
        # Load price data
        price_df = pd.read_csv(price_file, index_col=0)
        price_df = price_df.rename(columns={old_ticker: ticker})
        
        # Load market cap data
        mcap_df = pd.read_csv(mcap_file, index_col=0)
        mcap_df = mcap_df.rename(columns={old_ticker: ticker})
        
        # Get current date for latest data
        current_date = datetime.now().strftime('%Y-%m-%d')
        if current_date not in price_df.index:
            logger.warning(f"Current date {current_date} not found in historical data")
            # Use the most recent date instead
            current_date = price_df.index[-1]
            logger.info(f"Using most recent date {current_date} instead")
        
        # Fetch current data
        price, market_cap = fetch_xyz_data()
        
        if price is not None:
            logger.info(f"Updating price data for {ticker} on {current_date}: {price}")
            price_df.loc[current_date, ticker] = price
        else:
            logger.warning(f"Failed to get price for {ticker}, not updating")
            
        if market_cap is not None:
            logger.info(f"Updating market cap data for {ticker} on {current_date}: {market_cap:,}")
            mcap_df.loc[current_date, ticker] = market_cap
        else:
            logger.warning(f"Failed to get market cap for {ticker}, not updating")
        
        # Save updated data
        price_df.to_csv(price_file)
        mcap_df.to_csv(mcap_file)
        logger.info("Historical data files updated successfully")
        
        return True
        
    except Exception as e:
        logger.error(f"Error updating historical data: {e}")
        return False

if __name__ == "__main__":
    logger.info("Starting XYZ ticker data collection")
    success = update_historical_data()
    logger.info(f"Process completed with {'SUCCESS' if success else 'FAILURE'}")
    sys.exit(0 if success else 1)