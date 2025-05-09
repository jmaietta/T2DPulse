#!/usr/bin/env python3
# fetch_yelp_data.py
# -----------------------------------------------------------
# Dedicated script to fetch YELP data with exponential backoff
# This script focuses solely on the last remaining ticker to complete coverage

import pandas as pd
import numpy as np
import yfinance as yf
import finnhub
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
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('yelp_collection.log')
    ]
)
logger = logging.getLogger(__name__)

def fetch_finnhub_data(ticker):
    """Fetch data from Finnhub with retry logic"""
    try:
        api_key = os.environ.get("FINNHUB_API_KEY", "")
        if not api_key:
            logger.warning("Finnhub API key not found in environment variables")
            return None, None
            
        logger.info(f"Attempting to fetch {ticker} data from Finnhub")
        client = finnhub.Client(api_key=api_key)
        
        # Get quote
        quote = client.quote(ticker)
        price = quote.get('c') if quote and quote.get('c') > 0 else None
        
        if price:
            logger.info(f"Successfully got {ticker} price from Finnhub: {price}")
        else:
            logger.warning(f"Failed to get valid price from Finnhub for {ticker}")
            
        # Get company profile for market cap
        profile = client.company_profile2(symbol=ticker)
        market_cap = profile.get('marketCapitalization') * 1000000 if profile and profile.get('marketCapitalization') else None
        
        if market_cap:
            logger.info(f"Successfully got {ticker} market cap from Finnhub: {market_cap:,}")
        else:
            logger.warning(f"Failed to get valid market cap from Finnhub for {ticker}")
            
        return price, market_cap
        
    except Exception as e:
        logger.error(f"Error fetching data from Finnhub: {e}")
        return None, None

def fetch_yahoo_data(ticker, retry_count=3, initial_delay=10, backoff_factor=2.0):
    """Fetch data from Yahoo Finance with exponential backoff"""
    price = None
    market_cap = None
    
    for attempt in range(retry_count):
        delay = initial_delay * (backoff_factor ** attempt)
        logger.info(f"Yahoo attempt {attempt+1}/{retry_count} for {ticker} (delay: {delay:.1f}s)")
        time.sleep(delay)
        
        try:
            # Get price data
            data = yf.Ticker(ticker).history(period='1d')
            if not data.empty:
                price = data['Close'].iloc[-1]
                logger.info(f"Successfully got {ticker} price from Yahoo: {price}")
            
            # Add delay between requests to avoid rate limiting
            time.sleep(5)
            
            # Get market cap data
            info = yf.Ticker(ticker).info
            market_cap = info.get('marketCap')
            if market_cap:
                logger.info(f"Successfully got {ticker} market cap from Yahoo: {market_cap:,}")
                break  # Success, exit retry loop
                
        except Exception as e:
            logger.error(f"Error fetching {ticker} data from Yahoo: {e}")
    
    return price, market_cap

def update_historical_data(ticker, price, market_cap):
    """Update historical data with new ticker data"""
    try:
        # Load data files
        logger.info(f"Loading historical data files to update {ticker}")
        price_df = pd.read_csv('data/historical_ticker_prices.csv', index_col=0)
        mcap_df = pd.read_csv('data/historical_ticker_marketcap.csv', index_col=0)
        
        # Get current date
        latest_date = price_df.index[-1]
        logger.info(f"Updating {ticker} data for {latest_date}")
        
        # Update price data
        if price is not None:
            # Check if column exists
            if ticker not in price_df.columns:
                logger.info(f"Adding new column for {ticker} in price data")
                price_df[ticker] = np.nan
                
            logger.info(f"Setting {ticker} price to {price}")
            price_df.loc[latest_date, ticker] = price
        
        # Update market cap data
        if market_cap is not None:
            # Check if column exists
            if ticker not in mcap_df.columns:
                logger.info(f"Adding new column for {ticker} in market cap data")
                mcap_df[ticker] = np.nan
                
            logger.info(f"Setting {ticker} market cap to {market_cap:,}")
            mcap_df.loc[latest_date, ticker] = market_cap
        
        # Save updated data
        price_df.to_csv('data/historical_ticker_prices.csv')
        mcap_df.to_csv('data/historical_ticker_marketcap.csv')
        
        logger.info(f"Successfully updated historical data for {ticker}")
        return True
        
    except Exception as e:
        logger.error(f"Error updating historical data: {e}")
        return False

def collect_yelp_data():
    """Collect data for YELP ticker using multiple sources"""
    ticker = "YELP"
    logger.info(f"Starting dedicated collection for {ticker}")
    
    # Try Finnhub first
    price, market_cap = fetch_finnhub_data(ticker)
    
    # If both data points obtained, update and exit
    if price is not None and market_cap is not None:
        logger.info(f"Successfully collected all data for {ticker} from Finnhub")
        update_historical_data(ticker, price, market_cap)
        return True
        
    # Fall back to Yahoo with longer delays if either is missing
    if price is None or market_cap is None:
        logger.info(f"Incomplete data from Finnhub, trying Yahoo Finance")
        yahoo_price, yahoo_mcap = fetch_yahoo_data(ticker)
        
        # Use Yahoo data to fill any gaps
        final_price = price if price is not None else yahoo_price
        final_mcap = market_cap if market_cap is not None else yahoo_mcap
        
        # Update with best available data
        if final_price is not None or final_mcap is not None:
            logger.info(f"Updating with best available data for {ticker}")
            update_historical_data(ticker, final_price, final_mcap)
            
            # Check if we now have complete data
            if final_price is not None and final_mcap is not None:
                logger.info(f"Successfully collected complete data for {ticker}")
                return True
            else:
                logger.warning(f"Partial data collected for {ticker}, will need another attempt later")
                return False
        else:
            logger.error(f"Failed to collect any data for {ticker}")
            return False

if __name__ == "__main__":
    logger.info("Starting dedicated YELP data collection")
    success = collect_yelp_data()
    logger.info(f"YELP collection completed with {'SUCCESS' if success else 'PARTIAL DATA'}")