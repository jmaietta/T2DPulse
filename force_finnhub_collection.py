#!/usr/bin/env python3
# force_finnhub_collection.py
# -----------------------------------------------------------
# Script to aggressively collect missing ticker data using only Finnhub API

import os
import sys
import time
import pandas as pd
import datetime
import pytz
import logging
import requests
import numpy as np
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('force_finnhub_collection.log'),
        logging.StreamHandler()
    ]
)

def get_eastern_time():
    """Get current time in US Eastern timezone"""
    eastern = pytz.timezone('US/Eastern')
    return datetime.datetime.now(eastern)

def get_official_tickers():
    """Get the complete list of official tickers"""
    tickers = []
    try:
        with open('official_tickers.csv', 'r') as f:
            for line in f:
                ticker = line.strip()
                if ticker:
                    tickers.append(ticker)
        return tickers
    except FileNotFoundError:
        logging.error("official_tickers.csv not found")
        return []

def get_missing_tickers():
    """Get the list of tickers with missing data"""
    try:
        # Load data files
        price_df = pd.read_csv('data/historical_ticker_prices.csv', index_col=0)
        mcap_df = pd.read_csv('data/historical_ticker_marketcap.csv', index_col=0)
        
        # Get latest date
        latest_date = price_df.index[-1]
        
        # Get official tickers
        all_tickers = get_official_tickers()
        
        missing_price = []
        missing_mcap = []
        missing_both = []
        
        for ticker in all_tickers:
            missing_price_data = (ticker not in price_df.columns or 
                                pd.isna(price_df.loc[latest_date, ticker]))
            missing_mcap_data = (ticker not in mcap_df.columns or 
                               pd.isna(mcap_df.loc[latest_date, ticker]))
            
            if missing_price_data and missing_mcap_data:
                missing_both.append(ticker)
            elif missing_price_data:
                missing_price.append(ticker)
            elif missing_mcap_data:
                missing_mcap.append(ticker)
        
        return {
            'date': latest_date,
            'all_tickers': all_tickers,
            'missing_price': missing_price,
            'missing_mcap': missing_mcap,
            'missing_both': missing_both,
            'price_df': price_df,
            'mcap_df': mcap_df
        }
    
    except Exception as e:
        logging.error(f"Error getting missing tickers: {e}")
        return None

def get_ticker_data_finnhub(ticker, api_key, retries=3, delay=1):
    """Get both price and market cap data from Finnhub with retries"""
    price = None
    market_cap = None
    
    for attempt in range(retries):
        try:
            # Get price from Finnhub
            price_url = f"https://finnhub.io/api/v1/quote?symbol={ticker}&token={api_key}"
            price_response = requests.get(price_url)
            price_data = price_response.json()
            
            if 'c' in price_data and price_data['c'] > 0:
                price = price_data['c']
                logging.info(f"  ✓ Got price from Finnhub: {price}")
            else:
                logging.warning(f"  ✗ Failed to get price from Finnhub")
            
            time.sleep(delay)  # To avoid rate limiting
            
            # Get market cap from Finnhub
            profile_url = f"https://finnhub.io/api/v1/stock/profile2?symbol={ticker}&token={api_key}"
            profile_response = requests.get(profile_url)
            profile_data = profile_response.json()
            
            if 'marketCapitalization' in profile_data and profile_data['marketCapitalization'] > 0:
                # Convert from millions to actual value
                market_cap = profile_data['marketCapitalization'] * 1000000
                logging.info(f"  ✓ Got market cap from Finnhub: {market_cap:,.2f}")
            else:
                logging.warning(f"  ✗ Failed to get market cap from Finnhub")
            
            # If we got both price and market cap, return them
            if price is not None and market_cap is not None:
                return price, market_cap
            
            time.sleep(delay)  # To avoid rate limiting
        
        except Exception as e:
            logging.warning(f"Finnhub error for {ticker} (attempt {attempt+1}/{retries}): {e}")
            if attempt < retries - 1:
                time.sleep(delay * 2)  # Wait a bit longer before retry
    
    return price, market_cap

def update_historical_data(data, ticker, price, market_cap):
    """Update the historical data for a single ticker"""
    price_df = data['price_df'].copy()
    mcap_df = data['mcap_df'].copy()
    date = data['date']
    
    updates_made = 0
    
    if price is not None:
        if ticker not in price_df.columns:
            price_df[ticker] = np.nan
        price_df.loc[date, ticker] = price
        logging.info(f"  Filled in price data for {ticker}")
        updates_made += 1
    
    if market_cap is not None:
        if ticker not in mcap_df.columns:
            mcap_df[ticker] = np.nan
        mcap_df.loc[date, ticker] = market_cap
        logging.info(f"  Filled in market cap data for {ticker}")
        updates_made += 1
    
    # Save updated data if updates were made
    if updates_made > 0:
        price_df.to_csv('data/historical_ticker_prices.csv')
        mcap_df.to_csv('data/historical_ticker_marketcap.csv')
        logging.info(f"  Saved {updates_made} updates to historical data files")
    
    return updates_made

def force_complete_missing_data():
    """Complete all missing ticker data using only Finnhub"""
    logging.info("Starting forced Finnhub data completion...")
    
    # Get missing tickers
    data = get_missing_tickers()
    if not data:
        logging.error("Failed to get missing tickers data")
        return False
    
    # Get Finnhub API key
    api_key = os.environ.get('FINNHUB_API_KEY')
    if not api_key:
        logging.error("No Finnhub API key found in environment variables")
        return False
    
    # Show missing ticker stats
    missing_both = data['missing_both']
    missing_price = data['missing_price']
    missing_mcap = data['missing_mcap']
    all_missing = missing_both + missing_price + missing_mcap
    
    logging.info(f"Missing data stats for {data['date']}:")
    logging.info(f"- Missing both price and market cap: {len(missing_both)} tickers")
    logging.info(f"- Missing only price: {len(missing_price)} tickers")
    logging.info(f"- Missing only market cap: {len(missing_mcap)} tickers")
    logging.info(f"- Total tickers to process: {len(all_missing)}")
    
    if not all_missing:
        logging.info("No missing data! All tickers have complete data.")
        return True
    
    # Process all missing tickers with small delay between each to avoid rate limits
    logging.info("Processing missing tickers...")
    
    success_count = 0
    partial_count = 0
    fail_count = 0
    
    for ticker in tqdm(all_missing):
        logging.info(f"Processing {ticker}...")
        
        # Get data from Finnhub
        price, market_cap = get_ticker_data_finnhub(ticker, api_key)
        
        # Update historical data
        updates = update_historical_data(data, ticker, price, market_cap)
        
        # Track success/failure
        if price is not None and market_cap is not None:
            logging.info(f"SUCCESS: Complete data for {ticker}")
            success_count += 1
        elif price is not None or market_cap is not None:
            logging.info(f"PARTIAL: Got partial data for {ticker}")
            partial_count += 1
        else:
            logging.info(f"FAILED: Could not get data for {ticker}")
            fail_count += 1
        
        # Wait between requests to avoid rate limits
        time.sleep(1)
    
    # Print summary
    logging.info("=" * 50)
    logging.info("COLLECTION SUMMARY")
    logging.info("=" * 50)
    logging.info(f"Total tickers processed: {len(all_missing)}")
    logging.info(f"Complete success: {success_count} ({success_count/len(all_missing)*100:.1f}%)")
    logging.info(f"Partial success: {partial_count} ({partial_count/len(all_missing)*100:.1f}%)")
    logging.info(f"Failed: {fail_count} ({fail_count/len(all_missing)*100:.1f}%)")
    
    # Check if we now have 100% coverage
    new_data = get_missing_tickers()
    if not new_data:
        logging.error("Failed to get updated missing tickers data")
        return False
    
    new_missing = (new_data['missing_both'] + 
                  new_data['missing_price'] + 
                  new_data['missing_mcap'])
    
    coverage_pct = (len(new_data['all_tickers']) - len(new_missing)) / len(new_data['all_tickers']) * 100
    
    if not new_missing:
        logging.info(f"SUCCESS! Achieved 100% ticker coverage for {new_data['date']}")
        return True
    else:
        logging.info(f"Current coverage: {coverage_pct:.1f}% ({len(new_data['all_tickers']) - len(new_missing)}/{len(new_data['all_tickers'])} tickers)")
        logging.info(f"Still missing {len(new_missing)} tickers: {', '.join(new_missing)}")
        return False

if __name__ == "__main__":
    success = force_complete_missing_data()
    sys.exit(0 if success else 1)