#!/usr/bin/env python3
# complete_missing_tickers.py
# -----------------------------------------------------------
# Script to aggressively complete all missing ticker data at once

import os
import sys
import time
import pandas as pd
import datetime
import pytz
import logging
import concurrent.futures
import requests
import numpy as np
import yfinance as yf
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('complete_missing_tickers.log'),
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

def get_ticker_data_yahoo(ticker, retry_count=3):
    """Get both price and market cap data from Yahoo Finance"""
    for attempt in range(retry_count):
        try:
            ticker_obj = yf.Ticker(ticker)
            
            # Get latest price
            ticker_history = ticker_obj.history(period='1d')
            if ticker_history.empty:
                return None, None
            
            price = ticker_history['Close'].iloc[-1]
            
            # Get market cap
            info = ticker_obj.info
            market_cap = info.get('marketCap')
            
            return price, market_cap
        
        except Exception as e:
            logging.warning(f"Yahoo Finance error for {ticker} (attempt {attempt+1}/{retry_count}): {e}")
            if attempt < retry_count - 1:
                time.sleep(2)  # Wait before retry
    
    return None, None

def get_ticker_data_finnhub(ticker, api_key):
    """Get both price and market cap data from Finnhub"""
    try:
        # Get price from Finnhub
        price_url = f"https://finnhub.io/api/v1/quote?symbol={ticker}&token={api_key}"
        price_response = requests.get(price_url)
        price_data = price_response.json()
        
        if 'c' in price_data and price_data['c'] > 0:
            price = price_data['c']
        else:
            price = None
        
        # Get market cap from Finnhub
        profile_url = f"https://finnhub.io/api/v1/stock/profile2?symbol={ticker}&token={api_key}"
        profile_response = requests.get(profile_url)
        profile_data = profile_response.json()
        
        if 'marketCapitalization' in profile_data and profile_data['marketCapitalization'] > 0:
            # Convert from millions to actual value
            market_cap = profile_data['marketCapitalization'] * 1000000
        else:
            market_cap = None
        
        return price, market_cap
    
    except Exception as e:
        logging.warning(f"Finnhub error for {ticker}: {e}")
        return None, None

def process_ticker(ticker, data, api_keys, max_retries=3):
    """Process a single ticker to get its data"""
    logging.info(f"Processing {ticker}...")
    price = None
    market_cap = None
    
    # Try Finnhub first
    if 'FINNHUB_API_KEY' in api_keys:
        price, market_cap = get_ticker_data_finnhub(ticker, api_keys['FINNHUB_API_KEY'])
        if price and market_cap:
            return ticker, price, market_cap, 'finnhub'
    
    # Try Yahoo Finance next
    for attempt in range(max_retries):
        price, market_cap = get_ticker_data_yahoo(ticker)
        if price and market_cap:
            return ticker, price, market_cap, 'yahoo'
        time.sleep(1)  # Wait to avoid rate limiting
    
    # If we still don't have data, check if we might have one of the values
    return ticker, price, market_cap, 'incomplete'

def update_historical_data(data, results):
    """Update the historical data with new ticker information"""
    price_df = data['price_df'].copy()
    mcap_df = data['mcap_df'].copy()
    date = data['date']
    
    updates_made = 0
    
    for ticker, price, market_cap, source in results:
        if price is not None:
            if ticker not in price_df.columns:
                price_df[ticker] = np.nan
            price_df.loc[date, ticker] = price
            logging.info(f"Updated price for {ticker} to {price}")
            updates_made += 1
        
        if market_cap is not None:
            if ticker not in mcap_df.columns:
                mcap_df[ticker] = np.nan
            mcap_df.loc[date, ticker] = market_cap
            logging.info(f"Updated market cap for {ticker} to {market_cap}")
            updates_made += 1
    
    # Save updated data
    if updates_made > 0:
        price_df.to_csv('data/historical_ticker_prices.csv')
        mcap_df.to_csv('data/historical_ticker_marketcap.csv')
        logging.info(f"Saved {updates_made} updates to historical data files")
    
    return updates_made

def get_api_keys():
    """Get API keys from environment variables"""
    import os
    
    api_keys = {}
    
    # Finnhub
    finnhub_key = os.environ.get('FINNHUB_API_KEY')
    if finnhub_key:
        api_keys['FINNHUB_API_KEY'] = finnhub_key
    
    # AlphaVantage
    alpha_key = os.environ.get('ALPHAVANTAGE_API_KEY')
    if alpha_key:
        api_keys['ALPHAVANTAGE_API_KEY'] = alpha_key
    
    return api_keys

def complete_missing_data():
    """Complete all missing ticker data"""
    logging.info("Starting aggressive data completion...")
    
    # Get missing tickers
    data = get_missing_tickers()
    if not data:
        logging.error("Failed to get missing tickers data")
        return False
    
    # Get API keys
    api_keys = get_api_keys()
    if not api_keys:
        logging.warning("No API keys found. Some data sources will not be available.")
    
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
    
    # Process all missing tickers
    logging.info("Processing missing tickers...")
    results = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_ticker = {
            executor.submit(process_ticker, ticker, data, api_keys): ticker 
            for ticker in all_missing
        }
        
        for future in tqdm(concurrent.futures.as_completed(future_to_ticker), total=len(all_missing)):
            ticker = future_to_ticker[future]
            try:
                result = future.result()
                if result:
                    results.append(result)
                    if result[1] is not None and result[2] is not None:
                        logging.info(f"✓ Got complete data for {ticker} from {result[3]}")
                    elif result[1] is not None:
                        logging.info(f"✓ Got only price for {ticker} from {result[3]}")
                    elif result[2] is not None:
                        logging.info(f"✓ Got only market cap for {ticker} from {result[3]}")
                    else:
                        logging.warning(f"✗ Could not get data for {ticker}")
            except Exception as e:
                logging.error(f"Error processing {ticker}: {e}")
    
    # Update historical data
    updates = update_historical_data(data, results)
    logging.info(f"Made {updates} updates to historical data")
    
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
    success = complete_missing_data()
    sys.exit(0 if success else 1)