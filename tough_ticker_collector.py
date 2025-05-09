#!/usr/bin/env python3
# tough_ticker_collector.py
# -----------------------------------------------------------
# A specialized collector for difficult-to-get tickers
# Uses more aggressive retry logic and custom handling for problematic tickers

import os
import sys
import time
import random
import pandas as pd
import numpy as np
import datetime
import pytz
import logging
import requests
import yfinance as yf

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('tough_ticker_collection.log'),
        logging.StreamHandler()
    ]
)

def get_eastern_time():
    """Get current time in US Eastern timezone"""
    eastern = pytz.timezone('US/Eastern')
    return datetime.datetime.now(eastern)

def get_api_keys():
    """Get API keys from environment variables"""
    return {
        'finnhub': os.environ.get('FINNHUB_API_KEY'),
        'alphavantage': os.environ.get('ALPHAVANTAGE_API_KEY')
    }

def get_missing_tickers():
    """Get list of official tickers with missing data"""
    official_tickers = []
    try:
        with open('official_tickers.csv', 'r') as f:
            for line in f:
                ticker = line.strip()
                if ticker:
                    official_tickers.append(ticker)
    except Exception as e:
        logging.error(f"Error reading official tickers file: {e}")
        return None
    
    try:
        # Load data files
        price_df = pd.read_csv('data/historical_ticker_prices.csv', index_col=0)
        mcap_df = pd.read_csv('data/historical_ticker_marketcap.csv', index_col=0)
        
        # Get latest date
        latest_date = price_df.index[-1]
        
        missing_tickers = []
        
        for ticker in official_tickers:
            price_missing = ticker not in price_df.columns or pd.isna(price_df.loc[latest_date, ticker])
            mcap_missing = ticker not in mcap_df.columns or pd.isna(mcap_df.loc[latest_date, ticker])
            
            if price_missing or mcap_missing:
                missing_tickers.append(ticker)
        
        return {
            'date': latest_date,
            'total_official': len(official_tickers),
            'missing_count': len(missing_tickers),
            'complete_count': len(official_tickers) - len(missing_tickers),
            'coverage_pct': (len(official_tickers) - len(missing_tickers)) / len(official_tickers) * 100,
            'missing_tickers': missing_tickers,
            'price_df': price_df,
            'mcap_df': mcap_df
        }
    except Exception as e:
        logging.error(f"Error getting missing data: {e}")
        return None

def get_ticker_data_finnhub(ticker, api_key, retries=5, initial_delay=1):
    """Get both price and market cap data from Finnhub with exponential backoff retries"""
    price = None
    market_cap = None
    
    delay = initial_delay
    
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
            
            # Add jitter to avoid rate limiting
            jitter = random.uniform(0.5, 1.5)
            time.sleep(delay * jitter)  
            
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
            
            # Add jitter to avoid rate limiting
            jitter = random.uniform(0.5, 1.5)
            time.sleep(delay * jitter)
        
        except Exception as e:
            logging.warning(f"Finnhub error for {ticker} (attempt {attempt+1}/{retries}): {e}")
        
        # Exponential backoff
        delay = initial_delay * (2 ** attempt)
    
    return price, market_cap

def get_ticker_data_yahoo(ticker, retries=5, initial_delay=2):
    """Get both price and market cap data from Yahoo Finance with exponential backoff retries"""
    price = None
    market_cap = None
    
    delay = initial_delay
    
    for attempt in range(retries):
        try:
            # Use a different user agent on each attempt
            user_agents = [
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Safari/605.1.15',
                'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:90.0) Gecko/20100101 Firefox/90.0'
            ]
            
            user_agent = random.choice(user_agents)
            yf.utils.user_agent_headers['User-Agent'] = user_agent
            
            ticker_obj = yf.Ticker(ticker)
            
            # Get price
            ticker_hist = ticker_obj.history(period="1d")
            if not ticker_hist.empty and not pd.isna(ticker_hist['Close'].iloc[-1]):
                price = ticker_hist['Close'].iloc[-1]
                logging.info(f"  ✓ Got price from Yahoo: {price}")
            else:
                logging.warning(f"  ✗ Failed to get price from Yahoo")
            
            # Add jitter to avoid rate limiting
            jitter = random.uniform(0.8, 1.2)
            time.sleep(delay * jitter)
            
            # Get market cap
            ticker_info = ticker_obj.info
            if 'marketCap' in ticker_info and ticker_info['marketCap'] > 0:
                market_cap = ticker_info['marketCap']
                logging.info(f"  ✓ Got market cap from Yahoo: {market_cap:,.2f}")
            else:
                logging.warning(f"  ✗ Failed to get market cap from Yahoo")
            
            # If we got both price and market cap, return them
            if price is not None and market_cap is not None:
                return price, market_cap
            
            # Add jitter to avoid rate limiting
            jitter = random.uniform(0.8, 1.2)
            time.sleep(delay * jitter)
        
        except Exception as e:
            logging.warning(f"Yahoo error for {ticker} (attempt {attempt+1}/{retries}): {e}")
        
        # Exponential backoff
        delay = initial_delay * (2 ** attempt)
    
    return price, market_cap

def get_ticker_data_alphavantage(ticker, api_key, retries=2, delay=5):
    """Get both price and market cap data from AlphaVantage with retries"""
    price = None
    market_cap = None
    
    for attempt in range(retries):
        try:
            # Get price from AlphaVantage
            price_url = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={ticker}&apikey={api_key}"
            price_response = requests.get(price_url)
            price_data = price_response.json()
            
            if 'Global Quote' in price_data and '05. price' in price_data['Global Quote']:
                price_str = price_data['Global Quote']['05. price']
                price = float(price_str)
                logging.info(f"  ✓ Got price from AlphaVantage: {price}")
            else:
                logging.warning(f"  ✗ Failed to get price from AlphaVantage")
                if 'Information' in price_data:
                    logging.warning(f"  AlphaVantage API unexpected response (quote) for {ticker}: {price_data}")
            
            # Add significant delay to avoid rate limiting
            time.sleep(delay)
            
            # Get market cap from AlphaVantage OVERVIEW endpoint
            overview_url = f"https://www.alphavantage.co/query?function=OVERVIEW&symbol={ticker}&apikey={api_key}"
            overview_response = requests.get(overview_url)
            overview_data = overview_response.json()
            
            if 'MarketCapitalization' in overview_data:
                market_cap_str = overview_data['MarketCapitalization']
                try:
                    market_cap = float(market_cap_str)
                    logging.info(f"  ✓ Got market cap from AlphaVantage: {market_cap:,.2f}")
                except (ValueError, TypeError):
                    logging.warning(f"  ✗ Failed to convert AlphaVantage market cap: {market_cap_str}")
            else:
                logging.warning(f"  ✗ Failed to get market cap from AlphaVantage")
                if 'Information' in overview_data:
                    logging.warning(f"  AlphaVantage API unexpected response (overview) for {ticker}: {overview_data}")
            
            # If we got both price and market cap, return them
            if price is not None and market_cap is not None:
                return price, market_cap
            
            # Add significant delay to avoid rate limiting
            time.sleep(delay)
        
        except Exception as e:
            logging.warning(f"AlphaVantage error for {ticker} (attempt {attempt+1}/{retries}): {e}")
            time.sleep(delay)
    
    return price, market_cap

def get_manual_ticker_data(ticker):
    """Get price and market cap by searching multiple sources with specialized handling"""
    price = None
    market_cap = None
    
    # Special case handling for problematic tickers
    if ticker == 'IAD':
        # This is usually "Intelidata Inc" or similar - often hard to get
        # fallback values from reliable external sources could be entered here
        price = None
        market_cap = None
        
        # Add any special retry logic for IAD
        try:
            # Try with a more specific ticker (sometimes IAD.L works)
            ticker_obj = yf.Ticker('IAD.L')
            ticker_hist = ticker_obj.history(period="1d")
            if not ticker_hist.empty and not pd.isna(ticker_hist['Close'].iloc[-1]):
                price = ticker_hist['Close'].iloc[-1]
                logging.info(f"  ✓ Got price for IAD.L from Yahoo: {price}")
        except Exception as e:
            logging.warning(f"Error getting IAD.L data: {e}")
    
    elif ticker == 'TTD':
        # The Trade Desk
        try:
            # Try with exact matches only
            ticker_obj = yf.Ticker('TTD')
            ticker_hist = ticker_obj.history(period="1d")
            if not ticker_hist.empty and not pd.isna(ticker_hist['Close'].iloc[-1]):
                price = ticker_hist['Close'].iloc[-1]
                logging.info(f"  ✓ Got price for TTD from Yahoo: {price}")
            
            ticker_info = ticker_obj.info
            if 'marketCap' in ticker_info and ticker_info['marketCap'] > 0:
                market_cap = ticker_info['marketCap']
                logging.info(f"  ✓ Got market cap for TTD from Yahoo: {market_cap:,.2f}")
        except Exception as e:
            logging.warning(f"Error getting TTD data: {e}")
    
    # Add other special cases as needed
    
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

def process_tough_ticker(ticker, data, api_keys, max_retries=5):
    """Process a single tough ticker using all available methods"""
    logging.info(f"Processing tough ticker: {ticker}...")
    
    # Try Finnhub with more aggressive retry logic
    if api_keys['finnhub']:
        logging.info(f"  Trying Finnhub for {ticker} with aggressive retry...")
        price, market_cap = get_ticker_data_finnhub(ticker, api_keys['finnhub'], retries=max_retries, initial_delay=2)
        
        # If we got complete data from Finnhub, update and return
        if price is not None and market_cap is not None:
            update_historical_data(data, ticker, price, market_cap)
            return True
    else:
        logging.warning("Finnhub API key not available")
    
    # Try Yahoo with more aggressive retry logic
    logging.info(f"  Trying Yahoo Finance for {ticker} with aggressive retry...")
    price_y, market_cap_y = get_ticker_data_yahoo(ticker, retries=max_retries, initial_delay=3)
    
    # Capture any partial data
    price = price_y
    market_cap = market_cap_y
    
    # If we got complete data from Yahoo, update and return
    if price is not None and market_cap is not None:
        update_historical_data(data, ticker, price, market_cap)
        return True
    
    # Try AlphaVantage if we still need data
    if api_keys['alphavantage'] and (price is None or market_cap is None):
        logging.info(f"  Trying AlphaVantage for {ticker}...")
        price_av, market_cap_av = get_ticker_data_alphavantage(ticker, api_keys['alphavantage'], retries=2, delay=10)
        
        # Fill in any missing parts
        if price is None:
            price = price_av
        if market_cap is None:
            market_cap = market_cap_av
    else:
        logging.warning("AlphaVantage API key not available or not needed")
    
    # As a last resort, try manual data handling
    if price is None or market_cap is None:
        logging.info(f"  Trying specialized handling for {ticker}...")
        price_manual, market_cap_manual = get_manual_ticker_data(ticker)
        
        # Fill in any missing parts
        if price is None:
            price = price_manual
        if market_cap is None:
            market_cap = market_cap_manual
    
    # Update with whatever data we have (might be partial)
    if price is not None or market_cap is not None:
        update_historical_data(data, ticker, price, market_cap)
        return price is not None and market_cap is not None
    
    return False

def collect_tough_tickers(tickers=None, max_tickers=None, delay_between=15):
    """Collect data for especially tough tickers with aggressive retry logic"""
    logging.info("Starting tough ticker collection with aggressive retry logic")
    
    # Get API keys
    api_keys = get_api_keys()
    if not api_keys['finnhub'] and not api_keys['alphavantage']:
        logging.error("No API keys available. Cannot collect data.")
        return False
    
    # Get missing data
    data = get_missing_tickers()
    if not data:
        logging.error("Failed to get missing tickers data")
        return False
    
    # Use provided tickers or get all missing tickers
    missing_tickers = tickers if tickers else data['missing_tickers']
    
    # Limit number of tickers if requested
    if max_tickers and len(missing_tickers) > max_tickers:
        missing_tickers = missing_tickers[:max_tickers]
    
    logging.info(f"Processing {len(missing_tickers)} tough tickers out of {data['missing_count']} missing")
    
    # Process each ticker
    success_count = 0
    for i, ticker in enumerate(missing_tickers):
        logging.info(f"[{i+1}/{len(missing_tickers)}] Processing {ticker}...")
        success = process_tough_ticker(ticker, data, api_keys)
        
        if success:
            success_count += 1
            logging.info(f"SUCCESS: Complete data for {ticker}")
        else:
            logging.warning(f"WARNING: Could not get complete data for {ticker}")
        
        # Sleep between tickers to avoid rate limits
        if i < len(missing_tickers) - 1:
            sleep_time = delay_between + random.uniform(1, 5)  # Add randomness
            logging.info(f"Sleeping for {sleep_time:.1f} seconds to avoid rate limits...")
            time.sleep(sleep_time)
    
    # Get updated coverage
    updated_data = get_missing_tickers()
    if updated_data:
        logging.info("=" * 50)
        logging.info("COLLECTION RESULTS")
        logging.info("=" * 50)
        logging.info(f"Processed {len(missing_tickers)} tickers")
        logging.info(f"Successfully completed {success_count} tickers")
        logging.info(f"Current coverage: {updated_data['complete_count']}/{updated_data['total_official']} tickers ({updated_data['coverage_pct']:.1f}%)")
        
        if updated_data['missing_count'] > 0:
            logging.info(f"Still missing {updated_data['missing_count']} tickers:")
            for i in range(0, len(updated_data['missing_tickers']), 5):
                row = updated_data['missing_tickers'][i:i+5]
                logging.info(f"  {' '.join(f'{t:<6}' for t in row)}")
        else:
            logging.info("SUCCESS! 100% ticker coverage achieved!")
    
    return success_count > 0

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Collect data for tough tickers with aggressive retry logic')
    parser.add_argument('tickers', nargs='*', help='Specific tickers to collect (optional)')
    parser.add_argument('--max', type=int, default=None, help='Maximum number of tickers to process')
    parser.add_argument('--delay', type=int, default=15, help='Seconds to wait between processing tickers')
    args = parser.parse_args()
    
    collect_tough_tickers(tickers=args.tickers if args.tickers else None, 
                          max_tickers=args.max,
                          delay_between=args.delay)