#!/usr/bin/env python3
# remaining_tickers_plan.py
# -----------------------------------------------------------
# Strategy for completing remaining ticker data
# Creates a recovery plan for the 4 remaining tickers with significant delays
# to respect API rate limits

import pandas as pd
import numpy as np
import logging
import sys
import time
from datetime import datetime, timedelta

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('ticker_completion_plan.log')
    ]
)
logger = logging.getLogger(__name__)

def get_missing_tickers():
    """Get list of tickers with missing data"""
    try:
        # Load data files
        logger.info("Loading historical ticker data to identify missing tickers")
        price_df = pd.read_csv('data/historical_ticker_prices.csv', index_col=0)
        mcap_df = pd.read_csv('data/historical_ticker_marketcap.csv', index_col=0)
        
        # Get most recent date
        latest_date = price_df.index[-1]
        logger.info(f"Checking data completeness for {latest_date}")
        
        # Load official tickers
        tickers = []
        with open('official_tickers.csv', 'r') as f:
            for line in f:
                ticker = line.strip()
                if ticker:
                    tickers.append(ticker)
        
        # Find missing tickers
        missing_price = []
        missing_mcap = []
        missing_both = []
        
        for ticker in tickers:
            price_missing = (ticker not in price_df.columns or 
                             pd.isna(price_df.loc[latest_date, ticker]))
            mcap_missing = (ticker not in mcap_df.columns or 
                            pd.isna(mcap_df.loc[latest_date, ticker]))
            
            if price_missing and mcap_missing:
                missing_both.append(ticker)
            elif price_missing:
                missing_price.append(ticker)
            elif mcap_missing:
                missing_mcap.append(ticker)
        
        return missing_both, missing_price, missing_mcap
        
    except Exception as e:
        logger.error(f"Error identifying missing tickers: {e}")
        return [], [], []

def create_collection_plan():
    """Create a recovery plan for completing the remaining tickers"""
    missing_both, missing_price, missing_mcap = get_missing_tickers()
    
    all_missing = missing_both + missing_price + missing_mcap
    logger.info(f"Found {len(all_missing)} tickers with at least one missing data point")
    
    if missing_both:
        logger.info(f"Tickers missing both price and market cap ({len(missing_both)}): {', '.join(missing_both)}")
    if missing_price:
        logger.info(f"Tickers missing only price ({len(missing_price)}): {', '.join(missing_price)}")
    if missing_mcap:
        logger.info(f"Tickers missing only market cap ({len(missing_mcap)}): {', '.join(missing_mcap)}")
    
    if not all_missing:
        logger.info("No missing tickers found! Data collection is complete.")
        return
    
    # Generate collection plan with collection windows spread across the day
    logger.info("Generating collection plan with time windows to respect API rate limits")
    
    now = datetime.now()
    plan = []
    
    # Spread collection over next few hours with substantial gaps
    start_times = [
        now + timedelta(minutes=15),  # First batch in 15 min
        now + timedelta(minutes=45),  # Second batch in 45 min
        now + timedelta(hours=2),     # Third batch in 2 hours
        now + timedelta(hours=4)      # Final batch in 4 hours
    ]
    
    # Distribute tickers across start times
    for i, ticker in enumerate(all_missing):
        batch = i % len(start_times)
        collection_time = start_times[batch]
        
        if ticker in missing_both:
            data_needed = "price and market cap"
        elif ticker in missing_price:
            data_needed = "price only"
        else:
            data_needed = "market cap only"
        
        plan.append({
            'ticker': ticker,
            'collection_time': collection_time,
            'data_needed': data_needed,
            'batch': batch + 1,
            'scheduled_time': collection_time.strftime('%Y-%m-%d %H:%M:%S')
        })
    
    # Sort by collection time
    plan.sort(key=lambda x: x['collection_time'])
    
    # Print plan
    logger.info("TICKER COLLECTION PLAN:")
    logger.info("=" * 80)
    logger.info(f"{'Ticker':<6} | {'Batch':<5} | {'Data Needed':<20} | {'Scheduled Time':<20}")
    logger.info("-" * 80)
    
    for item in plan:
        logger.info(f"{item['ticker']:<6} | {item['batch']:<5} | {item['data_needed']:<20} | {item['scheduled_time']:<20}")
    
    logger.info("=" * 80)
    logger.info("To implement this plan, execute 'python get_xyz_data.py' followed by")
    logger.info("'python collect_remaining_tickers.py' which will handle collection with")
    logger.info("appropriate timing to avoid API rate limits.")
    
    # Save plan to file
    plan_df = pd.DataFrame(plan)
    plan_df.to_csv('ticker_collection_plan.csv', index=False)
    logger.info("Plan saved to ticker_collection_plan.csv")
    
    # Generate collection script
    generate_collection_script(plan)
    
    return plan

def generate_collection_script(plan):
    """Generate a Python script to implement the collection plan"""
    script_content = """#!/usr/bin/env python3
# collect_remaining_tickers.py
# -----------------------------------------------------------
# Automated collection of remaining tickers with timing controls
# Generated by remaining_tickers_plan.py

import pandas as pd
import numpy as np
import yfinance as yf
import logging
import sys
import time
from datetime import datetime, timedelta

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('ticker_collection.log')
    ]
)
logger = logging.getLogger(__name__)

def update_historical_data(ticker, price=None, market_cap=None):
    \"\"\"Update historical data files with new ticker data\"\"\"
    try:
        # Load data files
        price_df = pd.read_csv('data/historical_ticker_prices.csv', index_col=0)
        mcap_df = pd.read_csv('data/historical_ticker_marketcap.csv', index_col=0)
        
        # Get current date
        latest_date = price_df.index[-1]
        
        # Update data
        if price is not None:
            logger.info(f"Updating {ticker} price to {price}")
            price_df.loc[latest_date, ticker] = price
            
        if market_cap is not None:
            logger.info(f"Updating {ticker} market cap to {market_cap:,}")
            mcap_df.loc[latest_date, ticker] = market_cap
        
        # Save updated data
        price_df.to_csv('data/historical_ticker_prices.csv')
        mcap_df.to_csv('data/historical_ticker_marketcap.csv')
        
        return True
    except Exception as e:
        logger.error(f"Error updating historical data for {ticker}: {e}")
        return False

def fetch_ticker_data(ticker, retry_count=3, initial_delay=5):
    \"\"\"Fetch ticker data with retry logic\"\"\"
    price = None
    market_cap = None
    
    for attempt in range(retry_count):
        delay = initial_delay * (2 ** attempt)
        logger.info(f"Attempt {attempt+1}/{retry_count} for {ticker} (delay: {delay}s)")
        time.sleep(delay)
        
        try:
            # Get price
            data = yf.Ticker(ticker).history(period='1d')
            if not data.empty:
                price = data['Close'].iloc[-1]
                logger.info(f"Got price for {ticker}: {price}")
            
            # Add delay between requests
            time.sleep(5)
            
            # Get market cap
            info = yf.Ticker(ticker).info
            market_cap = info.get('marketCap')
            if market_cap:
                logger.info(f"Got market cap for {ticker}: {market_cap:,}")
            
            if price is not None and market_cap is not None:
                break
                
        except Exception as e:
            logger.error(f"Error fetching {ticker} data: {e}")
    
    return price, market_cap

def execute_plan():
    \"\"\"Execute the ticker collection plan\"\"\"
    try:
        plan = pd.read_csv('ticker_collection_plan.csv')
        logger.info(f"Loaded collection plan with {len(plan)} items")
        
        for _, item in plan.iterrows():
            ticker = item['ticker']
            scheduled_time = datetime.strptime(item['scheduled_time'], '%Y-%m-%d %H:%M:%S')
            data_needed = item['data_needed']
            
            # Calculate wait time until scheduled collection
            now = datetime.now()
            wait_time = (scheduled_time - now).total_seconds()
            
            if wait_time > 0:
                logger.info(f"Waiting {wait_time:.1f} seconds until scheduled time for {ticker}")
                time.sleep(wait_time)
            
            logger.info(f"Collecting data for {ticker} ({data_needed})")
            price, market_cap = fetch_ticker_data(ticker)
            
            # Update only needed data
            if data_needed == "price and market cap":
                update_historical_data(ticker, price, market_cap)
            elif data_needed == "price only":
                update_historical_data(ticker, price=price)
            elif data_needed == "market cap only":
                update_historical_data(ticker, market_cap=market_cap)
            
            # Additional delay after completion
            time.sleep(10)
            
        logger.info("Collection plan execution complete")
        
    except Exception as e:
        logger.error(f"Error executing collection plan: {e}")

if __name__ == "__main__":
    logger.info("Starting execution of ticker collection plan")
    execute_plan()
"""
    
    # Write script to file
    with open('collect_remaining_tickers.py', 'w') as f:
        f.write(script_content)
    
    logger.info("Generated collection script: collect_remaining_tickers.py")

if __name__ == "__main__":
    logger.info("Analyzing missing tickers and creating collection plan")
    create_collection_plan()
    logger.info("Plan generation complete")