"""
Fix market cap data to show proper daily changes over the 30-day period.

This script calculates accurate daily market caps based on:
1. Shares outstanding data from Polygon API
2. Historical daily price data for each ticker
3. Sector allocations for all tickers

The results are aggregated by sector and exported to CSV and text formats.
"""

import os
import pandas as pd
import numpy as np
import json
import sqlite3
from datetime import datetime, timedelta
import requests
import time
import random

# API Keys
POLYGON_API_KEY = os.environ.get('POLYGON_API_KEY')

# Configure logging
import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='fix_market_caps.log'
)

# Database connection
def get_db_connection():
    conn = sqlite3.connect('data/t2d_pulse.db')
    conn.row_factory = sqlite3.Row
    return conn

# Create tables if they don't exist
def create_tables():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Create share_counts table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS share_counts (
        ticker TEXT PRIMARY KEY,
        shares INTEGER,
        last_updated TEXT
    )
    ''')
    
    # Create ticker_prices table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS ticker_prices (
        ticker TEXT,
        date TEXT,
        close_price REAL,
        PRIMARY KEY (ticker, date)
    )
    ''')
    
    # Create ticker_market_caps table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS ticker_market_caps (
        ticker TEXT,
        date TEXT,
        market_cap REAL,
        PRIMARY KEY (ticker, date)
    )
    ''')
    
    # Create sector_tickers table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS sector_tickers (
        ticker TEXT,
        sector TEXT,
        PRIMARY KEY (ticker, sector)
    )
    ''')
    
    # Create sector_market_caps table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS sector_market_caps (
        date TEXT,
        sector TEXT,
        market_cap REAL,
        PRIMARY KEY (date, sector)
    )
    ''')
    
    conn.commit()
    conn.close()

# Get all unique tickers from the sector assignments
def get_all_tickers():
    try:
        # Read from CSV first if available (main source)
        sector_tickers_df = pd.read_csv('data/sector_tickers.csv')
        return sector_tickers_df['ticker'].unique().tolist()
    except Exception as e:
        logging.error(f"Error reading sector tickers from CSV: {e}")
        
        # Fall back to database if CSV fails
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT DISTINCT ticker FROM sector_tickers')
            tickers = [row[0] for row in cursor.fetchall()]
            conn.close()
            return tickers
        except Exception as e:
            logging.error(f"Error reading sector tickers from database: {e}")
            
            # Hard-coded fallback list of 92 tickers
            return [
                "AFRM", "AAPL", "ADBE", "ADSK", "AMD", "AMZN", "ABNB", "APP", "APPS", 
                "ARM", "AVGO", "BABA", "BILL", "BKNG", "CCCS", "CHKP", "CHWY", "COIN", 
                "CPRT", "CRM", "CRTO", "CRWD", "CSGP", "CSCO", "CTSH", "CYBR", "DDOG", 
                "DELL", "DV", "DXC", "EBAY", "ESTC", "ETSY", "FI", "FIS", "FISV", "FTNT", 
                "GPN", "GOOGL", "GTLB", "GWRE", "HUBS", "IBM", "ICE", "INFY", "INTC", 
                "INTU", "LOGI", "MDB", "META", "MGNI", "MSFT", "NET", "NFLX", "NOW", 
                "NVDA", "OKTA", "ORCL", "PANW", "PCOR", "PDD", "PINS", "PLTR", "PSTG", 
                "PUBM", "PYPL", "QCOM", "S", "SAP", "SE", "SHOP", "SMCI", "SNAP", "SNOW", 
                "SPOT", "SSNC", "SSYS", "STX", "TEAM", "TSM", "TTAN", "TTD", "TRIP", 
                "WDC", "WIT", "WMT", "WDAY", "XYZ", "YELP", "ZS", "ACN", "AMAT"
            ]

# Get sector assignments for each ticker
def get_sector_assignments():
    try:
        # Try to read from CSV first
        sector_tickers_df = pd.read_csv('data/sector_tickers.csv')
        sector_assignments = {}
        
        for _, row in sector_tickers_df.iterrows():
            ticker = row['ticker']
            sector = row['sector']
            
            if ticker not in sector_assignments:
                sector_assignments[ticker] = []
            
            sector_assignments[ticker].append(sector)
        
        return sector_assignments
    
    except Exception as e:
        logging.error(f"Error reading sector assignments from CSV: {e}")
        
        # Fall back to database
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT ticker, sector FROM sector_tickers')
            rows = cursor.fetchall()
            conn.close()
            
            sector_assignments = {}
            for row in rows:
                ticker = row[0]
                sector = row[1]
                
                if ticker not in sector_assignments:
                    sector_assignments[ticker] = []
                
                sector_assignments[ticker].append(sector)
            
            return sector_assignments
        
        except Exception as e:
            logging.error(f"Error reading sector assignments from database: {e}")
            
            # Return an empty dict if all else fails
            return {}

# Get shares outstanding for tickers from database
def get_shares_outstanding():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT ticker, shares FROM share_counts')
    shares_data = cursor.fetchall()
    conn.close()
    
    shares_dict = {}
    for row in shares_data:
        ticker = row[0]
        shares = row[1]
        shares_dict[ticker] = shares
    
    return shares_dict

# Get shares from Polygon API for tickers missing from the database
def fetch_shares_from_polygon(ticker):
    try:
        url = f"https://api.polygon.io/v3/reference/tickers/{ticker}?apiKey={POLYGON_API_KEY}"
        response = requests.get(url)
        data = response.json()
        
        # Check if we have the data
        if data.get('status') == 'OK' and 'results' in data:
            # Check for weighted shares outstanding (fully diluted)
            if 'weighted_shares_outstanding' in data['results']:
                return data['results']['weighted_shares_outstanding']
            # Fall back to regular shares outstanding
            elif 'share_class_shares_outstanding' in data['results']:
                return data['results']['share_class_shares_outstanding']
            # If neither is available, use the total shares value
            elif 'total_shares_outstanding' in data['results']:
                return data['results']['total_shares_outstanding']
        
        # If none of the above, try the ticker details endpoint
        url = f"https://api.polygon.io/v3/reference/tickers/{ticker}/snapshot?apiKey={POLYGON_API_KEY}"
        response = requests.get(url)
        data = response.json()
        
        if data.get('results') and data['results'].get('shares_outstanding'):
            return data['results']['shares_outstanding']
        
        # If still not found, return None
        return None
    
    except Exception as e:
        logging.error(f"Error fetching shares for {ticker} from Polygon: {e}")
        return None

# Fetch historical prices for a ticker within a date range
def fetch_historical_prices(ticker, start_date, end_date):
    logging.info(f"Fetching historical prices for {ticker} from {start_date} to {end_date}")
    try:
        url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start_date}/{end_date}?apiKey={POLYGON_API_KEY}"
        response = requests.get(url)
        data = response.json()
        
        if data.get('results'):
            # Convert timestamp to date and extract close price
            price_data = []
            for result in data['results']:
                # Convert milliseconds timestamp to date
                date = datetime.fromtimestamp(result['t'] / 1000).strftime('%Y-%m-%d')
                close_price = result['c']
                price_data.append({
                    'ticker': ticker,
                    'date': date,
                    'close_price': close_price
                })
            
            # Return as DataFrame
            return pd.DataFrame(price_data)
        
        return pd.DataFrame(columns=['ticker', 'date', 'close_price'])
    
    except Exception as e:
        logging.error(f"Error fetching historical prices for {ticker}: {e}")
        return pd.DataFrame(columns=['ticker', 'date', 'close_price'])

# Save historical prices to database
def save_prices_to_db(prices_df):
    if prices_df.empty:
        return
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    for _, row in prices_df.iterrows():
        try:
            cursor.execute(
                'INSERT OR REPLACE INTO ticker_prices (ticker, date, close_price) VALUES (?, ?, ?)',
                (row['ticker'], row['date'], row['close_price'])
            )
        except Exception as e:
            logging.error(f"Error saving price for {row['ticker']} on {row['date']}: {e}")
    
    conn.commit()
    conn.close()

# Calculate market cap for each ticker and date
def calculate_market_caps(tickers, shares_dict, start_date, end_date):
    logging.info(f"Calculating market caps for {len(tickers)} tickers from {start_date} to {end_date}")
    
    # Get dates in the range
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Clear existing market cap data for the date range
    cursor.execute(
        'DELETE FROM ticker_market_caps WHERE date BETWEEN ? AND ?', 
        (start_date, end_date)
    )
    
    # Process each ticker
    for ticker in tickers:
        # Skip if we don't have share count
        if ticker not in shares_dict:
            logging.warning(f"No share count available for {ticker}, skipping market cap calculation")
            continue
        
        # Get historical prices for this ticker
        cursor.execute(
            'SELECT date, close_price FROM ticker_prices WHERE ticker = ? AND date BETWEEN ? AND ?',
            (ticker, start_date, end_date)
        )
        prices = cursor.fetchall()
        
        for price_row in prices:
            date = price_row[0]
            close_price = price_row[1]
            shares = shares_dict[ticker]
            
            # Calculate market cap (price * shares)
            market_cap = close_price * shares
            
            # Save to database
            try:
                cursor.execute(
                    'INSERT OR REPLACE INTO ticker_market_caps (ticker, date, market_cap) VALUES (?, ?, ?)',
                    (ticker, date, market_cap)
                )
            except Exception as e:
                logging.error(f"Error saving market cap for {ticker} on {date}: {e}")
    
    conn.commit()
    conn.close()

# Calculate sector market caps (aggregate ticker market caps by sector)
def calculate_sector_market_caps(sector_assignments, start_date, end_date):
    logging.info(f"Calculating sector market caps from {start_date} to {end_date}")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Clear existing sector market cap data for the date range
    cursor.execute(
        'DELETE FROM sector_market_caps WHERE date BETWEEN ? AND ?', 
        (start_date, end_date)
    )
    
    # Get all dates in the range
    cursor.execute(
        'SELECT DISTINCT date FROM ticker_market_caps WHERE date BETWEEN ? AND ?',
        (start_date, end_date)
    )
    dates = [row[0] for row in cursor.fetchall()]
    
    # Get list of sectors
    unique_sectors = set()
    for sectors in sector_assignments.values():
        unique_sectors.update(sectors)
    
    for date in dates:
        for sector in unique_sectors:
            sector_mcap = 0
            
            # Get tickers in this sector
            tickers_in_sector = [ticker for ticker, sectors in sector_assignments.items() if sector in sectors]
            
            for ticker in tickers_in_sector:
                # Get market cap for this ticker on this date
                cursor.execute(
                    'SELECT market_cap FROM ticker_market_caps WHERE ticker = ? AND date = ?',
                    (ticker, date)
                )
                result = cursor.fetchone()
                
                if result:
                    market_cap = result[0]
                    sector_mcap += market_cap
            
            # Save sector market cap to database
            try:
                cursor.execute(
                    'INSERT OR REPLACE INTO sector_market_caps (date, sector, market_cap) VALUES (?, ?, ?)',
                    (date, sector, sector_mcap)
                )
            except Exception as e:
                logging.error(f"Error saving sector market cap for {sector} on {date}: {e}")
    
    conn.commit()
    conn.close()

# Export sector market caps to CSV
def export_sector_market_caps(start_date, end_date):
    logging.info(f"Exporting sector market caps from {start_date} to {end_date}")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get all dates in the range
    cursor.execute(
        'SELECT DISTINCT date FROM sector_market_caps WHERE date BETWEEN ? AND ? ORDER BY date',
        (start_date, end_date)
    )
    dates = [row[0] for row in cursor.fetchall()]
    
    # Get all sectors
    cursor.execute('SELECT DISTINCT sector FROM sector_market_caps')
    sectors = [row[0] for row in cursor.fetchall()]
    
    # Create a DataFrame to hold the data
    data = {'Date': dates}
    
    for sector in sectors:
        sector_mcaps = []
        
        for date in dates:
            cursor.execute(
                'SELECT market_cap FROM sector_market_caps WHERE date = ? AND sector = ?',
                (date, sector)
            )
            result = cursor.fetchone()
            
            if result:
                sector_mcaps.append(result[0])
            else:
                sector_mcaps.append(None)
        
        data[sector] = sector_mcaps
    
    conn.close()
    
    # Create DataFrame and export to CSV
    df = pd.DataFrame(data)
    df.to_csv('sector_marketcap_table.csv', index=False)
    
    # Also export to a well-formatted text file
    with open('30day_sector_marketcap_table.txt', 'w') as f:
        # Write header
        f.write('Historical Sector Market Capitalization Data (Last 30 Market Days, Values in Billions USD)\n\n')
        
        # Create a nice table
        header = f"{'Date':<12}"
        for sector in sectors:
            header += f"{sector:<18}"
        f.write(header + '\n')
        
        # Add separator line
        f.write('-' * (12 + 18 * len(sectors)) + '\n')
        
        # Write data rows
        for i, date in enumerate(dates):
            line = f"{date:<12}"
            for sector in sectors:
                market_cap = data[sector][i]
                if market_cap is not None:
                    line += f"{market_cap/1000000000:.2f}{'B':<15}"
                else:
                    line += f"{'N/A':<18}"
            f.write(line + '\n')
    
    logging.info(f"Exported sector market caps to CSV and text file")
    return df

# Main function
def main():
    logging.info("Starting market cap fix process")
    
    # Make sure data directory exists
    os.makedirs('data', exist_ok=True)
    
    # Create database tables if needed
    create_tables()
    
    # Get all tickers
    tickers = get_all_tickers()
    logging.info(f"Found {len(tickers)} tickers to process")
    
    # Get sector assignments
    sector_assignments = get_sector_assignments()
    logging.info(f"Found sector assignments for {len(sector_assignments)} tickers")
    
    # Get shares outstanding
    shares_dict = get_shares_outstanding()
    logging.info(f"Found shares outstanding for {len(shares_dict)} tickers")
    
    # Fetch shares for tickers missing from database
    for ticker in tickers:
        if ticker not in shares_dict:
            logging.info(f"Fetching shares for {ticker} from Polygon API")
            shares = fetch_shares_from_polygon(ticker)
            
            if shares:
                shares_dict[ticker] = shares
                
                # Save to database
                conn = get_db_connection()
                cursor = conn.cursor()
                cursor.execute(
                    'INSERT OR REPLACE INTO share_counts (ticker, shares, last_updated) VALUES (?, ?, ?)',
                    (ticker, shares, datetime.now().strftime('%Y-%m-%d'))
                )
                conn.commit()
                conn.close()
                
                logging.info(f"Saved shares for {ticker}: {shares}")
            
            # Sleep briefly to avoid API rate limits
            time.sleep(0.12)
    
    # Calculate date range (last 30 business days)
    end_date = datetime.now()
    # If today is weekend, use last Friday
    if end_date.weekday() >= 5:  # Saturday or Sunday
        end_date = end_date - timedelta(days=end_date.weekday() - 4)
    
    # Go back 42 calendar days to ensure we get 30 business days
    start_date = end_date - timedelta(days=42)
    
    # Format dates
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    
    # Fetch historical prices for each ticker
    for ticker in tickers:
        # Check if we have shares data
        if ticker not in shares_dict:
            logging.warning(f"No share count data for {ticker}, skipping price fetch")
            continue
        
        # Fetch prices from Polygon
        price_df = fetch_historical_prices(ticker, start_date_str, end_date_str)
        
        if not price_df.empty:
            # Save to database
            save_prices_to_db(price_df)
            logging.info(f"Saved {len(price_df)} days of price data for {ticker}")
        else:
            logging.warning(f"No price data returned for {ticker}")
        
        # Sleep briefly to avoid API rate limits
        time.sleep(0.12)
    
    # Calculate market caps
    calculate_market_caps(tickers, shares_dict, start_date_str, end_date_str)
    
    # Calculate sector market caps
    calculate_sector_market_caps(sector_assignments, start_date_str, end_date_str)
    
    # Export to CSV and text file
    export_sector_market_caps(start_date_str, end_date_str)
    
    logging.info("Market cap fix process completed successfully")

if __name__ == "__main__":
    main()