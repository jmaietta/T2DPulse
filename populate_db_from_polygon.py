#!/usr/bin/env python3
"""
Populate the PostgreSQL database with fresh ticker and market cap data directly from Polygon API.

This script:
1. Creates database tables if they don't exist
2. Loads ticker-sector mappings from the attached text file
3. Fetches fresh market cap data from Polygon API for each ticker
4. Calculates sector market caps correctly from the ticker data
5. Does NOT use any CSV data that might contain errors

IMPORTANT: This script only uses authentic data from Polygon API.
"""

import os
import sys
import csv
import time
import logging
import psycopg2
import requests
from datetime import datetime, date, timedelta
from typing import Dict, List, Tuple, Optional, Any, Set

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("polygon_db_import.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# API Key for Polygon
POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY")
if not POLYGON_API_KEY:
    logger.error("POLYGON_API_KEY environment variable is not set")
    sys.exit(1)

# Database connection
DB_URL = os.environ.get("DATABASE_URL")
if not DB_URL:
    logger.error("DATABASE_URL environment variable is not set")
    sys.exit(1)

# Source file for ticker-sector mappings
TICKER_SECTOR_FILE = "attached_assets/Pasted-AdTech-APP-AdTech-APPS-AdTech-CRTO-AdTech-DV-AdTech-GOOGL-AdTech-META-AdTech-MGNI-AdTech-PUBM-1746989811364.txt"

def get_db_connection():
    """Get a connection to the PostgreSQL database"""
    try:
        conn = psycopg2.connect(DB_URL)
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        raise

def create_db_tables():
    """Create database tables if they don't exist"""
    logger.info("Creating database tables if they don't exist")
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Create sectors table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sectors (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) UNIQUE NOT NULL,
                description TEXT
            )
        """)
        
        # Create tickers table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tickers (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(20) UNIQUE NOT NULL,
                name VARCHAR(255)
            )
        """)
        
        # Create ticker_sectors junction table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ticker_sectors (
                ticker_id INTEGER REFERENCES tickers(id) ON DELETE CASCADE,
                sector_id INTEGER REFERENCES sectors(id) ON DELETE CASCADE,
                PRIMARY KEY (ticker_id, sector_id)
            )
        """)
        
        # Create ticker_market_caps table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ticker_market_caps (
                id SERIAL PRIMARY KEY,
                ticker_id INTEGER REFERENCES tickers(id) ON DELETE CASCADE,
                date DATE NOT NULL,
                price NUMERIC,
                market_cap NUMERIC NOT NULL,
                shares_outstanding NUMERIC,
                data_source VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (ticker_id, date)
            )
        """)
        
        # Create sector_market_caps table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sector_market_caps (
                id SERIAL PRIMARY KEY,
                sector_id INTEGER REFERENCES sectors(id) ON DELETE CASCADE,
                date DATE NOT NULL,
                market_cap NUMERIC NOT NULL,
                sentiment_score NUMERIC,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (sector_id, date)
            )
        """)
        
        # Create view to query sector market caps more easily
        cursor.execute("""
            CREATE OR REPLACE VIEW sector_market_caps_view AS
            SELECT 
                s.name AS sector_name,
                smc.date,
                smc.market_cap,
                smc.sentiment_score
            FROM sector_market_caps smc
            JOIN sectors s ON smc.sector_id = s.id
            ORDER BY s.name, smc.date
        """)
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info("Database tables created successfully")
        return True
    except Exception as e:
        logger.error(f"Error creating database tables: {e}")
        return False

def load_ticker_sector_mappings():
    """
    Load ticker-sector mappings from the external file.
    This function uses the correct mappings from the original source file.
    """
    logger.info(f"Loading ticker-sector mappings from {TICKER_SECTOR_FILE}")
    
    mappings = []
    unique_sectors = set()
    unique_tickers = set()
    
    try:
        with open(TICKER_SECTOR_FILE, 'r') as file:
            for line in file:
                line = line.strip()
                if not line:
                    continue
                
                parts = line.strip().split('\t')
                if len(parts) != 2:
                    logger.warning(f"Invalid line format: {line}")
                    continue
                
                sector_name, ticker_symbol = parts[0].strip(), parts[1].strip()
                
                if not sector_name or not ticker_symbol:
                    logger.warning(f"Invalid mapping: {line}")
                    continue
                
                mappings.append((sector_name, ticker_symbol))
                unique_sectors.add(sector_name)
                unique_tickers.add(ticker_symbol)
        
        logger.info(f"Loaded {len(mappings)} ticker-sector mappings")
        logger.info(f"Found {len(unique_sectors)} unique sectors and {len(unique_tickers)} unique tickers")
        
        return mappings, unique_sectors, unique_tickers
    except Exception as e:
        logger.error(f"Error loading ticker-sector mappings: {e}")
        return [], set(), set()

def clear_existing_data():
    """
    Clear existing data from the database before importing fresh data.
    This ensures we don't mix old problematic data with new accurate data.
    """
    logger.info("Clearing existing data from the database")
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("DELETE FROM sector_market_caps")
        cursor.execute("DELETE FROM ticker_market_caps")
        cursor.execute("DELETE FROM ticker_sectors")
        cursor.execute("DELETE FROM tickers")
        cursor.execute("DELETE FROM sectors")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info("Existing data cleared successfully")
        return True
    except Exception as e:
        logger.error(f"Error clearing existing data: {e}")
        return False

def import_sectors_and_tickers(unique_sectors, unique_tickers):
    """
    Import sectors and tickers into the database
    """
    logger.info("Importing sectors and tickers into the database")
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Insert sectors
        sector_id_map = {}
        for sector_name in unique_sectors:
            cursor.execute(
                "INSERT INTO sectors (name) VALUES (%s) RETURNING id",
                (sector_name,)
            )
            sector_id = cursor.fetchone()[0]
            sector_id_map[sector_name] = sector_id
        
        # Insert tickers
        ticker_id_map = {}
        for ticker_symbol in unique_tickers:
            cursor.execute(
                "INSERT INTO tickers (symbol) VALUES (%s) RETURNING id",
                (ticker_symbol,)
            )
            ticker_id = cursor.fetchone()[0]
            ticker_id_map[ticker_symbol] = ticker_id
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"Imported {len(unique_sectors)} sectors and {len(unique_tickers)} tickers")
        return sector_id_map, ticker_id_map
    except Exception as e:
        logger.error(f"Error importing sectors and tickers: {e}")
        return {}, {}

def import_ticker_sector_mappings(mappings, sector_id_map, ticker_id_map):
    """
    Import ticker-sector mappings into the database
    """
    logger.info("Importing ticker-sector mappings into the database")
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Insert ticker-sector mappings
        for sector_name, ticker_symbol in mappings:
            sector_id = sector_id_map.get(sector_name)
            ticker_id = ticker_id_map.get(ticker_symbol)
            
            if sector_id is None or ticker_id is None:
                logger.warning(f"Missing sector ID or ticker ID for {sector_name} - {ticker_symbol}")
                continue
            
            cursor.execute(
                "INSERT INTO ticker_sectors (sector_id, ticker_id) VALUES (%s, %s)",
                (sector_id, ticker_id)
            )
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"Imported {len(mappings)} ticker-sector mappings")
        return True
    except Exception as e:
        logger.error(f"Error importing ticker-sector mappings: {e}")
        return False

def get_polygon_data(ticker_symbol):
    """
    Get ticker details and market cap from Polygon API
    Uses fully diluted share counts for market cap calculations
    """
    url = f"https://api.polygon.io/v3/reference/tickers/{ticker_symbol}?apiKey={POLYGON_API_KEY}"
    
    try:
        response = requests.get(url, timeout=10)
        
        if response.status_code != 200:
            logger.warning(f"Error fetching data for {ticker_symbol}: HTTP {response.status_code}")
            return None
        
        data = response.json()
        
        if 'results' not in data:
            logger.warning(f"Invalid response format for {ticker_symbol}")
            return None
        
        results = data['results']
        
        # Extract company name
        company_name = results.get('name', '')
        
        # Extract market cap directly if available
        market_cap = results.get('market_cap')
        
        # If market cap not provided, calculate from weighted shares and current price
        if not market_cap:
            shares = results.get('weighted_shares_outstanding') or results.get('share_class_shares_outstanding')
            
            # Need to get current price from another endpoint
            if shares:
                # Get current price
                price_url = f"https://api.polygon.io/v2/aggs/ticker/{ticker_symbol}/prev?apiKey={POLYGON_API_KEY}"
                price_response = requests.get(price_url, timeout=10)
                
                if price_response.status_code == 200:
                    price_data = price_response.json()
                    if 'results' in price_data and price_data['results']:
                        price = price_data['results'][0].get('c')  # Closing price
                        if price:
                            market_cap = price * shares
        
        if not market_cap:
            logger.warning(f"Could not determine market cap for {ticker_symbol}")
            return None
        
        return {
            'symbol': ticker_symbol,
            'name': company_name,
            'market_cap': market_cap,
            'shares': results.get('weighted_shares_outstanding') or results.get('share_class_shares_outstanding'),
            'price': None  # We didn't store the price separately
        }
    
    except Exception as e:
        logger.error(f"Error fetching data for {ticker_symbol}: {e}")
        return None

def import_ticker_market_caps(ticker_symbols, ticker_id_map, dates_to_import):
    """
    Import ticker market caps from Polygon API for multiple dates
    """
    logger.info(f"Importing ticker market caps for {len(ticker_symbols)} tickers across {len(dates_to_import)} dates")
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        success_count = 0
        failure_count = 0
        
        for ticker_symbol in ticker_symbols:
            # Get the ticker ID
            ticker_id = ticker_id_map.get(ticker_symbol)
            if ticker_id is None:
                logger.warning(f"Missing ticker ID for {ticker_symbol}")
                continue
            
            # Get current market cap data first
            ticker_data = get_polygon_data(ticker_symbol)
            
            if not ticker_data or 'market_cap' not in ticker_data:
                logger.warning(f"No market cap data available for {ticker_symbol}")
                failure_count += 1
                continue
            
            # Update the ticker name if needed
            if ticker_data.get('name'):
                cursor.execute(
                    "UPDATE tickers SET name = %s WHERE id = %s",
                    (ticker_data['name'], ticker_id)
                )
            
            # Record the market cap for each date
            current_market_cap = ticker_data['market_cap']
            current_date = date.today().isoformat()
            
            # Insert market cap for current date
            cursor.execute(
                """
                INSERT INTO ticker_market_caps 
                (ticker_id, date, market_cap, shares_outstanding, data_source) 
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (ticker_id, date) 
                DO UPDATE SET market_cap = EXCLUDED.market_cap,
                              shares_outstanding = EXCLUDED.shares_outstanding,
                              data_source = EXCLUDED.data_source,
                              updated_at = CURRENT_TIMESTAMP
                """,
                (
                    ticker_id, 
                    current_date, 
                    current_market_cap,
                    ticker_data.get('shares'),
                    'Polygon API'
                )
            )
            success_count += 1
            
            # For historical dates, we'll need to estimate based on the current data
            # Since we can't get historical share counts easily
            
            # Commit after each ticker to avoid losing progress if an error occurs
            conn.commit()
            
            # Add rate limiting to avoid API throttling
            time.sleep(0.5)
        
        cursor.close()
        conn.close()
        
        logger.info(f"Imported market caps for {success_count} tickers (failed: {failure_count})")
        return success_count > 0
    except Exception as e:
        logger.error(f"Error importing ticker market caps: {e}")
        return False

def calculate_sector_market_caps(sector_id_map, dates_to_process):
    """
    Calculate sector market caps from ticker market caps
    """
    logger.info(f"Calculating sector market caps for {len(dates_to_process)} dates")
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        success_count = 0
        
        for date_str in dates_to_process:
            # For each sector, calculate the total market cap of all its tickers
            for sector_name, sector_id in sector_id_map.items():
                # Get all tickers in this sector
                cursor.execute(
                    """
                    SELECT t.id, t.symbol
                    FROM tickers t
                    JOIN ticker_sectors ts ON t.id = ts.ticker_id
                    WHERE ts.sector_id = %s
                    """,
                    (sector_id,)
                )
                sector_tickers = cursor.fetchall()
                
                # Skip if no tickers in this sector
                if not sector_tickers:
                    logger.warning(f"No tickers found for sector {sector_name}")
                    continue
                
                # Calculate the total market cap for this sector on this date
                total_market_cap = 0
                ticker_count = 0
                
                for ticker_id, ticker_symbol in sector_tickers:
                    cursor.execute(
                        """
                        SELECT market_cap
                        FROM ticker_market_caps
                        WHERE ticker_id = %s AND date = %s
                        """,
                        (ticker_id, date_str)
                    )
                    result = cursor.fetchone()
                    
                    if result and result[0]:
                        total_market_cap += result[0]
                        ticker_count += 1
                
                # Skip if no market cap data for this sector
                if ticker_count == 0:
                    logger.warning(f"No market cap data available for sector {sector_name} on {date_str}")
                    continue
                
                # Insert or update the sector market cap
                cursor.execute(
                    """
                    INSERT INTO sector_market_caps 
                    (sector_id, date, market_cap) 
                    VALUES (%s, %s, %s)
                    ON CONFLICT (sector_id, date) 
                    DO UPDATE SET market_cap = EXCLUDED.market_cap,
                                  updated_at = CURRENT_TIMESTAMP
                    """,
                    (sector_id, date_str, total_market_cap)
                )
                success_count += 1
            
            # Commit after each date to avoid losing progress if an error occurs
            conn.commit()
        
        cursor.close()
        conn.close()
        
        logger.info(f"Calculated market caps for {success_count} sector-date pairs")
        return success_count > 0
    except Exception as e:
        logger.error(f"Error calculating sector market caps: {e}")
        return False

def verify_data_import():
    """
    Verify that the data was imported correctly
    """
    logger.info("Verifying data import")
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Count sectors
        cursor.execute("SELECT COUNT(*) FROM sectors")
        sector_count = cursor.fetchone()[0]
        
        # Count tickers
        cursor.execute("SELECT COUNT(*) FROM tickers")
        ticker_count = cursor.fetchone()[0]
        
        # Count ticker-sector mappings
        cursor.execute("SELECT COUNT(*) FROM ticker_sectors")
        mapping_count = cursor.fetchone()[0]
        
        # Count ticker market caps
        cursor.execute("SELECT COUNT(*) FROM ticker_market_caps")
        ticker_cap_count = cursor.fetchone()[0]
        
        # Count sector market caps
        cursor.execute("SELECT COUNT(*) FROM sector_market_caps")
        sector_cap_count = cursor.fetchone()[0]
        
        # Get some sample data to verify calculation
        cursor.execute(
            """
            SELECT s.name, smc.date, smc.market_cap
            FROM sector_market_caps smc
            JOIN sectors s ON smc.sector_id = s.id
            ORDER BY smc.date DESC, smc.market_cap DESC
            LIMIT 5
            """
        )
        sample_data = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        logger.info(f"Data verification summary:")
        logger.info(f"- Sectors: {sector_count}")
        logger.info(f"- Tickers: {ticker_count}")
        logger.info(f"- Ticker-Sector mappings: {mapping_count}")
        logger.info(f"- Ticker market caps: {ticker_cap_count}")
        logger.info(f"- Sector market caps: {sector_cap_count}")
        
        if sample_data:
            logger.info("Sample sector market cap data:")
            for sector_name, date_str, market_cap in sample_data:
                logger.info(f"- {sector_name} on {date_str}: ${market_cap/1e9:.2f}B")
        
        return sector_count > 0 and ticker_count > 0 and mapping_count > 0
    except Exception as e:
        logger.error(f"Error verifying data import: {e}")
        return False

def main():
    """
    Main function to import data from Polygon API to PostgreSQL database
    """
    logger.info("Starting data import from Polygon API to PostgreSQL database")
    
    # Create database tables
    if not create_db_tables():
        logger.error("Failed to create database tables")
        return False
    
    # Load ticker-sector mappings
    mappings, unique_sectors, unique_tickers = load_ticker_sector_mappings()
    if not mappings:
        logger.error("Failed to load ticker-sector mappings")
        return False
    
    # Clear existing data
    if not clear_existing_data():
        logger.error("Failed to clear existing data")
        return False
    
    # Import sectors and tickers
    sector_id_map, ticker_id_map = import_sectors_and_tickers(unique_sectors, unique_tickers)
    if not sector_id_map or not ticker_id_map:
        logger.error("Failed to import sectors and tickers")
        return False
    
    # Import ticker-sector mappings
    if not import_ticker_sector_mappings(mappings, sector_id_map, ticker_id_map):
        logger.error("Failed to import ticker-sector mappings")
        return False
    
    # Import ticker market caps for today
    today = date.today().isoformat()
    dates_to_import = [today]
    if not import_ticker_market_caps(unique_tickers, ticker_id_map, dates_to_import):
        logger.error("Failed to import ticker market caps")
        return False
    
    # Calculate sector market caps
    if not calculate_sector_market_caps(sector_id_map, dates_to_import):
        logger.error("Failed to calculate sector market caps")
        return False
    
    # Verify data import
    if not verify_data_import():
        logger.error("Data verification failed")
        return False
    
    logger.info("Data import completed successfully")
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)