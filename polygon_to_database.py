#!/usr/bin/env python3
"""
Populate database directly with authentic ticker and market cap data from Polygon API.

This script:
1. Connects to the PostgreSQL database
2. Uses the data collected by the Polygon 30-Day History Collector
3. Calculates sector market caps by summing authentic ticker data
4. Populates the database with both ticker and sector level data
5. Ensures accurate sector assignments (MSFT in multiple sectors, etc.)
"""

import os
import sys
import pandas as pd
import psycopg2
from psycopg2 import sql
import logging
from datetime import datetime, timedelta
import json

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('polygon_to_database.log')
    ]
)
logger = logging.getLogger()

# Database connection parameters from environment variables
DB_PARAMS = {
    'dbname': os.environ.get('PGDATABASE'),
    'user': os.environ.get('PGUSER'),
    'password': os.environ.get('PGPASSWORD'),
    'host': os.environ.get('PGHOST'),
    'port': os.environ.get('PGPORT')
}

# Files with ticker and market cap data
TICKER_SECTOR_FILE = 'T2D_Pulse_93_tickers_coverage.csv'
HISTORICAL_TICKER_DATA = 'T2D_Pulse_Full_Ticker_History.csv'
SENTIMENT_DATA = 'data/authentic_sector_history.csv'

def connect_to_db():
    """Connect to PostgreSQL database using environment variables"""
    try:
        logger.info("Connecting to PostgreSQL database...")
        conn = psycopg2.connect(**DB_PARAMS)
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        return None

def execute_query(conn, query, params=None):
    """Execute a database query with parameters"""
    try:
        with conn.cursor() as cursor:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        conn.rollback()
        return False

def fetch_query(conn, query, params=None):
    """Fetch results from a database query"""
    try:
        with conn.cursor() as cursor:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            results = cursor.fetchall()
            return results
    except Exception as e:
        logger.error(f"Error fetching query results: {e}")
        return []

def create_database_schema(conn):
    """Create the database schema if it doesn't exist"""
    logger.info("Creating/verifying database schema...")
    
    # SQL to create the necessary tables
    create_tables_sql = """
    -- Create table for sectors
    CREATE TABLE IF NOT EXISTS sectors (
        id SERIAL PRIMARY KEY,
        name TEXT UNIQUE NOT NULL,
        description TEXT
    );
    
    -- Create table for tickers
    CREATE TABLE IF NOT EXISTS tickers (
        id SERIAL PRIMARY KEY,
        symbol TEXT UNIQUE NOT NULL,
        name TEXT
    );
    
    -- Create junction table for ticker-sector relationships (many-to-many)
    CREATE TABLE IF NOT EXISTS ticker_sectors (
        ticker_id INTEGER REFERENCES tickers(id) ON DELETE CASCADE,
        sector_id INTEGER REFERENCES sectors(id) ON DELETE CASCADE,
        PRIMARY KEY (ticker_id, sector_id)
    );
    
    -- Create table for ticker market cap data
    CREATE TABLE IF NOT EXISTS ticker_market_caps (
        id SERIAL PRIMARY KEY,
        ticker_id INTEGER REFERENCES tickers(id) ON DELETE CASCADE,
        date DATE NOT NULL,
        price REAL,
        market_cap REAL NOT NULL,
        shares_outstanding REAL,
        data_source TEXT,
        UNIQUE (ticker_id, date)
    );
    
    -- Create table for sector market cap history
    CREATE TABLE IF NOT EXISTS sector_market_caps (
        id SERIAL PRIMARY KEY,
        sector_id INTEGER REFERENCES sectors(id) ON DELETE CASCADE,
        date DATE NOT NULL,
        market_cap REAL,
        sentiment_score REAL,
        UNIQUE (sector_id, date)
    );
    """
    
    return execute_query(conn, create_tables_sql)

def load_ticker_sector_mapping():
    """Load the mapping of tickers to sectors"""
    try:
        if not os.path.exists(TICKER_SECTOR_FILE):
            logger.error(f"Ticker-sector mapping file not found: {TICKER_SECTOR_FILE}")
            return None
        
        logger.info(f"Loading ticker-sector mapping from {TICKER_SECTOR_FILE}")
        df = pd.read_csv(TICKER_SECTOR_FILE, skiprows=7)
        
        # Create a mapping of tickers to their sectors
        ticker_sectors = {}
        for _, row in df.iterrows():
            ticker = row['Ticker']
            sector = row['Sector']
            
            if ticker not in ticker_sectors:
                ticker_sectors[ticker] = []
            
            ticker_sectors[ticker].append(sector)
        
        # Add the special handling for MSFT
        if 'MSFT' in ticker_sectors:
            required_sectors = ['Enterprise SaaS', 'Cloud Infrastructure', 'Enterprise Infra']
            for sector in required_sectors:
                if sector not in ticker_sectors['MSFT']:
                    ticker_sectors['MSFT'].append(sector)
        
        # Add special handling for META
        if 'META' in ticker_sectors:
            required_sectors = ['AdTech', 'Consumer Internet', 'AI Infrastructure', 'SMB SaaS']
            for sector in required_sectors:
                if sector not in ticker_sectors['META']:
                    ticker_sectors['META'].append(sector)
        
        return ticker_sectors
    except Exception as e:
        logger.error(f"Error loading ticker-sector mapping: {e}")
        return None

def load_historical_ticker_data():
    """Load historical ticker market cap data from the Polygon collector"""
    try:
        if not os.path.exists(HISTORICAL_TICKER_DATA):
            logger.error(f"Historical ticker data file not found: {HISTORICAL_TICKER_DATA}")
            return None
        
        logger.info(f"Loading historical ticker data from {HISTORICAL_TICKER_DATA}")
        df = pd.read_csv(HISTORICAL_TICKER_DATA)
        
        # Convert date to datetime
        df['date'] = pd.to_datetime(df['date'])
        
        return df
    except Exception as e:
        logger.error(f"Error loading historical ticker data: {e}")
        return None

def load_sentiment_data():
    """Load sector sentiment data"""
    try:
        if not os.path.exists(SENTIMENT_DATA):
            logger.error(f"Sentiment data file not found: {SENTIMENT_DATA}")
            return None
        
        logger.info(f"Loading sentiment data from {SENTIMENT_DATA}")
        df = pd.read_csv(SENTIMENT_DATA)
        
        # Convert date to datetime
        df['date'] = pd.to_datetime(df['date'])
        
        return df
    except Exception as e:
        logger.error(f"Error loading sentiment data: {e}")
        return None

def import_sectors(conn, sector_list):
    """Import sectors into the database"""
    logger.info("Importing sectors into the database...")
    
    sector_id_map = {}
    
    for sector in sector_list:
        # Check if sector exists
        query = "SELECT id FROM sectors WHERE name = %s"
        results = fetch_query(conn, query, (sector,))
        
        if results:
            sector_id_map[sector] = results[0][0]
        else:
            # Create sector
            query = "INSERT INTO sectors (name) VALUES (%s) RETURNING id"
            results = fetch_query(conn, query, (sector,))
            
            if results:
                sector_id_map[sector] = results[0][0]
                logger.info(f"Created new sector: {sector} with ID {results[0][0]}")
            else:
                logger.error(f"Failed to create sector: {sector}")
    
    logger.info(f"Processed {len(sector_id_map)} sectors")
    return sector_id_map

def import_tickers(conn, ticker_list):
    """Import tickers into the database"""
    logger.info("Importing tickers into the database...")
    
    ticker_id_map = {}
    
    for ticker in ticker_list:
        # Check if ticker exists
        query = "SELECT id FROM tickers WHERE symbol = %s"
        results = fetch_query(conn, query, (ticker,))
        
        if results:
            ticker_id_map[ticker] = results[0][0]
        else:
            # Create ticker
            query = "INSERT INTO tickers (symbol) VALUES (%s) RETURNING id"
            results = fetch_query(conn, query, (ticker,))
            
            if results:
                ticker_id_map[ticker] = results[0][0]
                logger.info(f"Created new ticker: {ticker} with ID {results[0][0]}")
            else:
                logger.error(f"Failed to create ticker: {ticker}")
    
    logger.info(f"Processed {len(ticker_id_map)} tickers")
    return ticker_id_map

def import_ticker_sector_relationships(conn, ticker_sectors, ticker_id_map, sector_id_map):
    """Import ticker-sector relationships into the database"""
    logger.info("Importing ticker-sector relationships into the database...")
    
    relationships_added = 0
    
    for ticker, sectors in ticker_sectors.items():
        if ticker not in ticker_id_map:
            logger.warning(f"Ticker {ticker} not found in ticker ID map")
            continue
        
        ticker_id = ticker_id_map[ticker]
        
        for sector in sectors:
            if sector not in sector_id_map:
                logger.warning(f"Sector {sector} not found in sector ID map")
                continue
            
            sector_id = sector_id_map[sector]
            
            # Check if relationship exists
            query = "SELECT 1 FROM ticker_sectors WHERE ticker_id = %s AND sector_id = %s"
            results = fetch_query(conn, query, (ticker_id, sector_id))
            
            if not results:
                # Create relationship
                query = "INSERT INTO ticker_sectors (ticker_id, sector_id) VALUES (%s, %s)"
                if execute_query(conn, query, (ticker_id, sector_id)):
                    relationships_added += 1
    
    logger.info(f"Added {relationships_added} ticker-sector relationships")
    return relationships_added > 0

def import_ticker_market_caps(conn, ticker_data, ticker_id_map):
    """Import ticker market cap data into the database"""
    logger.info("Importing ticker market cap data into the database...")
    
    records_added = 0
    
    for _, row in ticker_data.iterrows():
        ticker = row['ticker']
        
        if ticker not in ticker_id_map:
            logger.warning(f"Ticker {ticker} not found in ticker ID map")
            continue
        
        ticker_id = ticker_id_map[ticker]
        date = row['date'].strftime('%Y-%m-%d')
        market_cap = row['market_cap']
        
        # Import or update ticker market cap data
        query = """
        INSERT INTO ticker_market_caps (ticker_id, date, market_cap, data_source)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (ticker_id, date) DO UPDATE
        SET market_cap = EXCLUDED.market_cap
        """
        
        if execute_query(conn, query, (ticker_id, date, market_cap, 'Polygon API')):
            records_added += 1
            
            # Commit every 100 records
            if records_added % 100 == 0:
                logger.info(f"Added {records_added} ticker market cap records")
    
    logger.info(f"Added {records_added} ticker market cap records")
    return records_added > 0

def calculate_sector_market_caps(ticker_data, ticker_sectors):
    """Calculate sector market caps from ticker data"""
    logger.info("Calculating sector market caps from ticker data...")
    
    # Get all unique dates
    dates = ticker_data['date'].unique()
    
    # Create a dictionary to store sector market caps by date
    sector_market_caps = {}
    
    # Group the data by date
    ticker_data_by_date = {date: ticker_data[ticker_data['date'] == date] for date in dates}
    
    # Calculate sector market caps for each date
    for date in dates:
        date_data = ticker_data_by_date[date]
        
        # Initialize market caps for this date
        if date not in sector_market_caps:
            sector_market_caps[date] = {}
        
        # Add up market caps by sector
        for _, row in date_data.iterrows():
            ticker = row['ticker']
            market_cap = row['market_cap']
            
            if ticker not in ticker_sectors:
                logger.warning(f"Ticker {ticker} not found in ticker-sector mapping")
                continue
            
            # Add market cap to all sectors this ticker belongs to
            for sector in ticker_sectors[ticker]:
                if sector not in sector_market_caps[date]:
                    sector_market_caps[date][sector] = 0
                
                sector_market_caps[date][sector] += market_cap
    
    logger.info(f"Calculated sector market caps for {len(dates)} dates and {len(set(sum([list(caps.keys()) for caps in sector_market_caps.values()], [])))} sectors")
    return sector_market_caps

def import_sector_market_caps(conn, sector_market_caps, sentiment_data, sector_id_map):
    """Import sector market cap data into the database"""
    logger.info("Importing sector market cap data into the database...")
    
    records_added = 0
    
    for date, sectors in sector_market_caps.items():
        date_str = date.strftime('%Y-%m-%d')
        
        for sector, market_cap in sectors.items():
            if sector not in sector_id_map:
                logger.warning(f"Sector {sector} not found in sector ID map")
                continue
            
            sector_id = sector_id_map[sector]
            
            # Get sentiment score if available
            sentiment_score = None
            if sentiment_data is not None:
                matching_rows = sentiment_data[(sentiment_data['date'] == date)]
                if not matching_rows.empty and sector in matching_rows.columns:
                    sentiment_score = matching_rows[sector].iloc[0]
            
            # Import or update sector market cap data
            query = """
            INSERT INTO sector_market_caps (sector_id, date, market_cap, sentiment_score)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (sector_id, date) DO UPDATE
            SET market_cap = EXCLUDED.market_cap, sentiment_score = EXCLUDED.sentiment_score
            """
            
            if execute_query(conn, query, (sector_id, date_str, market_cap, sentiment_score)):
                records_added += 1
    
    logger.info(f"Added {records_added} sector market cap records")
    return records_added > 0

def verify_data(conn):
    """Verify the data in the database"""
    logger.info("Verifying data in the database...")
    
    # Check sector count
    results = fetch_query(conn, "SELECT COUNT(*) FROM sectors")
    logger.info(f"Sectors in database: {results[0][0]}")
    
    # Check ticker count
    results = fetch_query(conn, "SELECT COUNT(*) FROM tickers")
    logger.info(f"Tickers in database: {results[0][0]}")
    
    # Check ticker-sector relationship count
    results = fetch_query(conn, "SELECT COUNT(*) FROM ticker_sectors")
    logger.info(f"Ticker-sector relationships in database: {results[0][0]}")
    
    # Check ticker market cap count
    results = fetch_query(conn, "SELECT COUNT(*) FROM ticker_market_caps")
    logger.info(f"Ticker market cap records in database: {results[0][0]}")
    
    # Check sector market cap count
    results = fetch_query(conn, "SELECT COUNT(*) FROM sector_market_caps")
    logger.info(f"Sector market cap records in database: {results[0][0]}")
    
    # Check for NULL market caps
    results = fetch_query(conn, "SELECT COUNT(*) FROM sector_market_caps WHERE market_cap IS NULL")
    if results[0][0] > 0:
        logger.warning(f"Found {results[0][0]} sector market cap records with NULL market cap")
    
    # Check for NULL sentiment scores
    results = fetch_query(conn, "SELECT COUNT(*) FROM sector_market_caps WHERE sentiment_score IS NULL")
    if results[0][0] > 0:
        logger.warning(f"Found {results[0][0]} sector market cap records with NULL sentiment score")
    
    # Check MSFT sector assignments
    results = fetch_query(conn, """
    SELECT s.name
    FROM ticker_sectors ts
    JOIN sectors s ON ts.sector_id = s.id
    JOIN tickers t ON ts.ticker_id = t.id
    WHERE t.symbol = 'MSFT'
    """)
    
    logger.info(f"MSFT assigned to sectors: {', '.join([r[0] for r in results])}")
    
    # Check date range
    results = fetch_query(conn, "SELECT MIN(date), MAX(date) FROM sector_market_caps")
    if results and results[0][0] and results[0][1]:
        logger.info(f"Sector market cap data from {results[0][0]} to {results[0][1]}")
    
    # Show sample data
    results = fetch_query(conn, """
    SELECT s.name, smc.date, smc.market_cap, smc.sentiment_score
    FROM sector_market_caps smc
    JOIN sectors s ON smc.sector_id = s.id
    ORDER BY smc.date DESC, s.name
    LIMIT 15
    """)
    
    logger.info("Sample sector market cap data:")
    for row in results:
        market_cap_billions = row[2] / 1_000_000_000 if row[2] else None
        logger.info(f"{row[0]}: {row[1]} - Market Cap: ${market_cap_billions:.2f}B, Sentiment: {row[3]}")

def main():
    """Main function to populate the database from Polygon data"""
    # Connect to the database
    conn = connect_to_db()
    if not conn:
        logger.error("Failed to connect to the database")
        sys.exit(1)
    
    try:
        # Create the database schema
        if not create_database_schema(conn):
            logger.error("Failed to create database schema")
            sys.exit(1)
        
        # Load data from files
        ticker_sectors = load_ticker_sector_mapping()
        if not ticker_sectors:
            logger.error("Failed to load ticker-sector mapping")
            sys.exit(1)
        
        ticker_data = load_historical_ticker_data()
        if ticker_data is None:
            logger.error("Failed to load historical ticker data")
            sys.exit(1)
        
        sentiment_data = load_sentiment_data()
        
        # Get unique sector and ticker lists
        sectors = set()
        for sector_list in ticker_sectors.values():
            sectors.update(sector_list)
        
        tickers = set(ticker_sectors.keys())
        
        # Import sectors and tickers
        sector_id_map = import_sectors(conn, sectors)
        ticker_id_map = import_tickers(conn, tickers)
        
        # Import ticker-sector relationships
        import_ticker_sector_relationships(conn, ticker_sectors, ticker_id_map, sector_id_map)
        
        # Import ticker market caps
        import_ticker_market_caps(conn, ticker_data, ticker_id_map)
        
        # Calculate and import sector market caps
        sector_market_caps = calculate_sector_market_caps(ticker_data, ticker_sectors)
        import_sector_market_caps(conn, sector_market_caps, sentiment_data, sector_id_map)
        
        # Verify the data
        verify_data(conn)
        
        logger.info("Database population completed successfully")
    except Exception as e:
        logger.error(f"Error during database population: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()