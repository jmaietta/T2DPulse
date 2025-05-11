#!/usr/bin/env python3
"""
Migrate sector market cap data from CSV files to the PostgreSQL database.
This script will:
1. Load the latest market cap values from authentic_sector_market_caps.csv
2. Load the historical sector sentiment data from data/authentic_sector_history.csv
3. Import the data into the PostgreSQL database tables
4. Ensure MSFT is properly assigned to all three sectors: Enterprise SaaS, Cloud Infrastructure, and Enterprise Infra
"""

import os
import sys
import pandas as pd
import psycopg2
from psycopg2 import sql
from datetime import datetime, timedelta
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('migrate_sector_data.log')
    ]
)
logger = logging.getLogger()

# Database connection parameters
DB_PARAMS = {
    'dbname': os.environ.get('PGDATABASE'),
    'user': os.environ.get('PGUSER'),
    'password': os.environ.get('PGPASSWORD'),
    'host': os.environ.get('PGHOST'),
    'port': os.environ.get('PGPORT')
}

# Source files
LATEST_MARKET_CAPS = 'authentic_sector_market_caps.csv'
HISTORICAL_SENTIMENT = 'data/authentic_sector_history.csv'
HISTORICAL_MARKET_CAPS = 'data/sector_marketcap_30day_table.csv'

# Helper functions
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
            logger.debug(f"Query executed successfully: {query[:100]}...")
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

def load_latest_market_caps():
    """Load the latest sector market cap values"""
    try:
        if not os.path.exists(LATEST_MARKET_CAPS):
            logger.error(f"Market cap file not found: {LATEST_MARKET_CAPS}")
            return None
        
        logger.info(f"Loading latest market cap values from {LATEST_MARKET_CAPS}")
        df = pd.read_csv(LATEST_MARKET_CAPS)
        return df
    except Exception as e:
        logger.error(f"Error loading market cap data: {e}")
        return None

def load_historical_sentiment():
    """Load historical sector sentiment data"""
    try:
        if not os.path.exists(HISTORICAL_SENTIMENT):
            logger.error(f"Historical sentiment file not found: {HISTORICAL_SENTIMENT}")
            return None
        
        logger.info(f"Loading historical sentiment data from {HISTORICAL_SENTIMENT}")
        df = pd.read_csv(HISTORICAL_SENTIMENT)
        return df
    except Exception as e:
        logger.error(f"Error loading historical sentiment data: {e}")
        return None

def load_historical_market_caps():
    """Load historical sector market cap data"""
    try:
        if not os.path.exists(HISTORICAL_MARKET_CAPS):
            logger.error(f"Historical market cap file not found: {HISTORICAL_MARKET_CAPS}")
            return None
        
        logger.info(f"Loading historical market cap data from {HISTORICAL_MARKET_CAPS}")
        df = pd.read_csv(HISTORICAL_MARKET_CAPS)
        # Remove duplicate date columns
        if 'Date' in df.columns and 'Unnamed: 0' in df.columns:
            df = df.drop(columns=['Unnamed: 0'])
        elif df.columns[0] == 'Date' and df.columns[1] == 'Date':
            df = df.drop(columns=['Date'])
            df.columns = ['Date'] + list(df.columns[1:])
        
        # Convert T to trillion by replacing text and multiplying
        for col in df.columns:
            if col != 'Date':
                df[col] = df[col].astype(str).str.replace('T', '').astype(float) * 1e12
        
        return df
    except Exception as e:
        logger.error(f"Error loading historical market cap data: {e}")
        return None

def get_or_create_sector(conn, sector_name):
    """Get the sector ID or create a new sector record if it doesn't exist"""
    # Try to find the sector
    query = "SELECT id FROM sectors WHERE name = %s"
    results = fetch_query(conn, query, (sector_name,))
    
    if results:
        return results[0][0]
    
    # Create the sector if it doesn't exist
    query = "INSERT INTO sectors (name) VALUES (%s) RETURNING id"
    results = fetch_query(conn, query, (sector_name,))
    
    if results:
        logger.info(f"Created new sector: {sector_name} with ID {results[0][0]}")
        return results[0][0]
    
    logger.error(f"Failed to create sector: {sector_name}")
    return None

def ensure_msft_sector_assignments(conn):
    """Ensure MSFT is properly assigned to all three required sectors"""
    required_sectors = ['Enterprise SaaS', 'Cloud Infrastructure', 'Enterprise Infra']
    
    # Get MSFT ticker ID
    query = "SELECT id FROM tickers WHERE symbol = 'MSFT'"
    results = fetch_query(conn, query)
    
    if not results:
        # Create MSFT ticker if it doesn't exist
        query = "INSERT INTO tickers (symbol, name) VALUES ('MSFT', 'Microsoft Corporation') RETURNING id"
        results = fetch_query(conn, query)
        if not results:
            logger.error("Failed to create MSFT ticker")
            return False
    
    msft_id = results[0][0]
    logger.info(f"Found MSFT ticker with ID {msft_id}")
    
    # Ensure MSFT is assigned to all required sectors
    for sector_name in required_sectors:
        sector_id = get_or_create_sector(conn, sector_name)
        if not sector_id:
            continue
        
        # Check if assignment already exists
        query = "SELECT 1 FROM ticker_sectors WHERE ticker_id = %s AND sector_id = %s"
        results = fetch_query(conn, query, (msft_id, sector_id))
        
        if not results:
            # Create assignment
            query = "INSERT INTO ticker_sectors (ticker_id, sector_id) VALUES (%s, %s)"
            if execute_query(conn, query, (msft_id, sector_id)):
                logger.info(f"Assigned MSFT to sector: {sector_name}")
            else:
                logger.error(f"Failed to assign MSFT to sector: {sector_name}")
    
    return True

def import_market_cap_data(conn, hist_df, latest_df):
    """Import market cap data into the database"""
    if hist_df is None and latest_df is None:
        logger.error("No market cap data available for import")
        return False
    
    # Prepare historical data if available
    dates_processed = set()
    records_added = 0
    
    if hist_df is not None:
        logger.info("Processing historical market cap data...")
        
        # Process each date
        for _, row in hist_df.iterrows():
            date_str = row['Date']
            dates_processed.add(date_str)
            
            # Process each sector
            for sector in hist_df.columns:
                if sector == 'Date':
                    continue
                
                # Get market cap value
                market_cap = row[sector]
                
                # Get sector ID
                sector_id = get_or_create_sector(conn, sector)
                if not sector_id:
                    continue
                
                # Insert market cap record
                query = """
                INSERT INTO sector_market_caps (sector_id, date, market_cap)
                VALUES (%s, %s, %s)
                ON CONFLICT (sector_id, date) DO UPDATE
                SET market_cap = EXCLUDED.market_cap
                """
                if execute_query(conn, query, (sector_id, date_str, market_cap)):
                    records_added += 1
    
    # Process latest market cap data if available
    if latest_df is not None:
        logger.info("Processing latest market cap data...")
        
        # Get the current date
        today = datetime.now().strftime('%Y-%m-%d')
        
        # Process each sector
        for _, row in latest_df.iterrows():
            sector = row['Sector']
            
            # Skip if we already processed this date
            if today in dates_processed:
                continue
            
            # Convert market cap to a numeric value
            market_cap_str = row['Market Cap (USD)']
            market_cap = float(market_cap_str)
            
            # Get sector ID
            sector_id = get_or_create_sector(conn, sector)
            if not sector_id:
                continue
            
            # Insert market cap record
            query = """
            INSERT INTO sector_market_caps (sector_id, date, market_cap)
            VALUES (%s, %s, %s)
            ON CONFLICT (sector_id, date) DO UPDATE
            SET market_cap = EXCLUDED.market_cap
            """
            if execute_query(conn, query, (sector_id, today, market_cap)):
                records_added += 1
    
    logger.info(f"Added {records_added} market cap records")
    return records_added > 0

def import_sentiment_data(conn, sentiment_df):
    """Import sentiment data into the database"""
    if sentiment_df is None:
        logger.error("No sentiment data available for import")
        return False
    
    records_added = 0
    
    logger.info("Processing historical sentiment data...")
    
    # Process each date
    for _, row in sentiment_df.iterrows():
        date_str = row['date']
        
        # Process each sector
        for sector in sentiment_df.columns:
            if sector == 'date':
                continue
            
            # Get sentiment value
            sentiment_score = row[sector]
            
            # Get sector ID
            sector_id = get_or_create_sector(conn, sector)
            if not sector_id:
                continue
            
            # Check if we have a market cap record for this sector/date
            query = """
            SELECT id FROM sector_market_caps
            WHERE sector_id = %s AND date = %s
            """
            results = fetch_query(conn, query, (sector_id, date_str))
            
            if results:
                # Update sentiment score
                query = """
                UPDATE sector_market_caps
                SET sentiment_score = %s
                WHERE id = %s
                """
                if execute_query(conn, query, (sentiment_score, results[0][0])):
                    records_added += 1
            else:
                # Create a new record with sentiment score but null market cap
                query = """
                INSERT INTO sector_market_caps (sector_id, date, sentiment_score)
                VALUES (%s, %s, %s)
                ON CONFLICT (sector_id, date) DO UPDATE
                SET sentiment_score = EXCLUDED.sentiment_score
                """
                if execute_query(conn, query, (sector_id, date_str, sentiment_score)):
                    records_added += 1
    
    logger.info(f"Updated {records_added} sentiment records")
    return records_added > 0

def verify_data_migration(conn):
    """Verify that data was successfully migrated"""
    queries = [
        "SELECT COUNT(*) FROM sectors",
        "SELECT COUNT(*) FROM sector_market_caps",
        "SELECT MIN(date), MAX(date) FROM sector_market_caps",
        "SELECT COUNT(*) FROM sector_market_caps WHERE sentiment_score IS NOT NULL",
        "SELECT COUNT(*) FROM sector_market_caps WHERE market_cap IS NOT NULL",
        "SELECT s.name, COUNT(*) FROM sector_market_caps smc JOIN sectors s ON smc.sector_id = s.id GROUP BY s.name ORDER BY COUNT(*) DESC"
    ]
    
    logger.info("Verifying data migration...")
    
    for query in queries:
        results = fetch_query(conn, query)
        if query.startswith("SELECT COUNT(*)"):
            logger.info(f"{query}: {results[0][0]} records")
        elif query.startswith("SELECT MIN(date)"):
            logger.info(f"Date range: {results[0][0]} to {results[0][1]}")
        elif query.startswith("SELECT s.name"):
            logger.info("Record counts by sector:")
            for sector, count in results:
                logger.info(f"  {sector}: {count} records")
    
    # Verify MSFT sector assignments
    query = """
    SELECT s.name
    FROM ticker_sectors ts
    JOIN sectors s ON ts.sector_id = s.id
    JOIN tickers t ON ts.ticker_id = t.id
    WHERE t.symbol = 'MSFT'
    ORDER BY s.name
    """
    results = fetch_query(conn, query)
    
    if results:
        sectors = [r[0] for r in results]
        logger.info(f"MSFT assigned to sectors: {', '.join(sectors)}")
        
        required_sectors = ['Enterprise SaaS', 'Cloud Infrastructure', 'Enterprise Infra']
        missing = [s for s in required_sectors if s not in sectors]
        
        if missing:
            logger.warning(f"MSFT is missing assignments to: {', '.join(missing)}")
        else:
            logger.info("MSFT is correctly assigned to all required sectors")
    else:
        logger.warning("MSFT has no sector assignments")

def main():
    """Main function to migrate data to the database"""
    # Connect to the database
    conn = connect_to_db()
    if not conn:
        sys.exit(1)
    
    try:
        # Load data from files
        latest_df = load_latest_market_caps()
        hist_df = load_historical_market_caps()
        sentiment_df = load_historical_sentiment()
        
        # Ensure MSFT is properly assigned to all three sectors
        ensure_msft_sector_assignments(conn)
        
        # Import data into the database
        import_market_cap_data(conn, hist_df, latest_df)
        import_sentiment_data(conn, sentiment_df)
        
        # Verify data migration
        verify_data_migration(conn)
        
        logger.info("Data migration completed successfully")
    except Exception as e:
        logger.error(f"Error during data migration: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()