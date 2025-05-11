#!/usr/bin/env python3
"""
Populate the SQLite database with authentic sector market cap data.
This script uses the authentic data files:
- authentic_sector_market_caps.csv: Latest sector market caps
- data/authentic_sector_history.csv: Historical sector sentiment scores
- data/sector_marketcap_30day_table.csv: 30-day sector market cap history

It ensures the database properly stores and represents the authentic data
collected by the Polygon API.
"""

import os
import sys
import pandas as pd
import sqlite3
import logging
from datetime import datetime, timedelta

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('sqlite_migration.log')
    ]
)
logger = logging.getLogger()

# SQLite database file
DB_FILE = 'market_cap_data.db'

# Input files
LATEST_MARKET_CAPS = 'authentic_sector_market_caps.csv'
HISTORICAL_SENTIMENT = 'data/authentic_sector_history.csv'
HISTORICAL_MARKET_CAPS = 'data/sector_marketcap_30day_table.csv'

def connect_to_db():
    """Connect to SQLite database"""
    try:
        logger.info(f"Connecting to SQLite database at {DB_FILE}")
        conn = sqlite3.connect(DB_FILE)
        
        # Test the connection
        cursor = conn.cursor()
        cursor.execute("SELECT sqlite_version()")
        version = cursor.fetchone()
        logger.info(f"SQLite version: {version[0]}")
        
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        return None

def create_database_schema(conn):
    """Create the database schema if it doesn't exist"""
    logger.info("Creating/verifying database schema...")
    
    # SQL to create the necessary tables
    create_tables_sql = """
    -- Create table for sectors
    CREATE TABLE IF NOT EXISTS sectors (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE NOT NULL,
        description TEXT
    );
    
    -- Create table for tickers
    CREATE TABLE IF NOT EXISTS tickers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT UNIQUE NOT NULL,
        name TEXT
    );
    
    -- Create junction table for ticker-sector relationships (many-to-many)
    CREATE TABLE IF NOT EXISTS ticker_sectors (
        ticker_id INTEGER,
        sector_id INTEGER,
        PRIMARY KEY (ticker_id, sector_id),
        FOREIGN KEY (ticker_id) REFERENCES tickers(id) ON DELETE CASCADE,
        FOREIGN KEY (sector_id) REFERENCES sectors(id) ON DELETE CASCADE
    );
    
    -- Create table for ticker market cap data
    CREATE TABLE IF NOT EXISTS ticker_market_caps (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker_id INTEGER,
        date DATE NOT NULL,
        price REAL,
        market_cap REAL,
        shares_outstanding REAL,
        data_source TEXT,
        UNIQUE (ticker_id, date),
        FOREIGN KEY (ticker_id) REFERENCES tickers(id) ON DELETE CASCADE
    );
    
    -- Create table for sector market cap history
    CREATE TABLE IF NOT EXISTS sector_market_caps (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sector_id INTEGER,
        date DATE NOT NULL,
        market_cap REAL,
        sentiment_score REAL,
        UNIQUE (sector_id, date),
        FOREIGN KEY (sector_id) REFERENCES sectors(id) ON DELETE CASCADE
    );
    """
    
    try:
        cursor = conn.cursor()
        cursor.executescript(create_tables_sql)
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Error creating database schema: {e}")
        return False

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
        
        # Clean up the data
        if 'Date' in df.columns and 'Unnamed: 0' in df.columns:
            # Remove duplicate date column
            df = df.drop(columns=['Unnamed: 0'])
        elif df.columns[0] == 'Date' and df.columns[1] == 'Date':
            # Remove duplicate Date column
            df = df.iloc[:, 1:]
        
        # Convert T to trillion
        for col in df.columns:
            if col != 'Date':
                df[col] = df[col].astype(str).str.replace('T', '').astype(float) * 1e12
        
        return df
    except Exception as e:
        logger.error(f"Error loading historical market cap data: {e}")
        return None

def import_sectors(conn, sectors):
    """Import sectors into the database and return a mapping of sector names to IDs"""
    logger.info("Importing sectors into the database...")
    
    sector_map = {}
    cursor = conn.cursor()
    
    # Insert or ignore each sector
    for sector in sectors:
        try:
            cursor.execute(
                "INSERT OR IGNORE INTO sectors (name) VALUES (?)",
                (sector,)
            )
        except Exception as e:
            logger.error(f"Error inserting sector {sector}: {e}")
    
    conn.commit()
    
    # Build a mapping of sector names to IDs
    cursor.execute("SELECT id, name FROM sectors")
    for sector_id, sector_name in cursor.fetchall():
        sector_map[sector_name] = sector_id
    
    logger.info(f"Processed {len(sector_map)} sectors")
    return sector_map

def ensure_msft_in_multiple_sectors(conn):
    """Ensure MSFT is properly assigned to all three required sectors"""
    logger.info("Ensuring MSFT is assigned to multiple sectors...")
    
    required_sectors = ['Enterprise SaaS', 'Cloud Infrastructure', 'Enterprise Infra']
    cursor = conn.cursor()
    
    # Get or create MSFT ticker
    cursor.execute("SELECT id FROM tickers WHERE symbol = 'MSFT'")
    result = cursor.fetchone()
    
    if result:
        msft_id = result[0]
    else:
        cursor.execute(
            "INSERT INTO tickers (symbol, name) VALUES (?, ?)",
            ('MSFT', 'Microsoft Corporation')
        )
        msft_id = cursor.lastrowid
    
    # Get sector IDs
    for sector_name in required_sectors:
        # Check if sector exists
        cursor.execute("SELECT id FROM sectors WHERE name = ?", (sector_name,))
        result = cursor.fetchone()
        
        if result:
            sector_id = result[0]
        else:
            # Create sector if it doesn't exist
            cursor.execute("INSERT INTO sectors (name) VALUES (?)", (sector_name,))
            sector_id = cursor.lastrowid
        
        # Check if relationship exists
        cursor.execute(
            "SELECT 1 FROM ticker_sectors WHERE ticker_id = ? AND sector_id = ?",
            (msft_id, sector_id)
        )
        
        if not cursor.fetchone():
            # Create relationship
            cursor.execute(
                "INSERT INTO ticker_sectors (ticker_id, sector_id) VALUES (?, ?)",
                (msft_id, sector_id)
            )
            logger.info(f"Assigned MSFT to sector: {sector_name}")
    
    conn.commit()

def ensure_meta_in_multiple_sectors(conn):
    """Ensure META is properly assigned to all required sectors"""
    logger.info("Ensuring META is assigned to multiple sectors...")
    
    required_sectors = ['AdTech', 'Consumer Internet', 'AI Infrastructure', 'SMB SaaS']
    cursor = conn.cursor()
    
    # Get or create META ticker
    cursor.execute("SELECT id FROM tickers WHERE symbol = 'META'")
    result = cursor.fetchone()
    
    if result:
        meta_id = result[0]
    else:
        cursor.execute(
            "INSERT INTO tickers (symbol, name) VALUES (?, ?)",
            ('META', 'Meta Platforms, Inc.')
        )
        meta_id = cursor.lastrowid
    
    # Get sector IDs
    for sector_name in required_sectors:
        # Check if sector exists
        cursor.execute("SELECT id FROM sectors WHERE name = ?", (sector_name,))
        result = cursor.fetchone()
        
        if result:
            sector_id = result[0]
        else:
            # Create sector if it doesn't exist
            cursor.execute("INSERT INTO sectors (name) VALUES (?)", (sector_name,))
            sector_id = cursor.lastrowid
        
        # Check if relationship exists
        cursor.execute(
            "SELECT 1 FROM ticker_sectors WHERE ticker_id = ? AND sector_id = ?",
            (meta_id, sector_id)
        )
        
        if not cursor.fetchone():
            # Create relationship
            cursor.execute(
                "INSERT INTO ticker_sectors (ticker_id, sector_id) VALUES (?, ?)",
                (meta_id, sector_id)
            )
            logger.info(f"Assigned META to sector: {sector_name}")
    
    conn.commit()

def import_sector_market_caps(conn, latest_df, hist_df, sentiment_df, sector_map):
    """Import sector market cap data with sentiment scores"""
    logger.info("Importing sector market cap data...")
    
    records_added = 0
    cursor = conn.cursor()
    dates_processed = set()
    
    # Process historical data if available
    if hist_df is not None:
        logger.info("Processing historical market cap data...")
        
        # Convert Date column to datetime
        hist_df['Date'] = pd.to_datetime(hist_df['Date'])
        
        # Process historical data by date
        for _, row in hist_df.iterrows():
            date = row['Date']
            date_str = date.strftime('%Y-%m-%d')
            dates_processed.add(date_str)
            
            # Process each sector
            for sector in hist_df.columns:
                if sector == 'Date':
                    continue
                
                if sector not in sector_map:
                    logger.warning(f"Sector not found in database: {sector}")
                    continue
                
                sector_id = sector_map[sector]
                market_cap = row[sector]
                
                # Get sentiment score if available
                sentiment_score = None
                if sentiment_df is not None:
                    matching_rows = sentiment_df[sentiment_df['date'] == date_str]
                    if not matching_rows.empty and sector in matching_rows.columns:
                        sentiment_score = matching_rows[sector].iloc[0]
                
                # Insert or update sector market cap
                try:
                    cursor.execute(
                        """
                        INSERT OR REPLACE INTO sector_market_caps 
                        (sector_id, date, market_cap, sentiment_score)
                        VALUES (?, ?, ?, ?)
                        """,
                        (sector_id, date_str, market_cap, sentiment_score)
                    )
                    records_added += 1
                    
                    # Commit every 100 records
                    if records_added % 100 == 0:
                        conn.commit()
                        logger.info(f"Imported {records_added} sector market cap records")
                except Exception as e:
                    logger.error(f"Error importing market cap for {sector} on {date_str}: {e}")
    
    # Process latest data if available and not already processed
    if latest_df is not None:
        logger.info("Processing latest market cap data...")
        
        # Get the current date
        today = datetime.now().strftime('%Y-%m-%d')
        
        if today not in dates_processed:
            for _, row in latest_df.iterrows():
                sector = row['Sector']
                if sector not in sector_map:
                    logger.warning(f"Sector not found in database: {sector}")
                    continue
                
                sector_id = sector_map[sector]
                market_cap = row['Market Cap (USD)']
                
                # Get sentiment score if available
                sentiment_score = None
                if sentiment_df is not None:
                    matching_rows = sentiment_df[sentiment_df['date'] == today]
                    if not matching_rows.empty and sector in matching_rows.columns:
                        sentiment_score = matching_rows[sector].iloc[0]
                
                # Insert or update sector market cap
                try:
                    cursor.execute(
                        """
                        INSERT OR REPLACE INTO sector_market_caps 
                        (sector_id, date, market_cap, sentiment_score)
                        VALUES (?, ?, ?, ?)
                        """,
                        (sector_id, today, market_cap, sentiment_score)
                    )
                    records_added += 1
                except Exception as e:
                    logger.error(f"Error importing latest market cap for {sector}: {e}")
    
    # Final commit
    conn.commit()
    logger.info(f"Imported {records_added} sector market cap records")
    return records_added > 0

def import_sentiment_data(conn, sentiment_df, sector_map):
    """Import sentiment data that might not have market caps"""
    if sentiment_df is None:
        logger.warning("No sentiment data to import")
        return False
    
    logger.info("Importing sentiment data...")
    
    # Convert date column to datetime
    sentiment_df['date'] = pd.to_datetime(sentiment_df['date'])
    
    records_added = 0
    cursor = conn.cursor()
    
    # Process each date in the sentiment data
    for _, row in sentiment_df.iterrows():
        date_str = row['date'].strftime('%Y-%m-%d')
        
        # Process each sector
        for sector in sentiment_df.columns:
            if sector == 'date':
                continue
            
            if sector not in sector_map:
                logger.warning(f"Sector not found in database: {sector}")
                continue
            
            sector_id = sector_map[sector]
            sentiment_score = row[sector]
            
            # Check if we already have a record for this sector/date
            cursor.execute(
                "SELECT id, market_cap FROM sector_market_caps WHERE sector_id = ? AND date = ?",
                (sector_id, date_str)
            )
            result = cursor.fetchone()
            
            if result:
                # Update sentiment score on existing record
                cursor.execute(
                    "UPDATE sector_market_caps SET sentiment_score = ? WHERE id = ?",
                    (sentiment_score, result[0])
                )
            else:
                # Create a new record with just the sentiment score
                cursor.execute(
                    """
                    INSERT INTO sector_market_caps 
                    (sector_id, date, sentiment_score)
                    VALUES (?, ?, ?)
                    """,
                    (sector_id, date_str, sentiment_score)
                )
            
            records_added += 1
            
            # Commit every 100 records
            if records_added % 100 == 0:
                conn.commit()
                logger.info(f"Imported {records_added} sentiment records")
    
    # Final commit
    conn.commit()
    logger.info(f"Imported {records_added} sentiment records")
    return records_added > 0

def verify_data_migration(conn):
    """Verify the data was successfully migrated"""
    logger.info("Verifying data migration...")
    
    cursor = conn.cursor()
    
    # Check sectors count
    cursor.execute("SELECT COUNT(*) FROM sectors")
    logger.info(f"Sectors in database: {cursor.fetchone()[0]}")
    
    # Check sector market cap count
    cursor.execute("SELECT COUNT(*) FROM sector_market_caps")
    logger.info(f"Sector market cap records in database: {cursor.fetchone()[0]}")
    
    # Check for NULL market caps
    cursor.execute("SELECT COUNT(*) FROM sector_market_caps WHERE market_cap IS NULL")
    null_market_caps = cursor.fetchone()[0]
    if null_market_caps > 0:
        logger.warning(f"Found {null_market_caps} NULL market cap values")
    
    # Check for NULL sentiment scores
    cursor.execute("SELECT COUNT(*) FROM sector_market_caps WHERE sentiment_score IS NULL")
    null_sentiment = cursor.fetchone()[0]
    if null_sentiment > 0:
        logger.warning(f"Found {null_sentiment} NULL sentiment score values")
    
    # Check MSFT sector assignments
    cursor.execute("""
    SELECT s.name
    FROM ticker_sectors ts
    JOIN sectors s ON ts.sector_id = s.id
    JOIN tickers t ON ts.ticker_id = t.id
    WHERE t.symbol = 'MSFT'
    """)
    msft_sectors = [row[0] for row in cursor.fetchall()]
    logger.info(f"MSFT assigned to sectors: {', '.join(msft_sectors) if msft_sectors else 'None'}")
    
    # Check META sector assignments
    cursor.execute("""
    SELECT s.name
    FROM ticker_sectors ts
    JOIN sectors s ON ts.sector_id = s.id
    JOIN tickers t ON ts.ticker_id = t.id
    WHERE t.symbol = 'META'
    """)
    meta_sectors = [row[0] for row in cursor.fetchall()]
    logger.info(f"META assigned to sectors: {', '.join(meta_sectors) if meta_sectors else 'None'}")
    
    # Check date range
    cursor.execute("SELECT MIN(date), MAX(date) FROM sector_market_caps")
    date_range = cursor.fetchone()
    logger.info(f"Date range: {date_range[0]} to {date_range[1]}")
    
    # Display sample sector market cap data
    cursor.execute("""
    SELECT s.name, smc.date, smc.market_cap, smc.sentiment_score
    FROM sector_market_caps smc
    JOIN sectors s ON smc.sector_id = s.id
    ORDER BY smc.date DESC, s.name
    LIMIT 10
    """)
    
    logger.info("Sample sector market cap data:")
    for row in cursor.fetchall():
        market_cap_billions = row[2] / 1_000_000_000 if row[2] else None
        market_cap_str = f"${market_cap_billions:.2f}B" if market_cap_billions else "NULL"
        logger.info(f"{row[0]}: {row[1]} - Market Cap: {market_cap_str}, Sentiment: {row[3]}")
    
    return True

def main():
    """Main function to migrate data to the database"""
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
        latest_df = load_latest_market_caps()
        hist_df = load_historical_market_caps()
        sentiment_df = load_historical_sentiment()
        
        # Get all the sectors
        sectors = set()
        
        if latest_df is not None:
            sectors.update(latest_df['Sector'].tolist())
        
        if hist_df is not None:
            sectors.update([col for col in hist_df.columns if col != 'Date'])
        
        if sentiment_df is not None:
            sectors.update([col for col in sentiment_df.columns if col != 'date'])
        
        # Import sectors and get mapping
        sector_map = import_sectors(conn, sectors)
        if not sector_map:
            logger.error("Failed to import sectors")
            sys.exit(1)
        
        # Ensure special sector assignments
        ensure_msft_in_multiple_sectors(conn)
        ensure_meta_in_multiple_sectors(conn)
        
        # Import sector market caps and sentiment data
        if not import_sector_market_caps(conn, latest_df, hist_df, sentiment_df, sector_map):
            logger.warning("No sector market cap data was imported")
        
        # Import sentiment data (in case some dates only have sentiment without market caps)
        if sentiment_df is not None:
            import_sentiment_data(conn, sentiment_df, sector_map)
        
        # Verify the data migration
        verify_data_migration(conn)
        
        logger.info("Database migration completed successfully")
    except Exception as e:
        logger.error(f"Error during database migration: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()