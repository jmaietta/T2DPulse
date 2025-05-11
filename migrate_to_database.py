#!/usr/bin/env python3
"""
Migrate market cap data from CSV files to PostgreSQL database for reliable storage.
This migration ensures data integrity by storing sector and ticker data, market caps,
and derived sentiment scores in a properly structured database.
"""

import os
import sys
import pandas as pd
import numpy as np
import psycopg2
import logging
from psycopg2.extras import execute_values
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_db_connection():
    """Connect to the PostgreSQL database server using environment variables"""
    try:
        # Connect to the PostgreSQL server
        logger.info("Connecting to the PostgreSQL database...")
        conn = psycopg2.connect(os.environ.get('DATABASE_URL'))
        
        # Create a cursor
        cur = conn.cursor()
        
        # Execute a version check
        cur.execute('SELECT version()')
        db_version = cur.fetchone()
        logger.info(f"PostgreSQL database version: {db_version}")
        
        return conn
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f"Error connecting to the database: {error}")
        sys.exit(1)

def import_sectors(conn):
    """Import sectors into the database"""
    logger.info("Importing sectors into the database...")
    
    # List of sectors from the market cap data
    sectors = [
        'AI Infrastructure',
        'AdTech',
        'Cloud Infrastructure',
        'Consumer Internet',
        'Cybersecurity',
        'Dev Tools / Analytics',
        'Enterprise SaaS',
        'Fintech',
        'Hardware / Devices',
        'IT Services / Legacy Tech',
        'SMB SaaS',
        'Semiconductors',
        'Vertical SaaS',
        'eCommerce'
    ]
    
    cursor = conn.cursor()
    
    # Insert sectors
    for sector in sectors:
        try:
            cursor.execute(
                "INSERT INTO sectors (name) VALUES (%s) ON CONFLICT (name) DO NOTHING",
                (sector,)
            )
        except Exception as e:
            logger.error(f"Error inserting sector {sector}: {e}")
    
    conn.commit()
    logger.info(f"Imported {len(sectors)} sectors into the database")
    
    # Return a mapping of sector names to IDs
    cursor.execute("SELECT id, name FROM sectors")
    sector_map = {name: id for id, name in cursor.fetchall()}
    return sector_map

def import_tickers(conn):
    """Import tickers into the database from the coverage file"""
    logger.info("Importing tickers into the database...")
    
    ticker_file = 'T2D_Pulse_93_tickers_coverage.csv'
    
    if not os.path.exists(ticker_file):
        logger.error(f"Ticker file not found: {ticker_file}")
        return {}
    
    try:
        # Load the CSV file, skipping the first few rows which contain summary info
        df = pd.read_csv(ticker_file, skiprows=7)
        
        # Extract unique tickers
        tickers = df['Ticker'].unique().tolist()
        
        cursor = conn.cursor()
        
        # Insert tickers
        for ticker in tickers:
            try:
                cursor.execute(
                    "INSERT INTO tickers (symbol) VALUES (%s) ON CONFLICT (symbol) DO NOTHING",
                    (ticker,)
                )
            except Exception as e:
                logger.error(f"Error inserting ticker {ticker}: {e}")
        
        conn.commit()
        logger.info(f"Imported {len(tickers)} tickers into the database")
        
        # Return a mapping of ticker symbols to IDs
        cursor.execute("SELECT id, symbol FROM tickers")
        ticker_map = {symbol: id for id, symbol in cursor.fetchall()}
        
        # Also import ticker-sector relationships
        import_ticker_sectors(conn, df, ticker_map)
        
        return ticker_map
    
    except Exception as e:
        logger.error(f"Error importing tickers: {e}")
        return {}

def import_ticker_sectors(conn, df, ticker_map):
    """Import ticker-sector relationships into the database"""
    logger.info("Importing ticker-sector relationships into the database...")
    
    try:
        # Get sector mapping
        cursor = conn.cursor()
        cursor.execute("SELECT id, name FROM sectors")
        sector_map = {name: id for id, name in cursor.fetchall()}
        
        # Prepare relationships data
        relationships = []
        for _, row in df.iterrows():
            ticker = row['Ticker']
            sector = row['Sector']
            
            if ticker in ticker_map and sector in sector_map:
                relationships.append((ticker_map[ticker], sector_map[sector]))
        
        # Insert relationships in batches
        if relationships:
            args = ','.join(cursor.mogrify("(%s,%s)", r).decode('utf-8') for r in relationships)
            cursor.execute(
                f"INSERT INTO ticker_sectors (ticker_id, sector_id) VALUES {args} "
                f"ON CONFLICT (ticker_id, sector_id) DO NOTHING"
            )
            conn.commit()
            logger.info(f"Imported {len(relationships)} ticker-sector relationships")
        
    except Exception as e:
        logger.error(f"Error importing ticker-sector relationships: {e}")

def import_ticker_market_caps(conn, ticker_map):
    """Import ticker market cap data into the database"""
    logger.info("Importing ticker market cap data into the database...")
    
    market_cap_file = 'T2D_Pulse_Full_Ticker_History.csv'
    
    if not os.path.exists(market_cap_file):
        logger.error(f"Market cap file not found: {market_cap_file}")
        return False
    
    try:
        # Load the CSV file
        df = pd.read_csv(market_cap_file)
        
        # Convert date column to datetime
        df['date'] = pd.to_datetime(df['date'])
        
        # Prepare data for insertion
        data = []
        for _, row in df.iterrows():
            ticker = row['ticker']
            if ticker in ticker_map:
                data.append((
                    ticker_map[ticker],
                    row['date'].strftime('%Y-%m-%d'),
                    None,  # price (not available in this file)
                    row['market_cap'],
                    None,  # shares outstanding (not available)
                    'Polygon API'  # data source
                ))
        
        cursor = conn.cursor()
        
        # Insert market cap data in batches
        batch_size = 1000
        for i in range(0, len(data), batch_size):
            batch = data[i:i+batch_size]
            args = ','.join(cursor.mogrify("(%s,%s,%s,%s,%s,%s)", r).decode('utf-8') for r in batch)
            cursor.execute(
                f"INSERT INTO ticker_market_caps (ticker_id, date, price, market_cap, shares_outstanding, data_source) "
                f"VALUES {args} ON CONFLICT (ticker_id, date) DO UPDATE SET "
                f"market_cap = EXCLUDED.market_cap, updated_at = CURRENT_TIMESTAMP"
            )
            conn.commit()
            logger.info(f"Imported batch {i//batch_size + 1} of ticker market caps ({len(batch)} records)")
        
        logger.info(f"Imported {len(data)} ticker market cap records")
        return True
    
    except Exception as e:
        logger.error(f"Error importing ticker market caps: {e}")
        return False

def import_sector_market_caps(conn, sector_map):
    """Import sector market cap data into the database"""
    logger.info("Importing sector market cap data into the database...")
    
    sector_market_cap_file = 'corrected_sector_market_caps.csv'
    
    if not os.path.exists(sector_market_cap_file):
        logger.error(f"Sector market cap file not found: {sector_market_cap_file}")
        return False
    
    try:
        # Load the CSV file
        df = pd.read_csv(sector_market_cap_file)
        
        # Convert date column to datetime
        df['date'] = pd.to_datetime(df['date'])
        
        # Get sentiment scores
        sentiment_df = None
        if os.path.exists('data/authentic_sector_history.csv'):
            sentiment_df = pd.read_csv('data/authentic_sector_history.csv')
            sentiment_df['date'] = pd.to_datetime(sentiment_df['date'])
        
        # Prepare data for insertion
        data = []
        for sector_name, sector_id in sector_map.items():
            if sector_name in df.columns:
                for _, row in df.iterrows():
                    date = row['date']
                    market_cap = row[sector_name]
                    
                    # Find sentiment score if available
                    sentiment_score = None
                    if sentiment_df is not None:
                        matching_rows = sentiment_df[(sentiment_df['date'] == date)]
                        if not matching_rows.empty and sector_name in matching_rows.columns:
                            sentiment_score = matching_rows[sector_name].iloc[0]
                    
                    data.append((
                        sector_id,
                        date.strftime('%Y-%m-%d'),
                        market_cap,
                        sentiment_score
                    ))
        
        cursor = conn.cursor()
        
        # Insert sector market cap data in batches
        batch_size = 500
        for i in range(0, len(data), batch_size):
            batch = data[i:i+batch_size]
            args = ','.join(cursor.mogrify("(%s,%s,%s,%s)", r).decode('utf-8') for r in batch)
            cursor.execute(
                f"INSERT INTO sector_market_caps (sector_id, date, market_cap, sentiment_score) "
                f"VALUES {args} ON CONFLICT (sector_id, date) DO UPDATE SET "
                f"market_cap = EXCLUDED.market_cap, sentiment_score = EXCLUDED.sentiment_score, "
                f"updated_at = CURRENT_TIMESTAMP"
            )
            conn.commit()
            logger.info(f"Imported batch {i//batch_size + 1} of sector market caps ({len(batch)} records)")
        
        logger.info(f"Imported {len(data)} sector market cap records")
        return True
    
    except Exception as e:
        logger.error(f"Error importing sector market caps: {e}")
        return False

def verify_data_migration(conn):
    """Verify that the data migration was successful"""
    logger.info("Verifying data migration...")
    
    cursor = conn.cursor()
    
    # Check sectors count
    cursor.execute("SELECT COUNT(*) FROM sectors")
    sector_count = cursor.fetchone()[0]
    logger.info(f"Sectors in database: {sector_count}")
    
    # Check tickers count
    cursor.execute("SELECT COUNT(*) FROM tickers")
    ticker_count = cursor.fetchone()[0]
    logger.info(f"Tickers in database: {ticker_count}")
    
    # Check ticker-sector relationships count
    cursor.execute("SELECT COUNT(*) FROM ticker_sectors")
    relationship_count = cursor.fetchone()[0]
    logger.info(f"Ticker-sector relationships in database: {relationship_count}")
    
    # Check ticker market caps count
    cursor.execute("SELECT COUNT(*) FROM ticker_market_caps")
    ticker_market_cap_count = cursor.fetchone()[0]
    logger.info(f"Ticker market cap records in database: {ticker_market_cap_count}")
    
    # Check sector market caps count
    cursor.execute("SELECT COUNT(*) FROM sector_market_caps")
    sector_market_cap_count = cursor.fetchone()[0]
    logger.info(f"Sector market cap records in database: {sector_market_cap_count}")
    
    # Check for NULL values in market_cap column
    cursor.execute("SELECT COUNT(*) FROM sector_market_caps WHERE market_cap IS NULL")
    null_market_caps = cursor.fetchone()[0]
    if null_market_caps > 0:
        logger.warning(f"Found {null_market_caps} NULL market_cap values in sector_market_caps")
    
    # Print sample data
    logger.info("Sample sector market cap data:")
    cursor.execute("""
        SELECT s.name, smc.date, smc.market_cap, smc.sentiment_score
        FROM sector_market_caps smc
        JOIN sectors s ON smc.sector_id = s.id
        ORDER BY smc.date DESC, s.name
        LIMIT 14
    """)
    for row in cursor.fetchall():
        logger.info(f"{row[0]}: {row[1]} - Market Cap: {row[2]}, Sentiment: {row[3]}")
    
    return True

def main():
    """Main function to migrate data from CSV files to the database"""
    logger.info("Starting data migration from CSV files to PostgreSQL database...")
    
    conn = None
    try:
        # Connect to the database
        conn = get_db_connection()
        
        # Import sectors
        sector_map = import_sectors(conn)
        if not sector_map:
            logger.error("Failed to import sectors")
            return False
        
        # Import tickers
        ticker_map = import_tickers(conn)
        if not ticker_map:
            logger.error("Failed to import tickers")
            return False
        
        # Import ticker market caps
        if not import_ticker_market_caps(conn, ticker_map):
            logger.error("Failed to import ticker market caps")
            return False
        
        # Import sector market caps
        if not import_sector_market_caps(conn, sector_map):
            logger.error("Failed to import sector market caps")
            return False
        
        # Verify data migration
        if not verify_data_migration(conn):
            logger.error("Data verification failed")
            return False
        
        logger.info("Data migration completed successfully")
        return True
    
    except Exception as e:
        logger.error(f"Error during data migration: {e}")
        return False
    
    finally:
        if conn is not None:
            conn.close()
            logger.info("Database connection closed")

if __name__ == "__main__":
    main()