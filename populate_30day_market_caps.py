#!/usr/bin/env python3
"""
Populate 30-day historical sector market cap data in the SQLite database.
This script will read the CSV file with 30-day historical data and populate
the database with authentic historical market cap values.
"""

import os
import sys
import logging
import sqlite3
import pandas as pd
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# File paths
HISTORICAL_30DAY_FILE = 'data/sector_marketcap_30day_table.csv'
DB_FILE = 'market_cap_data.db'

def connect_to_db():
    """Connect to SQLite database"""
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        # Check SQLite version
        cursor.execute('SELECT sqlite_version();')
        version = cursor.fetchone()
        logger.info(f"SQLite version: {version[0]}")
        
        return conn
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        sys.exit(1)

def load_30day_historical_data():
    """Load 30-day historical market cap data from CSV"""
    try:
        if not os.path.exists(HISTORICAL_30DAY_FILE):
            logger.error(f"Historical data file not found: {HISTORICAL_30DAY_FILE}")
            return None
        
        logger.info(f"Loading 30-day historical market cap data from {HISTORICAL_30DAY_FILE}")
        df = pd.read_csv(HISTORICAL_30DAY_FILE)
        
        # The CSV has two Date columns, keep only the first one
        if 'Date' in df.columns and df.columns[0] == 'Date' and df.columns[1] == 'Date':
            df = df.drop(columns=[df.columns[1]])
        
        # Convert the Date column to datetime
        df['Date'] = pd.to_datetime(df['Date'])
        
        # Process market cap values (convert 'T' to trillions)
        for col in df.columns:
            if col != 'Date':
                if df[col].dtype == object:  # String values
                    df[col] = df[col].apply(lambda x: float(x.replace('T', '')) * 1e12 if isinstance(x, str) and 'T' in x else x)
        
        logger.info(f"Loaded {len(df)} days of historical market cap data")
        logger.info(f"Date range: {df['Date'].min().strftime('%Y-%m-%d')} to {df['Date'].max().strftime('%Y-%m-%d')}")
        
        return df
    except Exception as e:
        logger.error(f"Error loading historical data: {e}")
        return None

def get_sector_ids(conn):
    """Get sector IDs from the database"""
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT id, name FROM sectors")
        return {name: sector_id for sector_id, name in cursor.fetchall()}
    except Exception as e:
        logger.error(f"Error getting sector IDs: {e}")
        return {}

def clear_existing_data(conn, start_date=None, end_date=None):
    """
    Clear existing sector market cap data for the specified date range
    If no dates are specified, no data will be cleared
    """
    if not start_date or not end_date:
        logger.info("No date range specified, skipping data clearing")
        return
    
    try:
        cursor = conn.cursor()
        cursor.execute(
            "DELETE FROM sector_market_caps WHERE date BETWEEN ? AND ?", 
            (start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
        )
        conn.commit()
        logger.info(f"Cleared existing data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    except Exception as e:
        logger.error(f"Error clearing existing data: {e}")

def import_historical_data(conn, df, sector_ids):
    """Import historical market cap data into the database"""
    if df is None or df.empty:
        logger.error("No historical data to import")
        return False
    
    try:
        cursor = conn.cursor()
        records_inserted = 0
        
        for index, row in df.iterrows():
            date_value = row['Date'].strftime('%Y-%m-%d')
            
            for sector_name, sector_id in sector_ids.items():
                # Match the sector name with CSV column names (handling spaces, etc.)
                matching_cols = [col for col in df.columns if col != 'Date' and sector_name in col]
                
                if not matching_cols:
                    continue
                
                # For each sector, get the market cap value from the CSV
                market_cap = row[matching_cols[0]]
                
                # Skip if market cap is not available
                if pd.isna(market_cap) or market_cap == 0:
                    continue
                
                # Insert the market cap record
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO sector_market_caps (sector_id, date, market_cap)
                    VALUES (?, ?, ?)
                    """,
                    (sector_id, date_value, market_cap)
                )
                records_inserted += 1
                
                if records_inserted % 100 == 0:
                    logger.info(f"Inserted {records_inserted} records")
        
        conn.commit()
        logger.info(f"Successfully imported {records_inserted} historical market cap records")
        return True
    except Exception as e:
        logger.error(f"Error importing historical data: {e}")
        conn.rollback()
        return False

def verify_data(conn, df):
    """Verify that the data was imported correctly"""
    if df is None or df.empty:
        logger.error("No data to verify")
        return False
    
    try:
        cursor = conn.cursor()
        
        # Get the date range from the data
        start_date = df['Date'].min().strftime('%Y-%m-%d')
        end_date = df['Date'].max().strftime('%Y-%m-%d')
        
        # Count the number of records in the database for this date range
        cursor.execute(
            """
            SELECT COUNT(*) FROM sector_market_caps
            WHERE date BETWEEN ? AND ?
            """,
            (start_date, end_date)
        )
        count = cursor.fetchone()[0]
        
        # Calculate expected number of records (days * sectors)
        days = len(df)
        sectors = len([col for col in df.columns if col != 'Date'])
        expected = days * sectors
        
        logger.info(f"Database has {count} records for date range {start_date} to {end_date}")
        logger.info(f"Expected approximately {expected} records ({days} days * {sectors} sectors)")
        
        # Sample a few records to verify
        cursor.execute(
            """
            SELECT s.name, sm.date, sm.market_cap
            FROM sector_market_caps sm
            JOIN sectors s ON sm.sector_id = s.id
            WHERE sm.date BETWEEN ? AND ?
            ORDER BY sm.date, s.name
            LIMIT 10
            """,
            (start_date, end_date)
        )
        sample = cursor.fetchall()
        
        logger.info("Sample data in database:")
        for sector, date, market_cap in sample:
            logger.info(f"  {sector}: {date} - Market Cap: ${market_cap/1e9:.2f}B")
        
        return True
    except Exception as e:
        logger.error(f"Error verifying data: {e}")
        return False

def main():
    """Main function to populate 30-day historical market cap data"""
    logger.info("Starting 30-day historical market cap data import")
    
    # Connect to the database
    conn = connect_to_db()
    
    # Load the historical data
    df = load_30day_historical_data()
    if df is None:
        logger.error("Failed to load historical data")
        return
    
    # Get sector IDs
    sector_ids = get_sector_ids(conn)
    if not sector_ids:
        logger.error("Failed to get sector IDs")
        return
    
    logger.info(f"Found {len(sector_ids)} sectors in the database")
    
    # Clear existing data for the date range
    if df is not None and not df.empty:
        clear_existing_data(conn, df['Date'].min(), df['Date'].max())
    
    # Import the historical data
    success = import_historical_data(conn, df, sector_ids)
    
    # Verify the data
    if success:
        verify_data(conn, df)
    
    # Close the database connection
    conn.close()
    logger.info("Database connection closed")

if __name__ == "__main__":
    main()