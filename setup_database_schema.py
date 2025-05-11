#!/usr/bin/env python3
"""
Set up the PostgreSQL database schema for T2D Pulse.

This script:
1. Creates the necessary tables for tickers, sectors, and market cap data
2. Sets up the relationships between tickers and sectors to allow one ticker to belong to multiple sectors
3. Ensures that sector market caps are calculated correctly based on the authentic mappings

IMPORTANT: This script does NOT import any data, only sets up the schema.
"""

import os
import sys
import logging
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("setup_database.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Database connection
DB_URL = os.environ.get("DATABASE_URL")
if not DB_URL:
    logger.error("DATABASE_URL environment variable is not set")
    sys.exit(1)

def get_db_connection():
    """Get a connection to the PostgreSQL database"""
    try:
        conn = psycopg2.connect(DB_URL)
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        raise

def setup_schema():
    """Set up the database schema"""
    logger.info("Setting up database schema")
    
    try:
        conn = get_db_connection()
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
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
                name VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
        
        # Create view to look up tickers by sector
        cursor.execute("""
            CREATE OR REPLACE VIEW sector_tickers_view AS
            SELECT 
                s.id AS sector_id,
                s.name AS sector_name,
                t.id AS ticker_id,
                t.symbol AS ticker_symbol,
                t.name AS ticker_name
            FROM sectors s
            JOIN ticker_sectors ts ON s.id = ts.sector_id
            JOIN tickers t ON ts.ticker_id = t.id
            ORDER BY s.name, t.symbol
        """)
        
        # Create procedure to calculate sector market caps
        cursor.execute("""
            CREATE OR REPLACE PROCEDURE calculate_sector_market_caps(calculation_date DATE)
            LANGUAGE plpgsql
            AS $$
            DECLARE
                sector_rec RECORD;
                ticker_rec RECORD;
                sector_total NUMERIC;
                ticker_count INTEGER;
            BEGIN
                -- Loop through each sector
                FOR sector_rec IN SELECT id, name FROM sectors
                LOOP
                    sector_total := 0;
                    ticker_count := 0;
                    
                    -- For each ticker in this sector, add its full market cap
                    FOR ticker_rec IN 
                        SELECT t.id, t.symbol, tmc.market_cap
                        FROM tickers t
                        JOIN ticker_sectors ts ON t.id = ts.ticker_id
                        JOIN ticker_market_caps tmc ON t.id = tmc.ticker_id
                        WHERE ts.sector_id = sector_rec.id AND tmc.date = calculation_date
                    LOOP
                        -- Add the FULL market cap of this ticker to the sector total
                        sector_total := sector_total + ticker_rec.market_cap;
                        ticker_count := ticker_count + 1;
                    END LOOP;
                    
                    -- Skip if no market cap data available for this sector
                    IF ticker_count = 0 THEN
                        CONTINUE;
                    END IF;
                    
                    -- Insert or update the sector market cap
                    INSERT INTO sector_market_caps (sector_id, date, market_cap)
                    VALUES (sector_rec.id, calculation_date, sector_total)
                    ON CONFLICT (sector_id, date) 
                    DO UPDATE SET 
                        market_cap = EXCLUDED.market_cap,
                        updated_at = CURRENT_TIMESTAMP;
                END LOOP;
            END;
            $$;
        """)
        
        cursor.close()
        conn.close()
        
        logger.info("Database schema setup completed successfully")
        return True
    except Exception as e:
        logger.error(f"Error setting up database schema: {e}")
        return False

def verify_schema():
    """Verify that the schema was set up correctly"""
    logger.info("Verifying database schema")
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if all tables exist
        tables = ['sectors', 'tickers', 'ticker_sectors', 'ticker_market_caps', 'sector_market_caps']
        for table in tables:
            cursor.execute(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{table}')")
            if not cursor.fetchone()[0]:
                logger.error(f"Table {table} does not exist")
                return False
        
        # Check if views exist
        views = ['sector_market_caps_view', 'sector_tickers_view']
        for view in views:
            cursor.execute(f"SELECT EXISTS (SELECT FROM information_schema.views WHERE table_name = '{view}')")
            if not cursor.fetchone()[0]:
                logger.error(f"View {view} does not exist")
                return False
        
        # Check if procedure exists
        cursor.execute("SELECT proname FROM pg_proc WHERE proname = 'calculate_sector_market_caps'")
        if not cursor.fetchone():
            logger.error("Procedure calculate_sector_market_caps does not exist")
            return False
        
        cursor.close()
        conn.close()
        
        logger.info("Database schema verified successfully")
        return True
    except Exception as e:
        logger.error(f"Error verifying database schema: {e}")
        return False

def main():
    """Main function to set up database schema"""
    logger.info("Starting database schema setup")
    
    # Set up schema
    if not setup_schema():
        logger.error("Failed to set up database schema")
        return False
    
    # Verify schema
    if not verify_schema():
        logger.error("Failed to verify database schema")
        return False
    
    logger.info("Database schema setup completed successfully")
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)