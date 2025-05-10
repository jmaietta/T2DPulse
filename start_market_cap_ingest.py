#!/usr/bin/env python3
"""
Start the market cap ingest system to collect current data
and backfill historical data
"""
import os
import logging
from market_cap_ingest import migrate, collect_market_data, backfill_historical_data

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("start_market_cap_ingest.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def main():
    """Main function to run the market cap ingest system"""
    logger.info("Starting market cap ingest system...")
    
    # Make sure data directory exists
    os.makedirs('data', exist_ok=True)
    
    # Initialize database schema
    migrate()
    logger.info("Database initialized successfully")
    
    # Collect today's data first
    collect_market_data()
    logger.info("Today's market cap data collected successfully")
    
    # Backfill historical data for the past 30 days
    backfill_historical_data(days=30)
    logger.info("Historical market cap data backfilled successfully")
    
    # Run the data quality check
    logger.info("Running data quality check...")
    try:
        from check_market_cap_data import main as check_data
        check_data()
    except Exception as e:
        logger.error(f"Error running data quality check: {e}")
    
    logger.info("Market cap ingest system setup completed successfully")

if __name__ == "__main__":
    main()