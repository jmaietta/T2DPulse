#!/usr/bin/env python3
"""
Display the historical market caps for all 14 sectors for the past 30 days.
This script reads directly from the authentic data sources and formats it for display.
"""

import os
import pandas as pd
from datetime import datetime, timedelta
import sqlite3
from tabulate import tabulate
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

# Database file path
DB_FILE = 'market_cap_data.db'

# Sector market cap files
LATEST_MARKET_CAPS = 'authentic_sector_market_caps.csv'
HISTORICAL_MARKET_CAPS = 'data/sector_marketcap_30day_table.csv'

def format_market_cap(value):
    """Format market cap value for display"""
    if pd.isna(value):
        return "N/A"
    
    # Convert to billions
    billions = value / 1_000_000_000
    
    if billions >= 1000:
        # Show as trillions with 2 decimal places
        return f"${billions/1000:.2f}T"
    else:
        # Show as billions with 2 decimal places
        return f"${billions:.2f}B"

def get_sector_market_caps_from_db():
    """Get sector market caps from the SQLite database"""
    try:
        # Connect to the database
        if not os.path.exists(DB_FILE):
            logger.info(f"Database file {DB_FILE} not found, trying alternative sources")
            return None
        
        conn = sqlite3.connect(DB_FILE)
        
        # Get all sectors
        cursor = conn.cursor()
        cursor.execute("SELECT id, name FROM sectors")
        sectors = {sector_id: name for sector_id, name in cursor.fetchall()}
        
        if not sectors:
            logger.info("No sectors found in database, trying alternative sources")
            conn.close()
            return None
        
        # Get market cap data for the last 30 days
        thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        
        results = {}
        for sector_id, sector_name in sectors.items():
            cursor.execute("""
                SELECT date, market_cap
                FROM sector_market_caps
                WHERE sector_id = ? AND date >= ?
                ORDER BY date
            """, (sector_id, thirty_days_ago))
            
            results[sector_name] = {date: market_cap for date, market_cap in cursor.fetchall()}
        
        conn.close()
        
        if not any(results.values()):
            logger.info("No market cap data found in database, trying alternative sources")
            return None
        
        return results
    
    except Exception as e:
        logger.error(f"Error retrieving data from database: {e}")
        return None

def get_sector_market_caps_from_csv():
    """Get sector market caps from CSV files"""
    try:
        # Check if the historical market cap file exists
        if not os.path.exists(HISTORICAL_MARKET_CAPS):
            logger.info(f"Historical market cap file {HISTORICAL_MARKET_CAPS} not found")
            return None
        
        # Load the historical market caps
        logger.info(f"Loading market caps from {HISTORICAL_MARKET_CAPS}")
        df = pd.read_csv(HISTORICAL_MARKET_CAPS)
        
        # Handle the duplicate Date column in the first row
        if list(df.columns).count('Date') > 1:
            # Keep only the first Date column
            date_col = df.columns[0]
            cols_to_keep = [date_col] + [col for col in df.columns if col != 'Date' or col == date_col]
            df = df[cols_to_keep]
            # Rename to ensure we have a single Date column
            df = df.rename(columns={date_col: 'Date'})
        
        # Ensure proper date format
        df['Date'] = pd.to_datetime(df['Date'])
        
        # Convert T to trillions
        for col in df.columns:
            if col != 'Date':
                if df[col].dtype == object:  # String type
                    # Replace T with empty string and convert to float
                    df[col] = df[col].str.replace('T', '').astype(float) * 1e12
        
        # Convert to dictionary format
        results = {}
        for col in df.columns:
            if col != 'Date':
                results[col] = {row['Date'].strftime('%Y-%m-%d'): row[col] for _, row in df.iterrows()}
        
        return results
    
    except Exception as e:
        logger.error(f"Error retrieving data from CSV: {e}")
        return None

def display_market_caps(market_caps):
    """Display market caps in a tabular format"""
    if not market_caps:
        logger.info("No market cap data available")
        return
    
    # Get all unique dates across all sectors
    all_dates = set()
    for sector_data in market_caps.values():
        all_dates.update(sector_data.keys())
    
    # Sort dates
    date_list = sorted(all_dates)
    
    # Create a DataFrame for display
    data = []
    for date in date_list:
        row = [date]
        for sector in sorted(market_caps.keys()):
            market_cap = market_caps[sector].get(date)
            row.append(format_market_cap(market_cap) if market_cap else "N/A")
        data.append(row)
    
    # Create headers
    headers = ["Date"] + sorted(market_caps.keys())
    
    # Display table
    logger.info("\n" + tabulate(data, headers=headers, tablefmt="grid"))
    
    # Display totals for the most recent date
    if date_list:
        latest_date = date_list[-1]
        logger.info(f"\nSector Market Caps for {latest_date}:")
        
        total_market_cap = 0
        sector_values = []
        
        for sector in sorted(market_caps.keys()):
            market_cap = market_caps[sector].get(latest_date, 0) or 0
            total_market_cap += market_cap
            sector_values.append((sector, market_cap))
        
        # Sort by market cap value (descending)
        sector_values.sort(key=lambda x: x[1], reverse=True)
        
        # Display each sector's market cap and percentage of total
        for sector, market_cap in sector_values:
            percentage = (market_cap / total_market_cap * 100) if total_market_cap > 0 else 0
            logger.info(f"{sector:<25}: {format_market_cap(market_cap)} ({percentage:.2f}%)")
        
        logger.info(f"\nTotal Market Cap: {format_market_cap(total_market_cap)}")

def main():
    """Main function to display sector market caps"""
    logger.info("Retrieving sector market caps for the past 30 days...")
    
    # Try to get market caps from the database first
    market_caps = get_sector_market_caps_from_db()
    source = "database"
    
    # If not available in database, try CSV files
    if not market_caps:
        market_caps = get_sector_market_caps_from_csv()
        source = "CSV files"
    
    # Display the market caps
    if market_caps:
        logger.info(f"Displaying sector market caps from {source}:")
        display_market_caps(market_caps)
    else:
        logger.error("No sector market cap data available from any source")

if __name__ == "__main__":
    main()