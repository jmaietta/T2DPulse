#!/usr/bin/env python3
"""
Fix Market Cap Presentation

This script corrects the market cap data displayed in the T2D Pulse dashboard.
It ensures that the sector market cap table and dashboard visuals show the correct
authentic data from Polygon API.
"""

import os
import sys
import pandas as pd
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("fix_market_cap_presentation.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

def load_latest_market_caps():
    """Load the most recent market cap data from the sector_market_caps.csv file"""
    try:
        df = pd.read_csv('sector_market_caps.csv')
        
        # Get the latest date
        latest_date = df['date'].max()
        latest_data = df[df['date'] == latest_date]
        
        # Create a dictionary of sector -> market cap
        sector_market_caps = {}
        for _, row in latest_data.iterrows():
            sector_market_caps[row['sector']] = row['market_cap']
        
        return sector_market_caps, latest_date
    except Exception as e:
        logging.error(f"Error loading latest market caps: {e}")
        return None, None

def update_corrected_sector_market_caps(sector_market_caps):
    """Update the corrected_sector_market_caps.csv file with the latest authentic data"""
    if not sector_market_caps:
        logging.error("No market cap data to update")
        return False
    
    try:
        # Convert market caps to billions for readability
        data = []
        for sector, market_cap in sorted(sector_market_caps.items(), key=lambda x: x[1], reverse=True):
            data.append({
                'Sector': sector,
                'Market Cap (Billions USD)': market_cap / 1e9
            })
        
        df = pd.DataFrame(data)
        df.to_csv('corrected_sector_market_caps.csv', index=False)
        logging.info("Updated corrected_sector_market_caps.csv with latest authentic data")
        return True
    except Exception as e:
        logging.error(f"Error updating corrected sector market caps: {e}")
        return False

def update_formatted_sector_market_caps(sector_market_caps, latest_date):
    """Update the formatted_sector_market_caps.csv file with the latest authentic data"""
    if not sector_market_caps or not latest_date:
        logging.error("No market cap data to update")
        return False
    
    try:
        # Convert market caps to billions for readability
        data = []
        for sector, market_cap in sorted(sector_market_caps.items(), key=lambda x: x[1], reverse=True):
            data.append({
                'Sector': sector,
                'Date': latest_date,
                'Market Cap (Billions USD)': round(market_cap / 1e9, 2),
                'Data Source': 'Polygon API (Authentic)'
            })
        
        df = pd.DataFrame(data)
        df.to_csv('formatted_sector_market_caps.csv', index=False)
        logging.info("Updated formatted_sector_market_caps.csv with latest authentic data")
        return True
    except Exception as e:
        logging.error(f"Error updating formatted sector market caps: {e}")
        return False

def create_market_cap_table():
    """Create a formatted market cap table for the dashboard"""
    try:
        sector_market_caps, latest_date = load_latest_market_caps()
        if not sector_market_caps:
            return False
        
        # Convert to billions and format as a table
        table_data = []
        for sector, market_cap in sorted(sector_market_caps.items(), key=lambda x: x[1], reverse=True):
            market_cap_billions = market_cap / 1e9
            table_data.append({
                'Sector': sector,
                'Market Cap': f"${market_cap_billions:.2f}B"
            })
        
        df = pd.DataFrame(table_data)
        
        # Save as a formatted table
        with open('market_cap_table.txt', 'w') as f:
            f.write(f"T2D Pulse Sector Market Caps as of {latest_date}\n")
            f.write("Source: Polygon API (Authentic Data)\n")
            f.write("=" * 50 + "\n")
            f.write(df.to_string(index=False))
        
        logging.info("Created formatted market cap table")
        return True
    except Exception as e:
        logging.error(f"Error creating market cap table: {e}")
        return False

def main():
    """Main function to fix market cap presentation"""
    logging.info("Starting market cap presentation fix...")
    
    # Load the latest market cap data
    sector_market_caps, latest_date = load_latest_market_caps()
    if not sector_market_caps:
        return False
    
    # Update the corrected market caps file
    update_corrected_sector_market_caps(sector_market_caps)
    
    # Update the formatted market caps file
    update_formatted_sector_market_caps(sector_market_caps, latest_date)
    
    # Create a formatted market cap table
    create_market_cap_table()
    
    logging.info("Market cap presentation fix completed successfully!")
    return True

if __name__ == "__main__":
    success = main()
    if not success:
        logging.error("Failed to fix market cap presentation")