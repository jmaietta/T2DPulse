#!/usr/bin/env python3
"""
Fix the sector display issue in the T2D Pulse dashboard
by ensuring consistent data formatting and proper data loading
for sector sparklines.
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Try to use the database access module if available
try:
    import db_access
    USE_DATABASE = True
    logger.info("Using database for market cap data")
except ImportError:
    USE_DATABASE = False
    logger.info("Database module not available, using CSV files")

def ensure_consistent_sector_data():
    """
    Ensure that sector data is consistently formatted and available
    for all visualization components in the dashboard
    """
    # Get the latest 30 days of market cap data
    if USE_DATABASE:
        # Use database access module
        sector_data = db_access.get_sector_market_caps(days=30)
        sentiment_data = db_access.get_sector_sentiment_scores(days=30)
    else:
        # Fallback to CSV files
        sector_data = load_sector_market_caps_from_csv()
        sentiment_data = load_sector_sentiment_from_csv()
    
    # Ensure data is properly formatted
    sector_data = format_sector_data(sector_data)
    sentiment_data = format_sector_data(sentiment_data)
    
    # Save formatted data for dashboard use
    save_formatted_data(sector_data, 'data/sector_30day_history.csv')
    save_formatted_data(sentiment_data, 'data/sector_sentiment_30day.csv')
    
    logger.info(f"Saved formatted sector data with {len(sector_data)} days of data")
    return sector_data, sentiment_data

def load_sector_market_caps_from_csv():
    """Load sector market cap data from CSV file"""
    # Try different file paths
    file_paths = [
        'authentic_sector_market_caps.csv',
        'data/authentic_sector_market_caps.csv',
        'corrected_sector_market_caps.csv'
    ]
    
    for file_path in file_paths:
        if os.path.exists(file_path):
            logger.info(f"Loading sector market caps from {file_path}")
            df = pd.read_csv(file_path)
            df['date'] = pd.to_datetime(df['date'])
            return df
    
    logger.error("No sector market cap file found")
    return pd.DataFrame()

def load_sector_sentiment_from_csv():
    """Load sector sentiment data from CSV file"""
    # Try different file paths
    file_paths = [
        'data/authentic_sector_history.csv',
        'authentic_sector_history.csv',
        'sector_sentiment_history.csv'
    ]
    
    for file_path in file_paths:
        if os.path.exists(file_path):
            logger.info(f"Loading sector sentiment from {file_path}")
            df = pd.read_csv(file_path)
            df['date'] = pd.to_datetime(df['date'])
            return df
    
    logger.error("No sector sentiment file found")
    return pd.DataFrame()

def format_sector_data(df):
    """Format sector data with consistent columns and date format"""
    if df.empty:
        logger.warning("Empty data frame provided for formatting")
        return df
    
    # Ensure date is datetime type
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
    
    # Sort by date
    if 'date' in df.columns:
        df = df.sort_values('date')
    
    # Filter for business days only (exclude weekends)
    if 'date' in df.columns:
        df = df[df['date'].dt.dayofweek < 5]  # Monday-Friday are 0-4
    
    # Remove any duplicate dates
    if 'date' in df.columns:
        df = df.drop_duplicates(subset=['date'], keep='last')
    
    # Limit to last 30 days if more data is available
    if 'date' in df.columns and len(df) > 30:
        end_date = df['date'].max()
        start_date = end_date - timedelta(days=30)
        df = df[df['date'] >= start_date]
    
    return df

def save_formatted_data(df, output_path):
    """Save formatted data to CSV file"""
    # Create parent directory if it doesn't exist
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Save to CSV
    df.to_csv(output_path, index=False)
    logger.info(f"Saved formatted data to {output_path}")

def create_sector_sparkline_data():
    """Create properly formatted data for sector sparklines"""
    # Get consistent sector data
    sector_data, sentiment_data = ensure_consistent_sector_data()
    
    # Format as a dictionary for easy access in the dashboard
    sparkline_data = {}
    
    if 'date' in sector_data.columns:
        # For each sector, create a list of values for the sparkline
        sectors = [col for col in sector_data.columns if col != 'date']
        for sector in sectors:
            # Use market cap data
            values = sector_data[sector].tolist()
            dates = sector_data['date'].dt.strftime('%Y-%m-%d').tolist()
            sparkline_data[sector] = {
                'values': values,
                'dates': dates
            }
    
    # Save to a Python file for easy import
    with open('sector_sparkline_data.py', 'w') as f:
        f.write("#!/usr/bin/env python3\n")
        f.write('"""\nSector sparkline data for T2D Pulse dashboard\n"""\n\n')
        f.write(f"# Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write("SECTOR_SPARKLINE_DATA = {\n")
        for sector, data in sparkline_data.items():
            f.write(f"    '{sector}': {{\n")
            f.write(f"        'values': {data['values']},\n")
            f.write(f"        'dates': {data['dates']},\n")
            f.write("    },\n")
        f.write("}\n")
    
    logger.info(f"Created sector sparkline data for {len(sparkline_data)} sectors")
    return sparkline_data

if __name__ == "__main__":
    create_sector_sparkline_data()