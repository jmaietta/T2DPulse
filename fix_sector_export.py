#!/usr/bin/env python3
# fix_sector_export.py
"""
Fix sector export functionality by ensuring all necessary files exist
"""

import os
import sys
import logging
import pandas as pd
from datetime import datetime
import pytz
import json
import shutil

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Define file paths
DATA_DIR = 'data'
AUTHENTIC_HISTORY_JSON = os.path.join(DATA_DIR, 'authentic_sector_history.json')
AUTHENTIC_HISTORY_CSV = os.path.join(DATA_DIR, 'authentic_sector_history.csv')
SECTOR_SENTIMENT_HISTORY_CSV = os.path.join(DATA_DIR, 'sector_sentiment_history.csv')
SECTOR_SENTIMENT_HISTORY_EXCEL = os.path.join(DATA_DIR, 'sector_sentiment_history.xlsx')

def get_eastern_date():
    """Get the current date in US Eastern Time"""
    eastern = pytz.timezone('US/Eastern')
    today = datetime.now(eastern)
    return today.strftime('%Y-%m-%d')

def ensure_data_directory():
    """Ensure the data directory exists"""
    os.makedirs(DATA_DIR, exist_ok=True)
    logger.info(f"Ensured data directory exists: {DATA_DIR}")

def convert_raw_scores_to_display(df):
    """Convert scores from -1/+1 scale to 0-100 scale"""
    if df is None or df.empty:
        return None
    
    # Make a copy for conversion
    display_df = df.copy()
    
    # Convert each score column from -1/+1 scale to 0-100 scale
    for col in display_df.columns:
        if col != 'date' and col != 'Date':
            try:
                display_df[col] = ((display_df[col].astype(float) + 1) * 50).round(1)
            except Exception as e:
                logger.error(f"Error converting column {col}: {e}")
    
    return display_df

def load_authentic_history():
    """Load authentic sector history data"""
    # Try JSON file first
    if os.path.exists(AUTHENTIC_HISTORY_JSON):
        try:
            with open(AUTHENTIC_HISTORY_JSON, 'r') as f:
                data = json.load(f)
            
            # Convert to DataFrame
            df = pd.DataFrame(data)
            logger.info(f"Loaded authentic sector history from JSON: {len(df)} records")
            return df
        except Exception as e:
            logger.error(f"Error loading authentic sector history from JSON: {e}")
    
    # Try CSV file next
    if os.path.exists(AUTHENTIC_HISTORY_CSV):
        try:
            df = pd.read_csv(AUTHENTIC_HISTORY_CSV)
            logger.info(f"Loaded authentic sector history from CSV: {len(df)} records")
            return df
        except Exception as e:
            logger.error(f"Error loading authentic sector history from CSV: {e}")
    
    logger.warning("No authentic sector history found")
    return None

def create_sector_history_files():
    """Create sector history files in all required formats"""
    # Load authentic sector history
    df = load_authentic_history()
    if df is None:
        logger.error("Could not load authentic sector history")
        return False
    
    # Convert to display format (0-100 scale)
    display_df = convert_raw_scores_to_display(df)
    if display_df is None:
        logger.error("Could not convert sector scores to display format")
        return False
    
    # Save to CSV
    try:
        display_df.to_csv(SECTOR_SENTIMENT_HISTORY_CSV, index=False)
        logger.info(f"Saved sector sentiment history to CSV: {SECTOR_SENTIMENT_HISTORY_CSV}")
    except Exception as e:
        logger.error(f"Error saving sector sentiment history to CSV: {e}")
        return False
    
    # Save to Excel
    try:
        # Add current date to filename for dated version
        today = get_eastern_date()
        dated_excel = os.path.join(DATA_DIR, f'sector_sentiment_history_{today}.xlsx')
        
        # Save both regular and dated versions
        display_df.to_excel(SECTOR_SENTIMENT_HISTORY_EXCEL, index=False)
        display_df.to_excel(dated_excel, index=False)
        
        logger.info(f"Saved sector sentiment history to Excel: {SECTOR_SENTIMENT_HISTORY_EXCEL}")
        logger.info(f"Saved dated sector sentiment history to Excel: {dated_excel}")
    except Exception as e:
        logger.error(f"Error saving sector sentiment history to Excel: {e}")
        return False
    
    return True

def fix_sector_export():
    """Fix sector export functionality"""
    logger.info("Starting sector export fix")
    
    # Ensure data directory exists
    ensure_data_directory()
    
    # Create sector history files
    if create_sector_history_files():
        logger.info("Successfully fixed sector export functionality")
        return True
    else:
        logger.error("Failed to fix sector export functionality")
        return False

if __name__ == '__main__':
    success = fix_sector_export()
    sys.exit(0 if success else 1)