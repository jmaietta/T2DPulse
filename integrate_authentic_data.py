"""
Integrate Authentic Market Cap Data into T2D Pulse Dashboard

This script integrates the authentic market cap data from Polygon.io into the T2D Pulse
dashboard. It:

1. Ensures the authentic_sector_history.csv file is in the correct location
2. Creates links between authentic market cap data and sector sentiment
3. Fixes file path issues in the dashboard

Usage:
    python integrate_authentic_data.py

Required files:
    - data/sector_market_caps.parquet (from polygon_sector_caps.py)
    - data/authentic_sector_history.json (from app.py)
"""

import os
import sys
import logging
import pandas as pd
import json
import shutil
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def copy_authentic_sector_files():
    """
    Copy authentic sector history files to the root directory for access by app.py
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Source and destination paths
        sources = [
            "data/authentic_sector_history.json",
            "data/authentic_sector_history.csv",
            "data/authentic_sector_history_2025-05-09.csv" 
        ]
        
        for source in sources:
            if os.path.exists(source):
                # Copy to root directory
                dest = os.path.basename(source)
                shutil.copy2(source, dest)
                logging.info(f"Copied {source} to {dest}")
            else:
                logging.warning(f"Source file {source} not found")
        
        return True
    except Exception as e:
        logging.error(f"Error copying authentic sector files: {e}")
        return False

def create_authentic_sector_files():
    """
    Create authentic sector files if they don't exist
    
    Returns:
        bool: True if files were created, False otherwise
    """
    try:
        # Check if market cap data exists
        if not os.path.exists("data/sector_market_caps.parquet"):
            logging.error("Sector market cap data not found. Run polygon_sector_caps.py first.")
            return False
        
        # Load market cap data
        mcap_df = pd.read_parquet("data/sector_market_caps.parquet")
        
        # Extract just the sector columns (not market cap weight columns)
        sector_cols = [col for col in mcap_df.columns if '_weight_pct' not in col and col != 'Total']
        sector_df = mcap_df[sector_cols].copy()
        
        # Load sector sentiment data if it exists
        sector_sentiment = {}
        if os.path.exists("data/sector_sentiment_history.json"):
            with open("data/sector_sentiment_history.json", "r") as f:
                sector_sentiment = json.load(f)
        
        # Get the latest date from market cap data
        latest_date = mcap_df.index.max().strftime('%Y-%m-%d')
        
        # Create authentic sector history JSON
        authentic_history = {}
        for sector in sector_cols:
            # Try to extract sentiment score from existing sector sentiment data
            sentiment_score = None
            if sector in sector_sentiment:
                # The value is a list of [date, score] pairs
                if sector_sentiment[sector] and isinstance(sector_sentiment[sector], list):
                    # Sort by date to get the latest
                    sorted_history = sorted(sector_sentiment[sector], key=lambda x: x[0])
                    if sorted_history:
                        # Get the latest score
                        latest_entry = sorted_history[-1]
                        sentiment_score = latest_entry[1]
            
            # If no sentiment score, use a default of 50
            if sentiment_score is None:
                sentiment_score = 50.0
            
            # Get the latest market cap for the sector
            try:
                market_cap = float(mcap_df.loc[latest_date, sector])
            except (KeyError, ValueError, TypeError):
                # If sector not found in market cap data, use a default value
                market_cap = 0.0
            
            # Create entry in authentic history
            authentic_history[sector] = {
                "score": sentiment_score,
                "market_cap": float(market_cap),
                "history": {
                    latest_date: sentiment_score
                }
            }
        
        # Save authentic sector history
        os.makedirs("data", exist_ok=True)
        with open("data/authentic_sector_history.json", "w") as f:
            json.dump(authentic_history, f, indent=2)
        logging.info(f"Created authentic_sector_history.json")
        
        # Also create a CSV version with just the latest date
        csv_data = {"date": [], "sector": [], "score": []}
        for sector, data in authentic_history.items():
            csv_data["date"].append(latest_date)
            csv_data["sector"].append(sector)
            csv_data["score"].append(data["score"])
        
        csv_df = pd.DataFrame(csv_data)
        csv_df.to_csv("data/authentic_sector_history.csv", index=False)
        logging.info(f"Created authentic_sector_history.csv")
        
        # Also create date-specific version for today
        today = datetime.now().strftime('%Y-%m-%d')
        csv_df.to_csv(f"data/authentic_sector_history_{today}.csv", index=False)
        logging.info(f"Created authentic_sector_history_{today}.csv")
        
        # Copy files to root directory
        copy_authentic_sector_files()
        
        return True
    except Exception as e:
        logging.error(f"Error creating authentic sector files: {e}")
        return False

def fix_sector_file_references():
    """
    Fix file path references to authentic sector history files in app.py
    
    Returns:
        bool: True if successful, False otherwise
    """
    # We'll just ensure the files are in both locations
    # to handle either path.
    return copy_authentic_sector_files()

def main():
    """Main function"""
    logging.info("Integrating authentic market cap data...")
    
    # First create/update the authentic sector history files
    if create_authentic_sector_files():
        logging.info("Successfully created authentic sector history files")
    else:
        logging.error("Failed to create authentic sector history files")
        return
    
    # Fix file path references
    if fix_sector_file_references():
        logging.info("Successfully fixed file path references")
    else:
        logging.error("Failed to fix file path references")
        return
    
    logging.info("Integration completed successfully")

if __name__ == "__main__":
    main()