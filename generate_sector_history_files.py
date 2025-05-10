"""
Generate individual sector history files from the authentic sector history JSON

This utility fixes the issue where individual sector history files are missing,
which causes the application to fall back to synthetic data.
"""

import os
import sys
import json
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import pytz

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Define consistent data directory path
DATA_DIR = Path("data").resolve()

def load_authentic_sector_history():
    """
    Load authentic sector history from JSON file
    """
    json_path = DATA_DIR / "authentic_sector_history.json"
    
    if not json_path.exists():
        logging.error(f"Authentic sector history file not found: {json_path}")
        return {}
    
    try:
        with open(json_path, 'r') as f:
            data = json.load(f)
        logging.info(f"Loaded authentic sector history with {len(data)} dates")
        return data
    except Exception as e:
        logging.error(f"Failed to load authentic sector history: {e}")
        return {}

def generate_sector_history(sector, data):
    """
    Generate history for a single sector
    
    Args:
        sector (str): Sector name
        data (dict): Authentic sector history data
        
    Returns:
        pd.DataFrame: Sector history DataFrame
    """
    history = []
    
    # Extract this sector's history from all dates
    for date_str, sectors in data.items():
        if sector in sectors:
            history.append({
                'date': date_str,
                'score': sectors[sector]
            })
    
    if not history:
        logging.warning(f"No history data found for sector: {sector}")
        return pd.DataFrame()
    
    # Convert to DataFrame and ensure date is properly formatted
    df = pd.DataFrame(history)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')
    
    return df

def save_sector_history_files(data):
    """
    Save individual sector history files for all sectors
    
    Args:
        data (dict): Authentic sector history data
    """
    # Get list of all sectors from the most recent date
    latest_date = max(data.keys())
    sectors = list(data[latest_date].keys())
    
    created_files = 0
    for sector in sectors:
        # Generate the sector history DataFrame
        df = generate_sector_history(sector, data)
        
        if df.empty:
            continue
        
        # Create proper filename
        sector_file = sector.lower().replace(' ', '_').replace('/', '_')
        output_path = DATA_DIR / f"{sector_file}_history.parquet"
        
        # Save to parquet file
        try:
            df.to_parquet(output_path)
            logging.info(f"Saved sector history for {sector} to {output_path}")
            created_files += 1
        except Exception as e:
            logging.error(f"Failed to save sector history for {sector}: {e}")
    
    return created_files

def main():
    """
    Main function to generate sector history files
    """
    # Create data directory if it doesn't exist
    DATA_DIR.mkdir(exist_ok=True)
    
    # Load authentic sector history
    data = load_authentic_sector_history()
    
    if not data:
        logging.error("No authentic sector history data available")
        return False
    
    # Save individual sector history files
    created_files = save_sector_history_files(data)
    
    if created_files > 0:
        logging.info(f"Successfully created {created_files} sector history files")
        return True
    else:
        logging.error("Failed to create any sector history files")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)