"""
Update sector path references to use a consistent format and ensure they are accessible
by sector sentiment components
"""

import os
import sys
import json
import logging
import pandas as pd
from pathlib import Path
import shutil

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Define consistent data directory paths
DATA_DIR = Path("data").resolve()
ROOT_DIR = Path(".").resolve()

def find_sector_history_files():
    """Find all sector history files in the data directory"""
    sector_files = list(DATA_DIR.glob("*_history.parquet"))
    logging.info(f"Found {len(sector_files)} sector history files in {DATA_DIR}")
    return sector_files

def copy_sector_files_to_root():
    """Copy sector history files to the root directory to ensure they're accessible"""
    sector_files = find_sector_history_files()
    copied_count = 0
    
    for source_path in sector_files:
        dest_path = ROOT_DIR / source_path.name
        try:
            shutil.copy2(source_path, dest_path)
            logging.info(f"Copied {source_path} to {dest_path}")
            copied_count += 1
        except Exception as e:
            logging.error(f"Failed to copy {source_path} to {dest_path}: {e}")
    
    return copied_count

def copy_authentic_history_to_root():
    """Copy the authentic sector history file to the root directory"""
    json_source = DATA_DIR / "authentic_sector_history.json"
    csv_source = DATA_DIR / "authentic_sector_history.csv"
    
    copied_count = 0
    
    # Copy JSON file if it exists
    if json_source.exists():
        json_dest = ROOT_DIR / "authentic_sector_history.json"
        try:
            shutil.copy2(json_source, json_dest)
            logging.info(f"Copied {json_source} to {json_dest}")
            copied_count += 1
        except Exception as e:
            logging.error(f"Failed to copy {json_source} to {json_dest}: {e}")
    
    # Copy CSV file if it exists
    if csv_source.exists():
        csv_dest = ROOT_DIR / "authentic_sector_history.csv"
        try:
            shutil.copy2(csv_source, csv_dest)
            logging.info(f"Copied {csv_source} to {csv_dest}")
            copied_count += 1
        except Exception as e:
            logging.error(f"Failed to copy {csv_source} to {csv_dest}: {e}")
    
    return copied_count

def fix_authentic_sector_history_csv():
    """Fix the authentic sector history CSV file format"""
    csv_path = DATA_DIR / "authentic_sector_history.csv"
    
    if not csv_path.exists():
        logging.error(f"Authentic sector history CSV file not found: {csv_path}")
        return False
    
    try:
        # Read the CSV file
        df = pd.read_csv(csv_path)
        
        # Make sure 'date' column is properly parsed
        df['date'] = pd.to_datetime(df['date'])
        
        # Convert sector scores from difference values to absolute values (0-100 scale)
        for column in df.columns:
            if column != 'date':
                # Convert from difference to absolute score (50 + difference*100)
                df[column] = 50 + df[column] * 100
        
        # Save the fixed CSV file
        df.to_csv(csv_path, index=False)
        logging.info(f"Fixed authentic sector history CSV file: {csv_path}")
        
        # Also save to root directory
        root_csv_path = ROOT_DIR / "authentic_sector_history.csv"
        df.to_csv(root_csv_path, index=False)
        logging.info(f"Also saved fixed CSV to: {root_csv_path}")
        
        return True
    
    except Exception as e:
        logging.error(f"Failed to fix authentic sector history CSV: {e}")
        return False

def main():
    """Main function to update sector path references"""
    # Copy sector history files to the root directory
    sector_count = copy_sector_files_to_root()
    logging.info(f"Copied {sector_count} sector history files to the root directory")
    
    # Copy authentic sector history files to the root directory
    authentic_count = copy_authentic_history_to_root()
    logging.info(f"Copied {authentic_count} authentic sector history files to the root directory")
    
    # Fix the authentic sector history CSV file format
    fixed = fix_authentic_sector_history_csv()
    if fixed:
        logging.info("Successfully fixed authentic sector history CSV file format")
    else:
        logging.error("Failed to fix authentic sector history CSV file format")
    
    return sector_count > 0 and authentic_count > 0 and fixed

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)