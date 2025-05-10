"""
Check sector history files to ensure all sectors have authentic data
"""

import os
import logging
from pathlib import Path
import pandas as pd
import sentiment_engine 

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Define consistent data directory path
DATA_DIR = Path("data").resolve()

def check_sector_history_files():
    """
    Check if all sector history files exist and are properly formatted
    """
    print("Checking sector history files...")
    missing_sectors = []
    
    for sector in sentiment_engine.SECTORS:
        # Match the exact file naming pattern used in generate_sector_history_files.py
        sector_file = sector.lower().replace(' ', '_').replace('/', '___')
        parquet_path = DATA_DIR / f"{sector_file}_history.parquet"
        csv_path = DATA_DIR / f"{sector_file}_history.csv"
        
        if parquet_path.exists():
            print(f"{sector:25s} → OK (parquet) ({parquet_path.as_posix()})")
            # Optionally check content validity here
        elif csv_path.exists():
            print(f"{sector:25s} → OK (csv) ({csv_path.as_posix()})")
            # Optionally check content validity here
        else:
            print(f"{sector:25s} → MISSING ({parquet_path.as_posix()})")
            missing_sectors.append(sector)
    
    if missing_sectors:
        print("\nWARNING: The following sectors are missing historical data files:")
        for sector in missing_sectors:
            print(f"  - {sector}")
        print("\nThis will cause the application to fall back to synthetic data, which violates the data integrity policy.")
        print("Please ensure authentic historical data is available for all sectors.")
    else:
        print("\nAll sector history files are present.")

if __name__ == "__main__":
    check_sector_history_files()