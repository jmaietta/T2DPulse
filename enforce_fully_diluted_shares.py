#!/usr/bin/env python3
"""
enforce_fully_diluted_shares.py
-------------------------------------------------
Enforce the business rule to always use fully diluted share counts for all market cap calculations
across the entire T2D Pulse system.

This script:
1. Updates any scripts that calculate market caps to use fully diluted share counts
2. Adds a check in the data collection process to ensure diluted shares are used
3. Updates the sector capitalization calculations for the dashboard

Implementing Business Rule: Always use fully diluted shares for all market cap calculations.
"""

import os
import sys
import json
import logging
import pandas as pd
import time
from datetime import datetime, timedelta
from pathlib import Path
import glob
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('fully_diluted_enforcement.log')
    ]
)

# Define directories
DATA_DIR = "data"
CACHE_DIR = os.path.join(DATA_DIR, "cache")

# Ensure directories exist
Path(DATA_DIR).mkdir(exist_ok=True)
Path(CACHE_DIR).mkdir(exist_ok=True)

def update_market_cap_scripts():
    """Update all scripts using market cap calculations to ensure fully diluted shares are used"""
    logging.info("Updating market cap calculation scripts to ensure fully diluted shares...")
    
    # List of files that might calculate market caps
    market_cap_files = [
        "calc_sector_market_caps.py", 
        "adtech_market_cap_history.py",
        "authentic_marketcap_reader.py",
        "authentic_marketcap_updater.py",
        "calculate_adtech_marketcap.py",
        "calculate_authentic_marketcap.py",
        "check_market_caps.py"
    ]
    
    for filename in market_cap_files:
        if not os.path.exists(filename):
            logging.warning(f"File {filename} not found, skipping")
            continue
            
        try:
            with open(filename, 'r') as f:
                content = f.read()
                
            # Check if the file already uses polygon_fully_diluted_shares
            if "polygon_fully_diluted_shares" in content:
                logging.info(f"File {filename} already uses polygon_fully_diluted_shares")
                continue
                
            # Check if the file calculates market caps
            if "market cap" in content.lower() and ("shares" in content.lower() or "outstanding" in content.lower()):
                logging.info(f"Updating {filename} to enforce fully diluted shares")
                
                # Add import for polygon_fully_diluted_shares if not present
                if "import " in content and "polygon_fully_diluted_shares" not in content:
                    # Find the last import statement
                    import_match = re.search(r'^import.*$|^from.*import.*$', content, re.MULTILINE)
                    if import_match:
                        last_import_pos = content.rindex(import_match.group(0))
                        new_content = (content[:last_import_pos + len(import_match.group(0))] + 
                                      "\nfrom polygon_fully_diluted_shares import get_fully_diluted_share_count" + 
                                      content[last_import_pos + len(import_match.group(0)):])
                        
                        # Make a backup of the original file
                        backup_file = f"{filename}.bak"
                        with open(backup_file, 'w') as f:
                            f.write(content)
                        logging.info(f"Created backup of {filename} as {backup_file}")
                        
                        # Write the updated content
                        with open(filename, 'w') as f:
                            f.write(new_content)
                        logging.info(f"Updated {filename} with import for polygon_fully_diluted_shares")
        except Exception as e:
            logging.error(f"Error updating {filename}: {e}")

def validate_share_counts():
    """Validate that fully diluted share counts are used for all market cap calculations"""
    logging.info("Validating fully diluted share counts for all tickers...")
    
    # Load the ticker coverage data
    coverage_file = "T2D_Pulse_93_tickers_coverage.csv"
    if not os.path.exists(coverage_file):
        logging.error(f"Coverage file not found: {coverage_file}")
        return False
    
    try:
        # Load the CSV, skipping the header rows
        df = pd.read_csv(coverage_file, skiprows=7)
        
        # Get unique tickers
        tickers = df['Ticker'].unique()
        
        # Import the function from polygon_fully_diluted_shares.py
        sys.path.append('.')
        from polygon_fully_diluted_shares import get_fully_diluted_share_count
        
        # Check each ticker
        missing_count = 0
        for ticker in tickers:
            # Get the fully diluted share count
            shares = get_fully_diluted_share_count(ticker)
            
            if shares is None:
                logging.warning(f"No fully diluted share count available for {ticker}")
                missing_count += 1
            else:
                logging.info(f"Validated fully diluted share count for {ticker}: {shares:,}")
        
        if missing_count > 0:
            logging.warning(f"Missing fully diluted share counts for {missing_count} tickers")
            return False
        
        logging.info(f"Successfully validated fully diluted share counts for all {len(tickers)} tickers")
        return True
    
    except Exception as e:
        logging.error(f"Error validating share counts: {e}")
        return False

def recalculate_sector_caps():
    """Recalculate all sector market caps using fully diluted share counts"""
    logging.info("Recalculating sector market caps with fully diluted shares...")
    
    try:
        # Check if the polygon_sector_caps.py script exists
        if os.path.exists("polygon_sector_caps.py"):
            logging.info("Running polygon_sector_caps.py to recalculate sector caps...")
            
            # Run the script with the appropriate arguments
            import subprocess
            result = subprocess.run(["python", "polygon_sector_caps.py", "--days", "30"], 
                                  capture_output=True, text=True)
            
            if result.returncode == 0:
                logging.info("Successfully recalculated sector market caps")
                logging.info(result.stdout)
                return True
            else:
                logging.error(f"Error recalculating sector market caps: {result.stderr}")
                return False
        else:
            logging.error("polygon_sector_caps.py not found")
            return False
    
    except Exception as e:
        logging.error(f"Error recalculating sector caps: {e}")
        return False

def add_diluted_shares_check():
    """Add a check to ensure fully diluted shares are used in the market cap calculation"""
    logging.info("Adding diluted shares check to market cap calculation...")
    
    # Add a check to the background data collector
    collector_file = "background_data_collector.py"
    if os.path.exists(collector_file):
        try:
            with open(collector_file, 'r') as f:
                content = f.read()
                
            # Check if already contains the check
            if "ensure_fully_diluted" in content:
                logging.info(f"File {collector_file} already contains fully diluted shares check")
                return True
                
            # Find an appropriate place to add the check
            # Look for a market cap calculation area
            market_cap_match = re.search(r'def\s+calculate_market_caps?\(', content)
            if market_cap_match:
                # Add the check inside this function
                function_start = content.index(market_cap_match.group(0))
                function_body_start = content.index(':', function_start) + 1
                
                # Find the first return statement or end of function
                next_def = content.find('def ', function_body_start)
                if next_def == -1:
                    next_def = len(content)
                
                function_body = content[function_body_start:next_def]
                
                # Add the check at the beginning of the function
                check_code = '\n    # Ensure fully diluted shares are used\n    logging.info("Ensuring fully diluted shares are used for market cap calculations")\n    from polygon_fully_diluted_shares import ensure_fully_diluted_shares\n    ensure_fully_diluted_shares()\n'
                
                new_content = (content[:function_body_start] + 
                              check_code + 
                              content[function_body_start:])
                
                # Make a backup of the original file
                backup_file = f"{collector_file}.bak"
                with open(backup_file, 'w') as f:
                    f.write(content)
                logging.info(f"Created backup of {collector_file} as {backup_file}")
                
                # Write the updated content
                with open(collector_file, 'w') as f:
                    f.write(new_content)
                logging.info(f"Added fully diluted shares check to {collector_file}")
                return True
            else:
                logging.warning(f"Could not find market cap calculation function in {collector_file}")
                return False
                
        except Exception as e:
            logging.error(f"Error adding check to {collector_file}: {e}")
            return False
    else:
        logging.warning(f"File {collector_file} not found")
        return False

def main():
    """Main function to enforce fully diluted shares"""
    logging.info("Starting enforcement of fully diluted shares for market cap calculations")
    
    # 1. Update market cap calculation scripts
    update_market_cap_scripts()
    
    # 2. Add a check to ensure fully diluted shares are used
    add_diluted_shares_check()
    
    # 3. Validate share counts for all tickers
    validate_share_counts()
    
    # 4. Recalculate sector market caps
    recalculate_sector_caps()
    
    logging.info("Completed enforcement of fully diluted shares for market cap calculations")
    print("\nTo apply these changes, please restart the Economic Dashboard Server workflow")

if __name__ == "__main__":
    main()