#!/usr/bin/env python3
"""
Update FinTech ticker symbols in the system configuration
- FISV changed to FI (Fiserv)
- SQ changed to XYZ (Block, Inc.)

This approach updates the sector configuration files and data collection processes
rather than modifying CSV data directly.
"""

import os
import json
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)

# Define ticker changes
TICKER_CHANGES = {
    "FISV": "FI",    # Fiserv
    "SQ": "XYZ"      # Block, Inc
}

# Define configuration files to be updated
CONFIG_FILES = [
    "polygon_sector_caps.py",  # Core sector definition file
    "config.py",               # Configuration constants
    "background_data_collector.py",  # Background data collector
    "data_cache.py"            # Data caching system
]

def update_ticker_definitions(file_path):
    """
    Update ticker symbol definitions in configuration files
    
    This function modifies Python source code files to update ticker symbols.
    It intelligently updates list or dictionary definitions while preserving
    comments and formatting.
    """
    if not os.path.exists(file_path):
        logging.error(f"File not found: {file_path}")
        return False
    
    try:
        # Read file content
        with open(file_path, 'r') as f:
            lines = f.readlines()
        
        # Create backup
        with open(f"{file_path}.bak", 'w') as f:
            f.writelines(lines)
        
        # Process the file
        in_fintech_section = False
        sector_pattern = '"Fintech":|\'Fintech\':|"FinTech":|\'FinTech\':|Fintech:|FINTECH'
        modified = False
        
        for i, line in enumerate(lines):
            # Look for Fintech sector definition
            if any(pattern in line for pattern in sector_pattern.split('|')):
                in_fintech_section = True
            
            # Exit the Fintech section when we hit the next section or the end of a list/dict
            if in_fintech_section and ("]" in line or "}" in line):
                in_fintech_section = False
            
            # Only modify lines in the Fintech section that contain ticker symbols
            if in_fintech_section:
                # Process each ticker change
                for old_ticker, new_ticker in TICKER_CHANGES.items():
                    # Match tickers specified as strings (with quotes)
                    if f'"{old_ticker}"' in line:
                        lines[i] = line.replace(f'"{old_ticker}"', f'"{new_ticker}"')
                        modified = True
                        logging.info(f"Updated {old_ticker} -> {new_ticker} in {file_path}, line {i+1}")
                    elif f"'{old_ticker}'" in line:
                        lines[i] = line.replace(f"'{old_ticker}'", f"'{new_ticker}'")
                        modified = True
                        logging.info(f"Updated {old_ticker} -> {new_ticker} in {file_path}, line {i+1}")
        
        if modified:
            # Write updated content
            with open(file_path, 'w') as f:
                f.writelines(lines)
            logging.info(f"Updated {file_path}")
            return True
        else:
            logging.info(f"No Fintech ticker symbols found to update in {file_path}")
            return True
    
    except Exception as e:
        logging.error(f"Error updating {file_path}: {e}")
        return False

def update_ticker_map():
    """Update the ticker mapping cache if it exists"""
    ticker_map_file = "data/ticker_map.json"
    if os.path.exists(ticker_map_file):
        try:
            # Load existing ticker map
            with open(ticker_map_file, 'r') as f:
                ticker_map = json.load(f)
            
            # Create backup
            with open(f"{ticker_map_file}.bak", 'w') as f:
                json.dump(ticker_map, f, indent=2)
            
            # Update ticker mappings
            modified = False
            for old_ticker, new_ticker in TICKER_CHANGES.items():
                if old_ticker in ticker_map:
                    # Get existing data for old ticker
                    old_data = ticker_map.pop(old_ticker)
                    # Store data under new ticker
                    ticker_map[new_ticker] = old_data
                    modified = True
                    logging.info(f"Updated ticker map: {old_ticker} -> {new_ticker}")
            
            if modified:
                # Save updated map
                with open(ticker_map_file, 'w') as f:
                    json.dump(ticker_map, f, indent=2)
                logging.info(f"Updated {ticker_map_file}")
        
        except Exception as e:
            logging.error(f"Error updating ticker map: {e}")
    else:
        logging.info(f"Ticker map not found: {ticker_map_file} - skipping")

def update_api_mappers():
    """Update any API-specific ticker mappers"""
    # Files that might contain API-specific ticker mappings
    api_files = [
        "finnhub_data_collector.py",
        "polygon_data_collector.py",
        "alphavantage_client.py",
        "api_keys.py"
    ]
    
    for file_path in api_files:
        if os.path.exists(file_path):
            try:
                # Read file content
                with open(file_path, 'r') as f:
                    content = f.read()
                
                # Create backup
                with open(f"{file_path}.bak", 'w') as f:
                    f.write(content)
                
                # Replace ticker symbols
                modified = False
                for old_ticker, new_ticker in TICKER_CHANGES.items():
                    if old_ticker in content:
                        content = content.replace(f'"{old_ticker}"', f'"{new_ticker}"')
                        content = content.replace(f"'{old_ticker}'", f"'{new_ticker}'")
                        modified = True
                
                if modified:
                    # Write updated content
                    with open(file_path, 'w') as f:
                        f.write(content)
                    logging.info(f"Updated {file_path}")
            
            except Exception as e:
                logging.error(f"Error updating {file_path}: {e}")

def clear_data_caches():
    """Clear relevant data caches to force refresh with new tickers"""
    cache_dirs = [
        "data/cache",
        ".cache",
        "tmp"
    ]
    
    for cache_dir in cache_dirs:
        if os.path.exists(cache_dir):
            try:
                # Look for cache files that might contain ticker data
                cache_files = [
                    f for f in os.listdir(cache_dir) 
                    if f.endswith('.json') or f.endswith('.pkl') or f.endswith('.parquet')
                ]
                
                for cache_file in cache_files:
                    if any(old_ticker in cache_file for old_ticker in TICKER_CHANGES.keys()):
                        # Either rename or remove the cache file
                        file_path = os.path.join(cache_dir, cache_file)
                        os.rename(file_path, f"{file_path}.old")
                        logging.info(f"Renamed cache file: {file_path} to {file_path}.old")
            
            except Exception as e:
                logging.error(f"Error clearing cache in {cache_dir}: {e}")

def main():
    """Main function to update ticker symbols"""
    print(f"Updating FinTech ticker symbols in system configuration:")
    for old_ticker, new_ticker in TICKER_CHANGES.items():
        print(f"  - {old_ticker} -> {new_ticker}")
    
    success = True
    
    # 1. Update configuration files
    for file_path in CONFIG_FILES:
        if os.path.exists(file_path):
            if not update_ticker_definitions(file_path):
                print(f"Failed to update {file_path}")
                success = False
        else:
            logging.warning(f"File not found: {file_path} - skipping")
    
    # 2. Update ticker map
    update_ticker_map()
    
    # 3. Update API mappers
    update_api_mappers()
    
    # 4. Clear data caches
    clear_data_caches()
    
    if success:
        print("\nSuccessfully updated Fintech ticker symbol references!")
        print("\nNext steps:")
        print("1. Restart the Background Data Collection workflow")
        print("2. Restart the Economic Dashboard Server workflow")
        print("\nThis will ensure that the system uses the new ticker symbols for all future data collection.")
    else:
        print("\nSome updates failed. Please check the logs for details.")
    
    return success

if __name__ == "__main__":
    main()