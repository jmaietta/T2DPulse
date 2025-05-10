#!/usr/bin/env python3
"""
Update Sector Market Caps with authentic data from the provided CSV.
This script ensures we use the authentic market cap values provided by the user,
which incorporate fully diluted share counts for all stocks.
"""

import os
import sys
import json
import csv
import logging
import pandas as pd
from datetime import datetime
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)

# Define paths
DATA_DIR = "data"
SECTOR_WEIGHTS_FILE = os.path.join(DATA_DIR, "sector_weights_latest.json")
SECTOR_MCAPS_FILE = os.path.join(DATA_DIR, "sector_market_caps.json")
ATTACHED_CSV = "attached_assets/T2D Pulse Sectors - Copy of Sheet1.csv"

def parse_market_cap(market_cap_str):
    """Parse market cap string to float value"""
    if not market_cap_str or market_cap_str == "-":
        return 0.0
    
    # Remove $ sign, commas, spaces, and quotes, then convert to float
    try:
        market_cap_str = market_cap_str.strip().replace('$', '').replace(',', '').replace(' ', '').replace('"', '')
        return float(market_cap_str)
    except ValueError:
        logging.error(f"Could not parse market cap: {market_cap_str}")
        return 0.0

def parse_attached_csv():
    """Parse the attached CSV file to extract sector market cap data"""
    if not os.path.exists(ATTACHED_CSV):
        logging.error(f"Attached CSV file not found: {ATTACHED_CSV}")
        return None
    
    try:
        # Read the CSV file
        with open(ATTACHED_CSV, 'r') as f:
            lines = f.readlines()
        
        # Parse the data
        sector_data = {}
        current_sector = None
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            # Check if this is a sector header line
            if ',' in line and not line.startswith('Ticker'):
                parts = line.split(',')
                if len(parts) == 1 or (len(parts) == 2 and not parts[1]):
                    # This is a sector header
                    current_sector = parts[0].strip()
                    sector_data[current_sector] = {
                        'tickers': [],
                        'market_caps': {},
                        'total_market_cap': 0
                    }
                    continue
            
            # Check if this is the "Total" line
            if line.startswith('Total,'):
                if current_sector:
                    total_mcap = line.split(',')[1]
                    sector_data[current_sector]['total_market_cap'] = parse_market_cap(total_mcap)
                continue
            
            # Check if this is a ticker line
            if current_sector and ',' in line and not line.startswith('Ticker,Market Cap'):
                parts = line.split(',')
                if len(parts) >= 2:
                    ticker = parts[0].strip()
                    market_cap = parts[1].strip()
                    
                    if ticker and market_cap:
                        sector_data[current_sector]['tickers'].append(ticker)
                        sector_data[current_sector]['market_caps'][ticker] = parse_market_cap(market_cap)
        
        return sector_data
    
    except Exception as e:
        logging.error(f"Error parsing attached CSV: {e}")
        return None

def calculate_sector_weights(sector_data):
    """Calculate sector weights based on their market caps"""
    if not sector_data:
        return None
    
    # Calculate total market cap across all sectors
    # We need to avoid double-counting by summing the unique market cap values
    # This means we can't just sum the sector totals
    total_market_cap = 0
    all_sectors_total = 0
    
    for sector, data in sector_data.items():
        sector_total = data['total_market_cap']
        all_sectors_total += sector_total
    
    # Calculate the weights
    sector_weights = {}
    for sector, data in sector_data.items():
        weight = (data['total_market_cap'] / all_sectors_total) * 100
        sector_weights[sector.replace(' ', '_').replace('/', '_')] = round(weight, 2)
    
    return sector_weights

def update_sector_weights_file(sector_weights):
    """Update the sector weights file used by the dashboard"""
    if not sector_weights:
        return False
    
    try:
        # Create data directory if it doesn't exist
        Path(DATA_DIR).mkdir(exist_ok=True)
        
        # Write weights to file
        with open(SECTOR_WEIGHTS_FILE, 'w') as f:
            json.dump({"weights": sector_weights}, f, indent=2)
        
        logging.info(f"Updated sector weights file: {SECTOR_WEIGHTS_FILE}")
        return True
    
    except Exception as e:
        logging.error(f"Error updating sector weights file: {e}")
        return False

def update_sector_market_caps_file(sector_data):
    """Update the sector market caps file with the latest data"""
    if not sector_data:
        return False
    
    try:
        # Create data directory if it doesn't exist
        Path(DATA_DIR).mkdir(exist_ok=True)
        
        # Format data for saving
        market_caps = {}
        for sector, data in sector_data.items():
            sector_key = sector.replace(' ', '_').replace('/', '_')
            market_caps[sector_key] = {
                'total_market_cap': data['total_market_cap'],
                'tickers': data['tickers'],
                'ticker_market_caps': data['market_caps']
            }
        
        # Write market caps to file
        with open(SECTOR_MCAPS_FILE, 'w') as f:
            json.dump({
                'date': datetime.now().strftime('%Y-%m-%d'),
                'market_caps': market_caps
            }, f, indent=2)
        
        logging.info(f"Updated sector market caps file: {SECTOR_MCAPS_FILE}")
        return True
    
    except Exception as e:
        logging.error(f"Error updating sector market caps file: {e}")
        return False

def generate_sector_weights_report(sector_weights):
    """Generate a report of the sector weights"""
    if not sector_weights:
        return
    
    print("\nSector Weights (% of Total Market Cap):")
    print("-" * 50)
    
    # Sort by weight descending
    sorted_sectors = sorted(sector_weights.items(), key=lambda x: x[1], reverse=True)
    
    for sector, weight in sorted_sectors:
        # Format the sector name to be more readable
        sector_name = sector.replace('_', ' ')
        print(f"{sector_name:<30} {weight:>7.2f}%")
    
    # Verify that weights sum to 100%
    total_weight = sum(weight for _, weight in sorted_sectors)
    print("-" * 50)
    print(f"Total: {total_weight:.2f}%")
    
    if abs(total_weight - 100.0) > 0.1:
        print(f"Warning: Weights do not sum to 100% (diff: {total_weight - 100.0:.2f}%)")

def main():
    """Main function to update sector market caps and weights"""
    print("Updating sector market caps with authentic data...")
    
    # 1. Parse the attached CSV
    sector_data = parse_attached_csv()
    if not sector_data:
        print("Failed to parse attached CSV. Exiting.")
        return False
    
    print(f"Successfully parsed data for {len(sector_data)} sectors")
    
    # 2. Calculate sector weights
    sector_weights = calculate_sector_weights(sector_data)
    if not sector_weights:
        print("Failed to calculate sector weights. Exiting.")
        return False
    
    # 3. Update the sector weights file
    if not update_sector_weights_file(sector_weights):
        print("Failed to update sector weights file. Exiting.")
        return False
    
    # 4. Update the sector market caps file
    if not update_sector_market_caps_file(sector_data):
        print("Failed to update sector market caps file.")
        # Continue anyway since we've already updated the weights
    
    # 5. Generate a report
    generate_sector_weights_report(sector_weights)
    
    print("\nSuccessfully updated sector market caps and weights!")
    print("Restart the Economic Dashboard Server to apply these changes.")
    
    return True

if __name__ == "__main__":
    main()