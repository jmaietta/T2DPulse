#!/usr/bin/env python3
# run_daily.py
# -----------------------------------------------------------
# Daily script to collect sector market capitalization values

import os
import time
from config import SECTORS, FINNHUB_API_KEY
from ema_calculator import compute_sector_value
from storage import append_sector_values

def main():
    """
    Main function to collect sector values and save them
    """
    if not FINNHUB_API_KEY or FINNHUB_API_KEY == "your_finnhub_api_key_here":
        print("Error: Please set a valid Finnhub API key in config.py")
        return False
        
    print("Starting daily sector value collection...")
    results = {}
    
    # Process each sector
    for sector, tickers in SECTORS.items():
        print(f"Processing {sector} with {len(tickers)} tickers...")
        value = compute_sector_value(tickers)
        results[sector] = value
        print(f"{sector}: {value}B USD")
        time.sleep(1)  # Brief pause to avoid API rate limits
        
    # Save results to CSV
    success = append_sector_values(results)
    
    if success:
        print("Successfully collected and saved sector values")
    else:
        print("Failed to save sector values")
        
    return success

if __name__ == "__main__":
    main()