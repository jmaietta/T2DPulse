#!/usr/bin/env python3
# test_improved_data_collector.py
# -----------------------------------------------------------
# Test the improved data collection approach with a subset of tickers

import os
import pandas as pd
from improved_finnhub_data_collector import (
    get_eastern_date, 
    update_historical_data, 
    process_sector_data
)
import config

def main():
    """Run a limited test of the improved data collector"""
    print("Testing improved data collector with a subset of sectors...")
    
    # Use a smaller set of sectors and tickers for testing
    test_sectors = {
        "Cloud Infrastructure": ["MSFT", "AMZN"],
        "AI Infrastructure": ["NVDA", "GOOGL"],
        "Dev Tools / Analytics": ["DDOG", "TEAM"]
    }
    
    # Save original sectors and restore after test
    original_sectors = config.SECTORS
    config.SECTORS = test_sectors
    
    all_tickers = set()
    for tickers in test_sectors.values():
        all_tickers.update(tickers)
    all_tickers = list(all_tickers)
    
    try:
        print(f"Fetching historical data for {len(all_tickers)} unique tickers")
        historical_price_data, historical_marketcap_data = update_historical_data(all_tickers)
        
        print("\nHistorical price data summary:")
        print(f"- Shape: {historical_price_data.shape}")
        print(f"- Columns: {', '.join(historical_price_data.columns)}")
        print(f"- Date range: {historical_price_data.index.min()} to {historical_price_data.index.max()}")
        
        print("\nHistorical market cap data summary:")
        print(f"- Shape: {historical_marketcap_data.shape}")
        print(f"- Columns: {', '.join(historical_marketcap_data.columns)}")
        print(f"- Date range: {historical_marketcap_data.index.min()} to {historical_marketcap_data.index.max()}")
        
        print("\nProcessing sector data with historical data...")
        sector_data = process_sector_data(historical_price_data, historical_marketcap_data)
        
        print("\nResults:")
        for sector, data in sector_data.items():
            print(f"  {sector}: Market Cap=${data['market_cap']:.2f}, Momentum={data['momentum']:.2f}%")
        
        # Check if we got any zeros
        zero_sectors = [s for s, d in sector_data.items() if d['market_cap'] <= 0]
        if zero_sectors:
            print(f"\n❌ WARNING: Found {len(zero_sectors)} sectors with zero market cap: {zero_sectors}")
        else:
            print("\n✓ All sectors have valid non-zero market caps!")
            
        # Confirm that we have data for Dev Tools / Analytics
        if "Dev Tools / Analytics" in sector_data:
            dev_tools_data = sector_data["Dev Tools / Analytics"]
            dev_tools_market_cap = dev_tools_data['market_cap']
            if dev_tools_market_cap > 0:
                print(f"✓ Dev Tools / Analytics has a valid market cap: ${dev_tools_market_cap:.2f}")
            else:
                print(f"❌ Dev Tools / Analytics still has an invalid market cap: ${dev_tools_market_cap:.2f}")
        
        print("\n✓ Test completed")
        
    finally:
        # Restore original sectors
        config.SECTORS = original_sectors

if __name__ == "__main__":
    main()