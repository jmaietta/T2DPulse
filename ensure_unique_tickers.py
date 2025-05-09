#!/usr/bin/env python3
# ensure_unique_tickers.py
# -----------------------------------------------------------
# Utility to ensure we're only tracking the official 94 unique tickers

import os
import pandas as pd
import numpy as np
from config import SECTORS

def get_official_tickers():
    """Get the complete list of official tickers"""
    tickers = []
    try:
        with open('official_tickers.csv', 'r') as f:
            for line in f:
                ticker = line.strip()
                if ticker:
                    tickers.append(ticker)
        return sorted(tickers)
    except FileNotFoundError:
        print("Error: official_tickers.csv not found")
        return []

def extract_all_sector_tickers():
    """Extract all tickers from all sectors with potential duplicates"""
    all_tickers = []
    duplicate_tickers = []
    
    for sector, tickers in SECTORS.items():
        for ticker in tickers:
            if ticker in all_tickers:
                duplicate_tickers.append((ticker, sector))
            all_tickers.append(ticker)
    
    return {
        'all_tickers': all_tickers,
        'unique_tickers': sorted(set(all_tickers)),
        'duplicate_tickers': duplicate_tickers,
        'total_instances': len(all_tickers),
        'unique_count': len(set(all_tickers))
    }

def compare_with_official():
    """Compare sector tickers with official tickers list"""
    official = get_official_tickers()
    extracted = extract_all_sector_tickers()
    
    # Find tickers in official but not in sectors
    missing_from_sectors = [t for t in official if t not in extracted['unique_tickers']]
    
    # Find tickers in sectors but not in official
    extra_in_sectors = [t for t in extracted['unique_tickers'] if t not in official]
    
    return {
        'official_count': len(official),
        'unique_in_sectors': extracted['unique_count'],
        'total_sector_instances': extracted['total_instances'],
        'missing_from_sectors': missing_from_sectors,
        'extra_in_sectors': extra_in_sectors,
        'duplicates': extracted['duplicate_tickers']
    }

def check_historical_data_files():
    """Check if historical data files contain extra tickers"""
    official_tickers = get_official_tickers()
    
    try:
        # Load historical data files
        price_df = pd.read_csv('data/historical_ticker_prices.csv', index_col=0)
        mcap_df = pd.read_csv('data/historical_ticker_marketcap.csv', index_col=0)
        
        # Check for extra tickers
        price_extra = [t for t in price_df.columns if t not in official_tickers]
        mcap_extra = [t for t in mcap_df.columns if t not in official_tickers]
        
        # Check for missing tickers
        price_missing = [t for t in official_tickers if t not in price_df.columns]
        mcap_missing = [t for t in official_tickers if t not in mcap_df.columns]
        
        return {
            'price_columns': len(price_df.columns),
            'mcap_columns': len(mcap_df.columns),
            'price_extra': price_extra,
            'mcap_extra': mcap_extra,
            'price_missing': price_missing,
            'mcap_missing': mcap_missing
        }
    except Exception as e:
        print(f"Error checking historical data files: {e}")
        return None

def get_missing_data():
    """Get list of official tickers with missing data"""
    official_tickers = get_official_tickers()
    
    try:
        # Load data files
        price_df = pd.read_csv('data/historical_ticker_prices.csv', index_col=0)
        mcap_df = pd.read_csv('data/historical_ticker_marketcap.csv', index_col=0)
        
        # Get latest date
        latest_date = price_df.index[-1]
        
        missing_tickers = []
        complete_tickers = []
        
        for ticker in official_tickers:
            price_missing = ticker not in price_df.columns or pd.isna(price_df.loc[latest_date, ticker])
            mcap_missing = ticker not in mcap_df.columns or pd.isna(mcap_df.loc[latest_date, ticker])
            
            if price_missing or mcap_missing:
                missing_tickers.append(ticker)
            else:
                complete_tickers.append(ticker)
        
        coverage = len(complete_tickers) / len(official_tickers) * 100
        
        return {
            'date': latest_date,
            'total_official': len(official_tickers),
            'missing_count': len(missing_tickers),
            'complete_count': len(complete_tickers),
            'coverage_pct': coverage,
            'missing_tickers': missing_tickers,
            'complete_tickers': complete_tickers
        }
    except Exception as e:
        print(f"Error getting missing data: {e}")
        return None

def print_report():
    """Print a comprehensive report on ticker status"""
    print("=" * 80)
    print("T2D PULSE OFFICIAL TICKER VALIDATION REPORT")
    print("=" * 80)
    
    # Get official tickers
    official = get_official_tickers()
    print(f"Official ticker count: {len(official)}")
    
    # Compare with sector config
    comparison = compare_with_official()
    print(f"\nSector Configuration:")
    print(f"  Total tickers in sectors: {comparison['unique_in_sectors']} unique tickers")
    print(f"  Total sector instances: {comparison['total_sector_instances']} (including duplicates)")
    
    if comparison['missing_from_sectors']:
        print(f"\n  WARNING: {len(comparison['missing_from_sectors'])} tickers in official list but not in any sector:")
        print(f"    {', '.join(comparison['missing_from_sectors'])}")
    
    if comparison['extra_in_sectors']:
        print(f"\n  WARNING: {len(comparison['extra_in_sectors'])} tickers in sectors but not in official list:")
        print(f"    {', '.join(comparison['extra_in_sectors'])}")
    
    if comparison['duplicates']:
        print(f"\n  INFO: {len(comparison['duplicates'])} duplicate ticker instances across sectors:")
        for ticker, sector in comparison['duplicates']:
            print(f"    {ticker} also appears in {sector}")
    
    # Check historical data files
    data_check = check_historical_data_files()
    if data_check:
        print("\nHistorical Data Files:")
        print(f"  Price data columns: {data_check['price_columns']}")
        print(f"  Market cap data columns: {data_check['mcap_columns']}")
        
        if data_check['price_extra']:
            print(f"\n  WARNING: {len(data_check['price_extra'])} extra tickers in price data not in official list:")
            print(f"    {', '.join(data_check['price_extra'])}")
        
        if data_check['mcap_extra']:
            print(f"\n  WARNING: {len(data_check['mcap_extra'])} extra tickers in market cap data not in official list:")
            print(f"    {', '.join(data_check['mcap_extra'])}")
        
        if data_check['price_missing']:
            print(f"\n  INFO: {len(data_check['price_missing'])} official tickers missing from price data:")
            print(f"    {', '.join(data_check['price_missing'])}")
        
        if data_check['mcap_missing']:
            print(f"\n  INFO: {len(data_check['mcap_missing'])} official tickers missing from market cap data:")
            print(f"    {', '.join(data_check['mcap_missing'])}")
    
    # Get missing data
    missing = get_missing_data()
    if missing:
        print("\nMissing Data Report:")
        print(f"  Date: {missing['date']}")
        print(f"  Coverage: {missing['complete_count']}/{missing['total_official']} tickers ({missing['coverage_pct']:.1f}%)")
        print(f"  Missing tickers: {missing['missing_count']}")
        
        if missing['missing_tickers']:
            print(f"\n  Missing tickers list:")
            # Format as a grid (5 tickers per row)
            for i in range(0, len(missing['missing_tickers']), 5):
                row = missing['missing_tickers'][i:i+5]
                print(f"    {' '.join(f'{t:<6}' for t in row)}")
    
    print("=" * 80)

if __name__ == "__main__":
    print_report()