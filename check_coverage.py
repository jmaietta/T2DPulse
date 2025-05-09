#!/usr/bin/env python3
# check_coverage.py
# -----------------------------------------------------------
# Script to check ticker coverage against official ticker list

import pandas as pd
import os
import sys

def get_ticker_coverage():
    """Get comprehensive coverage stats for all official tickers"""
    # Read the official ticker list
    tickers = []
    try:
        with open('official_tickers.csv', 'r') as f:
            for line in f:
                ticker = line.strip()
                if ticker:
                    tickers.append(ticker)
    except FileNotFoundError:
        print("Error: official_tickers.csv not found. Please check the file exists.")
        return None

    # Load data files
    try:
        price_df = pd.read_csv('data/historical_ticker_prices.csv', index_col=0)
        marketcap_df = pd.read_csv('data/historical_ticker_marketcap.csv', index_col=0)
    except FileNotFoundError:
        print("Error: Historical data files not found.")
        return None
    
    # Get latest date
    latest_date = price_df.index[-1]
    
    # Prepare results
    results = {
        "date": latest_date,
        "total_tickers": len(tickers),
        "covered_tickers": [],
        "missing_tickers": [],
        "sectors": {}
    }
    
    # Count covered tickers
    for ticker in tickers:
        if (ticker in price_df.columns and not pd.isna(price_df.loc[latest_date, ticker]) and
            ticker in marketcap_df.columns and not pd.isna(marketcap_df.loc[latest_date, ticker])):
            results["covered_tickers"].append(ticker)
        else:
            results["missing_tickers"].append(ticker)
    
    # Import config for sector analysis if available
    try:
        import config
        for sector, sector_tickers in config.SECTORS.items():
            sector_total = len(sector_tickers)
            sector_covered = 0
            sector_missing = []
            
            for ticker in sector_tickers:
                if (ticker in price_df.columns and not pd.isna(price_df.loc[latest_date, ticker]) and
                    ticker in marketcap_df.columns and not pd.isna(marketcap_df.loc[latest_date, ticker])):
                    sector_covered += 1
                else:
                    sector_missing.append(ticker)
            
            results["sectors"][sector] = {
                "total": sector_total,
                "covered": sector_covered,
                "pct": sector_covered / sector_total * 100,
                "missing": sector_missing
            }
    except ImportError:
        print("Warning: config.py not found, skipping sector analysis.")
    
    return results

def print_coverage_report(results):
    """Print a formatted coverage report"""
    if not results:
        return
    
    # Print header
    print("=" * 80)
    print(f"T2D PULSE TICKER COVERAGE REPORT - {results['date']}")
    print("=" * 80)
    
    # Print overall summary
    total = results["total_tickers"]
    covered = len(results["covered_tickers"])
    pct = covered / total * 100
    print(f"\nOVERALL COVERAGE: {covered}/{total} tickers ({pct:.1f}%)")
    print(f"MISSING TICKERS: {len(results['missing_tickers'])}")
    
    # Print sector summary if available
    if results["sectors"]:
        print("\n" + "=" * 80)
        print("SECTOR COVERAGE")
        print("=" * 80)
        
        # Sort sectors by coverage percentage (lowest first)
        sorted_sectors = sorted(
            results["sectors"].items(),
            key=lambda x: x[1]["pct"]
        )
        
        for sector, data in sorted_sectors:
            missing_count = len(data["missing"])
            print(f"\n{sector}: {data['covered']}/{data['total']} ({data['pct']:.1f}%)")
            if missing_count > 0:
                print(f"  Missing ({missing_count}): {', '.join(data['missing'][:5])}")
                if len(data['missing']) > 5:
                    print(f"  ... and {len(data['missing']) - 5} more")
    
    # Print missing ticker details
    print("\n" + "=" * 80)
    print("MISSING TICKERS")
    print("=" * 80)
    
    # Show in columns for better readability
    missing = results["missing_tickers"]
    col_width = 8
    cols = 5
    for i in range(0, len(missing), cols):
        row = missing[i:i+cols]
        print("  ".join(item.ljust(col_width) for item in row))

if __name__ == "__main__":
    results = get_ticker_coverage()
    if results:
        print_coverage_report(results)
    else:
        sys.exit(1)