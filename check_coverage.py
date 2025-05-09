#!/usr/bin/env python3
# check_coverage.py
# -----------------------------------------------------------
# Script to check ticker coverage against official ticker list

import pandas as pd
import os
import sys
import datetime
import json

HISTORY_FILE = 'data/coverage_history.json'

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
            sector_covered_tickers = []
            
            for ticker in sector_tickers:
                if (ticker in price_df.columns and not pd.isna(price_df.loc[latest_date, ticker]) and
                    ticker in marketcap_df.columns and not pd.isna(marketcap_df.loc[latest_date, ticker])):
                    sector_covered += 1
                    sector_covered_tickers.append(ticker)
                else:
                    sector_missing.append(ticker)
            
            results["sectors"][sector] = {
                "total": sector_total,
                "covered": sector_covered,
                "covered_tickers": sector_covered_tickers,
                "pct": sector_covered / sector_total * 100,
                "missing": sector_missing
            }
    except ImportError:
        print("Warning: config.py not found, skipping sector analysis.")
    
    return results

def save_coverage_history(results):
    """Save the coverage results to a history file"""
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Create a record to save
    record = {
        "timestamp": timestamp,
        "date": results["date"],
        "total": results["total_tickers"],
        "covered": len(results["covered_tickers"]),
        "pct": len(results["covered_tickers"]) / results["total_tickers"] * 100,
        "covered_tickers": results["covered_tickers"]
    }
    
    # Add sector data
    if "sectors" in results:
        record["sectors"] = {}
        for sector, data in results["sectors"].items():
            record["sectors"][sector] = {
                "covered": data["covered"],
                "total": data["total"],
                "pct": data["pct"]
            }
    
    # Load existing history or create new
    history = []
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE, 'r') as f:
                history = json.load(f)
        except json.JSONDecodeError:
            print(f"Warning: Could not parse {HISTORY_FILE}, creating new history")
    
    # Add new record
    history.append(record)
    
    # Save back to file
    os.makedirs(os.path.dirname(HISTORY_FILE), exist_ok=True)
    with open(HISTORY_FILE, 'w') as f:
        json.dump(history, f, indent=2)
    
    return record

def get_newly_added_tickers(results):
    """Identify tickers that were newly added since the last check"""
    if not os.path.exists(HISTORY_FILE):
        return [], 0  # No history yet
    
    try:
        with open(HISTORY_FILE, 'r') as f:
            history = json.load(f)
            
        if len(history) < 2:
            return [], 0  # Not enough history to compare
            
        # Get the previous record
        prev_record = history[-2]
        
        # Find tickers that are in current covered list but weren't in previous
        current_covered = set(results["covered_tickers"])
        prev_covered = set(prev_record["covered_tickers"])
        
        newly_added = list(current_covered - prev_covered)
        newly_added.sort()  # Sort for consistent display
        
        # Calculate the coverage increase
        coverage_increase = len(results["covered_tickers"]) - prev_record["covered"]
        
        return newly_added, coverage_increase
    except Exception as e:
        print(f"Warning: Error analyzing history: {e}")
        return [], 0

def print_coverage_report(results):
    """Print a formatted coverage report"""
    if not results:
        return
    
    # Save to history and get newly added tickers
    save_coverage_history(results)
    newly_added, coverage_increase = get_newly_added_tickers(results)
    
    # Print header
    print("=" * 80)
    print(f"T2D PULSE TICKER COVERAGE REPORT - {results['date']}")
    print("=" * 80)
    
    # Print overall summary
    total = results["total_tickers"]
    covered = len(results["covered_tickers"])
    pct = covered / total * 100
    print(f"\nOVERALL COVERAGE: {covered}/{total} tickers ({pct:.1f}%)")
    
    # Show coverage change
    if coverage_increase > 0:
        print(f"COVERAGE CHANGE: +{coverage_increase} tickers since last check")
        print(f"NEWLY ADDED: {', '.join(newly_added)}")
    elif coverage_increase < 0:
        print(f"COVERAGE CHANGE: {coverage_increase} tickers since last check (DATA LOSS!)")
    
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
    
    # Print recently added tickers section if there are any
    if newly_added:
        print("\n" + "=" * 80)
        print("RECENTLY ADDED TICKERS")
        print("=" * 80)
        
        # Show in columns for better readability
        col_width = 8
        cols = 5
        for i in range(0, len(newly_added), cols):
            row = newly_added[i:i+cols]
            print("  ".join(item.ljust(col_width) for item in row))

def analyze_coverage_history():
    """Analyze coverage history to show progress over time"""
    if not os.path.exists(HISTORY_FILE):
        print("No coverage history available yet.")
        return
    
    try:
        with open(HISTORY_FILE, 'r') as f:
            history = json.load(f)
        
        if not history:
            print("Coverage history is empty.")
            return
        
        print("\n" + "=" * 80)
        print("COVERAGE HISTORY")
        print("=" * 80)
        
        # Calculate the total duration
        first_date = history[0]["timestamp"]
        last_date = history[-1]["timestamp"]
        print(f"Period: {first_date} to {last_date}")
        
        # Starting and ending coverage
        first_coverage = history[0]["pct"]
        last_coverage = history[-1]["pct"]
        total_improvement = last_coverage - first_coverage
        
        print(f"Starting coverage: {first_coverage:.1f}%")
        print(f"Current coverage: {last_coverage:.1f}%")
        print(f"Total improvement: {total_improvement:.1f}%")
        
        # Show most recent progress
        if len(history) > 1:
            previous = history[-2]
            current = history[-1]
            recent_improvement = current["pct"] - previous["pct"]
            print(f"Most recent improvement: {recent_improvement:.1f}%")
        
    except Exception as e:
        print(f"Error analyzing coverage history: {e}")

if __name__ == "__main__":
    results = get_ticker_coverage()
    if results:
        print_coverage_report(results)
        
        # If --history flag is provided, show coverage history analysis
        if len(sys.argv) > 1 and sys.argv[1] == "--history":
            analyze_coverage_history()
    else:
        sys.exit(1)