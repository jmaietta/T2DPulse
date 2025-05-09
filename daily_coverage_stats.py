#!/usr/bin/env python3
# daily_coverage_stats.py
# -----------------------------------------------------------
# Script to generate daily ticker collection statistics and progress report

import os
import sys
import pandas as pd
import json
import datetime
import pytz
from pathlib import Path

# Path to historical coverage data
HISTORY_FILE = 'data/coverage_history.json'

def get_daily_stats():
    """Get daily ticker collection statistics"""
    if not os.path.exists(HISTORY_FILE):
        print("Error: No coverage history found. Run check_coverage.py first.")
        return None
    
    try:
        # Load coverage history
        with open(HISTORY_FILE, 'r') as f:
            history = json.load(f)
        
        if not history:
            print("Error: Coverage history is empty.")
            return None
        
        # Convert timestamps to datetime objects
        for record in history:
            record['datetime'] = datetime.datetime.strptime(
                record['timestamp'], "%Y-%m-%d %H:%M:%S"
            )
            # Add date string for grouping
            record['date_str'] = record['datetime'].strftime("%Y-%m-%d")
        
        # Group by date to get daily stats
        dates = sorted(set(record['date_str'] for record in history))
        
        daily_stats = []
        for date_str in dates:
            # Get records for this date
            day_records = [r for r in history if r['date_str'] == date_str]
            
            # Get first and last record for the day
            first = min(day_records, key=lambda x: x['datetime'])
            last = max(day_records, key=lambda x: x['datetime'])
            
            # Calculate tickers completed this day
            tickers_start = first['covered']
            tickers_end = last['covered']
            tickers_added = tickers_end - tickers_start
            
            # If this is the only day, we don't know the starting point
            # so we'll just use the first record as a reference
            if len(dates) == 1 and len(day_records) == 1:
                tickers_added = 0
            
            # Calculate percentage improvement
            pct_start = first['pct']
            pct_end = last['pct']
            pct_improvement = pct_end - pct_start
            
            # Calculate new tickers list if we have multiple records
            new_tickers = []
            if len(day_records) > 1:
                first_tickers = set(first['covered_tickers'])
                last_tickers = set(last['covered_tickers'])
                new_tickers = list(last_tickers - first_tickers)
                new_tickers.sort()
            
            # Add stats for this day
            daily_stats.append({
                'date': date_str,
                'records': len(day_records),
                'tickers_start': tickers_start,
                'tickers_end': tickers_end,
                'tickers_added': tickers_added,
                'pct_start': pct_start,
                'pct_end': pct_end,
                'pct_improvement': pct_improvement,
                'new_tickers': new_tickers
            })
        
        return daily_stats
    
    except Exception as e:
        print(f"Error processing coverage history: {e}")
        return None

def generate_daily_report():
    """Generate a daily ticker collection report"""
    stats = get_daily_stats()
    if not stats:
        return
    
    # Print header
    print("=" * 80)
    print("T2D PULSE DAILY TICKER COLLECTION REPORT")
    print("=" * 80)
    
    # Determine if we have enough history for trend analysis
    if len(stats) >= 2:
        # We have multiple days of data
        latest = stats[-1]
        previous = stats[-2]
        
        print(f"\nDate: {latest['date']}")
        print(f"Checks performed today: {latest['records']}")
        
        # Report on today's progress
        if latest['tickers_added'] > 0:
            print(f"\nTickers added today: {latest['tickers_added']}")
            print(f"Coverage improvement: {latest['pct_improvement']:.1f}%")
            print(f"New tickers: {', '.join(latest['new_tickers'])}")
        else:
            print("\nNo new tickers added today.")
        
        # Compare with previous day
        prev_added = previous['tickers_added']
        if prev_added > 0:
            print(f"\nPrevious day added: {prev_added} tickers")
            print(f"Previous day improvement: {previous['pct_improvement']:.1f}%")
            
            # Compare rates
            if latest['tickers_added'] > prev_added:
                print(f"Today's collection rate improved by {latest['tickers_added'] - prev_added} tickers!")
            elif latest['tickers_added'] < prev_added:
                print(f"Today's collection rate decreased by {prev_added - latest['tickers_added']} tickers.")
    else:
        # We only have one day of data
        latest = stats[0]
        print(f"\nDate: {latest['date']}")
        print(f"Checks performed today: {latest['records']}")
        
        if latest['tickers_added'] > 0:
            print(f"\nTickers added today: {latest['tickers_added']}")
            print(f"Coverage improvement: {latest['pct_improvement']:.1f}%")
            print(f"New tickers: {', '.join(latest['new_tickers'])}")
        else:
            print("\nNo new tickers added in the recorded period.")
    
    # Print current progress
    print(f"\nCurrent coverage: {latest['tickers_end']}/{latest['tickers_end'] + (94 - latest['tickers_end'])} tickers ({latest['pct_end']:.1f}%)")
    
    # Generate progress chart
    if len(stats) > 1:
        print("\nCoverage Progress:")
        
        # Get values for the chart
        dates = [s['date'].split('-')[2] for s in stats]  # Just day of month
        values = [s['pct_end'] for s in stats]
        
        # Create simple ASCII chart
        max_val = max(values)
        min_val = min(values)
        range_val = max_val - min_val
        if range_val < 1:
            range_val = 1  # Avoid division by zero
        
        # Chart height
        height = 5
        
        # Generate chart rows
        for i in range(height, 0, -1):
            scaled_values = [(v - min_val) / range_val for v in values]
            row = " " * 4
            
            for val in scaled_values:
                if val >= i / height:
                    row += "█ "
                else:
                    row += "  "
            
            print(row)
        
        # Print x-axis
        print("    " + " ".join(dates))
        
        # Print axis labels
        print(f"    {min_val:.1f}%" + " " * (len(dates) * 2 - 10) + f"{max_val:.1f}%")

if __name__ == "__main__":
    generate_daily_report()