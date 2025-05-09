#!/usr/bin/env python3
# analyze_coverage_gaps.py
# -----------------------------------------------------------
# Script to identify and prioritize ticker coverage gaps

import pandas as pd
import os
import sys
from datetime import datetime, timedelta
import pytz
from check_coverage import get_ticker_coverage

def analyze_coverage_gaps():
    """Analyze coverage gaps and provide targeted recommendations"""
    # Get current coverage
    results = get_ticker_coverage()
    if not results:
        print("Error: Could not get current coverage data")
        return
    
    # Load price and market cap data
    price_df = pd.read_csv('data/historical_ticker_prices.csv', index_col=0)
    mcap_df = pd.read_csv('data/historical_ticker_marketcap.csv', index_col=0)
    
    # Get latest date
    latest_date = price_df.index[-1]
    
    # Print header
    print("=" * 80)
    print(f"T2D PULSE COVERAGE GAP ANALYSIS - {latest_date}")
    print("=" * 80)
    
    # Print current progress
    total = results["total_tickers"]
    covered = len(results["covered_tickers"])
    pct = covered / total * 100
    missing = len(results["missing_tickers"])
    print(f"\nCurrent Progress: {covered}/{total} tickers ({pct:.1f}%)")
    print(f"Missing Tickers: {missing}")
    
    # Print estimated completion with current rate
    # Assume we can complete 6 tickers per hour during market hours
    est_hours = missing / 6
    market_hours_per_day = 7  # 9am to 4pm ET
    est_days = est_hours / market_hours_per_day
    
    eastern = pytz.timezone('US/Eastern')
    now = datetime.now(eastern)
    est_completion = now + timedelta(days=est_days)
    
    print(f"\nEstimated completion at current rate:")
    print(f"- Hours needed: {est_hours:.1f} market hours")
    print(f"- Estimated completion: {est_completion.strftime('%Y-%m-%d %H:%M %Z')}")
    
    # Print sector gap analysis
    print("\n" + "=" * 80)
    print("SECTOR GAP ANALYSIS")
    print("=" * 80)
    
    # Sort sectors by coverage (lowest first)
    sector_gaps = []
    for sector, data in results["sectors"].items():
        sector_gaps.append({
            "sector": sector,
            "covered": data["covered"],
            "total": data["total"],
            "pct": data["pct"],
            "missing": len(data["missing"]),
            "missing_tickers": data["missing"]
        })
    
    # Sort by percentage covered (ascending)
    sector_gaps.sort(key=lambda x: x["pct"])
    
    for sector_data in sector_gaps:
        sector = sector_data["sector"]
        covered = sector_data["covered"]
        total = sector_data["total"]
        pct = sector_data["pct"]
        missing = sector_data["missing"]
        missing_tickers = sector_data["missing_tickers"]
        
        print(f"\n{sector}: {covered}/{total} ({pct:.1f}%)")
        if missing > 0:
            print(f"  Missing tickers ({missing}): {', '.join(missing_tickers)}")
            print(f"  Est. completion time: {missing / 6:.1f} hours ({missing / 2:.1f} collection cycles)")
    
    # Print completion plan
    print("\n" + "=" * 80)
    print("SUGGESTED ACTION PLAN")
    print("=" * 80)
    
    # Focus on completing sectors that are close to completion first
    close_to_completion = [s for s in sector_gaps if 0 < s["missing"] <= 3]
    close_to_completion.sort(key=lambda x: x["missing"])
    
    if close_to_completion:
        print("\nPriority 1: Complete these almost-finished sectors first:")
        for sector in close_to_completion:
            print(f"- {sector['sector']}: Need {sector['missing']} more tickers: {', '.join(sector['missing_tickers'])}")
    
    # Next, tackle sectors with moderate gaps
    moderate_gaps = [s for s in sector_gaps if 3 < s["missing"] <= 5]
    moderate_gaps.sort(key=lambda x: x["pct"], reverse=True)  # Higher percentage first
    
    if moderate_gaps:
        print("\nPriority 2: Improve these sectors with moderate gaps:")
        for sector in moderate_gaps:
            print(f"- {sector['sector']}: Need {sector['missing']} more tickers: {', '.join(sector['missing_tickers'])}")
    
    # Finally, address sectors with large gaps
    large_gaps = [s for s in sector_gaps if s["missing"] > 5]
    large_gaps.sort(key=lambda x: x["pct"], reverse=True)  # Higher percentage first
    
    if large_gaps:
        print("\nPriority 3: Address these sectors with larger gaps:")
        for sector in large_gaps:
            print(f"- {sector['sector']}: Need {sector['missing']} more tickers: {', '.join(sector['missing_tickers'])}")
    
    # Print completed sectors
    completed = [s for s in sector_gaps if s["missing"] == 0]
    if completed:
        print("\nCompleted Sectors:")
        for sector in completed:
            print(f"- {sector['sector']}: 100% complete with {sector['total']} tickers")

if __name__ == "__main__":
    analyze_coverage_gaps()