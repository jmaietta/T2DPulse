#!/usr/bin/env python3
"""
Verify Market Cap Data

This script verifies the market cap data in the system by comparing the historical data
with the latest authentic data from Polygon API.
"""

import os
import sys
import pandas as pd
from datetime import datetime, timedelta
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("verify_market_cap_data.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

def load_historical_data():
    """Load historical market cap data from the historical_sector_market_caps.csv file"""
    try:
        if os.path.exists('historical_sector_market_caps.csv'):
            df = pd.read_csv('historical_sector_market_caps.csv')
            return df
        else:
            logging.error("Historical market cap data file not found")
            return None
    except Exception as e:
        logging.error(f"Error loading historical market cap data: {e}")
        return None

def load_latest_data():
    """Load the latest market cap data from the sector_market_caps.csv file"""
    try:
        if os.path.exists('sector_market_caps.csv'):
            df = pd.read_csv('sector_market_caps.csv')
            return df
        else:
            logging.error("Latest market cap data file not found")
            return None
    except Exception as e:
        logging.error(f"Error loading latest market cap data: {e}")
        return None

def load_corrected_data():
    """Load the corrected market cap data from the corrected_sector_market_caps.csv file"""
    try:
        if os.path.exists('corrected_sector_market_caps.csv'):
            df = pd.read_csv('corrected_sector_market_caps.csv')
            return df
        else:
            logging.error("Corrected market cap data file not found")
            return None
    except Exception as e:
        logging.error(f"Error loading corrected market cap data: {e}")
        return None

def verify_data_consistency():
    """Verify the consistency of market cap data across different files"""
    historical_df = load_historical_data()
    latest_df = load_latest_data()
    corrected_df = load_corrected_data()
    
    if historical_df is None or latest_df is None:
        return False
    
    # Get the latest date in the historical data
    historical_latest_date = historical_df['date'].max()
    sector_latest_date = latest_df['date'].max()
    
    logging.info(f"Latest date in historical data: {historical_latest_date}")
    logging.info(f"Latest date in sector data: {sector_latest_date}")
    
    # Check if the latest date in both files matches
    if historical_latest_date != sector_latest_date:
        logging.warning(f"Latest date mismatch: {historical_latest_date} vs {sector_latest_date}")
    
    # Compare market caps for the latest date
    historical_latest = historical_df[historical_df['date'] == historical_latest_date]
    sector_latest = latest_df[latest_df['date'] == sector_latest_date]
    
    # Create a comparison table
    comparison_data = []
    for _, row in sector_latest.iterrows():
        sector = row['sector']
        sector_market_cap = row['market_cap']
        
        # Find the same sector in historical data
        historical_row = historical_latest[historical_latest['sector'] == sector]
        if len(historical_row) > 0:
            historical_market_cap = historical_row.iloc[0]['market_cap']
        else:
            historical_market_cap = None
        
        # Find the same sector in corrected data
        if corrected_df is not None:
            corrected_row = corrected_df[corrected_df['Sector'] == sector]
            if len(corrected_row) > 0:
                corrected_market_cap = corrected_row.iloc[0]['Market Cap (Billions USD)'] * 1e9
            else:
                corrected_market_cap = None
        else:
            corrected_market_cap = None
        
        # Calculate differences
        if historical_market_cap and sector_market_cap:
            diff_pct = ((sector_market_cap - historical_market_cap) / historical_market_cap) * 100
        else:
            diff_pct = None
        
        comparison_data.append({
            'Sector': sector,
            'Latest Market Cap (B)': sector_market_cap / 1e9,
            'Historical Market Cap (B)': historical_market_cap / 1e9 if historical_market_cap else None,
            'Corrected Market Cap (B)': corrected_market_cap / 1e9 if corrected_market_cap else None,
            'Diff %': diff_pct
        })
    
    # Create a DataFrame from the comparison data
    comparison_df = pd.DataFrame(comparison_data)
    
    # Sort by difference percentage
    if 'Diff %' in comparison_df.columns:
        comparison_df = comparison_df.sort_values('Diff %', ascending=False)
    
    # Display the comparison table
    print("\nMarket Cap Comparison:")
    pd.set_option('display.float_format', '${:.2f}B'.format)
    print(comparison_df.to_string(index=False))
    
    # Save the comparison to a file
    comparison_df.to_csv('market_cap_comparison.csv', index=False)
    logging.info("Saved market cap comparison to market_cap_comparison.csv")
    
    return True

def check_date_coverage():
    """Check the date coverage in the historical data"""
    historical_df = load_historical_data()
    if historical_df is None:
        return False
    
    # Get unique dates
    dates = historical_df['date'].unique()
    dates.sort()
    
    # Calculate expected date range (30 days)
    end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = end_date - timedelta(days=30)
    
    # Format dates
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    
    logging.info(f"Expected date range: {start_date_str} to {end_date_str}")
    logging.info(f"Actual date range: {dates[0]} to {dates[-1]}")
    
    # Check if we have all business days
    expected_business_days = []
    current_date = start_date
    while current_date <= end_date:
        # Monday = 0, Sunday = 6
        if current_date.weekday() < 5:  # Weekday
            expected_business_days.append(current_date.strftime('%Y-%m-%d'))
        current_date += timedelta(days=1)
    
    logging.info(f"Expected business days: {len(expected_business_days)}")
    logging.info(f"Actual days in data: {len(dates)}")
    
    # Find missing business days
    missing_days = set(expected_business_days) - set(dates)
    if missing_days:
        logging.warning(f"Missing {len(missing_days)} business days: {sorted(missing_days)}")
    else:
        logging.info("All expected business days are present in the data")
    
    return True

def main():
    """Main function to verify market cap data"""
    logging.info("Starting market cap data verification...")
    
    verify_data_consistency()
    check_date_coverage()
    
    logging.info("Market cap data verification completed!")
    return True

if __name__ == "__main__":
    success = main()
    if not success:
        logging.error("Market cap data verification failed")