#!/usr/bin/env python3
"""
Validate Ticker to Sector Aggregation

This script validates that ticker-level market cap data is being correctly aggregated into
sector totals. It provides a detailed breakdown by sector to ensure data integrity.
"""

import os
import sys
import pandas as pd
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("validate_aggregation.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

def load_ticker_data():
    """Load the ticker-level market cap data"""
    try:
        if os.path.exists('T2D_Pulse_Full_Ticker_History.csv'):
            df = pd.read_csv('T2D_Pulse_Full_Ticker_History.csv')
            return df
        else:
            logging.error("Ticker history file not found")
            return None
    except Exception as e:
        logging.error(f"Error loading ticker data: {e}")
        return None

def load_sector_data():
    """Load the sector-level market cap data"""
    try:
        if os.path.exists('historical_sector_market_caps.csv'):
            df = pd.read_csv('historical_sector_market_caps.csv')
            return df
        else:
            logging.error("Sector history file not found")
            return None
    except Exception as e:
        logging.error(f"Error loading sector data: {e}")
        return None

def load_ticker_to_sector_mapping():
    """Load the mapping of tickers to sectors"""
    try:
        df = pd.read_csv('T2D_Pulse_93_tickers_coverage.csv', skiprows=7)  # Skip the header rows
        ticker_to_sector = dict(zip(df['Ticker'], df['Sector']))
        return ticker_to_sector
    except Exception as e:
        logging.error(f"Error loading ticker to sector mapping: {e}")
        return None

def validate_aggregation(ticker_df, sector_df, ticker_to_sector):
    """Validate that ticker data is correctly aggregated into sector data"""
    if ticker_df is None or sector_df is None or ticker_to_sector is None:
        return False
    
    # Add sector column to ticker data
    ticker_df['sector'] = ticker_df['ticker'].map(ticker_to_sector)
    
    # Get most recent date with complete data
    ticker_dates = ticker_df['date'].unique()
    sector_dates = sector_df['date'].unique()
    common_dates = sorted(list(set(ticker_dates) & set(sector_dates)))
    
    if not common_dates:
        logging.error("No common dates between ticker and sector data")
        return False
    
    latest_date = common_dates[-1]
    logging.info(f"Validating aggregation for date: {latest_date}")
    
    # Filter data for the latest date
    ticker_latest = ticker_df[ticker_df['date'] == latest_date]
    sector_latest = sector_df[sector_df['date'] == latest_date]
    
    # Calculate sector totals from ticker data
    ticker_sector_totals = ticker_latest.groupby('sector')['market_cap'].sum().reset_index()
    
    # Compare with sector data
    comparison_data = []
    for _, row in sector_latest.iterrows():
        sector = row['sector']
        sector_market_cap = row['market_cap']
        
        # Find the same sector in ticker totals
        ticker_row = ticker_sector_totals[ticker_sector_totals['sector'] == sector]
        if len(ticker_row) > 0:
            ticker_market_cap = ticker_row.iloc[0]['market_cap']
        else:
            ticker_market_cap = 0
        
        # Calculate difference
        diff = sector_market_cap - ticker_market_cap
        diff_pct = (diff / sector_market_cap) * 100 if sector_market_cap else 0
        
        comparison_data.append({
            'Sector': sector,
            'Sector Market Cap (B)': sector_market_cap / 1e9,
            'Ticker Total (B)': ticker_market_cap / 1e9,
            'Difference (B)': diff / 1e9,
            'Difference %': diff_pct
        })
    
    # Create a DataFrame from the comparison data
    comparison_df = pd.DataFrame(comparison_data)
    
    # Sort by absolute difference
    comparison_df['Abs Diff %'] = comparison_df['Difference %'].abs()
    comparison_df = comparison_df.sort_values('Abs Diff %', ascending=False)
    comparison_df = comparison_df.drop('Abs Diff %', axis=1)
    
    # Display the comparison table
    print("\nSector vs. Ticker Aggregation Comparison:")
    pd.set_option('display.float_format', '${:.2f}B'.format)
    print(comparison_df.to_string(index=False))
    
    # Save the comparison to a file
    comparison_df.to_csv('sector_aggregation_validation.csv', index=False)
    logging.info("Saved sector aggregation validation to sector_aggregation_validation.csv")
    
    # Check for significant discrepancies
    large_discrepancies = comparison_df[comparison_df['Difference %'].abs() > 1]  # More than 1% difference
    if len(large_discrepancies) > 0:
        logging.warning(f"Found {len(large_discrepancies)} sectors with discrepancies > 1%")
        
        # For sectors with large discrepancies, show ticker details
        for _, row in large_discrepancies.iterrows():
            sector = row['Sector']
            logging.warning(f"\nDetailed breakdown for {sector}:")
            
            sector_tickers = ticker_latest[ticker_latest['sector'] == sector]
            if len(sector_tickers) > 0:
                for _, t_row in sector_tickers.iterrows():
                    ticker = t_row['ticker']
                    market_cap = t_row['market_cap'] / 1e9
                    logging.warning(f"  {ticker}: ${market_cap:.2f}B")
            else:
                logging.warning("  No ticker data found for this sector")
    else:
        logging.info("All sectors are correctly aggregated (within 1% tolerance)")
    
    return True

def main():
    """Main function to validate ticker to sector aggregation"""
    # Load data
    ticker_df = load_ticker_data()
    sector_df = load_sector_data()
    ticker_to_sector = load_ticker_to_sector_mapping()
    
    # Validate aggregation
    success = validate_aggregation(ticker_df, sector_df, ticker_to_sector)
    
    return success

if __name__ == "__main__":
    logging.info("Starting ticker to sector aggregation validation...")
    success = main()
    if success:
        logging.info("Validation completed successfully!")
    else:
        logging.error("Validation failed")