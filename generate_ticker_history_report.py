#!/usr/bin/env python3
# generate_ticker_history_report.py
# -----------------------------------------------------------
# Script to generate a comprehensive CSV report of historical ticker data

import pandas as pd
import numpy as np
import os
import sys
from datetime import datetime, timedelta

def load_historical_data():
    """Load historical ticker price and market cap data"""
    try:
        # Load data files
        price_df = pd.read_csv('data/historical_ticker_prices.csv', index_col=0)
        mcap_df = pd.read_csv('data/historical_ticker_marketcap.csv', index_col=0)
        
        return price_df, mcap_df
    except Exception as e:
        print(f"Error loading historical data: {e}")
        return None, None

def load_sector_mapping():
    """Load sector mapping from config"""
    try:
        import config
        
        # Create mapping of tickers to sectors
        ticker_to_sector = {}
        for sector, tickers in config.SECTORS.items():
            for ticker in tickers:
                ticker_to_sector[ticker] = sector
        
        return ticker_to_sector
    except ImportError:
        print("Warning: Could not import config.py, sector mapping will not be available")
        return {}

def calculate_data_metrics(price_df, mcap_df):
    """Calculate data completeness metrics for each ticker"""
    metrics = {}
    
    # Get all tickers (columns in either dataframe)
    all_tickers = sorted(set(list(price_df.columns) + list(mcap_df.columns)))
    
    for ticker in all_tickers:
        # Calculate price data stats
        price_available = ticker in price_df.columns
        price_count = price_df[ticker].count() if price_available else 0
        price_pct = (price_count / len(price_df)) * 100 if price_available else 0
        
        # Calculate market cap data stats
        mcap_available = ticker in mcap_df.columns
        mcap_count = mcap_df[ticker].count() if mcap_available else 0
        mcap_pct = (mcap_count / len(mcap_df)) * 100 if mcap_available else 0
        
        # Get most recent values
        latest_date = price_df.index[-1]
        latest_price = price_df.loc[latest_date, ticker] if price_available else np.nan
        latest_mcap = mcap_df.loc[latest_date, ticker] if mcap_available else np.nan
        
        # Store metrics
        metrics[ticker] = {
            'price_available': price_available,
            'price_count': price_count,
            'price_pct': price_pct,
            'mcap_available': mcap_available,
            'mcap_count': mcap_count,
            'mcap_pct': mcap_pct,
            'latest_price': latest_price,
            'latest_mcap': latest_mcap
        }
    
    return metrics

def generate_historical_report(output_file='ticker_historical_report.csv'):
    """Generate comprehensive historical data report for all tickers"""
    print(f"Generating historical ticker data report to {output_file}")
    
    # Load data
    price_df, mcap_df = load_historical_data()
    if price_df is None or mcap_df is None:
        print("Error: Failed to load historical data")
        return False
    
    # Load sector mapping
    ticker_to_sector = load_sector_mapping()
    
    # Calculate data metrics
    metrics = calculate_data_metrics(price_df, mcap_df)
    
    # Create report dataframe
    report_data = []
    
    for ticker, metric in metrics.items():
        sector = ticker_to_sector.get(ticker, "Unknown")
        
        # Create row for report
        row = {
            'Ticker': ticker,
            'Sector': sector,
            'Latest Price': metric['latest_price'],
            'Latest Market Cap': metric['latest_mcap'],
            'Market Cap (B)': metric['latest_mcap'] / 1000000000 if not pd.isna(metric['latest_mcap']) else np.nan,
            'Price Data Points': metric['price_count'],
            'Price Coverage %': round(metric['price_pct'], 2),
            'Market Cap Data Points': metric['mcap_count'],
            'Market Cap Coverage %': round(metric['mcap_pct'], 2),
            'Data Status': 'Complete' if (not pd.isna(metric['latest_price']) and not pd.isna(metric['latest_mcap'])) else 'Incomplete'
        }
        
        report_data.append(row)
    
    # Convert to dataframe
    report_df = pd.DataFrame(report_data)
    
    # Sort by sector and ticker
    report_df = report_df.sort_values(['Sector', 'Ticker'])
    
    # Create report summary
    summary_data = [
        {'Summary': "T2D Pulse Ticker Historical Data Report"},
        {'Summary': f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"},
        {'Summary': f"Total Tickers: {len(metrics)}"},
        {'Summary': f"Complete Data Tickers: {len(report_df[report_df['Data Status'] == 'Complete'])}"},
        {'Summary': f"Data Range: {price_df.index[0]} to {price_df.index[-1]}"},
        {'Summary': f"Total Days: {len(price_df)}"},
        {'Summary': ""}
    ]
    
    # Convert to dataframe
    summary_df = pd.DataFrame(summary_data)
    
    # Save to CSV
    summary_df.to_csv(output_file, index=False)
    report_df.to_csv(output_file, index=False, mode='a')
    
    print(f"Report successfully generated: {output_file}")
    
    # Also export recent data window (last 30 days)
    export_recent_price_data(price_df, report_df, 'recent_price_data.csv')
    export_recent_mcap_data(mcap_df, report_df, 'recent_marketcap_data.csv')
    
    return True

def export_recent_price_data(price_df, report_df, output_file='recent_price_data.csv', days=30):
    """Export the most recent days of price data"""
    try:
        # Get complete tickers only
        complete_tickers = report_df[report_df['Data Status'] == 'Complete']['Ticker'].tolist()
        
        # Get most recent data window
        recent_df = price_df.iloc[-days:].copy()
        
        # Select only complete tickers
        recent_df = recent_df[complete_tickers]
        
        # Add date as a column
        recent_df.reset_index(inplace=True)
        recent_df.rename(columns={'index': 'Date'}, inplace=True)
        
        # Save to CSV
        recent_df.to_csv(output_file, index=False)
        print(f"Recent price data (last {days} days) exported to {output_file}")
        
        return True
    except Exception as e:
        print(f"Error exporting recent price data: {e}")
        return False

def export_recent_mcap_data(mcap_df, report_df, output_file='recent_marketcap_data.csv', days=30):
    """Export the most recent days of market cap data"""
    try:
        # Get complete tickers only
        complete_tickers = report_df[report_df['Data Status'] == 'Complete']['Ticker'].tolist()
        
        # Get most recent data window
        recent_df = mcap_df.iloc[-days:].copy()
        
        # Select only complete tickers
        recent_df = recent_df[complete_tickers]
        
        # Convert to billions for readability
        for col in recent_df.columns:
            recent_df[col] = recent_df[col] / 1000000000
        
        # Add date as a column
        recent_df.reset_index(inplace=True)
        recent_df.rename(columns={'index': 'Date'}, inplace=True)
        
        # Save to CSV
        recent_df.to_csv(output_file, index=False)
        print(f"Recent market cap data (last {days} days) in billions exported to {output_file}")
        
        return True
    except Exception as e:
        print(f"Error exporting recent market cap data: {e}")
        return False

if __name__ == "__main__":
    # Get output filename from command line args if provided
    output_file = sys.argv[1] if len(sys.argv) > 1 else 'ticker_historical_report.csv'
    
    # Generate historical report
    generate_historical_report(output_file)