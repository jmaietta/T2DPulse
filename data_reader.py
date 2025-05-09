"""
Data reader module for T2D Pulse Dashboard

This module provides functions to read data from various formats,
with a preference for Parquet files when available, and falling back
to CSV files when necessary.
"""

import os
import logging
import pandas as pd
from datetime import datetime, timedelta
import pytz

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def read_data_file(filename, index_col=0, date_col=None):
    """
    Read data from a file, auto-detecting the format.
    
    Args:
        filename (str): Path to the data file
        index_col (int, optional): Column to use as index
        date_col (str, optional): Column containing dates to parse
        
    Returns:
        pd.DataFrame: Data from the file
    """
    _, ext = os.path.splitext(filename)
    
    try:
        if ext.lower() == '.parquet':
            df = pd.read_parquet(filename)
        elif ext.lower() == '.csv':
            if date_col:
                df = pd.read_csv(filename, index_col=index_col, parse_dates=[date_col])
            else:
                df = pd.read_csv(filename, index_col=index_col)
        else:
            logging.error(f"Unsupported file format: {ext}")
            return pd.DataFrame()
            
        logging.info(f"Successfully loaded {len(df)} rows from {filename}")
        return df
    except Exception as e:
        logging.error(f"Failed to read file {filename}: {e}")
        return pd.DataFrame()

def read_sector_data(filename):
    """Legacy function for read_data_file, kept for backward compatibility"""
    return read_data_file(filename)

def read_pulse_score(filename):
    """Legacy function for read_data_file, kept for backward compatibility"""
    return read_data_file(filename)

def read_market_data(filename):
    """Legacy function for read_data_file, kept for backward compatibility"""
    return read_data_file(filename)

def read_data(file_path, fallback_path=None, index_col=0, date_col=None):
    """
    Read data from a file, trying Parquet first and falling back to CSV.
    
    Args:
        file_path (str): Path to the data file (without extension)
        fallback_path (str, optional): Alternative path to try if first path fails
        index_col (int, optional): Column to use as index
        date_col (str, optional): Column containing dates to parse
        
    Returns:
        pd.DataFrame: Data from the file
    """
    # Try reading from Parquet file first
    parquet_path = f"{file_path}.parquet"
    if os.path.exists(parquet_path):
        try:
            df = pd.read_parquet(parquet_path)
            logging.info(f"Successfully loaded {len(df)} rows from {parquet_path}")
            return df
        except Exception as e:
            logging.warning(f"Failed to read Parquet file {parquet_path}: {e}")
    
    # Fall back to CSV file
    csv_path = f"{file_path}.csv"
    if os.path.exists(csv_path):
        try:
            if date_col:
                df = pd.read_csv(csv_path, index_col=index_col, parse_dates=[date_col])
            else:
                df = pd.read_csv(csv_path, index_col=index_col)
            logging.info(f"Successfully loaded {len(df)} rows from {csv_path}")
            return df
        except Exception as e:
            logging.warning(f"Failed to read CSV file {csv_path}: {e}")
    
    # Try fallback path if provided
    if fallback_path:
        return read_data(fallback_path, None, index_col, date_col)
    
    # Return empty DataFrame if all else fails
    logging.warning(f"No data found at {file_path}.*")
    return pd.DataFrame()

def read_sector_market_caps():
    """
    Read sector market cap data from Parquet file or fallback to CSV.
    
    Returns:
        pd.DataFrame: Sector market cap data
    """
    try:
        # Try to read from Parquet file
        df = read_data("data/sector_market_caps", None, 0)
        
        if not df.empty:
            # Make sure index is datetime
            if not isinstance(df.index, pd.DatetimeIndex):
                df.index = pd.to_datetime(df.index)
            
            # Sort index in ascending order
            df = df.sort_index()
            
            return df
            
        else:
            # No data found, generate notice
            logging.warning("Sector market cap data not found. Please run calc_sector_market_caps.py script.")
            return pd.DataFrame()
    
    except Exception as e:
        logging.error(f"Error reading sector market cap data: {e}")
        return pd.DataFrame()

def get_latest_sector_market_caps():
    """
    Get the latest sector market cap values.
    
    Returns:
        pd.Series: Latest sector market cap values
    """
    df = read_sector_market_caps()
    
    if df.empty:
        logging.warning("No sector market cap data available")
        return pd.Series()
    
    # Return the most recent row
    return df.iloc[-1]

def get_historical_sector_market_caps(days=30):
    """
    Get historical sector market cap values for the specified number of days.
    
    Args:
        days (int): Number of days of history to return
        
    Returns:
        pd.DataFrame: Historical sector market cap values
    """
    df = read_sector_market_caps()
    
    if df.empty:
        logging.warning("No sector market cap data available")
        return pd.DataFrame()
    
    # Return the last N days
    return df.iloc[-days:]