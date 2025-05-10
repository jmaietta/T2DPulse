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
from pathlib import Path
from filelock import FileLock
from functools import lru_cache

# Define consistent data directory path - can be overridden with environment variable
DATA_DIR = Path(os.getenv("DATA_DIR", "data")).resolve()

# Create data directory if it doesn't exist
DATA_DIR.mkdir(exist_ok=True)

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
    """
    Read sector data from file and raise descriptive exception if missing
    
    Args:
        filename (str): Path to the sector data file
        
    Returns:
        pd.DataFrame: Sector data
        
    Raises:
        FileNotFoundError: If the file doesn't exist
    """
    # Check if file exists before attempting to read
    if not os.path.exists(filename):
        raise FileNotFoundError(f"Critical sector data file not found: {filename}")
    
    df = read_data_file(filename)
    
    # If dataframe is empty after reading, something went wrong
    if df.empty:
        raise ValueError(f"Failed to load sector data from {filename} - file may be corrupted")
        
    return df

def read_pulse_score(filename):
    """
    Read pulse score data from file
    
    Args:
        filename (str): Path to the pulse score data file
        
    Returns:
        pd.DataFrame: Pulse score data
    """
    if not os.path.exists(filename):
        logging.warning(f"Pulse score file not found: {filename}")
        return pd.DataFrame()
        
    return read_data_file(filename)

def read_market_data(filename):
    """Legacy function for read_data_file, kept for backward compatibility"""
    return read_data_file(filename)

def read_data(file_path, fallback_path=None, index_col=0, date_col=None):
    """
    Read data from a file, trying Parquet first and falling back to CSV.
    Uses consistent path resolution with DATA_DIR and file locking to prevent race conditions.
    
    Args:
        file_path (str): Path to the data file (without extension)
        fallback_path (str, optional): Alternative path to try if first path fails
        index_col (int, optional): Column to use as index
        date_col (str, optional): Column containing dates to parse
        
    Returns:
        pd.DataFrame: Data from the file
    """
    # Convert string path to Path object for consistent handling
    path_obj = Path(file_path)
    
    # If the path doesn't include a parent directory and isn't absolute,
    # assume it's relative to DATA_DIR
    if not path_obj.parent.name and not path_obj.is_absolute():
        path_obj = DATA_DIR / path_obj
    
    # Try reading from Parquet file first
    parquet_path = path_obj.with_suffix('.parquet')
    if parquet_path.exists():
        lock_path = str(parquet_path) + '.lock'
        with FileLock(lock_path, timeout=10):
            try:
                df = pd.read_parquet(parquet_path)
                logging.info(f"Successfully loaded {len(df)} rows from {parquet_path}")
                return df
            except Exception as e:
                logging.warning(f"Failed to read Parquet file {parquet_path}: {e}")
    
    # Fall back to CSV file
    csv_path = path_obj.with_suffix('.csv')
    if csv_path.exists():
        lock_path = str(csv_path) + '.lock'
        with FileLock(lock_path, timeout=10):
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
    logging.warning(f"No data found at {path_obj}.*")
    return pd.DataFrame()

@lru_cache(maxsize=1)
def read_sector_market_caps():
    """
    Read sector market cap data from Parquet file or fallback to CSV.
    Uses file locking to prevent race conditions and caching to improve performance.
    
    Returns:
        pd.DataFrame: Sector market cap data
    """
    try:
        # Use consistent path with DATA_DIR
        file_path = DATA_DIR / "sector_market_caps"
        parquet_path = file_path.with_suffix('.parquet')
        csv_path = file_path.with_suffix('.csv')
        
        # Check if either file exists
        if not (parquet_path.exists() or csv_path.exists()):
            logging.warning("Sector market cap data not found. Please run calc_sector_market_caps.py script.")
            return pd.DataFrame()
        
        # Try to read from Parquet file first with file locking to prevent race conditions
        if parquet_path.exists():
            lock_path = str(parquet_path) + '.lock'
            with FileLock(lock_path, timeout=10):
                try:
                    df = pd.read_parquet(parquet_path)
                    logging.info(f"Successfully loaded {len(df)} rows from {parquet_path}")
                    
                    # Make sure index is datetime
                    if not isinstance(df.index, pd.DatetimeIndex):
                        df.index = pd.to_datetime(df.index)
                    
                    # Sort index in ascending order
                    df = df.sort_index()
                    
                    return df
                except Exception as e:
                    logging.warning(f"Failed to read Parquet file {parquet_path}: {e}")
        
        # Fall back to CSV file with file locking
        if csv_path.exists():
            lock_path = str(csv_path) + '.lock'
            with FileLock(lock_path, timeout=10):
                try:
                    df = pd.read_csv(csv_path, index_col=0)
                    logging.info(f"Successfully loaded {len(df)} rows from {csv_path}")
                    
                    # Make sure index is datetime
                    if not isinstance(df.index, pd.DatetimeIndex):
                        df.index = pd.to_datetime(df.index)
                    
                    # Sort index in ascending order
                    df = df.sort_index()
                    
                    return df
                except Exception as e:
                    logging.warning(f"Failed to read CSV file {csv_path}: {e}")
        
        # No data found or could not read files
        logging.warning("Could not read sector market cap data from any available file.")
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