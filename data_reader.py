#!/usr/bin/env python3
# data_reader.py
# -----------------------------------------------------------
# Efficient data reading from Parquet files for the T2D Pulse dashboard

import os
from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Constants for data directories
DATA_DIR = Path("data")
MACRO_DIR = DATA_DIR / "macro"
MARKET_DIR = DATA_DIR / "market"
DERIVED_DIR = DATA_DIR / "derived"

# File mapping from CSV to Parquet for backward compatibility
# This helps transition smoothly from the old CSV-based system
FILE_MAPPING = {
    # Macro data
    "gdp_data.csv": MACRO_DIR / "gdp.parquet",
    "pce_data.csv": MACRO_DIR / "pce.parquet",
    "unemployment_data.csv": MACRO_DIR / "unemployment.parquet",
    "inflation_data.csv": MACRO_DIR / "inflation.parquet",
    "interest_rate_data.csv": MACRO_DIR / "interest_rate.parquet",
    "treasury_yield_data.csv": MACRO_DIR / "treasury_yield.parquet",
    "nasdaq_data.csv": MACRO_DIR / "nasdaq_index.parquet",
    "consumer_sentiment_data.csv": MACRO_DIR / "consumer_sentiment.parquet",
    "job_postings_data.csv": MACRO_DIR / "job_postings.parquet",
    "software_ppi_data.csv": MACRO_DIR / "software_ppi.parquet",
    "data_processing_ppi_data.csv": MACRO_DIR / "data_processing_ppi.parquet",
    "pcepi_data.csv": MACRO_DIR / "pcepi.parquet",
    "vix_data.csv": MACRO_DIR / "vix.parquet",
    
    # Derived data
    "sector_30day_history.csv": DERIVED_DIR / "sector_history.parquet",
    "authentic_sector_history.csv": DERIVED_DIR / "authentic_sector_history.parquet",
    "definitive_sector_scores.csv": DERIVED_DIR / "definitive_sector_scores.parquet",
}

def read_data_file(filename, fallback_to_csv=True):
    """
    Read a data file, with automatic fallback between Parquet and CSV
    
    Args:
        filename (str): The original CSV filename (e.g., "gdp_data.csv")
        fallback_to_csv (bool): Whether to try the CSV file if Parquet doesn't exist
        
    Returns:
        DataFrame: The loaded data or None if not found
    """
    # Check if this is a file we've mapped to Parquet
    if filename in FILE_MAPPING:
        parquet_path = FILE_MAPPING[filename]
        
        # Try to read from Parquet first
        if parquet_path.exists():
            try:
                df = pd.read_parquet(parquet_path)
                logging.debug(f"Read {len(df)} rows from {parquet_path}")
                return df
            except Exception as e:
                logging.warning(f"Error reading Parquet file {parquet_path}: {e}")
                
                # Fall back to CSV if requested
                if fallback_to_csv:
                    csv_path = DATA_DIR / filename
                    if csv_path.exists():
                        try:
                            df = pd.read_csv(csv_path)
                            logging.debug(f"Fallback: Read {len(df)} rows from {csv_path}")
                            return df
                        except Exception as csv_e:
                            logging.error(f"Error reading CSV file {csv_path}: {csv_e}")
        # If Parquet doesn't exist but CSV does
        elif fallback_to_csv:
            csv_path = DATA_DIR / filename
            if csv_path.exists():
                try:
                    df = pd.read_csv(csv_path)
                    logging.debug(f"Read {len(df)} rows from {csv_path}")
                    return df
                except Exception as e:
                    logging.error(f"Error reading CSV file {csv_path}: {e}")
    else:
        # For files not in the mapping, just try to read the CSV
        csv_path = DATA_DIR / filename
        if csv_path.exists():
            try:
                df = pd.read_csv(csv_path)
                logging.debug(f"Read {len(df)} rows from {csv_path}")
                return df
            except Exception as e:
                logging.error(f"Error reading CSV file {csv_path}: {e}")
    
    # If we get here, we couldn't read the file
    logging.warning(f"Could not read data file {filename}")
    return None

def read_market_data(tickers=None, start_date=None, end_date=None):
    """
    Read market data (prices, market cap) efficiently using PyArrow
    
    Args:
        tickers (list): List of tickers to read (None for all)
        start_date (str): Start date in 'YYYY-MM-DD' format (None for all)
        end_date (str): End date in 'YYYY-MM-DD' format (None for all)
        
    Returns:
        DataFrame: Market data with ticker, date, price, market_cap columns
    """
    prices_dir = MARKET_DIR / "prices"
    
    if not prices_dir.exists():
        # Fall back to CSV for now
        csv_path = DATA_DIR / "T2D_Pulse_Full_Ticker_History.csv"
        if csv_path.exists():
            try:
                df = pd.read_csv(csv_path)
                
                # Apply filters if provided
                if tickers is not None:
                    df = df[df['ticker'].isin(tickers)]
                
                if start_date is not None:
                    df = df[df['date'] >= start_date]
                
                if end_date is not None:
                    df = df[df['date'] <= end_date]
                
                # Ensure date is datetime
                df['date'] = pd.to_datetime(df['date'])
                
                logging.debug(f"Read {len(df)} market data rows from CSV")
                return df
            except Exception as e:
                logging.error(f"Error reading market data from CSV: {e}")
                return pd.DataFrame()
        else:
            logging.warning(f"No market data found in CSV or Parquet")
            return pd.DataFrame()
    
    try:
        # Create filters for the query
        filters = []
        
        if tickers is not None:
            # For partitioned datasets, we need to filter by the partition column
            # Check each partition directory that matches our tickers
            existing_ticker_dirs = [d.name for d in prices_dir.glob('*') if d.is_dir()]
            matching_tickers = [t for t in tickers if t in existing_ticker_dirs]
            
            if not matching_tickers:
                logging.warning(f"None of the requested tickers found in Parquet dataset")
                return pd.DataFrame()
        else:
            # Read all tickers
            matching_tickers = [d.name for d in prices_dir.glob('*') if d.is_dir()]
        
        # Read data for each matching ticker and combine
        all_data = []
        
        for ticker in matching_tickers:
            ticker_dir = prices_dir / f"ticker={ticker}"
            if not ticker_dir.exists():
                ticker_dir = prices_dir / ticker  # Try without the "ticker=" prefix
            
            if ticker_dir.exists():
                # Create filters for dates if provided
                date_filters = []
                if start_date is not None:
                    date_filters.append(('date', '>=', pd.Timestamp(start_date)))
                
                if end_date is not None:
                    date_filters.append(('date', '<=', pd.Timestamp(end_date)))
                
                try:
                    # Read this ticker's data with filters
                    table = pq.read_table(
                        ticker_dir,
                        filters=date_filters if date_filters else None
                    )
                    
                    # Convert to DataFrame
                    df = table.to_pandas()
                    
                    # Add to list
                    all_data.append(df)
                except Exception as e:
                    logging.error(f"Error reading data for ticker {ticker}: {e}")
        
        # Combine all data
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            logging.debug(f"Read {len(combined_df)} market data rows from Parquet")
            return combined_df
        else:
            logging.warning("No market data could be read from Parquet")
            return pd.DataFrame()
    
    except Exception as e:
        logging.error(f"Error reading market data from Parquet: {e}")
        return pd.DataFrame()

def read_sector_data():
    """
    Read the latest sector sentiment data
    
    Returns:
        DataFrame: The sector sentiment data
    """
    # Try to read from Parquet first
    parquet_path = DERIVED_DIR / "definitive_sector_scores.parquet"
    
    if parquet_path.exists():
        try:
            df = pd.read_parquet(parquet_path)
            logging.debug(f"Read {len(df)} sector scores from {parquet_path}")
            return df
        except Exception as e:
            logging.warning(f"Error reading sector data from Parquet: {e}")
    
    # Fall back to CSV
    csv_path = DATA_DIR / "definitive_sector_scores.csv"
    
    if csv_path.exists():
        try:
            df = pd.read_csv(csv_path)
            logging.debug(f"Read {len(df)} sector scores from {csv_path}")
            return df
        except Exception as e:
            logging.error(f"Error reading sector data from CSV: {e}")
    
    logging.warning("No sector data found")
    return None

def read_pulse_score():
    """Read the current T2D Pulse score"""
    try:
        # Try the file in the derived directory first
        pulse_path = DERIVED_DIR / "current_pulse_score.txt"
        
        if not pulse_path.exists():
            # Fall back to the original location
            pulse_path = DATA_DIR / "current_pulse_score.txt"
        
        if pulse_path.exists():
            with open(pulse_path, 'r') as f:
                score_str = f.read().strip()
                score = float(score_str)
                return score
        else:
            logging.warning("Pulse score file not found")
            return None
    except Exception as e:
        logging.error(f"Error reading pulse score: {e}")
        return None

# Example usage
if __name__ == "__main__":
    # Test reading GDP data
    gdp_data = read_data_file("gdp_data.csv")
    if gdp_data is not None:
        print(f"GDP data: {len(gdp_data)} rows")
        print(gdp_data.head())
    
    # Test reading market data
    market_data = read_market_data(tickers=["AAPL", "MSFT"], start_date="2025-01-01")
    if not market_data.empty:
        print(f"Market data: {len(market_data)} rows")
        print(market_data.head())
    
    # Test reading sector data
    sector_data = read_sector_data()
    if sector_data is not None:
        print(f"Sector data: {len(sector_data)} rows")
        print(sector_data.head())
    
    # Test reading pulse score
    pulse_score = read_pulse_score()
    if pulse_score is not None:
        print(f"T2D Pulse score: {pulse_score}")