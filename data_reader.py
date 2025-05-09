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

def read_market_data(tickers=None, start_date=None, end_date=None, latest_only=False):
    """
    Read market data (prices, market cap) efficiently using PyArrow
    
    Args:
        tickers (list): List of tickers to read (None for all)
        start_date (str): Start date in 'YYYY-MM-DD' format (None for all)
        end_date (str): End date in 'YYYY-MM-DD' format (None for all)
        latest_only (bool): If True, only return the latest data point for each ticker
        
    Returns:
        DataFrame: Market data with ticker, date, price, market_cap columns
    """
    # First try the new location with the proper format
    ticker_data_dir = MARKET_DIR / "ticker_data"
    
    if ticker_data_dir.exists():
        try:
            # Create filters for the query
            filters = []
            
            if tickers is not None:
                # For partitioned datasets, we need to filter by the partition column
                # Get list of available ticker directories
                ticker_dirs = [d.name for d in ticker_data_dir.glob('*') if d.is_dir()]
                
                # Extract ticker names from directory names (which might have "ticker=" prefix)
                available_tickers = []
                for d in ticker_dirs:
                    if d.startswith("ticker="):
                        available_tickers.append(d[7:])  # Skip "ticker=" prefix
                    else:
                        available_tickers.append(d)
                
                # Find tickers that match our request
                matching_tickers = [t for t in tickers if t in available_tickers]
                
                if not matching_tickers:
                    logging.warning(f"None of the requested tickers found in Parquet dataset")
                    # Fall back to CSV
                    return _read_market_data_from_csv(tickers, start_date, end_date, latest_only)
                
                # Create a filter for tickers
                filters.append(('ticker', 'in', matching_tickers))
            
            # Add date filters
            if start_date is not None:
                filters.append(('date', '>=', pd.Timestamp(start_date)))
            
            if end_date is not None:
                filters.append(('date', '<=', pd.Timestamp(end_date)))
            
            # Add latest only filter
            if latest_only:
                filters.append(('is_latest', '=', True))
            
            # Read the data with filters
            # We use read_table first to avoid loading all data
            # then apply filters to reduce memory usage
            try:
                # Check if we have a single file or partitioned data
                table = pq.read_table(
                    ticker_data_dir,
                    filters=filters if filters else None
                )
                
                # Convert to DataFrame
                df = table.to_pandas()
                
                # Apply any remaining filters that couldn't be pushed down
                if tickers is not None and 'ticker' in df.columns:
                    df = df[df['ticker'].isin(tickers)]
                
                if start_date is not None and 'date' in df.columns:
                    df = df[df['date'] >= pd.Timestamp(start_date)]
                
                if end_date is not None and 'date' in df.columns:
                    df = df[df['date'] <= pd.Timestamp(end_date)]
                
                if latest_only and 'is_latest' in df.columns:
                    df = df[df['is_latest']]
                
                # Sort by date and ticker
                if not df.empty and 'date' in df.columns:
                    df = df.sort_values(['date', 'ticker'])
                
                logging.debug(f"Read {len(df)} market data rows from Parquet")
                return df
            except Exception as e:
                logging.error(f"Error reading data from ticker_data directory: {e}")
                logging.exception("Exception details:")
                # Fall back to CSV
                return _read_market_data_from_csv(tickers, start_date, end_date, latest_only)
        
        except Exception as e:
            logging.error(f"Error in read_market_data from Parquet: {e}")
            logging.exception("Exception details:")
            # Fall back to CSV
            return _read_market_data_from_csv(tickers, start_date, end_date, latest_only)
    
    # If we get here, we couldn't read from the new location
    # Try the legacy format next
    prices_dir = MARKET_DIR / "prices"
    if prices_dir.exists():
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
                    # Fall back to CSV
                    return _read_market_data_from_csv(tickers, start_date, end_date, latest_only)
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
                
                # Apply latest_only filter if needed
                if latest_only and 'date' in combined_df.columns:
                    # Group by ticker and get the latest date for each
                    latest_dates = combined_df.groupby('ticker')['date'].max()
                    
                    # Filter the dataframe to only include rows with the latest date for each ticker
                    combined_df = combined_df.merge(
                        latest_dates.reset_index().rename(columns={'date': 'latest_date'}),
                        on='ticker'
                    )
                    combined_df = combined_df[combined_df['date'] == combined_df['latest_date']]
                    combined_df = combined_df.drop(columns=['latest_date'])
                
                logging.debug(f"Read {len(combined_df)} market data rows from Parquet (legacy format)")
                return combined_df
            else:
                logging.warning("No market data could be read from Parquet")
                # Fall back to CSV
                return _read_market_data_from_csv(tickers, start_date, end_date, latest_only)
        
        except Exception as e:
            logging.error(f"Error reading market data from Parquet: {e}")
            # Fall back to CSV
            return _read_market_data_from_csv(tickers, start_date, end_date, latest_only)
    
    # If we get here, we couldn't read from either Parquet location
    # Fall back to CSV
    return _read_market_data_from_csv(tickers, start_date, end_date, latest_only)


def _read_market_data_from_csv(tickers=None, start_date=None, end_date=None, latest_only=False):
    """
    Fallback function to read market data from CSV files
    """
    try:
        # First check for the historical_ticker_*.csv files
        price_path = DATA_DIR / "historical_ticker_prices.csv"
        marketcap_path = DATA_DIR / "historical_ticker_marketcap.csv"
        
        if price_path.exists() and marketcap_path.exists():
            try:
                # Read the price and market cap data
                price_df = pd.read_csv(price_path, index_col=0)
                marketcap_df = pd.read_csv(marketcap_path, index_col=0)
                
                # Convert index to datetime if it's not already
                price_df.index = pd.to_datetime(price_df.index)
                marketcap_df.index = pd.to_datetime(marketcap_df.index)
                
                # Filter by date if needed
                if start_date is not None:
                    start_date = pd.Timestamp(start_date)
                    price_df = price_df.loc[price_df.index >= start_date]
                    marketcap_df = marketcap_df.loc[marketcap_df.index >= start_date]
                
                if end_date is not None:
                    end_date = pd.Timestamp(end_date)
                    price_df = price_df.loc[price_df.index <= end_date]
                    marketcap_df = marketcap_df.loc[marketcap_df.index <= end_date]
                
                # Filter by tickers if needed
                if tickers is not None:
                    # Keep only the columns that match our tickers
                    price_cols = [col for col in price_df.columns if col in tickers]
                    marketcap_cols = [col for col in marketcap_df.columns if col in tickers]
                    
                    # Make sure we still have data
                    if not price_cols or not marketcap_cols:
                        return pd.DataFrame()
                    
                    price_df = price_df[price_cols]
                    marketcap_df = marketcap_df[marketcap_cols]
                
                # Handle latest_only filter
                if latest_only:
                    price_df = price_df.iloc[[-1]]
                    marketcap_df = marketcap_df.iloc[[-1]]
                
                # Convert to long format
                price_long = price_df.reset_index().melt(
                    id_vars=['index'], 
                    var_name='ticker',
                    value_name='price'
                ).rename(columns={'index': 'date'})
                
                marketcap_long = marketcap_df.reset_index().melt(
                    id_vars=['index'], 
                    var_name='ticker',
                    value_name='market_cap'
                ).rename(columns={'index': 'date'})
                
                # Merge the price and market cap data
                merged_df = pd.merge(
                    price_long, 
                    marketcap_long, 
                    on=['date', 'ticker'], 
                    how='outer'
                )
                
                # Sort by date and ticker
                merged_df = merged_df.sort_values(['date', 'ticker'])
                
                logging.debug(f"Read {len(merged_df)} market data rows from historical_ticker CSVs")
                return merged_df
            
            except Exception as e:
                logging.error(f"Error reading market data from historical_ticker CSVs: {e}")
        
        # Fall back to summary files
        csv_path = DATA_DIR / "T2D_Pulse_Full_Ticker_History.csv"
        if not csv_path.exists():
            # Try root directory
            csv_path = Path("T2D_Pulse_Full_Ticker_History.csv")
        
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
                
                # Apply latest_only filter if needed
                if latest_only:
                    # Group by ticker and get the latest date for each
                    latest_dates = df.groupby('ticker')['date'].max()
                    
                    # Filter the dataframe to only include rows with the latest date for each ticker
                    df = df.merge(
                        latest_dates.reset_index().rename(columns={'date': 'latest_date'}),
                        on='ticker'
                    )
                    df = df[df['date'] == df['latest_date']]
                    df = df.drop(columns=['latest_date'])
                
                logging.debug(f"Read {len(df)} market data rows from summary CSV")
                return df
            
            except Exception as e:
                logging.error(f"Error reading market data from summary CSV: {e}")
        
        # If we get here, we couldn't find any suitable files
        logging.warning("No market data files found")
        return pd.DataFrame()
    
    except Exception as e:
        logging.error(f"Error in _read_market_data_from_csv: {e}")
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