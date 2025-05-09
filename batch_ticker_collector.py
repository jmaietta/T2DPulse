#!/usr/bin/env python3
# batch_ticker_collector.py
# -----------------------------------------------------------
# Efficient batch collection of ticker data using yfinance

import os
import pandas as pd
import yfinance as yf
import pyarrow as pa
import pyarrow.parquet as pq
import logging
from datetime import datetime, timedelta
import time
from pathlib import Path
import json
from tenacity import retry, stop_after_attempt, wait_random_exponential

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename='batch_ticker_collector.log'
)

# Constants
MARKET_DIR = Path("data/market")
PRICES_DIR = MARKET_DIR / "prices"
METADATA_FILE = MARKET_DIR / "metadata.json"
SCHEMA_VERSION = "1.0.0"

def load_sector_tickers():
    """Load the list of tickers from sector configuration"""
    try:
        # Import the sector tickers function
        from app import generate_sector_tickers
        
        # Get tickers
        sector_tickers = generate_sector_tickers()
        
        # Extract unique tickers
        all_tickers = []
        for sector_data in sector_tickers:
            tickers = sector_data.get('tickers', [])
            all_tickers.extend(tickers)
        
        # Deduplicate
        unique_tickers = list(set(all_tickers))
        
        logging.info(f"Loaded {len(unique_tickers)} unique tickers from sector configuration")
        return unique_tickers
    except Exception as e:
        logging.error(f"Error loading sector tickers: {e}")
        return []

def get_latest_date():
    """Get the latest date in the existing price data"""
    try:
        if not PRICES_DIR.exists():
            return None
        
        # Look for metadata first
        if METADATA_FILE.exists():
            with open(METADATA_FILE, 'r') as f:
                metadata = json.load(f)
                last_updated = metadata.get('last_updated')
                if last_updated:
                    return pd.Timestamp(last_updated)
        
        # If no metadata, check the actual files
        latest_date = None
        
        # List all parquet files in the partitioned dataset
        partition_dirs = [d for d in PRICES_DIR.glob('*') if d.is_dir()]
        
        for partition_dir in partition_dirs:
            parquet_files = list(partition_dir.glob('*.parquet'))
            if parquet_files:
                # Read just the date column from the first file
                for parquet_file in parquet_files:
                    try:
                        table = pq.read_table(parquet_file, columns=['date'])
                        df = table.to_pandas()
                        max_date = df['date'].max()
                        
                        if latest_date is None or max_date > latest_date:
                            latest_date = max_date
                    except Exception as e:
                        logging.warning(f"Error reading date from {parquet_file}: {e}")
        
        return latest_date
    except Exception as e:
        logging.error(f"Error getting latest date: {e}")
        return None

def update_metadata(last_updated):
    """Update the metadata file with the latest update timestamp"""
    try:
        metadata = {
            'schema_version': SCHEMA_VERSION,
            'last_updated': last_updated.strftime('%Y-%m-%d'),
            'last_run': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        with open(METADATA_FILE, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        logging.info(f"Updated metadata with last_updated: {last_updated}")
    except Exception as e:
        logging.error(f"Error updating metadata: {e}")

@retry(stop=stop_after_attempt(5), wait=wait_random_exponential(multiplier=1, max=60))
def fetch_batch_data(tickers, start_date, end_date=None):
    """
    Fetch batch data for multiple tickers using yfinance.
    Uses retry decorator for robust API access.
    """
    logging.info(f"Fetching batch data for {len(tickers)} tickers from {start_date} to {end_date or 'today'}")
    
    # Split tickers into chunks to avoid API limits (max ~100 per request)
    chunk_size = 50
    all_data = pd.DataFrame()
    
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i+chunk_size]
        logging.info(f"Processing chunk {i//chunk_size + 1}/{(len(tickers)-1)//chunk_size + 1} with {len(chunk)} tickers")
        
        try:
            # Download data for this chunk
            data = yf.download(
                chunk,
                start=start_date,
                end=end_date,
                group_by='ticker',
                auto_adjust=True,  # Adjust for splits and dividends
                threads=True,
                progress=False
            )
            
            if data.empty:
                logging.warning(f"No data returned for chunk starting with {chunk[0]}")
                continue
            
            # Process the data
            processed_chunk = process_yf_data(data)
            
            # Append to full dataset
            if not all_data.empty:
                all_data = pd.concat([all_data, processed_chunk])
            else:
                all_data = processed_chunk
            
            # Avoid overwhelming the API
            time.sleep(1)
            
        except Exception as e:
            logging.error(f"Error fetching data for chunk {i//chunk_size + 1}: {e}")
            # Let the retry decorator handle retries
            raise
    
    return all_data

def process_yf_data(data):
    """Process the multi-level DataFrame returned by yfinance"""
    # Check if we have data
    if data.empty:
        return pd.DataFrame()
    
    # Prepare a list to hold individual ticker dataframes
    ticker_dfs = []
    
    # Get the list of tickers from the columns
    tickers = list(set([col[0] for col in data.columns if isinstance(col, tuple)]))
    
    for ticker in tickers:
        # Extract data for this ticker
        try:
            # Select just this ticker's data
            ticker_data = data[ticker].copy()
            
            # Reset the column names (remove the ticker level)
            ticker_data.columns = ticker_data.columns.droplevel(0)
            
            # Add ticker column
            ticker_data['ticker'] = ticker
            
            # Select relevant columns and rename
            df = pd.DataFrame({
                'date': ticker_data.index,
                'ticker': ticker_data['ticker'],
                'close': ticker_data['Close'],
                'volume': ticker_data['Volume'],
                'high': ticker_data['High'],
                'low': ticker_data['Low'],
                'open': ticker_data['Open']
            })
            
            # Get market cap data if available
            try:
                # Use the get_ticker_stats function for market cap
                t = yf.Ticker(ticker)
                mkt_cap = t.info.get('marketCap')
                if mkt_cap:
                    df['market_cap'] = mkt_cap
            except Exception as e:
                logging.warning(f"Could not get market cap for {ticker}: {e}")
            
            # Add to list
            ticker_dfs.append(df)
            
        except Exception as e:
            logging.error(f"Error processing ticker {ticker}: {e}")
    
    # Combine all ticker data
    if ticker_dfs:
        combined_df = pd.concat(ticker_dfs, ignore_index=True)
        return combined_df
    else:
        return pd.DataFrame()

def append_to_parquet_dataset(new_data):
    """Append new data to the existing Parquet dataset"""
    if new_data.empty:
        logging.warning("No new data to append")
        return False
    
    try:
        # Make sure directory exists
        PRICES_DIR.mkdir(parents=True, exist_ok=True)
        
        # Convert to PyArrow table
        table = pa.Table.from_pandas(new_data)
        
        # Add schema version metadata
        metadata = table.schema.metadata or {}
        metadata[b'schema_version'] = SCHEMA_VERSION.encode()
        table = table.replace_schema_metadata(metadata)
        
        # Write to Parquet dataset, partitioned by ticker
        pq.write_to_dataset(
            table,
            root_path=str(PRICES_DIR),
            partition_cols=['ticker'],
            existing_data_behavior='delete_matching'  # Replace existing dates
        )
        
        logging.info(f"Appended {len(new_data)} rows to Parquet dataset")
        return True
    except Exception as e:
        logging.error(f"Error appending to Parquet dataset: {e}")
        return False

def run_batch_collection():
    """Run the batch ticker collection process"""
    start_time = datetime.now()
    logging.info(f"Starting batch ticker collection at {start_time}")
    
    # Load tickers
    tickers = load_sector_tickers()
    if not tickers:
        logging.error("No tickers loaded, aborting")
        return False
    
    # Determine date range
    latest_date = get_latest_date()
    
    if latest_date:
        # Add one day to avoid duplicate data
        start_date = (latest_date + timedelta(days=1)).strftime('%Y-%m-%d')
        logging.info(f"Continuing from last data point: {start_date}")
    else:
        # Start from beginning of 2024 if no data exists
        start_date = '2024-01-01'
        logging.info(f"No existing data found, starting from {start_date}")
    
    # Get today's date for end_date
    today = datetime.now().strftime('%Y-%m-%d')
    
    # Check if start_date is in the future or today
    if start_date >= today:
        logging.info(f"Data is already up to date as of {latest_date}")
        return True
    
    # Fetch new data
    new_data = fetch_batch_data(tickers, start_date)
    
    # Append to dataset
    if not new_data.empty:
        success = append_to_parquet_dataset(new_data)
        if success:
            # Update metadata with latest date
            max_date = new_data['date'].max()
            update_metadata(max_date)
    else:
        logging.warning("No new data fetched")
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    logging.info(f"Batch collection completed in {duration:.2f} seconds")
    
    return True

if __name__ == "__main__":
    run_batch_collection()