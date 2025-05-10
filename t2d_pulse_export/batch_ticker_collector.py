#!/usr/bin/env python3
# batch_ticker_collector.py
# -----------------------------------------------------------
# Efficient batch collection of ticker price and market cap data
# Uses yfinance to collect data in batches for better performance and resilience

import os
import sys
import json
import time
import logging
import pandas as pd
import numpy as np
import yfinance as yf
import datetime
import pytz
from pathlib import Path
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import pyarrow as pa
import pyarrow.parquet as pq

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('batch_ticker_collector.log'),
        logging.StreamHandler()
    ]
)

# Constants
DATA_DIR = Path("data")
MARKET_DIR = DATA_DIR / "market"
TICKER_DATA_DIR = MARKET_DIR / "ticker_data"
PRICE_HISTORY_CSV = DATA_DIR / "historical_ticker_prices.csv"
MCAP_HISTORY_CSV = DATA_DIR / "historical_ticker_marketcap.csv"
METADATA_FILE = MARKET_DIR / "metadata.json"

# Make sure directories exist
DATA_DIR.mkdir(exist_ok=True)
MARKET_DIR.mkdir(exist_ok=True)
TICKER_DATA_DIR.mkdir(exist_ok=True)

def generate_sector_tickers():
    """Generate representative ticker symbols for each sector using authentic tickers from T2D_Pulse coverage
    This is the same function as in app.py to ensure consistency
    """
    return {
        "SMB SaaS": ["ADBE", "BILL", "HUBS"],
        "Enterprise SaaS": ["CRM", "MSFT", "ORCL"],
        "Cloud Infrastructure": ["CSCO", "SNOW", "AMZN"],
        "AdTech": ["TTD", "PUBM", "META"],
        "Fintech": ["XYZ", "PYPL", "COIN"],
        "Consumer Internet": ["META", "NFLX", "SNAP"],
        "eCommerce": ["ETSY", "SHOP", "SE"],
        "Cybersecurity": ["PANW", "CRWD", "OKTA"],
        "Dev Tools / Analytics": ["DDOG", "MDB", "TEAM"],
        "Semiconductors": ["NVDA", "AMD", "TSM"],
        "AI Infrastructure": ["GOOGL", "META", "NVDA"],
        "Vertical SaaS": ["PCOR", "CSGP", "CCCS"],
        "IT Services / Legacy Tech": ["ACN", "PLTR", "CTSH"],
        "Hardware / Devices": ["AAPL", "DELL", "SMCI"]
    }

def load_sector_tickers():
    """Load the list of tickers from sector configuration"""
    all_tickers = []
    sector_data = generate_sector_tickers()
    
    for sector_name, sector_tickers in sector_data.items():
        # Add tickers from this sector
        all_tickers.extend(sector_tickers)
    
    # Remove duplicates and sort
    unique_tickers = sorted(list(set(all_tickers)))
    logging.info(f"Loaded {len(unique_tickers)} unique tickers from {len(sector_data)} sectors")
    
    return unique_tickers

def get_latest_date():
    """Get the latest date in the existing price data"""
    # First check parquet files
    try:
        # Try to read metadata
        if METADATA_FILE.exists():
            with open(METADATA_FILE, 'r') as f:
                metadata = json.load(f)
                last_updated = metadata.get("last_updated")
                if last_updated:
                    # Convert to datetime
                    return datetime.datetime.fromisoformat(last_updated).date()
    except Exception as e:
        logging.error(f"Error reading metadata: {e}")
    
    # If we get here, we need to check the CSV files
    try:
        if PRICE_HISTORY_CSV.exists():
            price_df = pd.read_csv(PRICE_HISTORY_CSV, index_col=0, nrows=1)
            if not price_df.empty:
                return pd.to_datetime(price_df.index[-1]).date()
    except Exception as e:
        logging.error(f"Error checking price history CSV: {e}")
    
    # If we get here, we don't have any data yet
    # Default to 30 days ago
    today = datetime.datetime.now().date()
    return today - datetime.timedelta(days=30)

def update_metadata(last_updated):
    """Update the metadata file with the latest update timestamp"""
    metadata = {
        "last_updated": last_updated.isoformat(),
        "total_tickers": 0,
        "complete_tickers": 0,
        "tickers_with_issues": []
    }
    
    try:
        # Check if file exists first
        if METADATA_FILE.exists():
            with open(METADATA_FILE, 'r') as f:
                existing_metadata = json.load(f)
                # Update only the last_updated field
                existing_metadata["last_updated"] = last_updated.isoformat()
                metadata = existing_metadata
        
        # Write the updated metadata
        with open(METADATA_FILE, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        logging.info(f"Updated metadata with last_updated={last_updated.isoformat()}")
    except Exception as e:
        logging.error(f"Error updating metadata: {e}")

@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=4, max=60),
    retry=retry_if_exception_type((Exception))
)
def fetch_batch_data(tickers, start_date, end_date=None):
    """
    Fetch batch data for multiple tickers using yfinance.
    Uses retry decorator for robust API access.
    """
    if not tickers:
        return None
    
    # Convert to strings if needed
    if isinstance(start_date, datetime.datetime) or isinstance(start_date, datetime.date):
        start_date = start_date.strftime("%Y-%m-%d")
    
    if end_date is None:
        # Use today's date
        end_date = datetime.datetime.now().strftime("%Y-%m-%d")
    elif isinstance(end_date, datetime.datetime) or isinstance(end_date, datetime.date):
        end_date = end_date.strftime("%Y-%m-%d")
    
    try:
        # Add extra delay to respect rate limits
        time.sleep(2)
        
        # Use yfinance to download data for all tickers at once
        data = yf.download(
            tickers=tickers,
            start=start_date,
            end=end_date,
            group_by='ticker',
            auto_adjust=True,
            progress=False
        )
        
        if data is None or data.empty:
            logging.error(f"No data returned for any tickers in batch of {len(tickers)} tickers")
            return None
        
        # Return the raw data for processing
        return data
    except Exception as e:
        if "Rate limited" in str(e):
            logging.warning(f"Rate limited. Waiting longer before retry...")
            # Add extra delay for rate limiting
            time.sleep(10)
        logging.error(f"Error fetching batch data: {e}")
        raise  # Re-raise for retry

def process_yf_data(data):
    """Process the multi-level DataFrame returned by yfinance"""
    if data is None or data.empty:
        return None, None
    
    # Check if it's a multi-ticker or single-ticker result
    is_single_ticker = not isinstance(data.columns, pd.MultiIndex)
    
    price_data = {}
    mcap_data = {}
    
    # Get the dates from the index
    dates = data.index
    
    if is_single_ticker:
        # Single ticker case (no multi-index)
        ticker = data.columns[0].split('.')[0]  # Extract ticker from column name
        
        # Extract close prices
        if 'Close' in data.columns:
            price_data[ticker] = data['Close']
        
        # Calculate market cap if we have both Close and Volume
        if 'Close' in data.columns and 'Volume' in data.columns:
            # Market cap = price * outstanding shares (approximated by volume)
            mcap_data[ticker] = data['Close'] * data['Volume']
    else:
        # Multi-ticker case
        for ticker in data.columns.levels[0]:
            try:
                # Extract close prices
                if ('Close' in data[ticker].columns) and (not data[ticker]['Close'].empty):
                    price_data[ticker] = data[ticker]['Close']
                
                # Calculate market cap if we have both Close and Volume
                if ('Close' in data[ticker].columns and 'Volume' in data[ticker].columns and 
                    not data[ticker]['Close'].empty and not data[ticker]['Volume'].empty):
                    # Market cap = price * outstanding shares (approximated by volume)
                    mcap_data[ticker] = data[ticker]['Close'] * data[ticker]['Volume']
            except Exception as e:
                logging.error(f"Error processing ticker {ticker}: {e}")
    
    # Convert to DataFrame
    price_df = pd.DataFrame(price_data, index=dates)
    mcap_df = pd.DataFrame(mcap_data, index=dates)
    
    return price_df, mcap_df

def append_to_parquet_dataset(new_data):
    """Append new data to the existing Parquet dataset"""
    if new_data is None or new_data.empty:
        logging.warning("No new data to append")
        return
    
    try:
        # Convert to long format
        long_df = new_data.reset_index().melt(
            id_vars=['date'], 
            var_name='ticker',
            value_name='value'
        )
        
        # Add is_latest flag
        # First get the last date for each ticker
        latest_dates = long_df.groupby('ticker')['date'].max().reset_index()
        latest_dates = latest_dates.rename(columns={'date': 'latest_date'})
        
        # Merge with the main dataframe
        long_df = pd.merge(long_df, latest_dates, on='ticker', how='left')
        
        # Set is_latest flag
        long_df['is_latest'] = long_df['date'] == long_df['latest_date']
        
        # Remove the latest_date column as it's no longer needed
        long_df = long_df.drop(columns=['latest_date'])
        
        # Convert to PyArrow table
        table = pa.Table.from_pandas(long_df)
        
        # Add schema metadata
        metadata = {
            "schema_version": "1.0",
            "created_at": datetime.datetime.now().isoformat()
        }
        table = table.replace_schema_metadata({**table.schema.metadata, **metadata})
        
        # Write to partitioned dataset
        pq.write_to_dataset(
            table,
            root_path=TICKER_DATA_DIR,
            partition_cols=['ticker']
        )
        
        logging.info(f"Successfully appended {len(long_df)} rows to Parquet dataset")
    except Exception as e:
        logging.error(f"Error appending to Parquet dataset: {e}")
        logging.exception("Exception details:")

def update_csv_files(price_df, mcap_df):
    """Update the CSV files with new data"""
    if price_df is None or mcap_df is None:
        logging.warning("No data to update CSV files")
        return
    
    try:
        # Load existing data if available
        existing_price_df = None
        existing_mcap_df = None
        
        if PRICE_HISTORY_CSV.exists():
            existing_price_df = pd.read_csv(PRICE_HISTORY_CSV, index_col=0)
            existing_price_df.index = pd.to_datetime(existing_price_df.index)
        
        if MCAP_HISTORY_CSV.exists():
            existing_mcap_df = pd.read_csv(MCAP_HISTORY_CSV, index_col=0)
            existing_mcap_df.index = pd.to_datetime(existing_mcap_df.index)
        
        # Combine with new data
        if existing_price_df is not None:
            # Add new columns
            for col in price_df.columns:
                if col not in existing_price_df.columns:
                    existing_price_df[col] = np.nan
            
            # Update with new data
            for date in price_df.index:
                if date in existing_price_df.index:
                    for ticker in price_df.columns:
                        if not pd.isna(price_df.loc[date, ticker]):
                            existing_price_df.loc[date, ticker] = price_df.loc[date, ticker]
                else:
                    # New date, append entire row
                    existing_price_df.loc[date] = price_df.loc[date]
            
            # Sort by date
            existing_price_df = existing_price_df.sort_index()
            
            # Save back to CSV
            existing_price_df.to_csv(PRICE_HISTORY_CSV)
        else:
            # No existing data, just save the new data
            price_df.to_csv(PRICE_HISTORY_CSV)
        
        # Same for market cap data
        if existing_mcap_df is not None:
            # Add new columns
            for col in mcap_df.columns:
                if col not in existing_mcap_df.columns:
                    existing_mcap_df[col] = np.nan
            
            # Update with new data
            for date in mcap_df.index:
                if date in existing_mcap_df.index:
                    for ticker in mcap_df.columns:
                        if not pd.isna(mcap_df.loc[date, ticker]):
                            existing_mcap_df.loc[date, ticker] = mcap_df.loc[date, ticker]
                else:
                    # New date, append entire row
                    existing_mcap_df.loc[date] = mcap_df.loc[date]
            
            # Sort by date
            existing_mcap_df = existing_mcap_df.sort_index()
            
            # Save back to CSV
            existing_mcap_df.to_csv(MCAP_HISTORY_CSV)
        else:
            # No existing data, just save the new data
            mcap_df.to_csv(MCAP_HISTORY_CSV)
        
        logging.info(f"Successfully updated CSV files")
    except Exception as e:
        logging.error(f"Error updating CSV files: {e}")
        logging.exception("Exception details:")

def run_batch_collection():
    """Run the batch ticker collection process"""
    logging.info("Starting batch ticker collection process")
    
    try:
        # Get all tickers
        all_tickers = load_sector_tickers()
        
        if not all_tickers:
            logging.error("No tickers found in sector configuration")
            return False
        
        # Get the latest date in the existing data
        latest_date = get_latest_date()
        
        # Set the start date to one day after the latest date
        start_date = latest_date + datetime.timedelta(days=1)
        
        # Set the end date to today
        end_date = datetime.datetime.now().date()
        
        # Skip if start_date is after end_date (no new data needed)
        if start_date > end_date:
            logging.info(f"Data already up to date (latest={latest_date}, today={end_date})")
            return True
        
        logging.info(f"Fetching data from {start_date} to {end_date} for {len(all_tickers)} tickers")
        
        # Split tickers into smaller batches to reduce rate limiting issues
        # Smaller batch size is more likely to succeed with rate limits
        batch_size = 5
        ticker_batches = [all_tickers[i:i+batch_size] for i in range(0, len(all_tickers), batch_size)]
        
        # Process each batch
        all_price_data = []
        all_mcap_data = []
        
        for i, batch in enumerate(ticker_batches):
            logging.info(f"Processing batch {i+1}/{len(ticker_batches)} with {len(batch)} tickers")
            
            try:
                # Fetch batch data
                batch_data = fetch_batch_data(batch, start_date, end_date)
                
                # Process into price and market cap data
                price_df, mcap_df = process_yf_data(batch_data)
                
                if price_df is not None and not price_df.empty:
                    all_price_data.append(price_df)
                
                if mcap_df is not None and not mcap_df.empty:
                    all_mcap_data.append(mcap_df)
                
                # Sleep to avoid rate limiting
                # Use a longer sleep between batches (5 seconds)
                time.sleep(5)
            except Exception as e:
                logging.error(f"Error processing batch {i+1}: {e}")
                logging.exception("Exception details:")
        
        # Combine all batches
        if all_price_data:
            # Get a list of unique indices across all DataFrames
            all_indices = sorted(list(set().union(*[df.index for df in all_price_data])))
            
            # Create a new DataFrame with all indices
            combined_price_df = pd.DataFrame(index=all_indices)
            
            # Merge all DataFrames
            for df in all_price_data:
                for col in df.columns:
                    combined_price_df[col] = df[col]
        else:
            combined_price_df = pd.DataFrame()
        
        if all_mcap_data:
            # Get a list of unique indices across all DataFrames
            all_indices = sorted(list(set().union(*[df.index for df in all_mcap_data])))
            
            # Create a new DataFrame with all indices
            combined_mcap_df = pd.DataFrame(index=all_indices)
            
            # Merge all DataFrames
            for df in all_mcap_data:
                for col in df.columns:
                    combined_mcap_df[col] = df[col]
        else:
            combined_mcap_df = pd.DataFrame()
        
        # Update CSV files
        if not combined_price_df.empty and not combined_mcap_df.empty:
            update_csv_files(combined_price_df, combined_mcap_df)
        
        # Convert price data to format for Parquet
        if not combined_price_df.empty:
            # Reset index to make date a column
            combined_price_df = combined_price_df.reset_index()
            combined_price_df = combined_price_df.rename(columns={'index': 'date'})
            
            # Convert to long format
            price_long = combined_price_df.melt(
                id_vars=['date'],
                var_name='ticker',
                value_name='price'
            )
            
            # Add corresponding market cap data
            if not combined_mcap_df.empty:
                # Reset index to make date a column
                combined_mcap_df = combined_mcap_df.reset_index()
                combined_mcap_df = combined_mcap_df.rename(columns={'index': 'date'})
                
                # Convert to long format
                mcap_long = combined_mcap_df.melt(
                    id_vars=['date'],
                    var_name='ticker',
                    value_name='market_cap'
                )
                
                # Merge the two datasets
                merged_df = pd.merge(
                    price_long,
                    mcap_long,
                    on=['date', 'ticker'],
                    how='outer'
                )
            else:
                merged_df = price_long
                merged_df['market_cap'] = np.nan
            
            # Append to Parquet dataset
            append_to_parquet_dataset(merged_df)
        
        # Update metadata with the latest date
        update_metadata(end_date)
        
        logging.info(f"Batch collection complete")
        return True
    
    except Exception as e:
        logging.error(f"Error in batch collection: {e}")
        logging.exception("Exception details:")
        return False

if __name__ == "__main__":
    # Run the batch collection process
    success = run_batch_collection()
    
    if success:
        logging.info("Batch ticker collection completed successfully")
        sys.exit(0)
    else:
        logging.error("Batch ticker collection failed")
        sys.exit(1)