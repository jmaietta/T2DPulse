#!/usr/bin/env python3
# data_conversion.py
# -----------------------------------------------------------
# Convert existing CSV data to Parquet format using the new data architecture

import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Constants for data directories
DATA_DIR = "data"
MACRO_DIR = os.path.join(DATA_DIR, "macro")
MARKET_DIR = os.path.join(DATA_DIR, "market")
DERIVED_DIR = os.path.join(DATA_DIR, "derived")

# Schema version tracking
SCHEMA_VERSION = "1.0.0"

def add_schema_version(table):
    """Add schema version metadata to a PyArrow table"""
    metadata = table.schema.metadata or {}
    metadata[b'schema_version'] = SCHEMA_VERSION.encode()
    return table.replace_schema_metadata(metadata)

def convert_macro_data():
    """Convert all macro economic data to Parquet format"""
    logging.info("Converting macro economic data to Parquet format")
    
    # List of macro data files and their corresponding Parquet names
    macro_files = [
        {"csv": "gdp_data.csv", "parquet": "gdp.parquet"},
        {"csv": "pce_data.csv", "parquet": "pce.parquet"},
        {"csv": "unemployment_data.csv", "parquet": "unemployment.parquet"},
        {"csv": "inflation_data.csv", "parquet": "inflation.parquet"},
        {"csv": "interest_rate_data.csv", "parquet": "interest_rate.parquet"},
        {"csv": "treasury_yield_data.csv", "parquet": "treasury_yield.parquet"},
        {"csv": "nasdaq_data.csv", "parquet": "nasdaq_index.parquet"},
        {"csv": "consumer_sentiment_data.csv", "parquet": "consumer_sentiment.parquet"},
        {"csv": "job_postings_data.csv", "parquet": "job_postings.parquet"},
        {"csv": "software_ppi_data.csv", "parquet": "software_ppi.parquet"},
        {"csv": "data_processing_ppi_data.csv", "parquet": "data_processing_ppi.parquet"},
        {"csv": "pcepi_data.csv", "parquet": "pcepi.parquet"},
        {"csv": "vix_data.csv", "parquet": "vix.parquet"}
    ]
    
    for file_info in macro_files:
        csv_path = os.path.join(DATA_DIR, file_info["csv"])
        parquet_path = os.path.join(MACRO_DIR, file_info["parquet"])
        
        if os.path.exists(csv_path):
            try:
                # Read CSV file
                df = pd.read_csv(csv_path)
                
                # Ensure date column is properly named
                date_col = None
                for col in df.columns:
                    if col.lower() == 'date':
                        date_col = col
                        break
                
                if date_col:
                    # Rename to standardized 'date' if needed
                    if date_col != 'date':
                        df = df.rename(columns={date_col: 'date'})
                    
                    # Convert to datetime if not already
                    df['date'] = pd.to_datetime(df['date'])
                    
                    # Sort by date
                    df = df.sort_values('date')
                    
                    # Remove duplicates based on date
                    df = df.drop_duplicates(subset=['date'])
                    
                    # Convert to PyArrow table
                    table = pa.Table.from_pandas(df)
                    
                    # Add schema version metadata
                    table = add_schema_version(table)
                    
                    # Write to Parquet
                    pq.write_table(table, parquet_path)
                    
                    logging.info(f"Converted {file_info['csv']} to {file_info['parquet']} with {len(df)} rows")
                else:
                    logging.error(f"No date column found in {file_info['csv']}")
            except Exception as e:
                logging.error(f"Error converting {file_info['csv']}: {e}")
        else:
            logging.warning(f"File {csv_path} does not exist, skipping")
    
    logging.info("Completed macro data conversion")

def convert_market_data():
    """Convert stock price and market cap data to Parquet format"""
    logging.info("Converting market data to Parquet format")

    # First, let's check for the actual historical ticker data files
    price_path = os.path.join(DATA_DIR, "historical_ticker_prices.csv")
    marketcap_path = os.path.join(DATA_DIR, "historical_ticker_marketcap.csv")
    
    if os.path.exists(price_path) and os.path.exists(marketcap_path):
        logging.info("Found historical ticker data files")
        
        try:
            # Read the price and market cap data
            price_df = pd.read_csv(price_path, index_col=0)
            marketcap_df = pd.read_csv(marketcap_path, index_col=0)
            
            # Convert index to datetime if it's not already
            price_df.index = pd.to_datetime(price_df.index)
            marketcap_df.index = pd.to_datetime(marketcap_df.index)
            
            # Make sure they have the same indices
            common_dates = price_df.index.intersection(marketcap_df.index)
            price_df = price_df.loc[common_dates]
            marketcap_df = marketcap_df.loc[common_dates]
            
            # Get list of all tickers
            all_tickers = list(set(price_df.columns) | set(marketcap_df.columns))
            logging.info(f"Found {len(all_tickers)} unique tickers across both files")
            
            # Convert wide format to long format for easier partitioning
            price_long = price_df.reset_index().melt(
                id_vars=['index'], 
                value_vars=price_df.columns,
                var_name='ticker',
                value_name='price'
            ).rename(columns={'index': 'date'})
            
            marketcap_long = marketcap_df.reset_index().melt(
                id_vars=['index'], 
                value_vars=marketcap_df.columns,
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
            
            # Get the last date with data for each ticker
            latest_data = merged_df.groupby('ticker')['date'].max().reset_index()
            latest_data = latest_data.rename(columns={'date': 'latest_date'})
            
            # Merge the latest date information back
            merged_df = pd.merge(merged_df, latest_data, on='ticker', how='left')
            
            # Flag rows that have the latest data for each ticker
            merged_df['is_latest'] = merged_df['date'] == merged_df['latest_date']
            
            # Remove the latest_date column
            merged_df = merged_df.drop(columns=['latest_date'])
            
            # Convert to PyArrow table
            table = pa.Table.from_pandas(merged_df)
            
            # Add schema version metadata
            table = add_schema_version(table)
            
            # Write to Parquet, partitioned by ticker for efficient queries
            pq.write_to_dataset(
                table, 
                root_path=os.path.join(MARKET_DIR, "ticker_data"), 
                partition_cols=['ticker']
            )
            
            logging.info(f"Converted historical ticker data to Parquet dataset with {len(merged_df)} rows across {len(all_tickers)} tickers")
            return  # Early return since we successfully converted the data
        except Exception as e:
            logging.error(f"Error converting historical ticker data: {e}")
            logging.exception("Exception details:")
    
    # If we get here, we need to try the alternative files
    # Handle the summary ticker data files
    try:
        # Check if the main ticker history file exists (first in data dir, then in root)
        ticker_file = "T2D_Pulse_Full_Ticker_History.csv"
        ticker_path = os.path.join(DATA_DIR, ticker_file)
        
        # If not in data dir, check root directory
        if not os.path.exists(ticker_path):
            ticker_path = ticker_file
        
        if os.path.exists(ticker_path):
            # Read the ticker data
            df = pd.read_csv(ticker_path)
            
            # Normalize column names
            df.columns = [col.lower() for col in df.columns]
            
            # Make sure we have the necessary columns
            required_cols = ['date', 'ticker']
            missing_cols = [col for col in required_cols if col not in df.columns]
            
            if not missing_cols:
                # Convert date to datetime
                df['date'] = pd.to_datetime(df['date'])
                
                # Remove duplicates
                df = df.drop_duplicates(subset=['date', 'ticker'])
                
                # Sort by date and ticker
                df = df.sort_values(['date', 'ticker'])
                
                # Convert to PyArrow table
                table = pa.Table.from_pandas(df)
                
                # Add schema version metadata
                table = add_schema_version(table)
                
                # Write to Parquet, partitioned by ticker for efficient queries
                pq.write_to_dataset(
                    table, 
                    root_path=os.path.join(MARKET_DIR, "summary"), 
                    partition_cols=['ticker']
                )
                
                logging.info(f"Converted {ticker_file} to Parquet dataset with {len(df)} rows across {df['ticker'].nunique()} tickers")
            else:
                logging.error(f"Missing required columns in {ticker_file}: {missing_cols}")
        else:
            # Try alternate ticker history files
            alternate_files = [
                "T2D_Pulse_93_tickers_coverage.csv",
                "T2D_Pulse_updated_coverage.csv",
                "T2D_Pulse_final_coverage.csv"
            ]
            
            for alt_file in alternate_files:
                alt_path = alt_file  # Check in root directory
                
                if os.path.exists(alt_path):
                    logging.info(f"Found alternate ticker file: {alt_path}")
                    
                    try:
                        # Let's check the file content for debugging
                        with open(alt_path, 'r') as f:
                            sample_lines = f.readlines()[:15]  # Get the first 15 lines
                            
                        logging.info(f"Sample of {alt_path} content:")
                        for i, line in enumerate(sample_lines):
                            logging.info(f"Line {i}: {line.strip()}")
                            
                        # Extract only the header row and data
                        header_line = None
                        data_start_line = None
                        
                        for i, line in enumerate(sample_lines):
                            if "Date,Ticker," in line:
                                header_line = i
                                data_start_line = i + 1
                                break
                        
                        if header_line is None:
                            logging.warning(f"Could not find header line in {alt_path}")
                            continue
                            
                        # Read the file using pandas, skipping the header info
                        logging.info(f"Reading {alt_path} from line {header_line}")
                        
                        # Read the file as a text file to handle the irregular format
                        with open(alt_path, 'r') as f:
                            all_lines = f.readlines()
                            
                        # Extract the header and data section
                        header = all_lines[header_line].strip()
                        data_lines = [all_lines[header_line]] + all_lines[data_start_line:]
                        
                        # Write to a temporary file that pandas can read cleanly
                        import tempfile
                        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as tmp:
                            tmp.writelines(data_lines)
                            tmp_filename = tmp.name
                        
                        # Read the data with pandas
                        try:
                            df = pd.read_csv(tmp_filename)
                            logging.info(f"Successfully read {len(df)} rows from {alt_path}")
                            
                            # Clean up temp file
                            os.unlink(tmp_filename)
                        except Exception as e:
                            logging.error(f"Error reading extracted data from {alt_path}: {e}")
                            # Clean up temp file
                            os.unlink(tmp_filename)
                            continue
                        
                        # Normalize column names
                        df.columns = [col.lower() for col in df.columns]
                        
                        # Check if this is a valid ticker history file with prices
                        if 'date' in df.columns and 'ticker' in df.columns:
                            # Convert date to datetime
                            df['date'] = pd.to_datetime(df['date'])
                            
                            # Remove duplicates
                            df = df.drop_duplicates(subset=['date', 'ticker'])
                            
                            # Sort by date and ticker
                            df = df.sort_values(['date', 'ticker'])
                            
                            # Convert to PyArrow table
                            table = pa.Table.from_pandas(df)
                            
                            # Add schema version metadata
                            table = add_schema_version(table)
                            
                            # Write to Parquet, partitioned by ticker for efficient queries
                            pq.write_to_dataset(
                                table, 
                                root_path=os.path.join(MARKET_DIR, "prices"), 
                                partition_cols=['ticker']
                            )
                            
                            logging.info(f"Converted alternate file {alt_file} to Parquet dataset with {len(df)} rows across {df['ticker'].nunique()} tickers")
                            break
                        else:
                            logging.warning(f"File {alt_path} doesn't have required date and ticker columns")
                    except Exception as alt_e:
                        logging.error(f"Error processing alternate file {alt_path}: {alt_e}")
            else:
                logging.warning(f"No suitable ticker history file found")
    except Exception as e:
        logging.error(f"Error converting market data: {e}")
    
    logging.info("Completed market data conversion")

def convert_derived_data():
    """Convert derived data including sector scores to Parquet format"""
    logging.info("Converting derived data to Parquet format")
    
    # List of derived data files to convert
    derived_files = [
        {"csv": "sector_30day_history.csv", "parquet": "sector_history.parquet"},
        {"csv": "authentic_sector_history.csv", "parquet": "authentic_sector_history.parquet"},
        {"csv": "definitive_sector_scores.csv", "parquet": "definitive_sector_scores.parquet"}
    ]
    
    for file_info in derived_files:
        csv_path = os.path.join(DATA_DIR, file_info["csv"])
        parquet_path = os.path.join(DERIVED_DIR, file_info["parquet"])
        
        if os.path.exists(csv_path):
            try:
                # Read CSV file
                df = pd.read_csv(csv_path)
                
                # Ensure date column is properly named
                date_col = None
                for col in df.columns:
                    if col.lower() == 'date':
                        date_col = col
                        break
                
                if date_col:
                    # Rename to standardized 'date' if needed
                    if date_col != 'date':
                        df = df.rename(columns={date_col: 'date'})
                    
                    # Convert to datetime if not already
                    df['date'] = pd.to_datetime(df['date'])
                    
                    # Sort by date
                    df = df.sort_values('date')
                    
                    # Remove duplicates based on date
                    df = df.drop_duplicates(subset=['date'])
                    
                    # Convert to PyArrow table
                    table = pa.Table.from_pandas(df)
                    
                    # Add schema version metadata
                    table = add_schema_version(table)
                    
                    # Write to Parquet
                    pq.write_table(table, parquet_path)
                    
                    logging.info(f"Converted {file_info['csv']} to {file_info['parquet']} with {len(df)} rows")
                else:
                    logging.error(f"No date column found in {file_info['csv']}")
            except Exception as e:
                logging.error(f"Error converting {file_info['csv']}: {e}")
        else:
            logging.warning(f"File {csv_path} does not exist, skipping")
    
    logging.info("Completed derived data conversion")

def run_conversion():
    """Run the full data conversion process"""
    start_time = datetime.now()
    logging.info(f"Starting data conversion at {start_time}")
    
    # Make sure directories exist
    os.makedirs(MACRO_DIR, exist_ok=True)
    os.makedirs(MARKET_DIR, exist_ok=True)
    os.makedirs(DERIVED_DIR, exist_ok=True)
    
    # Run conversions
    convert_macro_data()
    convert_market_data()
    convert_derived_data()
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    logging.info(f"Data conversion completed in {duration:.2f} seconds")

if __name__ == "__main__":
    run_conversion()