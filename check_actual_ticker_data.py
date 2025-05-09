#!/usr/bin/env python3
import pandas as pd
import numpy as np

print("Checking the actual ticker data files for null values...\n")

# Load the CSV files that have the authoritative data
print("Loading T2D_Pulse_Full_Ticker_History.csv:")
ticker_history = pd.read_csv('T2D_Pulse_Full_Ticker_History.csv', skiprows=8)
print(f"- Shape: {ticker_history.shape}")
print(f"- Columns: {list(ticker_history.columns)}")
print(f"- Number of tickers with 'Complete' status: {len(ticker_history[ticker_history['Data Status'] == 'Complete'])}")

# Now check the more detailed ticker data files
print("\nChecking the raw ticker data files:")
try:
    # Load the data files properly
    price_df = pd.read_csv('recent_price_data.csv')
    mcap_df = pd.read_csv('recent_marketcap_data.csv')
    
    # First column is typically the date
    date_col = price_df.columns[0]
    
    # Create actual dataframes with proper indexing
    if date_col == '' or 'Unnamed' in date_col:
        price_df = price_df.iloc[:, 1:]  # Skip the first column
        price_df.index = [f"Day {i+1}" for i in range(len(price_df))]
        
        mcap_df = mcap_df.iloc[:, 1:]  # Skip the first column
        mcap_df.index = [f"Day {i+1}" for i in range(len(mcap_df))]
    
    print(f"\nPrice data shape after cleaning: {price_df.shape}")
    print(f"Market cap data shape after cleaning: {price_df.shape}")
    
    # Check for NaN values
    price_null_count = price_df.isnull().sum().sum()
    mcap_null_count = mcap_df.isnull().sum().sum()
    
    print(f"\nPrice data NaN count: {price_null_count}")
    print(f"Market cap data NaN count: {mcap_null_count}")
    
    # Check for empty strings or 0 values which might be mistaken for valid data
    price_empty_count = (price_df == '').sum().sum() if price_df.dtypes.iloc[0] == 'object' else 0
    mcap_empty_count = (mcap_df == '').sum().sum() if mcap_df.dtypes.iloc[0] == 'object' else 0
    
    print(f"Price data empty string count: {price_empty_count}")
    print(f"Market cap data empty string count: {mcap_empty_count}")
    
    # Check specifically for our key tickers
    key_tickers = ['YELP', 'XYZ', 'FI']
    print("\nKey ticker check:")
    
    for ticker in key_tickers:
        if ticker in price_df.columns:
            null_count = price_df[ticker].isnull().sum()
            print(f"- {ticker} price null count: {null_count}")
            if null_count == 0:
                print(f"  {ticker} price data range: {price_df[ticker].min()} to {price_df[ticker].max()}")
        else:
            print(f"- {ticker} not found in price data columns!")
            
        if ticker in mcap_df.columns:
            null_count = mcap_df[ticker].isnull().sum()
            print(f"- {ticker} market cap null count: {null_count}")
            if null_count == 0:
                print(f"  {ticker} market cap data range: {mcap_df[ticker].min()} to {mcap_df[ticker].max()}")
        else:
            print(f"- {ticker} not found in market cap data columns!")
    
    # Check for columns that might be all NaN (these would be counted as missing ticker data)
    all_nan_price_cols = [col for col in price_df.columns if price_df[col].isnull().all()]
    all_nan_mcap_cols = [col for col in mcap_df.columns if mcap_df[col].isnull().all()]
    
    print(f"\nColumns with all NaN values:")
    print(f"- Price data: {len(all_nan_price_cols)} columns are all NaN")
    print(f"- Market cap data: {len(all_nan_mcap_cols)} columns are all NaN")
    
    if all_nan_price_cols:
        print(f"  Price columns that are all NaN: {', '.join(all_nan_price_cols[:5])}{'...' if len(all_nan_price_cols) > 5 else ''}")
    if all_nan_mcap_cols:
        print(f"  Market cap columns that are all NaN: {', '.join(all_nan_mcap_cols[:5])}{'...' if len(all_nan_mcap_cols) > 5 else ''}")
    
    # List actual valid tickers with data
    valid_price_tickers = [col for col in price_df.columns if not price_df[col].isnull().all()]
    valid_mcap_tickers = [col for col in mcap_df.columns if not mcap_df[col].isnull().all()]
    
    print(f"\nValid tickers with data:")
    print(f"- Price data: {len(valid_price_tickers)} valid tickers")
    print(f"- Market cap data: {len(valid_mcap_tickers)} valid tickers")
    
    # Check intersection with required tickers
    complete_tickers = set(ticker_history[ticker_history['Data Status'] == 'Complete']['Ticker'])
    print(f"\nRequired tickers from T2D_Pulse_Full_Ticker_History.csv: {len(complete_tickers)}")
    
    price_coverage = set(valid_price_tickers).intersection(complete_tickers)
    mcap_coverage = set(valid_mcap_tickers).intersection(complete_tickers)
    
    print(f"- Complete tickers with valid price data: {len(price_coverage)}/{len(complete_tickers)}")
    print(f"- Complete tickers with valid market cap data: {len(mcap_coverage)}/{len(complete_tickers)}")
    
    # List missing tickers
    missing_price = complete_tickers - set(valid_price_tickers)
    missing_mcap = complete_tickers - set(valid_mcap_tickers)
    
    if missing_price:
        print(f"\nTickers missing from price data: {missing_price}")
    if missing_mcap:
        print(f"Tickers missing from market cap data: {missing_mcap}")
        
    # Final conclusion
    all_price_present = len(missing_price) == 0
    all_mcap_present = len(missing_mcap) == 0
    full_coverage = all_price_present and all_mcap_present
    
    print(f"\nFINAL CONCLUSION: {full_coverage}")
    print(f"- All tickers have price data: {all_price_present}")
    print(f"- All tickers have market cap data: {all_mcap_present}")
    
except Exception as e:
    print(f"Error in analysis: {e}")
