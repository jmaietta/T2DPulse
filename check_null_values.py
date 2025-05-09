#!/usr/bin/env python3
import pandas as pd

# Load the data files
price_df = pd.read_csv('recent_price_data.csv')
mcap_df = pd.read_csv('recent_marketcap_data.csv')

# Convert the first column to index if it's not already
if price_df.columns[0] == '' or price_df.columns[0] == 'Unnamed: 0':
    price_df.set_index(price_df.columns[0], inplace=True)
if mcap_df.columns[0] == '' or mcap_df.columns[0] == 'Unnamed: 0':
    mcap_df.set_index(mcap_df.columns[0], inplace=True)

# Identify columns with null values
price_nulls_by_ticker = price_df.isnull().sum()
mcap_nulls_by_ticker = mcap_df.isnull().sum()

# Count missing values
price_nulls_count = price_nulls_by_ticker.sum()
mcap_nulls_count = mcap_nulls_by_ticker.sum()

# Print null values by ticker
print(f"MISSING PRICE DATA BY TICKER:")
for ticker, nulls in price_nulls_by_ticker[price_nulls_by_ticker > 0].items():
    print(f"- {ticker}: {nulls} missing values")

print(f"\nMISSING MARKET CAP DATA BY TICKER:")
for ticker, nulls in mcap_nulls_by_ticker[mcap_nulls_by_ticker > 0].items():
    print(f"- {ticker}: {nulls} missing values")

# Now specifically check our key tickers
key_tickers = ['YELP', 'XYZ', 'FI']
print(f"\nDETAILED CHECK OF KEY TICKERS:")
for ticker in key_tickers:
    if ticker in price_df.columns:
        price_nulls = price_df[ticker].isnull().sum()
        price_complete = "YES" if price_nulls == 0 else f"NO ({price_nulls} missing)"
        print(f"{ticker} price data complete: {price_complete}")
    else:
        print(f"{ticker} price data: TICKER NOT FOUND")
        
    if ticker in mcap_df.columns:
        mcap_nulls = mcap_df[ticker].isnull().sum()
        mcap_complete = "YES" if mcap_nulls == 0 else f"NO ({mcap_nulls} missing)"
        print(f"{ticker} market cap data complete: {mcap_complete}")
    else:
        print(f"{ticker} market cap data: TICKER NOT FOUND")

# Check if we have the required 93 tickers
official_ticker_count = 93  # The total number required
print(f"\nDo we have data for all {official_ticker_count} tickers?")
print(f"- Price data contains {len(price_df.columns)} tickers")
print(f"- Market cap data contains {len(mcap_df.columns)} tickers")

# Get number of complete tickers (with no nulls)
complete_price_tickers = sum(price_df.isnull().sum() == 0)
complete_mcap_tickers = sum(mcap_df.isnull().sum() == 0)
print(f"- Price data complete for {complete_price_tickers} tickers")
print(f"- Market cap data complete for {complete_mcap_tickers} tickers")

# Overall assessment
missing_price = price_df.isnull().sum().sum()
missing_mcap = mcap_df.isnull().sum().sum()
total_price_cells = price_df.size
total_mcap_cells = mcap_df.size
price_completion = 100 * (1 - missing_price / total_price_cells)
mcap_completion = 100 * (1 - missing_mcap / total_mcap_cells)

print(f"\nOVERALL DATA ASSESSMENT:")
print(f"- Days of data: {len(price_df)} (expected: 30)")
print(f"- Price data completion: {price_completion:.2f}%")
print(f"- Market cap data completion: {mcap_completion:.2f}%")
print(f"- Data coverage for price: {missing_price} missing values out of {total_price_cells} data points")
print(f"- Data coverage for market cap: {missing_mcap} missing values out of {total_mcap_cells} data points")

# Check data from T2D_Pulse_Full_Ticker_History.csv
try:
    ticker_history = pd.read_csv('T2D_Pulse_Full_Ticker_History.csv')
    print(f"\nT2D_Pulse_Full_Ticker_History.csv ANALYSIS:")
    print(f"- Contains {len(ticker_history)} rows")
    
    # Check for key columns
    expected_columns = ['Ticker', 'Sector', 'Price', 'Market Cap (B)', 'Market Cap (100B)', 
                       'Price Days', 'Price Coverage', 'MCap Days', 'MCap Coverage', 'Status']
    missing_columns = [col for col in expected_columns if col not in ticker_history.columns]
    
    if missing_columns:
        print(f"- Missing columns: {', '.join(missing_columns)}")
    else:
        print("- All expected columns present")
        
        # Show a few rows of data
        print("\nSample data (first 5 rows):")
        print(ticker_history[['Ticker', 'Sector', 'Price', 'Market Cap (B)', 'Price Coverage', 'MCap Coverage', 'Status']].head().to_string())
        
        # Check for complete coverage
        complete_rows = ticker_history[ticker_history['Status'] == 'Complete']
        print(f"\n- Tickers with complete data: {len(complete_rows)} out of {len(ticker_history)}")
        
        # Check key tickers
        key_tickers_df = ticker_history[ticker_history['Ticker'].isin(key_tickers)]
        if not key_tickers_df.empty:
            print("\nKey tickers information:")
            print(key_tickers_df[['Ticker', 'Sector', 'Price', 'Market Cap (B)', 'Price Coverage', 'MCap Coverage', 'Status']].to_string())
        else:
            print("\nKey tickers not found in the history file.")
except Exception as e:
    print(f"Error analyzing T2D_Pulse_Full_Ticker_History.csv: {e}")
