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

# Count days and tickers
price_days = len(price_df)
price_tickers = len(price_df.columns)
mcap_days = len(mcap_df)
mcap_tickers = len(mcap_df.columns)

# Check for any NULL values
price_nulls = price_df.isnull().sum().sum()
mcap_nulls = mcap_df.isnull().sum().sum()

# Print results
print(f"PRICE DATA:")
print(f"- Total days: {price_days}")
print(f"- Total tickers: {price_tickers}")
print(f"- Null values: {price_nulls}")
print(f"- Complete coverage: {price_days == 30 and price_nulls == 0}")

print(f"\nMARKET CAP DATA:")
print(f"- Total days: {mcap_days}")
print(f"- Total tickers: {mcap_tickers}")
print(f"- Null values: {mcap_nulls}")
print(f"- Complete coverage: {mcap_days == 30 and mcap_nulls == 0}")

# Calculate overall coverage percentage
required_tickers = 93
required_days = 30
price_coverage = (price_days / required_days) * min(price_tickers / required_tickers, 1.0) * 100
mcap_coverage = (mcap_days / required_days) * min(mcap_tickers / required_tickers, 1.0) * 100

print(f"\nOVERALL COVERAGE:")
print(f"- Price data coverage: {price_coverage:.2f}%")
print(f"- Market cap data coverage: {mcap_coverage:.2f}%")
print(f"- Complete 30-day coverage for all 93 tickers: {price_days == 30 and mcap_days == 30 and price_tickers >= required_tickers and mcap_tickers >= required_tickers and price_nulls == 0 and mcap_nulls == 0}")
