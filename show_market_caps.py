#!/usr/bin/env python3
"""
Show the 30-day market cap data for all sectors in a readable format.
"""
import pandas as pd

# Load the full data
df = pd.read_csv('data/sector_market_caps.csv')

# Extract just the columns we want (date and sector market caps, not weights)
columns_to_keep = ['Unnamed: 0']  # Date column
for col in df.columns:
    if col not in ['Unnamed: 0', 'Total'] and not col.endswith('_weight_pct'):
        columns_to_keep.append(col)

# Select columns and rename 'Unnamed: 0' to 'Date'
df = df[columns_to_keep]
df.columns = ['Date'] + [col for col in df.columns if col != 'Unnamed: 0']

# Format market caps in trillions
for col in df.columns[1:]:
    df[col] = (df[col] / 1e12).round(2).astype(str) + 'T'

# Print in a nicely formatted table
print(df.to_string(index=False))