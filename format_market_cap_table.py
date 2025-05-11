import csv
from tabulate import tabulate
import pandas as pd
from collections import defaultdict

# Load the data from CSV
data = defaultdict(dict)
sectors = set()
dates = []

with open('historical_sector_market_caps.csv', 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        date = row['date']
        sector = row['sector']
        market_cap = float(row['market_cap']) / 1e9  # Convert to billions
        
        data[date][sector] = round(market_cap, 2)
        sectors.add(sector)
        if date not in dates:
            dates.append(date)

# Sort dates and get the last 30
dates.sort()
recent_dates = dates[-30:]

# Create a list of lists for tabulate
table_data = []
key_sectors = ['AI Infrastructure', 'Cloud Infrastructure', 'Enterprise SaaS', 
               'Semiconductors', 'Consumer Internet', 'Fintech']

# Headers
headers = ['Date'] + key_sectors

# Data rows
for date in recent_dates:
    row = [date]
    for sector in key_sectors:
        row.append(data[date].get(sector, 'N/A'))
    table_data.append(row)

# Print the table
print(tabulate(table_data, headers=headers, tablefmt="grid", numalign="right", floatfmt=".2f"))

# Also create another table with the latest market cap data
print("\n\nLatest Market Cap Data (May 11, 2025):")
print("=" * 65)

# Load the latest data
with open('sector_market_caps.csv', 'r') as f:
    reader = csv.DictReader(f)
    latest_data = []
    for row in reader:
        if row['date'] == '2025-05-11':
            latest_data.append({
                'sector': row['sector'],
                'market_cap': float(row['market_cap']) / 1e9  # Convert to billions
            })

# Sort by market cap descending
latest_data.sort(key=lambda x: x['market_cap'], reverse=True)

# Create a table for the latest data
latest_table = []
for item in latest_data:
    latest_table.append([item['sector'], item['market_cap']])

# Print the table
print(tabulate(latest_table, headers=['Sector', 'Market Cap (Billions USD)'], 
               tablefmt="simple", numalign="right", floatfmt=".2f"))