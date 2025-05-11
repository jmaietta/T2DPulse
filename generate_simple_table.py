import csv
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

# Key sectors to include (to keep table width manageable)
key_sectors = ['AI Infrastructure', 'Cloud Infrastructure', 'Enterprise SaaS', 
               'Semiconductors', 'Consumer Internet', 'Fintech']

# Print the table header
print("30-Day Market Cap History for Key Sectors (Billions USD)")
print("=" * 100)

# Print column headers
header = f"{'Date':<12}"
for sector in key_sectors:
    short_name = sector.split()[0][:8]  # First word, up to 8 chars
    header += f"{short_name:>12}"
print(header)
print("-" * 100)

# Print data rows
for date in recent_dates:
    row = f"{date:<12}"
    for sector in key_sectors:
        value = data[date].get(sector, 0)
        row += f"{value:>12.2f}"
    print(row)

# Also create another table with the latest market cap data
print("\n\nLatest Market Cap Data (May 11, 2025):")
print("=" * 50)

# Load the latest data
latest_data = []
with open('sector_market_caps.csv', 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        if row['date'] == '2025-05-11':
            latest_data.append({
                'sector': row['sector'],
                'market_cap': float(row['market_cap']) / 1e9  # Convert to billions
            })

# Sort by market cap descending
latest_data.sort(key=lambda x: x['market_cap'], reverse=True)

# Print the latest data
print(f"{'Sector':<30}{'Market Cap ($B)':>15}")
print("-" * 50)
for item in latest_data:
    print(f"{item['sector']:<30}{item['market_cap']:>15,.2f}")