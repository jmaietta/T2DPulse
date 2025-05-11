import pandas as pd

# Load the data
df = pd.read_csv('historical_sector_market_caps.csv')

# Convert market_cap to billions and round to 2 decimal places
df['market_cap'] = round(df['market_cap'] / 1e9, 2)

# Get the last 30 days of data
last_30_days = sorted(df['date'].unique())[-30:]
recent_data = df[df['date'].isin(last_30_days)]

# Limit to just a few key sectors to make table width manageable
key_sectors = [
    'AI Infrastructure', 
    'Cloud Infrastructure', 
    'Enterprise SaaS', 
    'Semiconductors', 
    'Consumer Internet',
    'Fintech'
]

# Pivot the data to get sectors as columns and dates as rows
filtered_data = recent_data[recent_data['sector'].isin(key_sectors)]
pivot_df = filtered_data.pivot(index='date', columns='sector', values='market_cap')

# Print properly formatted table
print('30-Day Market Cap History for Key Sectors (Billions USD)')
print('=' * 80)
print(f"{'Date':<10} | {'AI Infra':>10} | {'Cloud Infra':>10} | {'Enterprise':>10} | {'Semicon':>10} | {'Consumer':>10} | {'Fintech':>10}")
print('-' * 80)

for date in pivot_df.index:
    ai = pivot_df.loc[date, 'AI Infrastructure']
    cloud = pivot_df.loc[date, 'Cloud Infrastructure']
    enterprise = pivot_df.loc[date, 'Enterprise SaaS']
    semicon = pivot_df.loc[date, 'Semiconductors']
    consumer = pivot_df.loc[date, 'Consumer Internet']
    fintech = pivot_df.loc[date, 'Fintech']
    
    print(f"{date:<10} | {ai:>10.2f} | {cloud:>10.2f} | {enterprise:>10.2f} | {semicon:>10.2f} | {consumer:>10.2f} | {fintech:>10.2f}")