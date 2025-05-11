import pandas as pd

# Load the data
df = pd.read_csv('historical_sector_market_caps.csv')

# Convert market_cap to billions and round to 2 decimal places
df['market_cap'] = round(df['market_cap'] / 1e9, 2)

# Get the last 30 days of data
last_30_days = sorted(df['date'].unique())[-30:]
recent_data = df[df['date'].isin(last_30_days)]

# Pivot the data to get sectors as columns and dates as rows
pivot_df = recent_data.pivot(index='date', columns='sector', values='market_cap')

# Save to Excel
pivot_df.to_excel('30day_sector_marketcap_history.xlsx', sheet_name='Market Cap History')

# Add current market cap data as a second sheet
current_df = pd.read_csv('sector_market_caps.csv')
current_df['market_cap'] = round(current_df['market_cap'] / 1e9, 2)
current_date = sorted(current_df['date'].unique())[-1]
current_data = current_df[current_df['date'] == current_date].copy()
current_data.sort_values('market_cap', ascending=False, inplace=True)

# Create a new Excel writer
with pd.ExcelWriter('30day_sector_marketcap_analysis.xlsx', engine='openpyxl') as writer:
    # Write the historical data
    pivot_df.to_excel(writer, sheet_name='30-Day History')
    
    # Write the current data
    current_data.to_excel(writer, sheet_name='Current Market Caps')

print("Excel files have been created:")
print("1. 30day_sector_marketcap_history.xlsx - Simple historical data")
print("2. 30day_sector_marketcap_analysis.xlsx - Contains multiple sheets with analysis")