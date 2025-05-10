"""
Print market cap data in a clean, easy-to-copy format.
"""

import pandas as pd
import json

# Try to load the CSV file
try:
    df = pd.read_csv('sector_marketcap_table.csv')
    print("Market Cap Data (CSV format):")
    print("=============================")
    print(df.to_string())
    print("\n\n")
except Exception as e:
    print(f"Error loading CSV: {e}")

# Try to load the TXT file and print it directly
try:
    with open('30day_sector_marketcap_table.txt', 'r') as f:
        txt_data = f.read()
    print("Market Cap Data (Text format):")
    print("==============================")
    print(txt_data)
    print("\n\n")
except Exception as e:
    print(f"Error loading TXT: {e}")

# Try to load the JSON file
try:
    with open('complete_market_cap_data.json', 'r') as f:
        json_data = json.load(f)
    print("Market Cap Data (JSON format sample - first entry):")
    print("==================================================")
    print(json.dumps(json_data[0] if isinstance(json_data, list) else next(iter(json_data.values())), indent=2))
    print("\n\n")
except Exception as e:
    print(f"Error loading JSON: {e}")

# Print information about sectors and dates
try:
    df = pd.read_csv('sector_marketcap_table.csv')
    print("Available Sectors:")
    print("=================")
    for col in df.columns[1:]:  # Skip the date column
        print(f"- {col}")
    
    print("\nDate Range:")
    print("===========")
    print(f"Start: {df['Date'].iloc[0]}")
    print(f"End: {df['Date'].iloc[-1]}")
    print(f"Total Days: {len(df)}")
except Exception as e:
    print(f"Error analyzing data: {e}")