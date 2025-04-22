import nasdaqdatalink
import pandas as pd
import os
import requests
import json
from datetime import datetime, timedelta

# Set the API key from environment variable
api_key = os.environ.get('NASDAQ_DATA_LINK_API_KEY')
if api_key:
    print(f"Using NASDAQ Data Link API key found in environment")
    nasdaqdatalink.ApiConfig.api_key = api_key
else:
    print("Warning: No NASDAQ_DATA_LINK_API_KEY found in environment")

print(f"API Configuration: {nasdaqdatalink.ApiConfig.api_key[:5]}... (first 5 chars shown)")

# Try specific datasets known to contain Treasury yield data
print("\nTesting specific Treasury yield datasets from NASDAQ Data Link...")

# List of datasets to try
treasury_datasets = [
    "USTREASURY/YIELD",  # US Treasury Yield Curve Rates
    "FRED/DGS10",        # 10-Year Treasury Constant Maturity Rate
    "FRED/GS10",         # 10-Year Treasury Constant Maturity Rate (older series)
    "USTREASURY/LONGTERMRATES"  # Long-Term Treasury Rates
]

# Test each dataset
for dataset in treasury_datasets:
    try:
        print(f"\nTrying dataset: {dataset}")
        data = nasdaqdatalink.get(dataset, rows=5)
        
        print(f"Successfully accessed {dataset}")
        print("Sample data:")
        print(data.head())
        print(f"Columns: {data.columns.tolist()}")
        
        # Create a formatted version for our application
        # For datasets with the 10-year yield in a specific column
        if dataset == "USTREASURY/YIELD" and "10 Yr" in data.columns:
            ten_year_data = data[["10 Yr"]].copy()
            ten_year_data.rename(columns={"10 Yr": "value"}, inplace=True)
            ten_year_data = ten_year_data.reset_index()
            ten_year_data.rename(columns={"Date": "date"}, inplace=True)
            
            print("Formatted for dashboard:")
            print(ten_year_data.head())
            
            # Save to CSV for verification
            sample_file = "nasdaq_10yr_treasury_sample.csv"
            ten_year_data.to_csv(sample_file, index=False)
            print(f"Saved sample to {sample_file}")
            
        # For datasets with a single value column (like FRED/DGS10)
        elif dataset in ["FRED/DGS10", "FRED/GS10"]:
            # These datasets typically have a single 'Value' column
            formatted_data = pd.DataFrame({
                'date': data.index,
                'value': data['Value']
            })
            formatted_data = formatted_data.reset_index(drop=True)
            
            print("Formatted for dashboard:")
            print(formatted_data.head())
            
            # Save to CSV for verification
            sample_file = f"{dataset.replace('/', '_')}_sample.csv"
            formatted_data.to_csv(sample_file, index=False)
            print(f"Saved sample to {sample_file}")
            
    except Exception as e:
        print(f"Error accessing {dataset}: {str(e)}")

# Try a direct HTTP request to the NASDAQ Data Link API
# This is a backup approach if the Python SDK has issues
print("\n\nTrying direct API access for Treasury yield data...")

try:
    api_url = f"https://data.nasdaq.com/api/v3/datasets/FRED/DGS10?api_key={api_key}&rows=5"
    print("Sending direct HTTP request to NASDAQ Data Link API...")
    
    response = requests.get(api_url)
    
    if response.status_code == 200:
        print("API request successful!")
        data = response.json()
        
        if 'dataset' in data and 'data' in data['dataset']:
            print(f"Retrieved {len(data['dataset']['data'])} rows of data")
            print("\nRaw data sample:")
            for row in data['dataset']['data'][:3]:
                print(row)
                
            # Format the data for our dashboard
            formatted_data = []
            for row in data['dataset']['data']:
                formatted_data.append({
                    'date': row[0],
                    'value': row[1] if row[1] is not None else float('nan')
                })
            
            # Convert to DataFrame and save to CSV
            df = pd.DataFrame(formatted_data)
            print("\nFormatted data for dashboard:")
            print(df.head())
            
            sample_file = "direct_api_treasury_sample.csv"
            df.to_csv(sample_file, index=False)
            print(f"Saved direct API sample to {sample_file}")
        else:
            print("API response doesn't contain expected dataset structure")
    else:
        print(f"API request failed with status code: {response.status_code}")
        print(f"Response: {response.text}")
        
except Exception as e:
    print(f"Error with direct API request: {str(e)}")
    
print("\n\nIf all attempts fail, we'll continue using Yahoo Finance as our data source.")