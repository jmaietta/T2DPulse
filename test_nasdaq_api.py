import nasdaqdatalink
import pandas as pd
import os
from datetime import datetime, timedelta

# Set the API key from environment variable
api_key = os.environ.get('NASDAQ_DATA_LINK_API_KEY')
if api_key:
    print(f"Using NASDAQ Data Link API key found in environment")
    nasdaqdatalink.ApiConfig.api_key = api_key
else:
    print("Warning: No NASDAQ_DATA_LINK_API_KEY found in environment")

print(f"API Configuration: {nasdaqdatalink.ApiConfig.api_key[:5]}... (first 5 chars shown)")

# Try different Treasury datasets
datasets_to_test = [
    "ML/USTSY10", # MarketWatch 10-Year Treasury Yield
    "USTREASURY/YIELD", # US Treasury Yield Curve
    "FRED/DGS10", # 10-Year Treasury Constant Maturity Rate
    "FRED/GS10", # 10-Year Treasury Constant Maturity Rate (older series)
]

print("Testing different Treasury yield datasets from NASDAQ Data Link...")

for dataset in datasets_to_test:
    try:
        print(f"\n\nAttempting to access {dataset}...")
        test_data = nasdaqdatalink.get(dataset, rows=5)
        print(f"Successfully accessed {dataset}")
        print("Data sample:")
        print(test_data.head())
        print(f"Columns: {test_data.columns.tolist()}")
        
        # If we found a dataset that works, try to format it to match our needs
        if dataset == "FRED/DGS10" or dataset == "FRED/GS10" or dataset == "ML/USTSY10":
            # These datasets should have a single value column with the yield
            if 'Value' in test_data.columns:
                formatted_data = test_data.copy()
                formatted_data.rename(columns={'Value': 'value'}, inplace=True)
                formatted_data = formatted_data.reset_index()
                formatted_data.rename(columns={'Date': 'date'}, inplace=True)
                
                print("\nFormatted data for dashboard:")
                print(formatted_data.head())
                
                # Save sample to CSV
                sample_file = f"{dataset.replace('/', '_')}_sample.csv"
                formatted_data.to_csv(sample_file, index=False)
                print(f"Saved sample to {sample_file}")
        
        elif dataset == "USTREASURY/YIELD" and "10 Yr" in test_data.columns:
            # The Treasury Yield Curve dataset has multiple columns for different maturities
            ten_year_data = test_data[["10 Yr"]].copy()
            ten_year_data.rename(columns={"10 Yr": "value"}, inplace=True)
            ten_year_data = ten_year_data.reset_index()  # Convert index to column
            ten_year_data.rename(columns={"Date": "date"}, inplace=True)
            
            print("\nFormatted 10-Year Treasury data for dashboard:")
            print(ten_year_data.head())
            
            # Save sample to CSV
            sample_file = "treasury_yield_10yr_sample.csv"
            ten_year_data.to_csv(sample_file, index=False)
            print(f"Saved sample to {sample_file}")
            
    except Exception as e:
        print(f"Error accessing {dataset}: {str(e)}")

print("\n\nIf all datasets failed, we'll continue using Yahoo Finance as our data source.")