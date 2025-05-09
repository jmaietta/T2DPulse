#!/usr/bin/env python3
"""
create_temp_mcap_data.py
-------------------------------------------------
Generate temporary market cap data CSV file based on real sector weights
but with different daily values to show variation.

This script is a temporary measure until we can resolve the API rate limiting issues
with the Yahoo Finance API.
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Sector weights based on real data
SECTOR_WEIGHTS = {
    "SMB SaaS": 42.6,
    "Enterprise SaaS": 472.6,
    "Cloud Infrastructure": 5295.9,
    "AdTech": 1917.5, 
    "Fintech": 118.5,
    "Consumer Internet": 3416.0,
    "eCommerce": 2246.0,
    "Cybersecurity": 302.8,
    "Dev Tools / Analytics": 105.2,
    "Semiconductors": 4010.1,
    "AI Infrastructure": 3046.8,
    "Vertical SaaS": 69.1,
    "IT Services / Legacy Tech": 431.8,
    "Hardware / Devices": 3058.3
}

def generate_market_cap_data(days=30, base_date=None):
    """
    Generate market cap data for each sector with daily variations.
    
    Args:
        days (int): Number of days of data to generate
        base_date (datetime, optional): Base date for the data
        
    Returns:
        pd.DataFrame: Market cap data
    """
    if base_date is None:
        base_date = datetime.now()
    
    # Generate date range
    date_range = [base_date - timedelta(days=i) for i in range(days)]
    date_range.reverse()  # Put in ascending order
    
    # Create DataFrame
    df = pd.DataFrame(index=date_range)
    
    # Add data for each sector
    for sector, base_weight in SECTOR_WEIGHTS.items():
        # Generate variations - small random fluctuations from the base weight
        # Uses a random walk with small daily changes
        np.random.seed(int(base_weight))  # For reproducibility
        
        # Create a random walk with 0.5-2% daily variation
        daily_pct_changes = np.random.normal(0, 0.01, days)  # Mean 0, std 1%
        daily_multipliers = 1 + daily_pct_changes
        
        # Calculate cumulative effect (but in reverse to have latest value match base weight)
        cumulative_multipliers = np.cumprod(daily_multipliers[::-1])[::-1]
        
        # Apply to base weight to get time series
        values = base_weight * cumulative_multipliers
        
        # Make sure the latest value exactly matches the known weight
        latest_ratio = base_weight / values[-1]
        values = values * latest_ratio
        
        # Add to DataFrame
        df[sector] = values
    
    # Convert to billions (already in billions in SECTOR_WEIGHTS)
    df = df.round(1)
    
    return df

def save_market_cap_data(output_dir="data", parquet=True, csv=True):
    """
    Generate and save market cap data.
    
    Args:
        output_dir (str): Directory to save data
        parquet (bool): Whether to save as Parquet
        csv (bool): Whether to save as CSV
        
    Returns:
        bool: Success flag
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate data
    df = generate_market_cap_data(days=30)
    
    # Save data
    success = False
    
    if parquet:
        try:
            parquet_path = os.path.join(output_dir, "sector_market_caps.parquet")
            df.to_parquet(parquet_path, compression="zstd")
            print(f"Saved market cap data to {parquet_path}")
            success = True
        except Exception as e:
            print(f"Error saving Parquet file: {e}")
    
    if csv:
        try:
            csv_path = os.path.join(output_dir, "sector_market_caps.csv")
            df.to_csv(csv_path)
            print(f"Saved market cap data to {csv_path}")
            success = True
        except Exception as e:
            print(f"Error saving CSV file: {e}")
    
    # Print sample of data
    print("\n===== SECTOR MARKET CAP DATA (SAMPLE) =====")
    print(df.tail().to_string())
    
    return success

if __name__ == "__main__":
    save_market_cap_data()