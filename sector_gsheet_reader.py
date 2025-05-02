"""Sector data reader for T2D Pulse dashboard.

This module reads sector data from a published Google Sheet containing
sector-specific market performance data for the T2D Pulse dashboard.
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import requests
import json
import pytz

# Data directory for caching sheet data
DATA_DIR = 'data'
SECTOR_DATA_DIR = os.path.join(DATA_DIR, 'sector_data')
os.makedirs(SECTOR_DATA_DIR, exist_ok=True)

# Google Sheet published URL (to be set by user)
# The URL should be in the format: https://docs.google.com/spreadsheets/d/SHEET_ID/export?format=csv&gid=SHEET_GID
GOOGLE_SHEET_URL = ""

# Cache timeout in hours (how long to use cached data before refreshing)
CACHE_TIMEOUT_HOURS = 24

# Define sector names
SECTOR_NAMES = [
    "AdTech",
    "Cloud",
    "Fintech",
    "eCommerce",
    "Cybersecurity",
    "Dev Tools / Analytics",
    "Semiconductors",
    "AI Infrastructure",
    "Enterprise SaaS",
    "Vertical SaaS",
    "Consumer Internet",
    "Gaming",
    "IT Services / Legacy Tech",
    "Hardware / Devices"
]

# Mapping from spreadsheet sector names to T2D Pulse sector names (if they differ)
SECTOR_MAPPING = {
    "SMB SaaS": "Vertical SaaS",  # Map SMB SaaS to Vertical SaaS tickers
    "Enterprise SaaS": "Enterprise SaaS",
    "Cloud Infrastructure": "Cloud",
    "AdTech": "AdTech",
    "Fintech": "Fintech",
    "Consumer Internet": "Consumer Internet",
    "eCommerce": "eCommerce",
    "Cybersecurity": "Cybersecurity",
    "Dev Tools / Analytics": "Dev Tools / Analytics",
    "Semiconductors": "Semiconductors",
    "AI Infrastructure": "AI Infrastructure",
    "Vertical SaaS": "Vertical SaaS",
    "IT Services / Legacy Tech": "IT Services / Legacy Tech",
    "Hardware / Devices": "Hardware / Devices"
}

def set_google_sheet_url(url):
    """Set the Google Sheet URL to use for data fetching.
    
    Args:
        url (str): Published Google Sheet URL
    """
    global GOOGLE_SHEET_URL
    GOOGLE_SHEET_URL = url
    print(f"Set Google Sheet URL to: {url}")

def get_latest_trading_day():
    """Get the latest trading day (most recent weekday).
    
    Returns:
        datetime: Date representing the latest trading day
    """
    today = datetime.now(pytz.timezone('US/Eastern'))
    
    # If today is a weekend, adjust to the most recent Friday
    if today.weekday() > 4:  # Saturday or Sunday
        days_to_subtract = today.weekday() - 4  # Adjust to Friday
        today = today - timedelta(days=days_to_subtract)
        
    return today.replace(hour=0, minute=0, second=0, microsecond=0)

def get_cache_filename():
    """Get the filename for the cached Google Sheet data.
    
    Returns:
        str: Path to the cache file
    """
    return os.path.join(SECTOR_DATA_DIR, 'google_sheet_data.csv')

def is_cache_valid():
    """Check if the cached data is still valid (not expired).
    
    Returns:
        bool: True if cache is valid, False otherwise
    """
    cache_file = get_cache_filename()
    
    if not os.path.exists(cache_file):
        return False
        
    file_mtime = datetime.fromtimestamp(os.path.getmtime(cache_file))
    age_hours = (datetime.now() - file_mtime).total_seconds() / 3600
    
    return age_hours < CACHE_TIMEOUT_HOURS

def fetch_google_sheet_data():
    """Fetch the latest data from the Google Sheet.
    
    Returns:
        pd.DataFrame: DataFrame containing the Google Sheet data
    """
    if not GOOGLE_SHEET_URL:
        print("Google Sheet URL not configured")
        return pd.DataFrame()
    
    try:
        # Check if we should use cached data
        cache_file = get_cache_filename()
        if is_cache_valid():
            print("Using cached Google Sheet data")
            return pd.read_csv(cache_file)
        
        # Fetch the data from Google Sheets
        print("Fetching fresh data from Google Sheet")
        response = requests.get(GOOGLE_SHEET_URL)
        response.raise_for_status()  # Raise an error for bad responses
        
        # Save to a temp file and read with pandas
        with open(cache_file, 'wb') as f:
            f.write(response.content)
        
        # Read the data
        df = pd.read_csv(cache_file)
        print(f"Successfully fetched Google Sheet data with {len(df)} rows")
        
        return df
    except Exception as e:
        print(f"Error fetching Google Sheet data: {str(e)}")
        
        # If fetching failed but we have cached data, use that even if expired
        if os.path.exists(cache_file):
            print("Using expired cached data as fallback")
            return pd.read_csv(cache_file)
            
        return pd.DataFrame()

def get_sector_data():
    """Get all sector data from the Google Sheet.
    
    Returns:
        dict: Dictionary mapping sector names to their data
    """
    # Fetch the Google Sheet data
    df = fetch_google_sheet_data()
    
    if df.empty:
        print("No data available from Google Sheet")
        return {}
    
    # Determine the structure of the sheet and extract sector data
    # This will depend on how you structure your Google Sheet
    # For now, we'll assume each row contains data for one sector
    # with columns: Sector, Date, Value, EMA20, Gap_Pct
    
    sector_data = {}
    
    # Check the column names to determine sheet format
    expected_columns = ['Sector', 'Date', 'Value', 'EMA20', 'Gap_Pct']
    if all(col in df.columns for col in expected_columns):
        # One row per sector format
        for _, row in df.iterrows():
            sector = row['Sector']
            
            # Map sector name if needed
            mapped_sector = SECTOR_MAPPING.get(sector, sector)
            
            # Store the data
            sector_data[mapped_sector] = {
                'date': pd.to_datetime(row['Date']),
                'value': float(row['Value']),
                'ema': float(row['EMA20']),
                'gap_pct': float(row['Gap_Pct'])
            }
    else:
        print(f"Unexpected column format in Google Sheet. Found columns: {df.columns.tolist()}")
        print(f"Expected columns: {expected_columns}")
    
    return sector_data

def get_sector_momentum(sector_name):
    """Get the momentum (gap between current value and EMA) for a sector.
    
    Args:
        sector_name (str): Name of the sector
        
    Returns:
        float: Gap percentage (momentum indicator)
    """
    # Get all sector data
    sector_data = get_sector_data()
    
    # Check if this sector exists in the data
    if sector_name not in sector_data:
        print(f"No data found for sector: {sector_name}")
        return 0.0
    
    # Get the gap percentage
    gap_pct = sector_data[sector_name]['gap_pct']
    print(f"{sector_name} momentum: {gap_pct:.2f}%")
    
    return gap_pct

def get_all_sector_momentums():
    """Get momentum indicators for all sectors.
    
    Returns:
        dict: Dictionary mapping sector names to their momentum values
    """
    # Get all sector data
    sector_data = get_sector_data()
    
    # Extract gap percentage for each sector
    momentums = {}
    for sector, data in sector_data.items():
        momentums[sector] = data['gap_pct']
    
    print(f"Retrieved sector momentums for {len(momentums)} sectors")
    return momentums

def integrate_momentum_with_sentiment(sector_name, base_score, momentum, weight=0.4):
    """Integrate sector momentum into the sentiment score.
    
    Args:
        sector_name (str): Name of the sector
        base_score (float): Original sentiment score (0-100)
        momentum (float): Momentum value (gap percentage)
        weight (float): Weight to give the momentum in the final score (0-1)
        
    Returns:
        float: Adjusted sentiment score (0-100)
    """
    # Scale momentum to a 0-100 range for integration
    # Typical EMA gap percentages range from -5% to +5%, so scale accordingly
    # Values beyond this range will be capped
    scaled_momentum = max(0, min(100, (momentum + 5) * 10))
    
    # Blend the base score with the momentum score
    adjusted_score = base_score * (1 - weight) + scaled_momentum * weight
    
    return adjusted_score

# Main function to test the module
if __name__ == "__main__":
    # Test URL (this needs to be replaced with the actual Google Sheet URL)
    test_url = "https://docs.google.com/spreadsheets/d/YOUR_SHEET_ID/export?format=csv&gid=YOUR_SHEET_GID"
    
    # Set the Google Sheet URL
    set_google_sheet_url(test_url)
    
    # Fetch the data
    sector_data = get_sector_data()
    
    if sector_data:
        print(f"\nSuccessfully retrieved data for {len(sector_data)} sectors:")
        for sector, data in sector_data.items():
            print(f"{sector}: value={data['value']:.2f}, ema={data['ema']:.2f}, gap={data['gap_pct']:.2f}%")
            
        # Test the momentum retrieval
        print("\nTesting momentum retrieval:")
        momentums = get_all_sector_momentums()
        
        # Test integration with sentiment
        print("\nTesting sentiment integration:")
        for sector, momentum in momentums.items():
            base_score = 60.0  # Example base sentiment score
            adjusted_score = integrate_momentum_with_sentiment(sector, base_score, momentum)
            print(f"{sector}: Base={base_score:.1f}, Momentum-adjusted={adjusted_score:.1f}")
    else:
        print("\nNo sector data retrieved")
        print("Please set a valid Google Sheet URL with the set_google_sheet_url() function")
