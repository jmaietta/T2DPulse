"""Sector data reader for T2D Pulse dashboard.

This module reads sector data from CSV files that are updated at the end of each trading day.
It provides sector-specific market indices and momentum indicators for the T2D Pulse dashboard.
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import pytz

# Data directory for sector data
DATA_DIR = 'data'
SECTOR_DATA_DIR = os.path.join(DATA_DIR, 'sector_data')
os.makedirs(SECTOR_DATA_DIR, exist_ok=True)

# Ensure the sector indices directory exists (for backward compatibility)
SECTOR_INDICES_DIR = os.path.join(DATA_DIR, 'sector_indices')
os.makedirs(SECTOR_INDICES_DIR, exist_ok=True)

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

# Mapping from sector_tickers.py sector names to T2D Pulse sector names
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

def is_trading_day(date):
    """Check if a given date is a trading day (weekday, not a holiday).
    
    This is a simplified check that only excludes weekends, not holidays.
    
    Args:
        date (datetime): Date to check
        
    Returns:
        bool: True if the date is a trading day, False otherwise
    """
    # Check if it's a weekday (Monday=0, Sunday=6)
    return date.weekday() < 5  # 0-4 are weekdays

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

def get_sector_data_filename(sector_name):
    """Get the filename for a sector's data CSV.
    
    Args:
        sector_name (str): Name of the sector
        
    Returns:
        str: Path to the sector data CSV file
    """
    return os.path.join(SECTOR_DATA_DIR, f"{sector_name.replace('/', '_')}_data.csv")

def get_sector_momentum(sector_name):
    """Get the momentum (gap between current value and EMA) for a sector.
    
    Args:
        sector_name (str): Name of the sector
        
    Returns:
        float: Gap percentage (momentum indicator)
    """
    # Get the sector data from CSV
    filename = get_sector_data_filename(sector_name)
    
    if not os.path.exists(filename):
        print(f"No data file found for {sector_name}")
        return 0.0
    
    try:
        # Read the CSV file
        df = pd.read_csv(filename)
        
        # Check if the dataframe has the necessary columns
        if df.empty or 'date' not in df.columns or 'gap_pct' not in df.columns:
            print(f"Invalid data format for {sector_name}")
            return 0.0
        
        # Convert date column to datetime
        df['date'] = pd.to_datetime(df['date'])
        
        # Sort by date and get the latest entry
        df = df.sort_values('date', ascending=False)
        
        # Get the latest momentum value
        latest_gap = df['gap_pct'].iloc[0]
        print(f"{sector_name} momentum: {latest_gap:.2f}%")
        
        return latest_gap
    except Exception as e:
        print(f"Error reading sector data for {sector_name}: {str(e)}")
        return 0.0

def get_all_sector_momentums():
    """Get momentum indicators for all sectors.
    
    Returns:
        dict: Dictionary mapping sector names to their momentum values
    """
    momentums = {}
    for sector_name in SECTOR_NAMES:
        momentum = get_sector_momentum(sector_name)
        momentums[sector_name] = momentum
    
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

def load_sample_data():
    """Load sample data for each sector if no real data exists.
    
    This is used for initial setup or testing only and creates minimal
    placeholder files that will be replaced by the real data update process.
    """
    latest_date = get_latest_trading_day()
    
    for sector_name in SECTOR_NAMES:
        filename = get_sector_data_filename(sector_name)
        
        # Only create sample data if the file doesn't exist
        if not os.path.exists(filename):
            print(f"Creating minimal sample data file for {sector_name}")
            
            # Create a very basic dataframe with just one row containing latest date
            # The gap_pct value is initialized to 0%
            df = pd.DataFrame({
                'date': [latest_date],
                'value': [100.0],  # Placeholder index value
                'ema': [100.0],    # Equal to value initially
                'gap_pct': [0.0]   # No gap initially
            })
            
            # Save to CSV
            df.to_csv(filename, index=False)

# Create sample data files if they don't exist (for backwards compatibility)
load_sample_data()
