"""
Authentic Market Cap Reader

This module provides functions to load and process authentic market cap data
from Polygon.io API results saved by polygon_sector_caps.py.

The data represents actual market capitalizations for each sector, allowing
for accurate sector weighting and analysis based on real market values.
"""

import os
import logging
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def load_authentic_market_caps(filepath="data/sector_market_caps.parquet", fallback_csv="data/sector_market_caps.csv"):
    """
    Load authentic market cap data for all sectors from the parquet file
    created by polygon_sector_caps.py.
    
    Args:
        filepath (str): Path to the parquet file
        fallback_csv (str): Path to CSV fallback file
        
    Returns:
        pd.DataFrame: Market cap data with date index and sector columns
    """
    try:
        # Attempt to load from parquet (faster and more efficient)
        if os.path.exists(filepath):
            df = pd.read_parquet(filepath)
            logging.info(f"Loaded authentic market cap data from {filepath}")
            return df
        
        # Fall back to CSV if parquet not available
        elif os.path.exists(fallback_csv):
            df = pd.read_csv(fallback_csv, index_col=0, parse_dates=True)
            logging.info(f"Loaded authentic market cap data from {fallback_csv}")
            return df
        
        else:
            logging.warning(f"No authentic market cap data found at {filepath} or {fallback_csv}")
            return pd.DataFrame()
            
    except Exception as e:
        logging.error(f"Error loading authentic market cap data: {e}")
        return pd.DataFrame()

def get_latest_sector_weights():
    """
    Get the latest available sector weights calculated from authentic market caps.
    
    Returns:
        dict: Dictionary with sector weights as percentages
    """
    try:
        # Try to load pre-calculated weights from the JSON file
        if os.path.exists("data/sector_weights_latest.json"):
            with open("data/sector_weights_latest.json", 'r') as f:
                weights = json.load(f)
            
            # Clean up weights dictionary to remove _weight_pct suffix
            cleaned_weights = {}
            for key, value in weights.items():
                sector_name = key.replace('_weight_pct', '')
                cleaned_weights[sector_name] = value
                
            logging.info(f"Loaded latest sector weights from data/sector_weights_latest.json")
            return cleaned_weights
        
        # Calculate weights from raw market cap data if JSON not available
        mcap_df = load_authentic_market_caps()
        if not mcap_df.empty:
            # Get most recent date
            latest_date = mcap_df.index.max()
            
            # Get market caps for that date
            latest_mcaps = mcap_df.loc[latest_date]
            
            # Calculate total market cap (excluding the 'Total' column if it exists)
            if 'Total' in latest_mcaps:
                total_mcap = latest_mcaps['Total']
            else:
                # Filter out weight columns
                sector_cols = [col for col in latest_mcaps.index if '_weight_pct' not in col]
                total_mcap = latest_mcaps[sector_cols].sum()
            
            # Calculate weights for each sector
            weights = {}
            for sector in mcap_df.columns:
                if sector != 'Total' and '_weight_pct' not in sector:
                    weights[sector] = round((latest_mcaps[sector] / total_mcap) * 100, 2)
            
            logging.info(f"Calculated sector weights from market cap data for {latest_date}")
            return weights
        
        logging.warning("No market cap data or weights available")
        return {}
        
    except Exception as e:
        logging.error(f"Error getting latest sector weights: {e}")
        return {}

def get_sector_weightings():
    """
    Get sector market cap weightings from the most recent data.
    Falls back to equal weighting if no authentic data is available.
    
    Returns:
        dict: Dictionary with sector weights where values sum to 100
    """
    weights = get_latest_sector_weights()
    
    # If we have valid weights, return them
    if weights and sum(weights.values()) > 0:
        return weights
    
    # Otherwise load the sector tickers and return equal weighting
    try:
        from polygon_sector_caps import SECTOR_TICKERS
        equal_weights = {}
        for sector in SECTOR_TICKERS.keys():
            equal_weights[sector] = round(100 / len(SECTOR_TICKERS), 2)
        logging.warning("Using equal sector weights (no authentic market cap data available)")
        return equal_weights
    except ImportError:
        # If SECTOR_TICKERS is not available, we'll use a hard-coded list
        sectors = [
            "AdTech", "Cloud Infrastructure", "Fintech", "eCommerce", 
            "Consumer Internet", "IT Services", "Hardware/Devices", 
            "Cybersecurity", "Dev Tools", "AI Infrastructure", 
            "Semiconductors", "Vertical SaaS", "Enterprise SaaS", "SMB SaaS"
        ]
        equal_weights = {sector: round(100 / len(sectors), 2) for sector in sectors}
        logging.warning("Using equal sector weights (fallback)")
        return equal_weights

def generate_market_cap_report():
    """
    Generate a human-readable report of the latest market cap data.
    
    Returns:
        str: Formatted report text
    """
    df = load_authentic_market_caps()
    if df.empty:
        return "No authentic market cap data available."
    
    # Get the last 5 trading days of data
    recent_df = df.tail(5)
    
    # Convert to billions for readability
    billions_df = (recent_df / 1_000_000_000).round(1)
    
    # Format as string table
    report = "===== SECTOR MARKET CAP TABLE (BILLIONS USD) =====\n"
    report += billions_df.reset_index().to_string(index=False)
    
    # Add sector weights
    weights = get_latest_sector_weights()
    report += "\n\n===== SECTOR WEIGHTS (MARKET CAP %) =====\n"
    for sector, weight in sorted(weights.items(), key=lambda x: x[1], reverse=True):
        report += f"{sector}: {weight:.2f}%\n"
    
    return report

if __name__ == "__main__":
    # Simple test/demo
    print(generate_market_cap_report())