"""Sector market indices tracking and analysis for T2D Pulse dashboard.

This module leverages sector-specific ticker lists to create market-cap weighted
indices for each of the 14 technology sectors in the T2D Pulse dashboard.

These indices track real market performance and contribute to the sentiment scores
by providing sector-specific momentum indicators similar to the NASDAQ EMA gap.
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sector_tickers
import json

# Data directory for caching index data
DATA_DIR = 'data'
os.makedirs(os.path.join(DATA_DIR, 'sector_indices'), exist_ok=True)

def get_sector_index(sector_name, use_cache=True, days=60):
    """Get the market-cap weighted index for a sector.
    
    Args:
        sector_name (str): Name of the sector
        use_cache (bool): Whether to use cached data if available
        days (int): Number of days of historical data to use if not using cache
        
    Returns:
        pd.DataFrame: DataFrame with date, value, ema, and gap_pct columns
    """
    cache_file = os.path.join(DATA_DIR, 'sector_indices', f"{sector_name.replace('/', '_')}_index.csv")
    
    # Check if cache file exists and should be used
    if use_cache and os.path.exists(cache_file):
        try:
            cached_data = pd.read_csv(cache_file)
            cached_data['date'] = pd.to_datetime(cached_data['date'])
            
            # Check if the data is recent enough (within the last day)
            last_date = cached_data['date'].max()
            if datetime.now() - last_date < timedelta(days=1):
                print(f"Using cached index data for {sector_name}")
                return cached_data
            else:
                print(f"Cached data for {sector_name} is outdated, fetching fresh data")
        except Exception as e:
            print(f"Error reading cached index data: {str(e)}")
    
    # Get fresh data
    print(f"Fetching market index data for {sector_name}")
    index_df, weights = sector_tickers.get_sector_index_with_ema(sector_name, days=days)
    
    # Save weights to a separate file
    weights_file = os.path.join(DATA_DIR, 'sector_indices', f"{sector_name.replace('/', '_')}_weights.json")
    try:
        with open(weights_file, 'w') as f:
            json.dump(weights, f, indent=2)
        print(f"Saved weights for {sector_name} to {weights_file}")
    except Exception as e:
        print(f"Error saving weights: {str(e)}")
    
    # Cache the data if it's not empty
    if not index_df.empty:
        try:
            index_df.to_csv(cache_file, index=False)
            print(f"Cached index data for {sector_name} with {len(index_df)} rows")
        except Exception as e:
            print(f"Error caching index data: {str(e)}")
    
    return index_df

def get_all_sector_indices(use_cache=True):
    """Get indices for all sectors.
    
    Args:
        use_cache (bool): Whether to use cached data if available
        
    Returns:
        dict: Dictionary mapping sector names to their index DataFrames
    """
    sector_indices = {}
    for sector_name in sector_tickers.SECTOR_TICKERS.keys():
        index_df = get_sector_index(sector_name, use_cache=use_cache)
        sector_indices[sector_name] = index_df
    
    return sector_indices

def get_sector_momentum(sector_name, use_cache=True):
    """Get the momentum (gap between current value and EMA) for a sector.
    
    Args:
        sector_name (str): Name of the sector
        use_cache (bool): Whether to use cached data if available
        
    Returns:
        float: Gap percentage (momentum indicator)
    """
    index_df = get_sector_index(sector_name, use_cache=use_cache)
    
    if index_df.empty or 'gap_pct' not in index_df.columns:
        print(f"No valid momentum data for {sector_name}")
        return 0.0
    
    latest_gap = index_df['gap_pct'].iloc[-1]
    print(f"{sector_name} momentum: {latest_gap:.2f}%")
    
    return latest_gap

def get_all_sector_momentums(use_cache=True):
    """Get momentum indicators for all sectors.
    
    Args:
        use_cache (bool): Whether to use cached data if available
        
    Returns:
        dict: Dictionary mapping sector names to their momentum values
    """
    momentums = {}
    for sector_name in sector_tickers.SECTOR_TICKERS.keys():
        momentum = get_sector_momentum(sector_name, use_cache=use_cache)
        momentums[sector_name] = momentum
    
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
    # Test with a sample sector
    sector_name = "Cloud"
    print(f"Testing with {sector_name} sector")
    
    # Get the sector index
    index_df = get_sector_index(sector_name, use_cache=False)
    
    # Print the latest values
    if not index_df.empty:
        latest = index_df.iloc[-1]
        print(f"\nLatest index value: {latest['value']:.2f}")
        if 'ema' in latest:
            print(f"Latest EMA (20-day): {latest['ema']:.2f}")
            print(f"Latest momentum (gap %): {latest['gap_pct']:.2f}%")
        
        # Test integration with sentiment
        base_score = 60.0  # Example base sentiment score
        momentum = latest['gap_pct'] if 'gap_pct' in latest else 0.0
        adjusted_score = integrate_momentum_with_sentiment(sector_name, base_score, momentum)
        print(f"\nBase sentiment score: {base_score:.1f}")
        print(f"Momentum-adjusted score: {adjusted_score:.1f}")
    else:
        print("\nNo index data available")
