import pandas as pd
import numpy as np
import os
from config import SECTORS, EMA_SPAN
from fetcher import fetch_market_cap

def compute_sector_value(sector_tickers):
    """
    Compute total market capitalization for a sector based on its tickers
    
    Args:
        sector_tickers (list): List of stock ticker symbols
        
    Returns:
        float: Total market capitalization in billions USD
    """
    total = 0
    for ticker in sector_tickers:
        try:
            market_cap = fetch_market_cap(ticker)
            if market_cap:
                total += market_cap
        except Exception as e:
            print(f"Error processing ticker {ticker}: {str(e)}")
            continue
    return round(total, 2)

def calculate_ema(values, span=EMA_SPAN):
    """
    Calculate Exponential Moving Average (EMA) for a series of values
    
    Args:
        values (list): List of numeric values
        span (int): Period for EMA calculation
        
    Returns:
        list: List of EMA values
    """
    if not values or len(values) == 0:
        return []
        
    ema_values = []
    alpha = 2 / (span + 1)
    
    for i, val in enumerate(values):
        if i == 0:
            ema_values.append(val)
        else:
            ema = alpha * val + (1 - alpha) * ema_values[-1]
            ema_values.append(ema)
            
    return ema_values

def load_sector_emas(filepath="data/sector_values.csv", days=EMA_SPAN, include_raw=False):
    """
    Load sector values and calculate EMAs
    
    Args:
        filepath (str): Path to the CSV file with sector values
        days (int): Number of days for EMA calculation
        include_raw (bool): Whether to include raw values in results
        
    Returns:
        dict: Dictionary with sector EMAs {sector: (latest_ema, percent_change)}
    """
    if not os.path.exists(filepath):
        print(f"No sector EMA data file found at {filepath}")
        return {}
        
    # Load data from CSV
    try:
        df = pd.read_csv(filepath)
        if df.empty:
            print("Sector values CSV file is empty")
            return {}
    except Exception as e:
        print(f"Error loading sector values CSV: {str(e)}")
        return {}
        
    # Calculate EMAs and percent changes for each sector
    results = {}
    
    for sector in df.columns[1:]:  # Skip the Date column
        values = df[sector].dropna().values
        if len(values) > 1:  # Need at least 2 values for EMA
            if len(values) >= days:
                emas = calculate_ema(values, span=days)
                latest_ema = emas[-1]
                prev_ema = emas[-2] if len(emas) > 1 else latest_ema
            else:
                # If we don't have enough data, use simple average
                latest_ema = np.mean(values)
                prev_ema = np.mean(values[:-1]) if len(values) > 1 else latest_ema
                
            percent_change = (latest_ema - prev_ema) / prev_ema * 100 if prev_ema > 0 else 0
            
            if include_raw:
                results[sector] = (latest_ema, percent_change, values[-1])
            else:
                results[sector] = (latest_ema, percent_change)
                
    return results