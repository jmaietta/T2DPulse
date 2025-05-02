"""Sector-specific ticker lists for T2D Pulse dashboard.

This module defines representative ticker baskets for each of the 14 technology sectors
in the T2D Pulse dashboard. Each ticker list contains the most relevant publicly traded
companies that represent that sector, focusing on market leaders by size and influence.

The tickers will be used to create market-cap weighted indices for each sector.
These indices track real market performance and contribute to the sentiment scores.
"""

import os
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta

# Define ticker lists for each sector
SECTOR_TICKERS = {
    # AdTech companies
    "AdTech": [
        "GOOGL",  # Alphabet (Google)
        "META",   # Meta Platforms
        "TTD",    # The Trade Desk
        "MGNI",   # Magnite
        "APPS",   # Digital Turbine
        "CRTO"    # Criteo
    ],
    
    # Cloud companies
    "Cloud": [
        "MSFT",   # Microsoft (Azure)
        "AMZN",   # Amazon (AWS)
        "GOOGL",  # Alphabet (Google Cloud)
        "CRM",    # Salesforce
        "NET",    # Cloudflare
        "DDOG",   # Datadog
        "SNOW",   # Snowflake
        "ZS"      # Zscaler
    ],
    
    # Fintech companies
    "Fintech": [
        "SQ",     # Block (Square)
        "PYPL",   # PayPal
        "ADYEY",  # Adyen
        "COIN",   # Coinbase
        "AFRM",   # Affirm
        "FISV",   # Fiserv
        "FIS"     # Fidelity National Information
    ],
    
    # eCommerce companies
    "eCommerce": [
        "AMZN",   # Amazon
        "SHOP",   # Shopify
        "MELI",   # MercadoLibre
        "CPNG",   # Coupang
        "SE",     # Sea Limited
        "W",      # Wayfair
        "ETSY"    # Etsy
    ],
    
    # Cybersecurity companies
    "Cybersecurity": [
        "PANW",   # Palo Alto Networks
        "CRWD",   # CrowdStrike
        "FTNT",   # Fortinet
        "ZS",     # Zscaler
        "OKTA",   # Okta
        "S",      # SentinelOne
        "CYBR"    # CyberArk
    ],
    
    # Developer Tools / Analytics companies
    "Dev Tools / Analytics": [
        "MSFT",   # Microsoft (GitHub)
        "TEAM",   # Atlassian
        "DDOG",   # Datadog
        "NET",    # Cloudflare
        "GTLB",   # GitLab
        "ESTC",   # Elastic
        "MDB",    # MongoDB
        "SPLK"    # Splunk
    ],
    
    # Semiconductors companies
    "Semiconductors": [
        "NVDA",   # NVIDIA
        "AMD",    # Advanced Micro Devices
        "INTC",   # Intel
        "TSM",    # Taiwan Semiconductor
        "AVGO",   # Broadcom
        "QCOM",   # Qualcomm
        "ARM",    # ARM Holdings
        "AMAT"    # Applied Materials
    ],
    
    # AI Infrastructure companies
    "AI Infrastructure": [
        "NVDA",   # NVIDIA
        "AMD",    # Advanced Micro Devices
        "GOOGL",  # Alphabet (Google)
        "MSFT",   # Microsoft
        "AMZN",   # Amazon (AWS)
        "IBM",    # IBM
        "TSM",    # Taiwan Semiconductor
        "MU"      # Micron Technology
    ],
    
    # Enterprise SaaS companies
    "Enterprise SaaS": [
        "CRM",    # Salesforce
        "MSFT",   # Microsoft
        "WDAY",   # Workday
        "NOW",    # ServiceNow
        "ZM",     # Zoom
        "TEAM",   # Atlassian
        "DOCU",   # DocuSign
        "ZS"      # Zscaler
    ],
    
    # Vertical SaaS companies
    "Vertical SaaS": [
        "HUBS",   # HubSpot
        "VEEV",   # Veeva Systems
        "PCTY",   # Paylocity
        "ASAN",   # Asana
        "FRSH",   # Freshworks
        "PD",     # PagerDuty
        "ZI"      # ZoomInfo
    ],
    
    # Consumer Internet companies
    "Consumer Internet": [
        "GOOGL",  # Alphabet (Google)
        "META",   # Meta Platforms
        "NFLX",   # Netflix
        "UBER",   # Uber
        "ABNB",   # Airbnb
        "SNAP",   # Snap
        "PINS",   # Pinterest
        "SPOT"    # Spotify
    ],
    
    # Gaming companies
    "Gaming": [
        "ATVI",   # Activision Blizzard (Microsoft)
        "EA",     # Electronic Arts
        "TTWO",   # Take-Two Interactive
        "RBLX",   # Roblox
        "U",      # Unity Software
        "CRSR",   # Corsair Gaming
        "MTCH"    # Match Group
    ],
    
    # IT Services / Legacy Tech companies
    "IT Services / Legacy Tech": [
        "IBM",    # IBM
        "ACN",    # Accenture
        "CTSH",   # Cognizant
        "ORCL",   # Oracle
        "XRX",    # Xerox
        "HPQ",    # HP
        "DELL"    # Dell Technologies
    ],
    
    # Hardware / Devices companies
    "Hardware / Devices": [
        "AAPL",   # Apple
        "DELL",   # Dell Technologies
        "HPQ",    # HP
        "LOGI",   # Logitech
        "SONO",   # Sonos
        "FIT",    # Fitbit
        "GoPro"   # GoPro
    ]
}

def get_sector_tickers(sector_name):
    """Get the list of tickers for a specific sector.
    
    Args:
        sector_name (str): Name of the sector
        
    Returns:
        list: List of ticker symbols
    """
    if sector_name in SECTOR_TICKERS:
        return SECTOR_TICKERS[sector_name]
    else:
        print(f"Warning: No tickers defined for sector '{sector_name}'")
        return []

def get_market_caps(tickers):
    """Get market capitalization data for a list of tickers.
    
    Args:
        tickers (list): List of ticker symbols
        
    Returns:
        dict: Dictionary mapping tickers to their market caps
    """
    market_caps = {}
    for ticker in tickers:
        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            if 'marketCap' in info and info['marketCap'] is not None:
                market_caps[ticker] = info['marketCap']
            else:
                print(f"Warning: Could not retrieve market cap for {ticker}")
        except Exception as e:
            print(f"Error retrieving data for {ticker}: {str(e)}")
    
    return market_caps

def calculate_weights(market_caps):
    """Calculate market-cap weights for a dictionary of tickers and their market caps.
    
    Args:
        market_caps (dict): Dictionary mapping tickers to their market caps
        
    Returns:
        dict: Dictionary mapping tickers to their weights (0-1)
    """
    if not market_caps:
        return {}
        
    total_market_cap = sum(market_caps.values())
    weights = {ticker: cap / total_market_cap for ticker, cap in market_caps.items()}
    
    return weights

def get_historical_prices(tickers, days=30):
    """Get historical price data for a list of tickers.
    
    Args:
        tickers (list): List of ticker symbols
        days (int): Number of days of historical data to fetch
        
    Returns:
        pd.DataFrame: DataFrame with date indices and ticker columns
    """
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    # Use yfinance to download data for all tickers at once
    try:
        data = yf.download(tickers, start=start_date, end=end_date, progress=False)
        
        # Extract the closing prices
        if len(tickers) > 1:
            prices = data['Close']
        else:
            # Handle the single ticker case
            prices = pd.DataFrame(data['Close'])
            prices.columns = [tickers[0]]
            
        return prices
    except Exception as e:
        print(f"Error downloading historical prices: {str(e)}")
        return pd.DataFrame()

def calculate_sector_index(sector_name, days=30):
    """Calculate a market-cap weighted index for a sector.
    
    Args:
        sector_name (str): Name of the sector
        days (int): Number of days of historical data to use
        
    Returns:
        tuple: (index_df, weights) where index_df is a DataFrame with date and index value,
               and weights is a dictionary of ticker weights
    """
    # Get tickers for the sector
    tickers = get_sector_tickers(sector_name)
    if not tickers:
        return pd.DataFrame(), {}
    
    # Get market caps and calculate weights
    market_caps = get_market_caps(tickers)
    weights = calculate_weights(market_caps)
    
    # Get historical prices
    prices = get_historical_prices(tickers, days)
    if prices.empty:
        return pd.DataFrame(), weights
    
    # Calculate the weighted index
    index_values = pd.Series(0.0, index=prices.index)
    for ticker in tickers:
        if ticker in prices.columns and ticker in weights:
            # Calculate the contribution of this ticker to the index
            ticker_series = prices[ticker].dropna()
            if not ticker_series.empty:
                # Normalize to start at 100
                normalized = ticker_series / ticker_series.iloc[0] * 100
                # Weight the normalized series
                weighted = normalized * weights.get(ticker, 0)
                # Add to the index
                index_values = index_values.add(weighted, fill_value=0)
    
    # Create a DataFrame with date and index value
    index_df = pd.DataFrame({
        'date': index_values.index,
        'value': index_values.values
    })
    
    return index_df, weights

def calculate_index_ema(index_df, span=20):
    """Calculate the exponential moving average for an index.
    
    Args:
        index_df (pd.DataFrame): DataFrame with date and index value
        span (int): EMA span (window size)
        
    Returns:
        pd.DataFrame: DataFrame with date, value, ema, and gap_pct columns
    """
    if index_df.empty or len(index_df) < span:
        return index_df
    
    # Calculate EMA
    index_df['ema'] = index_df['value'].ewm(span=span, adjust=False).mean()
    
    # Calculate gap percentage (current value vs EMA)
    index_df['gap_pct'] = (index_df['value'] - index_df['ema']) / index_df['ema'] * 100
    
    return index_df

def get_sector_index_with_ema(sector_name, span=20, days=60):
    """Get a sector index with EMA calculation.
    
    Args:
        sector_name (str): Name of the sector
        span (int): EMA span (window size)
        days (int): Number of days of historical data to use
        
    Returns:
        tuple: (index_df, weights) where index_df is a DataFrame with date, value, ema, and gap_pct,
               and weights is a dictionary of ticker weights
    """
    # Calculate the sector index
    index_df, weights = calculate_sector_index(sector_name, days=days)
    
    if index_df.empty:
        return index_df, weights
    
    # Calculate EMA and gap percentage
    index_with_ema = calculate_index_ema(index_df, span=span)
    
    return index_with_ema, weights

def get_latest_sector_momentum(sector_name):
    """Get the latest sector momentum (gap between current value and EMA).
    
    Args:
        sector_name (str): Name of the sector
        
    Returns:
        float: Latest gap percentage (momentum indicator)
    """
    index_df, _ = get_sector_index_with_ema(sector_name)
    
    if index_df.empty or 'gap_pct' not in index_df.columns:
        return 0.0
    
    # Get the most recent gap percentage
    latest_gap = index_df['gap_pct'].iloc[-1]
    
    return latest_gap

# Main function to test the module
if __name__ == "__main__":
    # Test with a sample sector
    sector_name = "Cloud"
    print(f"Testing with {sector_name} sector")
    
    # Get the sector index with EMA
    index_df, weights = get_sector_index_with_ema(sector_name)
    
    # Print the weights
    print("\nMarket Cap Weights:")
    for ticker, weight in weights.items():
        print(f"{ticker}: {weight:.2%}")
    
    # Print the latest values
    if not index_df.empty:
        latest = index_df.iloc[-1]
        print(f"\nLatest index value: {latest['value']:.2f}")
        print(f"Latest EMA ({20}-day): {latest['ema']:.2f}")
        print(f"Latest momentum (gap %): {latest['gap_pct']:.2f}%")
    else:
        print("\nNo index data available")
