import pandas as pd
from data_cache import get_data
from app import load_data_from_csv

# Dictionary mapping data types to their file paths
DATA_FILES = {
    "gdp": "gdp_data.csv",
    "unemployment": "unemployment_data.csv",
    "cpi": "inflation_data.csv",
    "pcepi": "pcepi_data.csv",
    "interest_rate": "interest_rate_data.csv",
    "pce": "pce_data.csv",
    "treasury_yield": "treasury_yield_data.csv",
    "vix": "vix_data.csv",
    "nasdaq": "nasdaq_data.csv",
    "consumer_sentiment": "consumer_sentiment_data.csv"
}

# Function to get data preferring cache then file
def get_indicator_data(data_type):
    """Get data for an economic indicator, preferring cache then file"""
    # First try to get from cache (fastest)
    df = get_data(data_type)
    
    # If not in cache, try to load from file
    if df is None or df.empty:
        df = load_data_from_csv(DATA_FILES[data_type])
    
    return df

# Helper functions for specific indicators
def get_gdp_data():
    return get_indicator_data("gdp")

def get_unemployment_data():
    return get_indicator_data("unemployment")

def get_cpi_data():
    return get_indicator_data("cpi")

def get_pcepi_data():
    return get_indicator_data("pcepi")

def get_interest_rate_data():
    return get_indicator_data("interest_rate")

def get_pce_data():
    return get_indicator_data("pce")

def get_treasury_yield_data():
    return get_indicator_data("treasury_yield")

def get_vix_data():
    return get_indicator_data("vix")

def get_nasdaq_data():
    return get_indicator_data("nasdaq")

def get_consumer_sentiment_data():
    return get_indicator_data("consumer_sentiment")
