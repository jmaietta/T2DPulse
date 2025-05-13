import requests
import os
from config import FINNHUB_API_KEY

BASE_URL = "https://finnhub.io/api/v1"

def fetch_eod_price(ticker):
    """
    Fetch end-of-day price for a stock ticker from Finnhub
    
    Args:
        ticker (str): Stock ticker symbol
        
    Returns:
        float: Current price or None if not available
    """
    try:
        url = f"{BASE_URL}/quote?symbol={ticker}&token={FINNHUB_API_KEY}"
        response = requests.get(url)
        if response.status_code == 200:
            res = response.json()
            return res.get("c")  # Current price
        else:
            print(f"Failed to fetch price for {ticker}: {response.status_code}")
            return None
    except Exception as e:
        print(f"Error fetching price for {ticker}: {str(e)}")
        return None

def fetch_market_cap(ticker):
    """
    Fetch market capitalization for a stock ticker from Finnhub
    
    Args:
        ticker (str): Stock ticker symbol
        
    Returns:
        float: Market capitalization in USD or None if not available
    """
    try:
        url = f"{BASE_URL}/stock/profile2?symbol={ticker}&token={FINNHUB_API_KEY}"
        response = requests.get(url)
        if response.status_code == 200:
            res = response.json()
            return res.get("marketCapitalization")
        else:
            print(f"Failed to fetch market cap for {ticker}: {response.status_code}")
            return None
    except Exception as e:
        print(f"Error fetching market cap for {ticker}: {str(e)}")
        return None
