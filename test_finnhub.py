import os
import requests
import time
import json
from datetime import datetime
import pytz
from config import FINNHUB_API_KEY

def test_api_key():
    """Test if the Finnhub API key is valid"""
    if not FINNHUB_API_KEY:
        print("Finnhub API key is not set. Please set it in config.py or as an environment variable.")
        return False
        
    url = f"https://finnhub.io/api/v1/stock/symbol?exchange=US&token={FINNHUB_API_KEY}"
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        print(f"API key is valid. Retrieved {len(data)} symbols.")
        return True
    else:
        print(f"API key test failed. Status code: {response.status_code}")
        print(f"Response: {response.text}")
        return False

def test_market_cap(ticker="AAPL"):
    """Test fetching market cap for a given ticker"""
    url = f"https://finnhub.io/api/v1/stock/profile2?symbol={ticker}&token={FINNHUB_API_KEY}"
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        if data and 'marketCapitalization' in data:
            market_cap = data['marketCapitalization']
            print(f"{ticker} market cap: {market_cap:.2f} million USD")
            print(f"Full company profile: {json.dumps(data, indent=2)}")
            return True
    
    print(f"Failed to fetch market cap for {ticker}. Status code: {response.status_code}")
    print(f"Response: {response.text}")
    return False

def test_stock_price(ticker="AAPL"):
    """Test fetching current price for a given ticker"""
    url = f"https://finnhub.io/api/v1/quote?symbol={ticker}&token={FINNHUB_API_KEY}"
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        current_price = data.get('c', 0)  # Current price
        previous_close = data.get('pc', 0)  # Previous close
        
        print(f"{ticker} current price: ${current_price:.2f}")
        print(f"{ticker} previous close: ${previous_close:.2f}")
        print(f"Change: {((current_price - previous_close) / previous_close * 100):.2f}%")
        print(f"Full quote data: {json.dumps(data, indent=2)}")
        return True
    
    print(f"Failed to fetch price for {ticker}. Status code: {response.status_code}")
    print(f"Response: {response.text}")
    return False

def main():
    """Run all tests"""
    print(f"Testing Finnhub API integration on {datetime.now(pytz.timezone('US/Eastern')).strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"Using API key: {FINNHUB_API_KEY[:5]}...{FINNHUB_API_KEY[-3:]}")
    
    api_valid = test_api_key()
    if not api_valid:
        print("API key validation failed. Exiting.")
        return False
        
    print("\nTesting market cap fetch...")
    market_cap_valid = test_market_cap("AAPL")
    
    print("\nTesting stock price fetch...")
    price_valid = test_stock_price("MSFT")
    
    print("\nTest summary:")
    print(f"API key validation: {'✓' if api_valid else '✗'}")
    print(f"Market cap fetch: {'✓' if market_cap_valid else '✗'}")
    print(f"Stock price fetch: {'✓' if price_valid else '✗'}")
    
    return api_valid and market_cap_valid and price_valid

if __name__ == "__main__":
    success = main()
    print(f"\nTest {'succeeded' if success else 'failed'}")
