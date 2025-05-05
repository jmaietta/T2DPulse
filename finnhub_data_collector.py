import os
import requests
import time
import pandas as pd
import json
from datetime import datetime, timedelta
import pytz
import config
import csv

# Ensure we have the Finnhub API key
FINNHUB_API_KEY = config.FINNHUB_API_KEY
SECTORS = config.SECTORS

def get_eastern_date():
    """Get the current date in US Eastern Time"""
    eastern = pytz.timezone('US/Eastern')
    now = datetime.now(eastern)
    return now.strftime('%Y-%m-%d')

def fetch_market_cap(ticker):
    """Fetch market cap for a given ticker from Finnhub"""
    url = f"https://finnhub.io/api/v1/stock/profile2?symbol={ticker}&token={FINNHUB_API_KEY}"
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        if data and 'marketCapitalization' in data:
            # Finnhub returns market cap in millions, convert to billions
            return data['marketCapitalization'] * 1000000  # Convert to actual value
    elif response.status_code == 429:
        print(f"Failed to fetch market cap for {ticker}: 429")
        time.sleep(1)  # Rate limit - wait a second before next request
    else:
        print(f"Failed to fetch market cap for {ticker}: {response.status_code}")
    
    return 0

def fetch_stock_price(ticker):
    """Fetch latest stock price and 20-day EMA for a given ticker"""
    # Get current price
    url = f"https://finnhub.io/api/v1/quote?symbol={ticker}&token={FINNHUB_API_KEY}"
    response = requests.get(url)
    
    if response.status_code != 200:
        print(f"Failed to fetch price for {ticker}: {response.status_code}")
        if response.status_code == 429:
            time.sleep(1)  # Rate limit - wait a second before next request
        return None, None
    
    quote_data = response.json()
    current_price = quote_data.get('c', 0)  # Current price
    
    # Get historical data for EMA calculation
    end_date = int(time.time())  # Current time
    start_date = end_date - (86400 * 30)  # 30 days back
    
    url = f"https://finnhub.io/api/v1/stock/candle?symbol={ticker}&resolution=D&from={start_date}&to={end_date}&token={FINNHUB_API_KEY}"
    response = requests.get(url)
    
    if response.status_code != 200:
        print(f"Failed to fetch historical data for {ticker}: {response.status_code}")
        if response.status_code == 429:
            time.sleep(1)  # Rate limit - wait a second before next request
        return current_price, None
    
    candle_data = response.json()
    
    # Check if we have valid data
    if candle_data['s'] != 'ok':
        print(f"No valid candle data for {ticker}")
        return current_price, None
    
    # Calculate 20-day EMA
    if len(candle_data['c']) >= 20:
        prices = pd.Series(candle_data['c'])
        ema20 = prices.ewm(span=20, adjust=False).mean().iloc[-1]
        return current_price, ema20
    else:
        print(f"Not enough data for EMA calculation for {ticker}")
        return current_price, None

def process_sector_data():
    """Process and save sector data for all defined sectors"""
    today = get_eastern_date()
    sectors_data = {}
    sector_values = []
    
    print(f"Processing sector data for {today}...")
    
    for sector, tickers in SECTORS.items():
        print(f"Processing {sector} with {len(tickers)} tickers...")
        total_market_cap = 0
        weighted_momentum = 0
        valid_momentum_count = 0
        
        for ticker in tickers:
            # Fetch market cap
            market_cap = fetch_market_cap(ticker)
            total_market_cap += market_cap
            
            # Fetch price and EMA
            current_price, ema20 = fetch_stock_price(ticker)
            
            # Calculate momentum if we have both price and EMA
            if current_price and ema20 and ema20 > 0:
                momentum = ((current_price - ema20) / ema20) * 100  # Percentage difference
                weighted_momentum += momentum * market_cap  # Weight by market cap
                valid_momentum_count += 1
            
            # Respect API rate limits
            time.sleep(0.1)
        
        # Calculate market-cap weighted momentum
        if total_market_cap > 0 and valid_momentum_count > 0:
            avg_momentum = weighted_momentum / total_market_cap
        else:
            avg_momentum = 0
        
        # Store sector data
        sectors_data[sector] = {
            'market_cap': total_market_cap,
            'momentum': avg_momentum
        }
        
        print(f"{sector}: {total_market_cap:.2f}B USD")
        
        # Add to sector_values list for CSV export
        sector_values.append({
            'date': today,
            'sector': sector,
            'market_cap': total_market_cap,
            'momentum': avg_momentum
        })
    
    # Save to CSV
    csv_path = os.path.join('data', 'sector_values.csv')
    with open(csv_path, 'w', newline='') as csvfile:
        fieldnames = ['date', 'sector', 'market_cap', 'momentum']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in sector_values:
            writer.writerow(row)
    
    print(f"Sector values for {today} saved to {csv_path}")
    return sectors_data

def collect_daily_sector_data():
    """Main function to collect daily sector data"""
    try:
        today = get_eastern_date()
        print(f"Running daily sector data collection for {today}...")
        
        # Process sector data
        sector_data = process_sector_data()
        
        print(f"Successfully collected and saved sector values")
        return True
    except Exception as e:
        print(f"Error in daily sector data collection: {e}")
        return False

if __name__ == "__main__":
    collect_daily_sector_data()
