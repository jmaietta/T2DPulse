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

# Cache to avoid duplicate API calls for the same ticker
MARKET_CAP_CACHE = {}
PRICE_EMA_CACHE = {}

def get_eastern_date():
    """Get the current date in US Eastern Time"""
    eastern = pytz.timezone('US/Eastern')
    now = datetime.now(eastern)
    return now.strftime('%Y-%m-%d')

def fetch_market_cap(ticker):
    """Fetch market cap for a given ticker from Finnhub with caching"""
    # Check cache first
    if ticker in MARKET_CAP_CACHE:
        print(f"Using cached market cap for {ticker}")
        return MARKET_CAP_CACHE[ticker]
    
    # Make API call if not in cache
    url = f"https://finnhub.io/api/v1/stock/profile2?symbol={ticker}&token={FINNHUB_API_KEY}"
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        if data and 'marketCapitalization' in data:
            # Finnhub returns market cap in millions, convert to billions
            market_cap = data['marketCapitalization'] * 1000000  # Convert to actual value
            # Cache the result
            MARKET_CAP_CACHE[ticker] = market_cap
            return market_cap
    elif response.status_code == 429:
        print(f"Failed to fetch market cap for {ticker}: 429 (rate limited)")
        time.sleep(1)  # Rate limit - wait a second before next request
    else:
        print(f"Failed to fetch market cap for {ticker}: {response.status_code}")
    
    # Cache failed results as 0 to avoid repeated failures
    MARKET_CAP_CACHE[ticker] = 0
    return 0

def fetch_stock_price(ticker):
    """Fetch latest stock price and 20-day EMA for a given ticker with caching"""
    # Check cache first
    if ticker in PRICE_EMA_CACHE:
        print(f"Using cached price/EMA for {ticker}")
        return PRICE_EMA_CACHE[ticker]
    
    # Get current price
    url = f"https://finnhub.io/api/v1/quote?symbol={ticker}&token={FINNHUB_API_KEY}"
    response = requests.get(url)
    
    if response.status_code != 200:
        print(f"Failed to fetch price for {ticker}: {response.status_code}")
        if response.status_code == 429:
            print(f"Rate limited while fetching price for {ticker}")
            time.sleep(1)  # Rate limit - wait a second before next request
        # Cache the failure to avoid repeated API calls
        PRICE_EMA_CACHE[ticker] = (None, None)
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
            print(f"Rate limited while fetching historical data for {ticker}")
            time.sleep(1)  # Rate limit - wait a second before next request
        # Cache the current price with no EMA
        PRICE_EMA_CACHE[ticker] = (current_price, None)
        return current_price, None
    
    candle_data = response.json()
    
    # Check if we have valid data
    if candle_data['s'] != 'ok':
        print(f"No valid candle data for {ticker}")
        # Cache the result
        PRICE_EMA_CACHE[ticker] = (current_price, None)
        return current_price, None
    
    # Calculate 20-day EMA
    if len(candle_data['c']) >= 20:
        prices = pd.Series(candle_data['c'])
        ema20 = prices.ewm(span=20, adjust=False).mean().iloc[-1]
        # Cache the result
        PRICE_EMA_CACHE[ticker] = (current_price, ema20)
        return current_price, ema20
    else:
        print(f"Not enough data for EMA calculation for {ticker}")
        # Cache the result
        PRICE_EMA_CACHE[ticker] = (current_price, None)
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
    
    # Save to CSV in a format compatible with update_sector_history.py
    csv_path = os.path.join('data', 'sector_values.csv')
    
    # First gather all unique dates
    dates = sorted(set([row['date'] for row in sector_values]))
    
    # Create a dict to convert sector data to columns
    sector_dict = {}
    for date in dates:
        sector_dict[date] = {'Date': date}
        # Initialize with 0 values
        for sector in SECTORS.keys():
            sector_dict[date][sector] = 0
    
    # Fill in the market cap values
    for row in sector_values:
        date = row['date']
        sector = row['sector']
        market_cap = row['market_cap']
        sector_dict[date][sector] = market_cap
    
    # Convert to rows for CSV
    csv_rows = []
    for date, data in sector_dict.items():
        csv_rows.append(data)
    
    # Get all column names (Date + all sectors)
    fieldnames = ['Date'] + list(SECTORS.keys())
    
    # Write CSV file with date in first column and sectors as other columns
    with open(csv_path, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in csv_rows:
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

def test_run():
    """A test run with limited scope to verify our code works"""
    print("Running limited test of Finnhub data collector with caching...")
    # Use a smaller set of sectors for testing to avoid rate limits
    global SECTORS
    test_sectors = {
        "Cloud Infrastructure": ["MSFT", "AMZN"],
        "AI Infrastructure": ["MSFT", "NVDA", "AMZN"]
    }
    old_sectors = SECTORS
    SECTORS = test_sectors
    
    try:
        # Run the data collection with our reduced sector set
        result = collect_daily_sector_data()
        print(f"Test result: {'Success' if result else 'Failed'}")
        
        # Verify that caching worked
        print(f"Market cap cache has {len(MARKET_CAP_CACHE)} entries")
        print(f"Price/EMA cache has {len(PRICE_EMA_CACHE)} entries")
        
        # We should only have 3 unique cached tickers (MSFT, AMZN, NVDA)
        # despite having 5 total ticker references
        if len(MARKET_CAP_CACHE) == 3 and len(PRICE_EMA_CACHE) == 3:
            print("✓ Caching is working correctly! We have 3 cache entries for 5 ticker references.")
        else:
            print("✗ Caching may not be working optimally.")
            
    finally:
        # Restore original sectors
        SECTORS = old_sectors

if __name__ == "__main__":
    # Choose whether to run full collection or just test
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        test_run()
    else:
        collect_daily_sector_data()
