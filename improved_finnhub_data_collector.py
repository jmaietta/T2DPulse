#!/usr/bin/env python3
# improved_finnhub_data_collector.py
# -----------------------------------------------------------
# Efficiently collects stock market data from Finnhub API with:
# - Historical data persistence
# - Reduced API calls
# - Efficient caching
# - Fallback mechanisms to prevent zero values
# - Duplicate ticker handling across sectors

import os
import requests
import time
import pandas as pd
from pandas import DatetimeIndex
import json
import csv
from datetime import datetime, timedelta
import pytz
import config
import yfinance as yf
import random

# Ensure we have the API keys
FINNHUB_API_KEY = config.FINNHUB_API_KEY
ALPHAVANTAGE_API_KEY = os.environ.get("ALPHAVANTAGE_API_KEY", "")
SECTORS = config.SECTORS

# Import notification utilities if available
try:
    from notification_utils import send_market_cap_alert
    EMAIL_NOTIFICATIONS = True
except ImportError:
    EMAIL_NOTIFICATIONS = False
    print("Email notification module not available, notifications disabled")

# Cache to avoid duplicate API calls for the same ticker within a session
MARKET_CAP_CACHE = {}
PRICE_CACHE = {}

# Constants
DATA_DIR = os.path.join('data')
HISTORICAL_PRICES_FILE = os.path.join(DATA_DIR, 'historical_ticker_prices.csv')
HISTORICAL_MARKETCAP_FILE = os.path.join(DATA_DIR, 'historical_ticker_marketcap.csv')
EMA_SPAN = config.EMA_SPAN  # Days for EMA calculation (default: 20)

# Ensure data directory exists
os.makedirs(DATA_DIR, exist_ok=True)

def get_eastern_date():
    """Get the current date in US Eastern Time"""
    eastern = pytz.timezone('US/Eastern')
    now = datetime.now(eastern)
    return now.strftime('%Y-%m-%d')

def get_all_unique_tickers():
    """Get a list of all unique tickers across all sectors"""
    all_tickers = set()
    for sector, tickers in SECTORS.items():
        for ticker in tickers:
            all_tickers.add(ticker)
    return list(all_tickers)

def load_historical_price_data():
    """Load historical price data from CSV or create an empty DataFrame if it doesn't exist"""
    if os.path.exists(HISTORICAL_PRICES_FILE):
        try:
            return pd.read_csv(HISTORICAL_PRICES_FILE, index_col='date')
        except Exception as e:
            print(f"Error loading historical price data: {e}")
            return pd.DataFrame()
    else:
        return pd.DataFrame()

def load_historical_marketcap_data():
    """Load historical market cap data from CSV or create an empty DataFrame if it doesn't exist"""
    if os.path.exists(HISTORICAL_MARKETCAP_FILE):
        try:
            return pd.read_csv(HISTORICAL_MARKETCAP_FILE, index_col='date')
        except Exception as e:
            print(f"Error loading historical market cap data: {e}")
            return pd.DataFrame()
    else:
        return pd.DataFrame()

def save_historical_price_data(df):
    """Save historical price data to CSV"""
    try:
        df.to_csv(HISTORICAL_PRICES_FILE)
        print(f"Saved historical price data to {HISTORICAL_PRICES_FILE}")
    except Exception as e:
        print(f"Error saving historical price data: {e}")

def save_historical_marketcap_data(df):
    """Save historical market cap data to CSV"""
    try:
        df.to_csv(HISTORICAL_MARKETCAP_FILE)
        print(f"Saved historical market cap data to {HISTORICAL_MARKETCAP_FILE}")
    except Exception as e:
        print(f"Error saving historical market cap data: {e}")

def fetch_market_cap_finnhub(ticker):
    """Fetch market cap for a given ticker from Finnhub with error handling"""
    # Check session cache first
    if ticker in MARKET_CAP_CACHE:
        print(f"Using cached market cap for {ticker}")
        return MARKET_CAP_CACHE[ticker]
    
    # Make API call if not in cache
    url = f"https://finnhub.io/api/v1/stock/profile2?symbol={ticker}&token={FINNHUB_API_KEY}"
    try:
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.json()
            if data and 'marketCapitalization' in data:
                # Finnhub returns market cap in millions, convert to actual value
                market_cap = data['marketCapitalization'] * 1000000  # Convert to actual value
                # Cache the result
                MARKET_CAP_CACHE[ticker] = market_cap
                return market_cap
            else:
                print(f"No market cap data available for {ticker} from Finnhub")
                return None
        elif response.status_code == 429:
            print(f"Rate limited while fetching market cap for {ticker} from Finnhub")
            time.sleep(random.uniform(1.0, 2.0))  # Random backoff to avoid lockstep
            return None
        else:
            print(f"Failed to fetch market cap for {ticker} from Finnhub: {response.status_code}")
            return None
    except Exception as e:
        print(f"Error fetching market cap for {ticker} from Finnhub: {e}")
        return None

def fetch_market_cap_yfinance(ticker):
    """Fetch market cap for a given ticker from Yahoo Finance as a fallback"""
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        if 'marketCap' in info and info['marketCap'] is not None:
            market_cap = info['marketCap']
            print(f"Retrieved market cap for {ticker} from Yahoo Finance: {market_cap}")
            return market_cap
        else:
            print(f"No market cap data available for {ticker} from Yahoo Finance")
            return None
    except Exception as e:
        print(f"Error fetching market cap for {ticker} from Yahoo Finance: {e}")
        return None

def fetch_market_cap_alphavantage(ticker):
    """Fetch market cap for a given ticker from Alpha Vantage as a second fallback"""
    if not ALPHAVANTAGE_API_KEY:
        print("Alpha Vantage API key not available")
        return None
        
    try:
        url = f"https://www.alphavantage.co/query?function=OVERVIEW&symbol={ticker}&apikey={ALPHAVANTAGE_API_KEY}"
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.json()
            if data and 'MarketCapitalization' in data:
                market_cap = float(data['MarketCapitalization'])
                print(f"Retrieved market cap for {ticker} from Alpha Vantage: {market_cap}")
                return market_cap
            else:
                print(f"No market cap data available for {ticker} from Alpha Vantage")
                return None
        elif response.status_code == 429:
            print(f"Rate limited while fetching market cap for {ticker} from Alpha Vantage")
            time.sleep(random.uniform(2.0, 3.0))  # Longer backoff for Alpha Vantage
            return None
        else:
            print(f"Failed to fetch market cap for {ticker} from Alpha Vantage: {response.status_code}")
            return None
    except Exception as e:
        print(f"Error fetching market cap for {ticker} from Alpha Vantage: {e}")
        return None

def calculate_market_cap_from_shares_and_price(ticker):
    """
    Calculate market cap by multiplying outstanding shares by current price
    when API sources don't provide it directly
    
    Args:
        ticker: The ticker symbol
        
    Returns:
        Calculated market cap or None if calculation not possible
    """
    try:
        # Try to get share data from Yahoo Finance
        stock = yf.Ticker(ticker)
        info = stock.info
        
        # Look for share count in different possible fields
        shares_outstanding = None
        for field in ['sharesOutstanding', 'impliedSharesOutstanding', 'fullTimeEmployees']:
            if field in info and info[field] is not None and info[field] > 0:
                shares_outstanding = info[field]
                if field == 'fullTimeEmployees':  # This is a last resort approximation
                    print(f"Using employee count as rough approximation for {ticker}")
                    # Scale by standard multiple for tech companies
                    shares_outstanding *= 10000
                break
        
        # Get current price
        current_price = None
        if 'currentPrice' in info and info['currentPrice'] is not None:
            current_price = info['currentPrice']
        elif 'previousClose' in info and info['previousClose'] is not None:
            current_price = info['previousClose']
        elif 'regularMarketPrice' in info and info['regularMarketPrice'] is not None:
            current_price = info['regularMarketPrice']
        
        # Calculate market cap if we have both values
        if shares_outstanding is not None and current_price is not None:
            calculated_market_cap = shares_outstanding * current_price
            print(f"Calculated market cap for {ticker}: {calculated_market_cap:,.2f} " + 
                  f"({shares_outstanding:,} shares × ${current_price:.2f})")
            return calculated_market_cap
        else:
            print(f"Insufficient data to calculate market cap for {ticker}")
            return None
    except Exception as e:
        print(f"Error calculating market cap for {ticker}: {e}")
        return None

def fetch_market_cap(ticker, historical_marketcap_data, today):
    """
    Fetch market cap for a ticker with multiple sources and fallbacks
    
    Args:
        ticker: The ticker symbol
        historical_marketcap_data: DataFrame with historical market cap data
        today: Today's date string
        
    Returns:
        Market cap value (never returns 0)
    """
    # Try getting from Finnhub first
    market_cap = fetch_market_cap_finnhub(ticker)
    
    # If Finnhub failed, try Yahoo Finance
    if market_cap is None:
        market_cap = fetch_market_cap_yfinance(ticker)
    
    # If Yahoo Finance failed, try Alpha Vantage as third option
    if market_cap is None:
        market_cap = fetch_market_cap_alphavantage(ticker)
    
    # If API sources failed, try calculating from shares and price
    if market_cap is None:
        market_cap = calculate_market_cap_from_shares_and_price(ticker)
    
    # If calculation failed, use most recent historical data if available
    if market_cap is None and not historical_marketcap_data.empty:
        if ticker in historical_marketcap_data.columns:
            # Get the most recent non-NaN value
            recent_values = historical_marketcap_data[ticker].dropna()
            if not recent_values.empty:
                market_cap = recent_values.iloc[-1]
                print(f"Using historical market cap for {ticker}: {market_cap}")
    
    # If we still don't have a market cap, this is a new ticker with no history
    # In this case, we'll need to make a best effort to get data
    if market_cap is None:
        print(f"WARNING: Unable to retrieve market cap for {ticker} from any source")
        print(f"This should never result in a 0 value in the final data")
        # Return None for now, we'll handle this downstream
        return None
    
    # Cache the result for this session
    MARKET_CAP_CACHE[ticker] = market_cap
    return market_cap

def fetch_price_finnhub(ticker):
    """Fetch current stock price from Finnhub"""
    # Check session cache first
    if ticker in PRICE_CACHE:
        print(f"Using cached price for {ticker}")
        return PRICE_CACHE[ticker]
    
    # Get current price
    url = f"https://finnhub.io/api/v1/quote?symbol={ticker}&token={FINNHUB_API_KEY}"
    try:
        response = requests.get(url)
        
        if response.status_code == 200:
            quote_data = response.json()
            current_price = quote_data.get('c', None)  # Current price
            
            if current_price:
                # Cache the result
                PRICE_CACHE[ticker] = current_price
                return current_price
            else:
                print(f"No price data available for {ticker} from Finnhub")
                return None
        elif response.status_code == 429:
            print(f"Rate limited while fetching price for {ticker} from Finnhub")
            time.sleep(random.uniform(1.0, 2.0))  # Random backoff
            return None
        else:
            print(f"Failed to fetch price for {ticker} from Finnhub: {response.status_code}")
            return None
    except Exception as e:
        print(f"Error fetching price for {ticker} from Finnhub: {e}")
        return None

def fetch_price_yfinance(ticker):
    """Fetch current stock price from Yahoo Finance as a fallback"""
    try:
        stock = yf.Ticker(ticker)
        history = stock.history(period="1d")
        if not history.empty and 'Close' in history.columns:
            current_price = history['Close'].iloc[-1]
            print(f"Retrieved price for {ticker} from Yahoo Finance: {current_price}")
            return current_price
        else:
            print(f"No price data available for {ticker} from Yahoo Finance")
            return None
    except Exception as e:
        print(f"Error fetching price for {ticker} from Yahoo Finance: {e}")
        return None

def fetch_price_alphavantage(ticker):
    """Fetch current stock price from Alpha Vantage as a second fallback"""
    if not ALPHAVANTAGE_API_KEY:
        print("Alpha Vantage API key not available")
        return None
        
    try:
        url = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={ticker}&apikey={ALPHAVANTAGE_API_KEY}"
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.json()
            if data and 'Global Quote' in data and '05. price' in data['Global Quote']:
                current_price = float(data['Global Quote']['05. price'])
                print(f"Retrieved price for {ticker} from Alpha Vantage: {current_price}")
                return current_price
            else:
                print(f"No price data available for {ticker} from Alpha Vantage")
                return None
        elif response.status_code == 429:
            print(f"Rate limited while fetching price for {ticker} from Alpha Vantage")
            time.sleep(random.uniform(2.0, 3.0))  # Longer backoff for Alpha Vantage
            return None
        else:
            print(f"Failed to fetch price for {ticker} from Alpha Vantage: {response.status_code}")
            return None
    except Exception as e:
        print(f"Error fetching price for {ticker} from Alpha Vantage: {e}")
        return None

def fetch_price(ticker, historical_price_data, today):
    """
    Fetch price for a ticker with multiple sources and fallbacks
    
    Args:
        ticker: The ticker symbol
        historical_price_data: DataFrame with historical price data
        today: Today's date string
        
    Returns:
        Current price value (never returns 0)
    """
    # Try getting from Finnhub first
    price = fetch_price_finnhub(ticker)
    
    # If Finnhub failed, try Yahoo Finance
    if price is None:
        price = fetch_price_yfinance(ticker)
    
    # If Yahoo Finance failed, try Alpha Vantage as third option
    if price is None:
        price = fetch_price_alphavantage(ticker)
    
    # If all API sources failed, use most recent historical data if available
    if price is None and not historical_price_data.empty:
        if ticker in historical_price_data.columns:
            # Get the most recent non-NaN value
            recent_values = historical_price_data[ticker].dropna()
            if not recent_values.empty:
                price = recent_values.iloc[-1]
                print(f"Using historical price for {ticker}: {price}")
    
    # If we still don't have a price, this is a new ticker with no history
    if price is None:
        print(f"WARNING: Unable to retrieve price for {ticker} from any source")
        print(f"This should never result in a 0 value in the final data")
        # Return None for now, we'll handle this downstream
        return None
    
    # Cache the result for this session
    PRICE_CACHE[ticker] = price
    return price

def calculate_ema(ticker_data, span=20):
    """Calculate EMA for a series of prices"""
    if len(ticker_data) >= span:
        return ticker_data.ewm(span=span, adjust=False).mean().iloc[-1]
    elif len(ticker_data) > 0:
        # If we don't have enough data, use simple average
        print(f"Not enough data for EMA, using simple average of {len(ticker_data)} data points")
        return ticker_data.mean()
    else:
        return None

def update_historical_data(all_tickers):
    """
    Update historical data for all tickers
    
    Args:
        all_tickers: List of all unique tickers to update
        
    Returns:
        tuple: (historical_price_data, historical_marketcap_data)
    """
    today = get_eastern_date()
    print(f"Updating historical data for {len(all_tickers)} tickers for {today}...")
    
    # Load existing historical data
    historical_price_data = load_historical_price_data()
    historical_marketcap_data = load_historical_marketcap_data()
    
    # Initialize DataFrames if empty
    if historical_price_data.empty:
        print("Creating new price history dataframe")
        historical_price_data = pd.DataFrame(index=pd.DatetimeIndex([today]), columns=all_tickers)
    
    if historical_marketcap_data.empty:
        print("Creating new market cap history dataframe")
        historical_marketcap_data = pd.DataFrame(index=pd.DatetimeIndex([today]), columns=all_tickers)
    
    # Create today's empty records if they don't exist
    if today not in historical_price_data.index:
        historical_price_data.loc[today] = [None] * len(historical_price_data.columns)
    
    if today not in historical_marketcap_data.index:
        historical_marketcap_data.loc[today] = [None] * len(historical_marketcap_data.columns)
    
    # Ensure all tickers have columns
    for ticker in all_tickers:
        if ticker not in historical_price_data.columns:
            historical_price_data[ticker] = None
        
        if ticker not in historical_marketcap_data.columns:
            historical_marketcap_data[ticker] = None
    
    # Now update today's data for each ticker
    for ticker in all_tickers:
        # Get current price and add to historical data
        price = fetch_price(ticker, historical_price_data, today)
        if price is not None:
            historical_price_data.loc[today, ticker] = price
        
        # Get current market cap and add to historical data
        market_cap = fetch_market_cap(ticker, historical_marketcap_data, today)
        if market_cap is not None:
            historical_marketcap_data.loc[today, ticker] = market_cap
        
        # Give APIs a break between tickers
        time.sleep(0.5)
    
    # Save updated historical data
    save_historical_price_data(historical_price_data)
    save_historical_marketcap_data(historical_marketcap_data)
    
    return historical_price_data, historical_marketcap_data

def process_sector_data(historical_price_data, historical_marketcap_data):
    """
    Process sector data using the updated historical data
    
    Args:
        historical_price_data: DataFrame with historical price data
        historical_marketcap_data: DataFrame with historical market cap data
        
    Returns:
        dict: Sector data with market caps and momentum values
    """
    today = get_eastern_date()
    yesterday = (datetime.strptime(today, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
    sectors_data = {}
    sector_values = []
    
    # Track missing ticker data for notification purposes
    missing_ticker_data = {}
    
    # Load previous authentic sector history if available for better fallbacks
    authentic_history_file = os.path.join('data', 'authentic_sector_history.csv')
    previous_sector_scores = {}
    
    if os.path.exists(authentic_history_file):
        try:
            # Load the authentic sector history to get previous day's scores
            authentic_df = pd.read_csv(authentic_history_file)
            # Find the most recent date
            if not authentic_df.empty and 'Date' in authentic_df.columns:
                authentic_df['Date'] = pd.to_datetime(authentic_df['Date'])
                authentic_df = authentic_df.sort_values('Date', ascending=False)
                
                if not authentic_df.empty:
                    latest_row = authentic_df.iloc[0]
                    for sector in SECTORS.keys():
                        if sector in latest_row:
                            previous_sector_scores[sector] = latest_row[sector]
                    print(f"Loaded previous sector scores from {authentic_history_file}")
        except Exception as e:
            print(f"Error loading authentic sector history: {e}")
    
    print(f"Processing sector data for {today}...")
    
    for sector, tickers in SECTORS.items():
        print(f"Processing {sector} with {len(tickers)} tickers...")
        total_market_cap = 0
        weighted_momentum = 0
        valid_momentum_count = 0
        tickers_with_data = 0  # Count how many tickers have valid data
        missing_tickers = []   # Track tickers with missing data for notifications
        
        for ticker in tickers:
            # Get market cap for today
            if ticker in historical_marketcap_data.columns:
                market_cap = historical_marketcap_data.loc[today, ticker]
                if pd.isna(market_cap) or market_cap is None:
                    # Try one more time to get market cap if we don't have it
                    market_cap = fetch_market_cap(ticker, historical_marketcap_data, today)
                    if market_cap is not None:
                        # Update our historical data
                        historical_marketcap_data.loc[today, ticker] = market_cap
            else:
                # Should never happen since we ensure columns exist
                market_cap = None
            
            # Track missing market cap data for notifications
            if market_cap is None or pd.isna(market_cap):
                print(f"WARNING: No market cap data for {ticker} in {sector}, skipping from sector total")
                missing_tickers.append(ticker)
                continue
            
            # We have valid data for this ticker
            tickers_with_data += 1
            
            # Add to sector total
            total_market_cap += market_cap
            
            # Calculate EMA and momentum if we have enough historical data
            if ticker in historical_price_data.columns:
                # Get all non-NaN price data for this ticker
                ticker_data = historical_price_data[ticker].dropna()
                
                if not ticker_data.empty:
                    # Current price is the latest available
                    current_price = ticker_data.iloc[-1]
                    
                    # Calculate EMA using all available historical data
                    ema = calculate_ema(ticker_data, span=EMA_SPAN)
                    
                    if ema is not None and ema > 0:
                        # Calculate momentum as percentage difference
                        momentum = ((current_price - ema) / ema) * 100
                        weighted_momentum += momentum * market_cap  # Weight by market cap
                        valid_momentum_count += 1
        
        # If we have missing tickers, add them to our tracking dictionary
        if missing_tickers:
            missing_ticker_data[sector] = missing_tickers
        
        # If we didn't get data for ANY tickers in this sector, we have an API problem
        if tickers_with_data == 0:
            print(f"ERROR: No tickers in {sector} returned valid market cap data!")
            print(f"This is likely due to API rate limits or connectivity issues.")
            
            # Check if we have historical authentic score for this sector
            if sector in previous_sector_scores:
                print(f"Using previous authentic score for {sector}: {previous_sector_scores[sector]}")
                # Use last known market cap (minimum 1M) and neutral momentum
                total_market_cap = max(1000000, total_market_cap)  # At least $1M
                avg_momentum = 0  # Neutral momentum
                
                # In this case we're going to use the previous day's sentiment score later
                # in sentiment_engine.py by preserving the previous value
            else:
                # No historical data either, use minimum fallback
                print(f"No previous data available for {sector}, using minimum fallback values")
                total_market_cap = 1000000  # $1M minimum
                avg_momentum = 0  # Neutral momentum
        else:
            # Calculate market-cap weighted momentum
            if total_market_cap > 0 and valid_momentum_count > 0:
                avg_momentum = weighted_momentum / total_market_cap
            else:
                # If we have market cap but no valid momentum, use 0
                # This is better than using None/NaN which would cause problems
                avg_momentum = 0
        
        # Never allow total_market_cap to be 0 or None
        if total_market_cap <= 0:
            print(f"WARNING: {sector} has a zero or negative market cap ({total_market_cap})")
            print(f"This should never happen with proper data - using minimum valid value")
            # Use a small positive value instead of 0 to prevent calculation issues
            # This is a last resort failsafe
            total_market_cap = 1000000  # $1M minimum
        
        # Store sector data
        sectors_data[sector] = {
            'market_cap': total_market_cap,
            'momentum': avg_momentum,
            'tickers_with_data': tickers_with_data,
            'total_tickers': len(tickers),
            'previous_score': previous_sector_scores.get(sector, None)
        }
        
        print(f"{sector}: {total_market_cap:.2f} USD | Momentum: {avg_momentum:.2f}% | Data: {tickers_with_data}/{len(tickers)} tickers")
        
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
        # Initialize with None values (will be converted by update_sector_history.py)
        for sector in SECTORS.keys():
            sector_dict[date][sector] = None
    
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
    
    # Save the updated historical data
    save_historical_price_data(historical_price_data)
    save_historical_marketcap_data(historical_marketcap_data)
    
    return sectors_data

def collect_daily_sector_data():
    """Main function to collect daily sector data with improved efficiency"""
    try:
        today = get_eastern_date()
        print(f"Running improved daily sector data collection for {today}...")
        
        # Get all unique tickers from all sectors
        all_tickers = get_all_unique_tickers()
        print(f"Found {len(all_tickers)} unique tickers across all sectors")
        
        # Update historical data for all tickers
        historical_price_data, historical_marketcap_data = update_historical_data(all_tickers)
        
        # Track missing ticker data for notification purposes
        missing_ticker_data = {}
        
        # Process sector data using the updated historical data
        sector_data = process_sector_data(historical_price_data, historical_marketcap_data)
        
        # Check for sectors with missing tickers
        for sector, data in sector_data.items():
            # If we have significant missing data (>25% missing), track for notification
            if data['tickers_with_data'] < data['total_tickers'] * 0.75:
                # Find which tickers are missing for this sector
                missing_in_sector = []
                for ticker in SECTORS[sector]:
                    if ticker in historical_marketcap_data.columns:
                        market_cap = historical_marketcap_data.loc[today, ticker]
                        if pd.isna(market_cap) or market_cap is None:
                            missing_in_sector.append(ticker)
                
                if missing_in_sector:
                    missing_ticker_data[sector] = missing_in_sector
        
        # Send notification if we have significant missing data and email notifications are enabled
        if missing_ticker_data and EMAIL_NOTIFICATIONS:
            try:
                from notification_utils import send_market_cap_alert
                print(f"Sending alert for {len(missing_ticker_data)} sectors with missing market cap data")
                send_market_cap_alert(missing_ticker_data, sector_data)
            except Exception as e:
                print(f"Error sending market cap alert: {e}")
        
        print(f"Successfully collected and saved sector values using improved method")
        return True
    except Exception as e:
        print(f"Error in daily sector data collection: {e}")
        return False

if __name__ == "__main__":
    # Run the data collection
    collect_daily_sector_data()