#!/usr/bin/env python3
# historical_data_manager.py
# -----------------------------------------------------------
# Comprehensive historical data management system for ticker prices and market caps
# This ensures we always maintain at least 30 days of historical data for all tickers

import os
import pandas as pd
import json
from datetime import datetime, timedelta
import pytz
import config
from improved_finnhub_data_collector import (
    fetch_market_cap_finnhub,
    fetch_market_cap_yfinance,
    fetch_market_cap_alphavantage,
    fetch_price_finnhub,
    fetch_price_yfinance,
    fetch_price_alphavantage,
    calculate_market_cap_from_shares_and_price
)

# Constants
DATA_DIR = 'data'
HISTORICAL_PRICES_FILE = os.path.join(DATA_DIR, 'historical_ticker_prices.csv')
HISTORICAL_MARKETCAP_FILE = os.path.join(DATA_DIR, 'historical_ticker_marketcap.csv')
HISTORICAL_DATA_LOG = os.path.join(DATA_DIR, 'historical_data_log.json')
MIN_HISTORY_DAYS = 30

# Ensure data directory exists
os.makedirs(DATA_DIR, exist_ok=True)

def get_eastern_date():
    """Get the current date in US Eastern Time"""
    eastern = pytz.timezone('US/Eastern')
    now = datetime.now(eastern)
    return now.strftime('%Y-%m-%d')

def get_all_tickers():
    """Get all tickers from all sectors"""
    all_tickers = []
    for sector, tickers in config.SECTORS.items():
        all_tickers.extend(tickers)
    return sorted(list(set(all_tickers)))

def initialize_historical_dataframes(all_tickers, start_date, end_date=None):
    """Initialize historical dataframes with proper date index and all ticker columns
    
    Args:
        all_tickers: List of all tickers to include
        start_date: Start date for historical data (datetime or string YYYY-MM-DD)
        end_date: End date for historical data (defaults to today)
        
    Returns:
        tuple: (historical_price_df, historical_marketcap_df)
    """
    # Convert start_date to datetime if it's a string
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, '%Y-%m-%d')
    
    # Default end_date to today if not provided
    if end_date is None:
        end_date = datetime.now()
    elif isinstance(end_date, str):
        end_date = datetime.strptime(end_date, '%Y-%m-%d')
    
    # Create date range
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    
    # Create empty DataFrames with date index
    price_df = pd.DataFrame(index=date_range.strftime('%Y-%m-%d'))
    marketcap_df = pd.DataFrame(index=date_range.strftime('%Y-%m-%d'))
    
    # Add columns for all tickers
    for ticker in all_tickers:
        price_df[ticker] = None
        marketcap_df[ticker] = None
    
    return price_df, marketcap_df

def load_historical_data():
    """Load historical price and market cap data or create new if files don't exist
    
    Returns:
        tuple: (historical_price_df, historical_marketcap_df, data_exists)
    """
    all_tickers = get_all_tickers()
    today = datetime.now()
    start_date = today - timedelta(days=MIN_HISTORY_DAYS)
    
    # Check if files exist
    price_exists = os.path.exists(HISTORICAL_PRICES_FILE)
    marketcap_exists = os.path.exists(HISTORICAL_MARKETCAP_FILE)
    
    if price_exists and marketcap_exists:
        try:
            # Load existing data
            price_df = pd.read_csv(HISTORICAL_PRICES_FILE)
            marketcap_df = pd.read_csv(HISTORICAL_MARKETCAP_FILE)
            
            # Ensure date column is present
            if 'date' not in price_df.columns or 'date' not in marketcap_df.columns:
                print("Error: Date column missing in historical data files")
                price_df, marketcap_df = initialize_historical_dataframes(all_tickers, start_date)
                return price_df, marketcap_df, False
            
            # Set date as index if it's not already
            if price_df.index.name != 'date':
                price_df.set_index('date', inplace=True)
            
            if marketcap_df.index.name != 'date':
                marketcap_df.set_index('date', inplace=True)
            
            # Check for missing tickers and add them
            for ticker in all_tickers:
                if ticker not in price_df.columns:
                    print(f"Adding new ticker {ticker} to historical price data")
                    price_df[ticker] = None
                
                if ticker not in marketcap_df.columns:
                    print(f"Adding new ticker {ticker} to historical market cap data")
                    marketcap_df[ticker] = None
            
            # Check if we have at least MIN_HISTORY_DAYS of history
            min_date = pd.to_datetime(price_df.index.min())
            if min_date > start_date:
                print(f"Historical data doesn't go back {MIN_HISTORY_DAYS} days, extending...")
                # Create new dataframes with extended date range
                new_price_df, new_marketcap_df = initialize_historical_dataframes(
                    all_tickers, start_date, today
                )
                
                # Merge existing data into new dataframes
                for date in price_df.index:
                    if date in new_price_df.index:
                        for ticker in all_tickers:
                            if ticker in price_df.columns:
                                new_price_df.loc[date, ticker] = price_df.loc[date, ticker]
                            if ticker in marketcap_df.columns:
                                new_marketcap_df.loc[date, ticker] = marketcap_df.loc[date, ticker]
                
                price_df = new_price_df
                marketcap_df = new_marketcap_df
            
            return price_df, marketcap_df, True
            
        except Exception as e:
            print(f"Error loading historical data: {e}")
            # Create new dataframes if there was an error
            price_df, marketcap_df = initialize_historical_dataframes(all_tickers, start_date)
            return price_df, marketcap_df, False
    else:
        # Create new dataframes if files don't exist
        print("Historical data files not found, creating new ones...")
        price_df, marketcap_df = initialize_historical_dataframes(all_tickers, start_date)
        return price_df, marketcap_df, False

def save_historical_data(price_df, marketcap_df):
    """Save historical price and market cap data to CSV files
    
    Args:
        price_df: DataFrame with historical price data
        marketcap_df: DataFrame with historical market cap data
    """
    try:
        # Ensure the index is named 'date'
        price_df.index.name = 'date'
        marketcap_df.index.name = 'date'
        
        # Save to CSV
        price_df.to_csv(HISTORICAL_PRICES_FILE)
        marketcap_df.to_csv(HISTORICAL_MARKETCAP_FILE)
        
        # Log the save
        log_data_update("Data saved successfully", {
            "price_rows": len(price_df),
            "price_columns": len(price_df.columns),
            "marketcap_rows": len(marketcap_df),
            "marketcap_columns": len(marketcap_df.columns)
        })
        
        print(f"Historical data saved to {HISTORICAL_PRICES_FILE} and {HISTORICAL_MARKETCAP_FILE}")
    except Exception as e:
        print(f"Error saving historical data: {e}")
        log_data_update("Error saving data", {"error": str(e)})

def fetch_ticker_data(ticker):
    """Fetch current price and market cap data for a ticker from all available sources
    
    Args:
        ticker: Ticker symbol
        
    Returns:
        dict: Dictionary with price and market cap data from different sources
    """
    result = {
        "ticker": ticker,
        "date": get_eastern_date(),
        "price": None,
        "market_cap": None,
        "sources_tried": {
            "price": [],
            "market_cap": []
        }
    }
    
    # Try all price sources
    price_sources = [
        ("finnhub", fetch_price_finnhub),
        ("yfinance", fetch_price_yfinance),
        ("alphavantage", fetch_price_alphavantage)
    ]
    
    for source_name, fetch_func in price_sources:
        try:
            result["sources_tried"]["price"].append(source_name)
            price = fetch_func(ticker)
            if price is not None:
                result["price"] = price
                result["price_source"] = source_name
                break
        except Exception as e:
            print(f"Error fetching {ticker} price from {source_name}: {e}")
    
    # Try all market cap sources
    marketcap_sources = [
        ("finnhub", fetch_market_cap_finnhub),
        ("yfinance", fetch_market_cap_yfinance),
        ("alphavantage", fetch_market_cap_alphavantage),
        ("calculated", calculate_market_cap_from_shares_and_price)
    ]
    
    for source_name, fetch_func in marketcap_sources:
        try:
            result["sources_tried"]["market_cap"].append(source_name)
            market_cap = fetch_func(ticker)
            if market_cap is not None:
                result["market_cap"] = market_cap
                result["market_cap_source"] = source_name
                break
        except Exception as e:
            print(f"Error fetching {ticker} market cap from {source_name}: {e}")
    
    return result

def update_historical_data():
    """Update historical price and market cap data with the latest values
    
    Returns:
        tuple: (updated_price_df, updated_marketcap_df, success)
    """
    today = get_eastern_date()
    print(f"Updating historical data for {today}...")
    
    # Load existing data
    price_df, marketcap_df, data_exists = load_historical_data()
    
    # Get all tickers
    all_tickers = get_all_tickers()
    print(f"Updating data for {len(all_tickers)} tickers")
    
    # Track updated tickers
    updated_tickers = {
        "price": [],
        "market_cap": [],
        "missing_price": [],
        "missing_market_cap": []
    }
    
    # Update data for each ticker
    for ticker in all_tickers:
        print(f"Fetching data for {ticker}...")
        result = fetch_ticker_data(ticker)
        
        # Update price data
        if result["price"] is not None:
            price_df.loc[today, ticker] = result["price"]
            updated_tickers["price"].append(ticker)
        else:
            updated_tickers["missing_price"].append(ticker)
            print(f"Warning: No price data available for {ticker}")
        
        # Update market cap data
        if result["market_cap"] is not None:
            marketcap_df.loc[today, ticker] = result["market_cap"]
            updated_tickers["market_cap"].append(ticker)
        else:
            updated_tickers["missing_market_cap"].append(ticker)
            print(f"Warning: No market cap data available for {ticker}")
    
    # Save updated data
    save_historical_data(price_df, marketcap_df)
    
    # Log results
    log_data_update("Data update completed", {
        "updated_price": len(updated_tickers["price"]),
        "updated_market_cap": len(updated_tickers["market_cap"]),
        "missing_price": updated_tickers["missing_price"],
        "missing_market_cap": updated_tickers["missing_market_cap"]
    })
    
    return price_df, marketcap_df, len(updated_tickers["missing_price"]) == 0 and len(updated_tickers["missing_market_cap"]) == 0

def verify_historical_data():
    """Verify historical data for completeness and consistency
    
    Returns:
        tuple: (is_valid, issues_found)
    """
    issues = []
    
    # Load historical data
    try:
        price_df = pd.read_csv(HISTORICAL_PRICES_FILE)
        marketcap_df = pd.read_csv(HISTORICAL_MARKETCAP_FILE)
    except Exception as e:
        issues.append(f"Error loading historical data files: {e}")
        return False, issues
    
    # Check date column
    if 'date' not in price_df.columns:
        issues.append("Price data missing date column")
    if 'date' not in marketcap_df.columns:
        issues.append("Market cap data missing date column")
    
    # Set date as index if present
    if 'date' in price_df.columns:
        price_df.set_index('date', inplace=True)
    if 'date' in marketcap_df.columns:
        marketcap_df.set_index('date', inplace=True)
    
    # Get all current tickers
    all_tickers = get_all_tickers()
    
    # Check for missing tickers
    for ticker in all_tickers:
        if ticker not in price_df.columns:
            issues.append(f"Price data missing ticker {ticker}")
        if ticker not in marketcap_df.columns:
            issues.append(f"Market cap data missing ticker {ticker}")
    
    # Check for minimum history
    today = datetime.now()
    min_date = today - timedelta(days=MIN_HISTORY_DAYS)
    
    if pd.to_datetime(price_df.index.min()) > min_date:
        issues.append(f"Price data history doesn't go back {MIN_HISTORY_DAYS} days")
    if pd.to_datetime(marketcap_df.index.min()) > min_date:
        issues.append(f"Market cap data history doesn't go back {MIN_HISTORY_DAYS} days")
    
    # Check for missing data in the last week
    one_week_ago = today - timedelta(days=7)
    recent_dates = pd.date_range(one_week_ago, today).strftime('%Y-%m-%d')
    
    missing_dates = []
    for date in recent_dates:
        if date not in price_df.index:
            missing_dates.append(date)
    
    if missing_dates:
        issues.append(f"Missing price data for dates: {', '.join(missing_dates)}")
    
    # Log verification results
    log_data_update("Data verification completed", {
        "issues_found": len(issues),
        "issues": issues
    })
    
    return len(issues) == 0, issues

def log_data_update(action, details=None):
    """Log data update actions to a JSON file
    
    Args:
        action: Description of the action
        details: Additional details about the action
    """
    log_entry = {
        "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "action": action
    }
    
    if details:
        log_entry["details"] = details
    
    # Read existing log
    log_data = []
    if os.path.exists(HISTORICAL_DATA_LOG):
        try:
            with open(HISTORICAL_DATA_LOG, 'r') as f:
                log_data = json.load(f)
        except:
            log_data = []
    
    # Add new entry
    log_data.append(log_entry)
    
    # Write updated log
    try:
        with open(HISTORICAL_DATA_LOG, 'w') as f:
            json.dump(log_data, f, indent=2)
    except Exception as e:
        print(f"Error writing to log file: {e}")

def rebuild_historical_data():
    """Completely rebuild historical data files from scratch
    
    Returns:
        bool: Success or failure
    """
    # Get all tickers
    all_tickers = get_all_tickers()
    
    # Calculate start date (30 days ago)
    today = datetime.now()
    start_date = today - timedelta(days=MIN_HISTORY_DAYS)
    
    # Initialize empty dataframes
    price_df, marketcap_df = initialize_historical_dataframes(all_tickers, start_date)
    
    # Save empty dataframes
    save_historical_data(price_df, marketcap_df)
    
    # Update with current data
    update_historical_data()
    
    # Verify the data
    is_valid, issues = verify_historical_data()
    
    return is_valid

def analyze_missing_data():
    """Analyze historical data for missing values and patterns
    
    Returns:
        dict: Analysis results
    """
    try:
        # Load historical data
        price_df = pd.read_csv(HISTORICAL_PRICES_FILE)
        marketcap_df = pd.read_csv(HISTORICAL_MARKETCAP_FILE)
        
        # Set date as index if needed
        if 'date' in price_df.columns:
            price_df.set_index('date', inplace=True)
        if 'date' in marketcap_df.columns:
            marketcap_df.set_index('date', inplace=True)
        
        # Get all current tickers
        all_tickers = get_all_tickers()
        
        # Initialize results
        results = {
            "total_tickers": len(all_tickers),
            "price_missing_count": {},
            "marketcap_missing_count": {},
            "completely_missing": [],
            "partially_missing": [],
            "fully_populated": []
        }
        
        # Analyze each ticker
        for ticker in all_tickers:
            # Check if ticker exists in dataframes
            price_exists = ticker in price_df.columns
            marketcap_exists = ticker in marketcap_df.columns
            
            if not price_exists and not marketcap_exists:
                results["completely_missing"].append(ticker)
                continue
            
            # Count missing values
            price_missing = 0
            if price_exists:
                price_missing = price_df[ticker].isna().sum()
                results["price_missing_count"][ticker] = price_missing
            else:
                results["price_missing_count"][ticker] = "column_missing"
            
            marketcap_missing = 0
            if marketcap_exists:
                marketcap_missing = marketcap_df[ticker].isna().sum()
                results["marketcap_missing_count"][ticker] = marketcap_missing
            else:
                results["marketcap_missing_count"][ticker] = "column_missing"
            
            # Categorize ticker based on completeness
            if price_missing == 0 and marketcap_missing == 0:
                results["fully_populated"].append(ticker)
            else:
                results["partially_missing"].append(ticker)
        
        # Add summary stats
        results["completely_missing_count"] = len(results["completely_missing"])
        results["partially_missing_count"] = len(results["partially_missing"])
        results["fully_populated_count"] = len(results["fully_populated"])
        
        return results
        
    except Exception as e:
        print(f"Error analyzing missing data: {e}")
        return {"error": str(e)}

def get_missing_data_by_sector():
    """Get missing data organized by sector
    
    Returns:
        dict: Sector-level missing data analysis
    """
    try:
        # Get all tickers and analyze missing data
        missing_data = analyze_missing_data()
        
        # Initialize sector results
        results = {}
        for sector, tickers in config.SECTORS.items():
            results[sector] = {
                "total_tickers": len(tickers),
                "completely_missing": [],
                "partially_missing": [],
                "fully_populated": []
            }
        
        # Categorize tickers by sector
        for sector, tickers in config.SECTORS.items():
            for ticker in tickers:
                if ticker in missing_data["completely_missing"]:
                    results[sector]["completely_missing"].append(ticker)
                elif ticker in missing_data["partially_missing"]:
                    results[sector]["partially_missing"].append(ticker)
                elif ticker in missing_data["fully_populated"]:
                    results[sector]["fully_populated"].append(ticker)
        
        # Add counts
        for sector in results:
            results[sector]["completely_missing_count"] = len(results[sector]["completely_missing"])
            results[sector]["partially_missing_count"] = len(results[sector]["partially_missing"])
            results[sector]["fully_populated_count"] = len(results[sector]["fully_populated"])
            results[sector]["total_missing_count"] = (
                results[sector]["completely_missing_count"] + 
                results[sector]["partially_missing_count"]
            )
            results[sector]["missing_percentage"] = (
                results[sector]["total_missing_count"] / results[sector]["total_tickers"] * 100
                if results[sector]["total_tickers"] > 0 else 0
            )
        
        return results
        
    except Exception as e:
        print(f"Error analyzing sector missing data: {e}")
        return {"error": str(e)}

def print_missing_data_summary():
    """Print a summary of missing data analysis"""
    print("\n========== HISTORICAL DATA ANALYSIS ==========")
    
    # Analyze missing data
    missing_data = analyze_missing_data()
    
    if "error" in missing_data:
        print(f"Error analyzing data: {missing_data['error']}")
        return
    
    print(f"Total tickers: {missing_data['total_tickers']}")
    print(f"Completely missing tickers: {missing_data['completely_missing_count']}")
    if missing_data["completely_missing"]:
        print(f"  {', '.join(missing_data['completely_missing'])}")
    
    print(f"Partially missing tickers: {missing_data['partially_missing_count']}")
    print(f"Fully populated tickers: {missing_data['fully_populated_count']}")
    
    print("\n========== MISSING DATA BY SECTOR ==========")
    sector_data = get_missing_data_by_sector()
    
    if "error" in sector_data:
        print(f"Error analyzing sector data: {sector_data['error']}")
        return
    
    # Sort sectors by missing percentage
    sorted_sectors = sorted(
        sector_data.items(),
        key=lambda x: x[1]["missing_percentage"],
        reverse=True
    )
    
    for sector, data in sorted_sectors:
        print(f"{sector}: {data['missing_percentage']:.1f}% missing data " +
              f"({data['total_missing_count']}/{data['total_tickers']} tickers)")
        
        if data["completely_missing"]:
            print(f"  Completely missing: {', '.join(data['completely_missing'])}")

if __name__ == "__main__":
    # Check if historical data exists and is valid
    if os.path.exists(HISTORICAL_PRICES_FILE) and os.path.exists(HISTORICAL_MARKETCAP_FILE):
        is_valid, issues = verify_historical_data()
        
        if not is_valid:
            print("Historical data has issues:")
            for issue in issues:
                print(f"  - {issue}")
                
            rebuild = input("Would you like to rebuild the historical data? (y/n): ").lower() == 'y'
            if rebuild:
                print("Rebuilding historical data...")
                rebuild_historical_data()
            else:
                print("Attempting to fix issues by updating data...")
                update_historical_data()
        else:
            print("Historical data is valid. Updating with latest values...")
            update_historical_data()
    else:
        print("Historical data files not found. Creating new ones...")
        update_historical_data()
    
    # Print missing data summary
    print_missing_data_summary()