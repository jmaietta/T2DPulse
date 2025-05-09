"""
Verify Historical Market Cap Calculations

This script verifies the historical market cap calculations for the past 30 days,
with a focus on AdTech sector integrity.
"""
import pandas as pd
import os
import numpy as np
from datetime import datetime, timedelta

def load_market_caps(filepath="data/sector_market_caps.parquet", fallback_csv="data/sector_market_caps.csv"):
    """Load market cap data from parquet or CSV file"""
    try:
        if os.path.exists(filepath):
            df = pd.read_parquet(filepath)
            print(f"Successfully loaded market cap data from {filepath}")
            return df
        elif os.path.exists(fallback_csv):
            df = pd.read_csv(fallback_csv, index_col=0, parse_dates=True)
            print(f"Using fallback CSV data from {fallback_csv}")
            return df
        else:
            print(f"Error: Could not find market cap data files at {filepath} or {fallback_csv}")
            return None
    except Exception as e:
        print(f"Error loading market cap data: {e}")
        return None

def analyze_adtech_history(df):
    """Analyze historical AdTech market cap values"""
    if df is None or df.empty:
        print("No data available to analyze")
        return
    
    # Sort DataFrame by date
    df = df.sort_index()
    
    # Extract AdTech data
    if 'AdTech' in df.columns:
        adtech_series = df['AdTech']
        
        # Convert to billions for readability
        adtech_billions = adtech_series / 1_000_000_000
        
        print(f"\nAdTech Market Cap History (past 30 days):")
        print(f"=========================================")
        
        # Calculate statistics
        latest_value = adtech_billions.iloc[-1]
        highest_value = adtech_billions.max()
        lowest_value = adtech_billions.min()
        average_value = adtech_billions.mean()
        
        # Display daily values
        print("Daily values (in billions USD):")
        for date, value in adtech_billions.items():
            print(f"  {date.strftime('%Y-%m-%d')}: ${value:.2f}")
        
        # Display statistics
        print("\nStatistics:")
        print(f"  Latest value: ${latest_value:.2f} billion")
        print(f"  Highest value (30 days): ${highest_value:.2f} billion")
        print(f"  Lowest value (30 days): ${lowest_value:.2f} billion")
        print(f"  Average value (30 days): ${average_value:.2f} billion")
        
        # Calculate change
        first_value = adtech_billions.iloc[0]
        change_pct = ((latest_value - first_value) / first_value) * 100
        print(f"  Change over period: {change_pct:.2f}%")
        
        # Skip plotting since we don't have matplotlib

def verify_ticker_data(sector="AdTech"):
    """Verify the ticker data for the specified sector"""
    # Look for ticker price cache
    cache_file = "data/cache/historical_prices.pkl"
    shares_cache = "data/cache/shares_outstanding.json"
    
    if not os.path.exists(cache_file):
        print(f"Error: Could not find historical price cache at {cache_file}")
        return
    
    if not os.path.exists(shares_cache):
        print(f"Error: Could not find shares outstanding cache at {shares_cache}")
        return
    
    # Load cached ticker data
    try:
        ticker_prices = pd.read_pickle(cache_file)
        print(f"Successfully loaded historical price data for {len(ticker_prices)} tickers")
        
        import json
        with open(shares_cache, 'r') as f:
            shares_outstanding = json.load(f)
        print(f"Successfully loaded shares outstanding data for {len(shares_outstanding)} tickers")
        
        # Define the sector tickers to verify (using the same list from polygon_sector_caps.py)
        sector_tickers = {
            "AdTech": ["APP", "APPS", "CRTO", "DV", "GOOGL", "META", "MGNI", "PUBM", "TTD"],
            "Cloud Infrastructure": ["AMZN", "CRM", "CSCO", "GOOGL", "MSFT", "NET", "ORCL", "SNOW"],
            "Fintech": ["AFRM", "BILL", "COIN", "FIS", "FI", "GPN", "PYPL", "SSNC"],
            "eCommerce": ["AMZN", "BABA", "BKNG", "CHWY", "EBAY", "ETSY", "PDD", "SE", "SHOP", "WMT"],
            "Consumer Internet": ["ABNB", "BKNG", "GOOGL", "META", "NFLX", "PINS", "SNAP", "SPOT", "TRIP", "YELP"],
            "IT Services": ["ACN", "CTSH", "DXC", "HPQ", "IBM", "INFY", "PLTR", "WIT"],
            "Hardware/Devices": ["AAPL", "DELL", "HPQ", "LOGI", "PSTG", "SMCI", "SSYS", "STX", "WDC"],
            "Cybersecurity": ["CHKP", "CRWD", "CYBR", "FTNT", "NET", "OKTA", "PANW", "S", "ZS"],
            "Dev Tools": ["DDOG", "ESTC", "GTLB", "MDB", "TEAM"],
            "AI Infrastructure": ["AMZN", "GOOGL", "IBM", "META", "MSFT", "NVDA", "ORCL"],
            "Semiconductors": ["AMAT", "AMD", "ARM", "AVGO", "INTC", "NVDA", "QCOM", "TSM"],
            "Vertical SaaS": ["CCCS", "CPRT", "CSGP", "GWRE", "ICE", "PCOR", "SSNC", "TTAN"],
            "Enterprise SaaS": ["ADSK", "AMZN", "CRM", "IBM", "MSFT", "NOW", "ORCL", "SAP", "WDAY"],
            "SMB SaaS": ["ADBE", "BILL", "GOOGL", "HUBS", "INTU", "META"]
        }
        
        # Get tickers for the specified sector
        if sector in sector_tickers:
            tickers = sector_tickers[sector]
            print(f"\nVerifying {len(tickers)} tickers for {sector} sector:")
            
            # Verify each ticker
            for ticker in tickers:
                if ticker in ticker_prices and ticker in shares_outstanding:
                    latest_price = ticker_prices[ticker].iloc[-1]
                    shares = shares_outstanding[ticker]
                    market_cap = latest_price * shares
                    market_cap_billions = market_cap / 1_000_000_000
                    
                    print(f"  {ticker}: ${market_cap_billions:.2f} billion")
                    print(f"    - Latest price: ${latest_price:.2f}")
                    print(f"    - Shares outstanding: {shares:,}")
                else:
                    missing = []
                    if ticker not in ticker_prices:
                        missing.append("price data")
                    if ticker not in shares_outstanding:
                        missing.append("shares outstanding")
                    print(f"  {ticker}: Missing {', '.join(missing)}")
            
            # Calculate total sector market cap
            total_market_cap = 0
            for ticker in tickers:
                if ticker in ticker_prices and ticker in shares_outstanding:
                    latest_price = ticker_prices[ticker].iloc[-1]
                    shares = shares_outstanding[ticker]
                    market_cap = latest_price * shares
                    total_market_cap += market_cap
            
            total_market_cap_billions = total_market_cap / 1_000_000_000
            print(f"\nTotal {sector} Market Cap: ${total_market_cap_billions:.2f} billion")
            
        else:
            print(f"Error: Sector '{sector}' not found")
    
    except Exception as e:
        print(f"Error analyzing ticker data: {e}")

def main():
    """Main function"""
    print("Verifying historical market cap calculations...")
    
    # Load historical market cap data
    market_cap_df = load_market_caps()
    
    if market_cap_df is not None:
        # Analyze AdTech history
        analyze_adtech_history(market_cap_df)
        
        # Verify ticker-level calculations
        verify_ticker_data("AdTech")
    
    print("\nVerification complete.")

if __name__ == "__main__":
    main()