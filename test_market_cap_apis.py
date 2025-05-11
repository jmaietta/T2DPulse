"""
Test market cap retrieval from Polygon.io and NASDAQ Data Link APIs
"""

import os
import pandas as pd
from pprint import pprint
from sector_market_cap import _polygon_market_cap, _nasdaq_market_cap, _yf_market_cap, SECTORS

def test_polygon_api():
    """Test retrieving market cap data for a few tickers from Polygon API"""
    print("\n==== Testing Polygon.io API Market Cap Retrieval ====")
    
    test_tickers = ["AAPL", "GOOGL", "MSFT", "AMZN", "META"]
    results = {}
    
    for ticker in test_tickers:
        market_cap = _polygon_market_cap(ticker)
        if market_cap:
            market_cap_billion = market_cap / 1_000_000_000
            results[ticker] = f"${market_cap_billion:.2f}B"
            print(f"✓ {ticker}: ${market_cap_billion:.2f}B")
        else:
            results[ticker] = "Failed"
            print(f"✗ {ticker}: Failed to retrieve market cap")
    
    return results

def test_nasdaq_api():
    """Test retrieving market cap data for a few tickers from NASDAQ Data Link API"""
    print("\n==== Testing NASDAQ Data Link API Market Cap Retrieval ====")
    
    test_tickers = ["AAPL", "GOOGL", "MSFT", "AMZN", "META"]
    results = {}
    
    for ticker in test_tickers:
        market_cap = _nasdaq_market_cap(ticker)
        if market_cap:
            market_cap_billion = market_cap / 1_000_000_000
            results[ticker] = f"${market_cap_billion:.2f}B"
            print(f"✓ {ticker}: ${market_cap_billion:.2f}B")
        else:
            results[ticker] = "Failed"
            print(f"✗ {ticker}: Failed to retrieve market cap")
    
    return results

def test_yfinance_api():
    """Test retrieving market cap data for a few tickers from YFinance API"""
    print("\n==== Testing YFinance API Market Cap Retrieval ====")
    
    test_tickers = ["AAPL", "GOOGL", "MSFT", "AMZN", "META"]
    results = {}
    
    for ticker in test_tickers:
        market_cap = _yf_market_cap(ticker)
        if market_cap:
            market_cap_billion = market_cap / 1_000_000_000
            results[ticker] = f"${market_cap_billion:.2f}B"
            print(f"✓ {ticker}: ${market_cap_billion:.2f}B")
        else:
            results[ticker] = "Failed"
            print(f"✗ {ticker}: Failed to retrieve market cap")
    
    return results

def analyze_coverage():
    """Analyze API coverage across all sectors"""
    print("\n==== Analyzing API Coverage Across All Sectors ====")
    
    all_tickers = set()
    for sector, tickers in SECTORS.items():
        for ticker in tickers:
            all_tickers.add(ticker)
    
    print(f"Total unique tickers: {len(all_tickers)}")
    print("Testing a sample of 5 tickers from each API...")
    
    # Sample 5 tickers for testing
    sample_tickers = list(all_tickers)[:5]
    
    results = {
        "Polygon": {},
        "NASDAQ": {},
        "YFinance": {}
    }
    
    for ticker in sample_tickers:
        # Test Polygon
        polygon_mc = _polygon_market_cap(ticker)
        if polygon_mc:
            results["Polygon"][ticker] = f"${polygon_mc/1_000_000_000:.2f}B"
        else:
            results["Polygon"][ticker] = "Failed"
            
        # Test NASDAQ
        nasdaq_mc = _nasdaq_market_cap(ticker)
        if nasdaq_mc:
            results["NASDAQ"][ticker] = f"${nasdaq_mc/1_000_000_000:.2f}B"
        else:
            results["NASDAQ"][ticker] = "Failed"
            
        # Test YFinance
        yf_mc = _yf_market_cap(ticker)
        if yf_mc:
            results["YFinance"][ticker] = f"${yf_mc/1_000_000_000:.2f}B"
        else:
            results["YFinance"][ticker] = "Failed"
    
    # Create a DataFrame to show the results
    print("\nAPI Coverage Matrix for Sample Tickers:")
    df = pd.DataFrame(results)
    print(df)
    
    return df

if __name__ == "__main__":
    print("Testing market cap APIs...")
    
    # Check for API keys
    polygon_key = os.environ.get("POLYGON_API_KEY")
    nasdaq_key = os.environ.get("NASDAQ_DATA_LINK_API_KEY")
    
    if not polygon_key:
        print("⚠️ POLYGON_API_KEY not found in environment!")
    else:
        print("✓ POLYGON_API_KEY found")
        
    if not nasdaq_key:
        print("⚠️ NASDAQ_DATA_LINK_API_KEY not found in environment!")
    else:
        print("✓ NASDAQ_DATA_LINK_API_KEY found")
    
    # Run tests
    if polygon_key:
        polygon_results = test_polygon_api()
    
    if nasdaq_key:
        nasdaq_results = test_nasdaq_api()
    
    yfinance_results = test_yfinance_api()
    
    # Analyze coverage
    coverage_df = analyze_coverage()
    
    print("\nAPI Testing Complete")