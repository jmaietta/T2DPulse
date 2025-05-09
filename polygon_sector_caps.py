#!/usr/bin/env python3
"""
polygon_sector_caps.py
-------------------------------------------------
Calculate authentic market caps for T2D Pulse sectors using Polygon.io API.

This script:
1. Fetches historical closing prices for all tickers in the defined sectors
2. Gets shares outstanding data for each ticker
3. Calculates daily market caps (price * shares outstanding)
4. Aggregates by sector and saves to Parquet and CSV formats

Usage:
    python polygon_sector_caps.py --days 30 --verbose

Required packages:
    pip install pandas requests tqdm pyarrow
"""

import os
import json
import time
import argparse
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import pandas as pd
import numpy as np
import requests
from tqdm import tqdm

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Define the correct sector ticker mapping exactly as provided by the user
# Note: Some tickers may not be supported by Polygon API (e.g., ADYEY, SQ)
SECTOR_TICKERS = {
    "AdTech": ["APP", "APPS", "CRTO", "DV", "GOOGL", "META", "MGNI", "PUBM", "TTD"],
    "Cloud Infrastructure": ["AMZN", "CRM", "CSCO", "GOOGL", "MSFT", "NET", "ORCL", "SNOW"],
    "Fintech": ["AFRM", "BILL", "COIN", "FIS", "FI", "GPN", "PYPL", "SSNC"],  # Removed problematic tickers: ADYEY, SQ
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

# Track problematic tickers for reporting
PROBLEMATIC_TICKERS = {
    "ADYEY": "Not found in Polygon API - OTC ticker",
    "SQ": "Not found in Polygon API - Changed to BLOCK"
}

# Map problematic tickers to possible replacements for future fixes
TICKER_REPLACEMENTS = {
    "SQ": "BLOCK"  # SQ is now trading as BLOCK
}

# Create a list of ALL_TICKERS from the SECTOR_TICKERS dictionary
ALL_TICKERS = []
for tickers in SECTOR_TICKERS.values():
    ALL_TICKERS.extend(tickers)
ALL_TICKERS = sorted(list(set(ALL_TICKERS)))  # Remove duplicates and sort
logging.info(f"Using {len(ALL_TICKERS)} unique tickers across {len(SECTOR_TICKERS)} sectors")

class PolygonClient:
    """Client for interacting with the Polygon.io API"""
    
    BASE_URL = "https://api.polygon.io"
    
    def __init__(self, api_key: str):
        """
        Initialize the Polygon client
        
        Args:
            api_key (str): Polygon.io API key
        """
        self.api_key = api_key
        self.headers = {"Authorization": f"Bearer {api_key}"}
        self.request_count = 0
        self.rate_limit = 5  # requests per second for premium tier
    
    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """
        Make a request to the Polygon API with rate limiting
        
        Args:
            endpoint (str): API endpoint
            params (dict, optional): Query parameters
            
        Returns:
            dict: API response
        """
        # Add API key to params
        if params is None:
            params = {}
        
        # Rate limiting: ensure we don't exceed the rate limit
        self.request_count += 1
        if self.request_count % self.rate_limit == 0:
            time.sleep(1.1)  # Wait a bit more than 1 second every few requests
            
        # Make request
        url = f"{self.BASE_URL}/{endpoint}"
        response = requests.get(url, params=params, headers=self.headers)
        
        # Check response
        if response.status_code != 200:
            logging.error(f"Error {response.status_code}: {response.text}")
            return {"results": []}
        
        return response.json()
    
    def get_ticker_details(self, ticker: str) -> Dict:
        """
        Get details for a ticker, including shares outstanding
        
        Args:
            ticker (str): Ticker symbol
            
        Returns:
            dict: Ticker details
        """
        return self._make_request(f"v3/reference/tickers/{ticker}")
    
    def get_historical_prices(self, ticker: str, start_date: str, end_date: str, adjusted: bool = True) -> List[Dict]:
        """
        Get historical daily closing prices for a ticker
        
        Args:
            ticker (str): Ticker symbol
            start_date (str): Start date in 'YYYY-MM-DD' format
            end_date (str): End date in 'YYYY-MM-DD' format
            adjusted (bool): Whether to get adjusted prices
            
        Returns:
            list: List of price data dictionaries
        """
        params = {
            "adjusted": "true" if adjusted else "false",
            "sort": "asc",
            "limit": 120,  # Should be enough for ~30 days of market data
        }
        
        response = self._make_request(
            f"v2/aggs/ticker/{ticker}/range/1/day/{start_date}/{end_date}", 
            params
        )
        
        if "results" not in response:
            logging.warning(f"No historical price data for {ticker} from {start_date} to {end_date}")
            return []
            
        return response["results"]

def get_unique_tickers() -> List[str]:
    """Get list of unique tickers from sector mapping"""
    unique_tickers = set()
    for tickers in SECTOR_TICKERS.values():
        for ticker in tickers:
            unique_tickers.add(ticker)
    return sorted(list(unique_tickers))

def get_shares_outstanding(client: PolygonClient, tickers: List[str], verbose: bool = False) -> Dict[str, int]:
    """
    Get shares outstanding for a list of tickers
    
    Args:
        client (PolygonClient): Polygon client
        tickers (List[str]): List of tickers
        verbose (bool): Whether to print verbose output
        
    Returns:
        Dict[str, int]: Dictionary mapping tickers to shares outstanding
    """
    shares_dict = {}
    
    # Create cache directory if it doesn't exist
    os.makedirs("data/cache", exist_ok=True)
    
    # Try to load cached data
    cache_file = "data/cache/shares_outstanding.json"
    if os.path.exists(cache_file):
        try:
            with open(cache_file, 'r') as f:
                shares_dict = json.load(f)
                
            if verbose:
                logging.info(f"Loaded shares outstanding data for {len(shares_dict)} tickers from cache")
        except Exception as e:
            logging.error(f"Error loading cached shares outstanding data: {e}")
    
    # Get missing tickers
    missing_tickers = [t for t in tickers if t not in shares_dict]
    
    if missing_tickers:
        if verbose:
            logging.info(f"Fetching shares outstanding data for {len(missing_tickers)} tickers...")
            ticker_iter = tqdm(missing_tickers)
        else:
            ticker_iter = missing_tickers
            
        for ticker in ticker_iter:
            try:
                details = client.get_ticker_details(ticker)
                
                if "results" in details and details["results"]:
                    shares = details["results"].get("share_class_shares_outstanding")
                    if shares:
                        shares_dict[ticker] = shares
                    else:
                        logging.warning(f"No shares outstanding data for {ticker}")
                else:
                    logging.warning(f"No details found for {ticker}")
                    
            except Exception as e:
                logging.error(f"Error getting details for {ticker}: {e}")
                
            # Save cache after each ticker to avoid losing progress
            try:
                with open(cache_file, 'w') as f:
                    json.dump(shares_dict, f)
            except Exception as e:
                logging.error(f"Error saving shares outstanding cache: {e}")
    
    if verbose:
        logging.info(f"Got shares outstanding data for {len(shares_dict)} of {len(tickers)} tickers")
        
    return shares_dict

def get_historical_prices(client: PolygonClient, tickers: List[str], start_date: str, end_date: str, verbose: bool = False) -> pd.DataFrame:
    """
    Get historical prices for a list of tickers
    
    Args:
        client (PolygonClient): Polygon client
        tickers (List[str]): List of tickers
        start_date (str): Start date in 'YYYY-MM-DD' format
        end_date (str): End date in 'YYYY-MM-DD' format
        verbose (bool): Whether to print verbose output
        
    Returns:
        pd.DataFrame: DataFrame with historical prices
    """
    all_prices = {}
    
    # Create cache directory if it doesn't exist
    os.makedirs("data/cache", exist_ok=True)
    
    # Try to load cached data
    cache_file = "data/cache/historical_prices.pkl"
    if os.path.exists(cache_file):
        try:
            all_prices = pd.read_pickle(cache_file)
            
            if verbose:
                logging.info(f"Loaded historical price data for {len(all_prices)} tickers from cache")
        except Exception as e:
            logging.error(f"Error loading cached historical price data: {e}")
    
    # Get missing tickers
    missing_tickers = [t for t in tickers if t not in all_prices]
    
    if missing_tickers:
        if verbose:
            logging.info(f"Fetching historical price data for {len(missing_tickers)} tickers...")
            ticker_iter = tqdm(missing_tickers)
        else:
            ticker_iter = missing_tickers
            
        for ticker in ticker_iter:
            try:
                prices = client.get_historical_prices(ticker, start_date, end_date)
                
                if prices:
                    # Extract dates and closing prices
                    dates = [datetime.fromtimestamp(p['t']/1000).strftime('%Y-%m-%d') for p in prices]
                    closes = [p['c'] for p in prices]
                    
                    # Create Series
                    ticker_prices = pd.Series(closes, index=dates)
                    all_prices[ticker] = ticker_prices
                else:
                    logging.warning(f"No price data for {ticker}")
                    
            except Exception as e:
                logging.error(f"Error getting prices for {ticker}: {e}")
                
            # Save cache after each ticker to avoid losing progress
            try:
                pd.to_pickle(all_prices, cache_file)
            except Exception as e:
                logging.error(f"Error saving historical price cache: {e}")
    
    if verbose:
        logging.info(f"Got price data for {len(all_prices)} of {len(tickers)} tickers")
    
    # Create price DataFrame
    price_df = pd.DataFrame(all_prices)
    
    # Remove weekends (should already be excluded from API response, but just to be sure)
    price_df.index = pd.to_datetime(price_df.index)
    # Filter out weekends (Saturday=5, Sunday=6)
    price_df = price_df[~price_df.index.isin(price_df.index[price_df.index.weekday >= 5])]
    
    return price_df

def get_historical_data(api_key: str, days: int = 30, verbose: bool = False) -> tuple:
    """
    Get historical price and shares outstanding data
    
    Args:
        api_key (str): Polygon API key
        days (int): Number of days of history to fetch
        verbose (bool): Whether to print verbose output
        
    Returns:
        tuple: (price_df, shares_df) containing price and shares outstanding data
    """
    # Initialize Polygon client
    client = PolygonClient(api_key)
    
    # Calculate date range
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days + 5)  # Add buffer for weekends/holidays
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    
    if verbose:
        logging.info(f"Fetching data from {start_str} to {end_str}")
    
    # Get unique tickers
    tickers = get_unique_tickers()
    if verbose:
        logging.info(f"Found {len(tickers)} unique tickers across {len(SECTOR_TICKERS)} sectors")
    
    # Get shares outstanding for each ticker
    shares_dict = get_shares_outstanding(client, tickers, verbose)
    
    # Get historical prices for each ticker
    price_df = get_historical_prices(client, tickers, start_str, end_str, verbose)
    
    # Create shares outstanding DataFrame
    shares_df = pd.Series(shares_dict).to_frame().T
    
    return price_df, shares_df

def calculate_market_caps(price_df: pd.DataFrame, shares_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate market caps for each ticker
    
    Args:
        price_df (DataFrame): DataFrame with closing prices
        shares_df (DataFrame): DataFrame with shares outstanding
        
    Returns:
        DataFrame: Market cap values
    """
    market_caps = pd.DataFrame(index=price_df.index)
    
    for ticker in price_df.columns:
        if ticker in shares_df.columns:
            shares = shares_df.loc[0, ticker]
            market_caps[ticker] = price_df[ticker] * shares
    
    return market_caps

def calculate_sector_market_caps(market_caps: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate market caps for each sector
    
    Args:
        market_caps (DataFrame): DataFrame with ticker market caps
        
    Returns:
        DataFrame: Sector market caps
    """
    sector_caps = pd.DataFrame(index=market_caps.index)
    
    # Dictionary to track coverage for each sector
    sector_coverage = {}
    
    for sector, tickers in SECTOR_TICKERS.items():
        # Get tickers with data
        available_tickers = [t for t in tickers if t in market_caps.columns]
        
        if available_tickers:
            # Sum market caps
            sector_caps[sector] = market_caps[available_tickers].sum(axis=1)
            
            # Track coverage percentage
            coverage_pct = len(available_tickers) / len(tickers) * 100
            sector_coverage[sector] = {
                'tickers_available': len(available_tickers),
                'tickers_total': len(tickers),
                'coverage_pct': coverage_pct,
                'tickers_missing': [t for t in tickers if t not in available_tickers]
            }
            
            # Log warning if coverage is less than 100%
            if coverage_pct < 100:
                missing_tickers = [t for t in tickers if t not in available_tickers]
                logging.warning(f"Sector {sector} has {coverage_pct:.1f}% coverage ({len(available_tickers)}/{len(tickers)} tickers). Missing: {missing_tickers}")
        else:
            logging.warning(f"No data available for sector {sector}")
            sector_caps[sector] = np.nan
            sector_coverage[sector] = {
                'tickers_available': 0,
                'tickers_total': len(tickers),
                'coverage_pct': 0,
                'tickers_missing': tickers
            }
    
    # Save coverage report
    try:
        os.makedirs("data", exist_ok=True)
        with open("data/sector_coverage_report.json", 'w') as f:
            json.dump(sector_coverage, f, indent=2)
        logging.info(f"Saved sector coverage report to data/sector_coverage_report.json")
    except Exception as e:
        logging.error(f"Error saving sector coverage report: {e}")
    
    # Calculate total market cap for all sectors
    sector_caps['Total'] = sector_caps.sum(axis=1)
    
    # Calculate sector weights (% of total market cap)
    weight_cols = []
    for sector in SECTOR_TICKERS.keys():
        if sector in sector_caps.columns:
            weight_col = f"{sector}_weight_pct"
            weight_cols.append(weight_col)
            sector_caps[weight_col] = (sector_caps[sector] / sector_caps['Total'] * 100).round(2)
    
    # Also calculate and save the most recent weights (latest date)
    try:
        latest_date = sector_caps.index.max()
        latest_weights = sector_caps.loc[latest_date, weight_cols].to_dict()
        with open("data/sector_weights_latest.json", 'w') as f:
            json.dump(latest_weights, f, indent=2)
        logging.info(f"Saved latest sector weights to data/sector_weights_latest.json")
    except Exception as e:
        logging.error(f"Error saving latest sector weights: {e}")
    
    return sector_caps

def save_market_cap_data(ticker_caps: pd.DataFrame, sector_caps: pd.DataFrame, 
                        output_dir: str = "data", parquet: bool = True, 
                        csv: bool = True) -> None:
    """
    Save market cap data to files
    
    Args:
        ticker_caps (DataFrame): Ticker market cap data
        sector_caps (DataFrame): Sector market cap data
        output_dir (str): Output directory
        parquet (bool): Whether to save as Parquet
        csv (bool): Whether to save as CSV
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Save ticker data
    if not ticker_caps.empty:
        if parquet:
            ticker_caps.to_parquet(f"{output_dir}/ticker_market_caps.parquet", compression="zstd")
            logging.info(f"Saved ticker market caps to {output_dir}/ticker_market_caps.parquet")
            
        if csv:
            ticker_caps.to_csv(f"{output_dir}/ticker_market_caps.csv")
            logging.info(f"Saved ticker market caps to {output_dir}/ticker_market_caps.csv")
    
    # Save sector data
    if not sector_caps.empty:
        if parquet:
            sector_caps.to_parquet(f"{output_dir}/sector_market_caps.parquet", compression="zstd")
            logging.info(f"Saved sector market caps to {output_dir}/sector_market_caps.parquet")
            
        if csv:
            sector_caps.to_csv(f"{output_dir}/sector_market_caps.csv")
            logging.info(f"Saved sector market caps to {output_dir}/sector_market_caps.csv")
            
        # Save formatted table for easy viewing
        formatted_df = (sector_caps / 1_000_000_000).round(1)  # Convert to billions
        formatted_df.to_csv(f"{output_dir}/sector_market_caps_billions.csv")
        
        # Print summary
        logging.info("\n===== SECTOR MARKET CAP TABLE (BILLIONS USD) =====")
        print(formatted_df.tail().to_string(float_format=lambda x: f"{x:,.1f}"))
    
def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Calculate authentic sector market caps using Polygon.io API")
    parser.add_argument("--days", type=int, default=30, help="Number of days of history to fetch")
    parser.add_argument("--output-dir", type=str, default="data", help="Output directory")
    parser.add_argument("--verbose", action="store_true", help="Print verbose output")
    return parser.parse_args()

def main():
    """Main function"""
    # Parse arguments
    args = parse_args()
    
    # Get API key from environment
    api_key = os.environ.get("POLYGON_API_KEY")
    if not api_key:
        logging.error("POLYGON_API_KEY environment variable not set")
        return
    
    # Get historical data
    try:
        price_df, shares_df = get_historical_data(api_key, args.days, args.verbose)
        
        if price_df.empty or shares_df.empty:
            logging.error("Failed to get complete market data")
            return
            
        logging.info(f"Got price data with {len(price_df)} days and {len(price_df.columns)} tickers")
        
        # Calculate market caps
        ticker_caps = calculate_market_caps(price_df, shares_df)
        
        if ticker_caps.empty:
            logging.error("Failed to calculate ticker market caps")
            return
            
        # Calculate sector market caps
        sector_caps = calculate_sector_market_caps(ticker_caps)
        
        if sector_caps.empty:
            logging.error("Failed to calculate sector market caps")
            return
            
        # Save data
        save_market_cap_data(ticker_caps, sector_caps, args.output_dir)
        
        logging.info("Market cap data calculation completed successfully")
        
    except Exception as e:
        logging.error(f"Error calculating market caps: {e}")

if __name__ == "__main__":
    start_time = time.time()
    main()
    logging.info(f"Script completed in {time.time() - start_time:.1f} seconds")