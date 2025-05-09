"""
Improved Sector Market Cap Calculator

This script ensures complete and accurate calculation of market caps for all sectors
using the full ticker list for each sector.

Features:
1. Fetches data for all tickers in all sectors
2. Calculates daily market cap for each ticker
3. Aggregates by sector using the complete ticker list
4. Reports detailed coverage statistics
5. Logs any data collection issues
"""

import os
import json
import time
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import requests
from tqdm import tqdm

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Define directory for data and cache
DATA_DIR = "data"
CACHE_DIR = os.path.join(DATA_DIR, "cache")
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(CACHE_DIR, exist_ok=True)

# Complete sector ticker mapping (from polygon_sector_caps.py)
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

class ImprovedPolygonClient:
    """Enhanced client for Polygon.io API with better error handling and rate limiting"""
    
    BASE_URL = "https://api.polygon.io"
    
    def __init__(self, api_key, max_retries=3, retry_delay=2):
        self.api_key = api_key
        self.headers = {"Authorization": f"Bearer {api_key}"}
        self.request_count = 0
        self.rate_limit = 5  # requests per second for premium tier
        self.max_retries = max_retries
        self.retry_delay = retry_delay
    
    def _make_request(self, endpoint, params=None):
        """Make a request to the Polygon API with rate limiting and retries"""
        # Add API key to params
        if params is None:
            params = {}
        
        # Rate limiting: ensure we don't exceed the rate limit
        self.request_count += 1
        if self.request_count % self.rate_limit == 0:
            time.sleep(1.1)  # Wait a bit more than 1 second every few requests
        
        # Retry logic
        for attempt in range(self.max_retries):
            try:
                # Make request
                url = f"{self.BASE_URL}/{endpoint}"
                response = requests.get(url, params=params, headers=self.headers)
                
                # Check response
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:  # Rate limit exceeded
                    logging.warning(f"Rate limit exceeded. Waiting {self.retry_delay * (attempt+1)} seconds...")
                    time.sleep(self.retry_delay * (attempt+1))
                else:
                    logging.error(f"Error {response.status_code}: {response.text}")
                    if attempt < self.max_retries - 1:
                        logging.info(f"Retrying in {self.retry_delay} seconds...")
                        time.sleep(self.retry_delay)
            except Exception as e:
                logging.error(f"Request error: {e}")
                if attempt < self.max_retries - 1:
                    logging.info(f"Retrying in {self.retry_delay} seconds...")
                    time.sleep(self.retry_delay)
        
        logging.error(f"Failed to get data after {self.max_retries} attempts")
        return {"results": []}
    
    def get_ticker_details(self, ticker):
        """Get details for a ticker, including shares outstanding"""
        return self._make_request(f"v3/reference/tickers/{ticker}")
    
    def get_historical_prices(self, ticker, start_date, end_date, adjusted=True):
        """Get historical daily closing prices for a ticker"""
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

def get_all_tickers():
    """Get a list of all unique tickers across all sectors"""
    all_tickers = set()
    for sector_tickers in SECTOR_TICKERS.values():
        all_tickers.update(sector_tickers)
    return sorted(list(all_tickers))

def load_shares_outstanding_cache():
    """Load cached shares outstanding data"""
    cache_file = os.path.join(CACHE_DIR, "shares_outstanding.json")
    if os.path.exists(cache_file):
        try:
            with open(cache_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Error loading shares outstanding cache: {e}")
    return {}

def save_shares_outstanding_cache(shares_dict):
    """Save shares outstanding data to cache"""
    cache_file = os.path.join(CACHE_DIR, "shares_outstanding.json")
    try:
        with open(cache_file, 'w') as f:
            json.dump(shares_dict, f)
    except Exception as e:
        logging.error(f"Error saving shares outstanding cache: {e}")

def get_shares_outstanding(client, tickers, verbose=False):
    """Get shares outstanding for all tickers"""
    # Load cached data
    shares_dict = load_shares_outstanding_cache()
    
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
            save_shares_outstanding_cache(shares_dict)
    
    if verbose:
        logging.info(f"Got shares outstanding data for {len(shares_dict)} of {len(tickers)} tickers")
        
    return shares_dict

def load_price_cache():
    """Load cached price data"""
    cache_file = os.path.join(CACHE_DIR, "historical_prices.pkl")
    if os.path.exists(cache_file):
        try:
            return pd.read_pickle(cache_file)
        except Exception as e:
            logging.error(f"Error loading price cache: {e}")
    return {}

def save_price_cache(price_dict):
    """Save price data to cache"""
    cache_file = os.path.join(CACHE_DIR, "historical_prices.pkl")
    try:
        pd.to_pickle(price_dict, cache_file)
    except Exception as e:
        logging.error(f"Error saving price cache: {e}")

def get_historical_prices(client, tickers, start_date, end_date, verbose=False):
    """Get historical prices for all tickers"""
    # Load cached data
    price_dict = load_price_cache()
    
    # Get missing tickers
    missing_tickers = [t for t in tickers if t not in price_dict]
    
    if missing_tickers:
        if verbose:
            logging.info(f"Fetching price data for {len(missing_tickers)} tickers...")
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
                    price_dict[ticker] = ticker_prices
                else:
                    logging.warning(f"No price data for {ticker}")
                    
            except Exception as e:
                logging.error(f"Error getting prices for {ticker}: {e}")
                
            # Save cache after each ticker to avoid losing progress
            save_price_cache(price_dict)
    
    if verbose:
        logging.info(f"Got price data for {len(price_dict)} of {len(tickers)} tickers")
    
    # Create price DataFrame
    if price_dict:
        price_df = pd.DataFrame(price_dict)
        
        # Convert index to datetime and sort
        price_df.index = pd.to_datetime(price_df.index)
        price_df = price_df.sort_index()
        
        # Remove weekends (should already be excluded from API response, but just to be sure)
        price_df = price_df[~price_df.index.weekday.isin([5, 6])]  # 5=Saturday, 6=Sunday
        
        return price_df
    else:
        return pd.DataFrame()

def calculate_market_caps(price_df, shares_dict):
    """Calculate market cap for each ticker on each date"""
    market_caps = pd.DataFrame(index=price_df.index)
    
    for ticker in price_df.columns:
        if ticker in shares_dict:
            shares = shares_dict[ticker]
            market_caps[ticker] = price_df[ticker] * shares
    
    return market_caps

def calculate_sector_market_caps(market_caps, report_file=None):
    """Calculate market cap for each sector using the full ticker list"""
    sector_caps = pd.DataFrame(index=market_caps.index)
    coverage_report = {}
    
    # Calculate total market cap (used for weighting)
    total_market_cap = pd.Series(0, index=market_caps.index)
    
    # Process each sector
    for sector, tickers in SECTOR_TICKERS.items():
        # Get tickers with data
        available_tickers = [t for t in tickers if t in market_caps.columns]
        
        if available_tickers:
            # Sum market caps for the sector
            sector_caps[sector] = market_caps[available_tickers].sum(axis=1)
            
            # Add to total market cap
            total_market_cap += sector_caps[sector]
            
            # Track coverage
            coverage_pct = len(available_tickers) / len(tickers) * 100
            missing_tickers = [t for t in tickers if t not in available_tickers]
            
            coverage_report[sector] = {
                'tickers_available': len(available_tickers),
                'tickers_total': len(tickers),
                'coverage_pct': coverage_pct,
                'tickers_missing': missing_tickers,
                'available_tickers': available_tickers
            }
            
            # Log warning if coverage is less than 100%
            if coverage_pct < 100:
                logging.warning(f"Sector {sector} has {coverage_pct:.1f}% coverage ({len(available_tickers)}/{len(tickers)} tickers). Missing: {missing_tickers}")
        else:
            logging.warning(f"No data available for sector {sector}")
            sector_caps[sector] = np.nan
            coverage_report[sector] = {
                'tickers_available': 0,
                'tickers_total': len(tickers),
                'coverage_pct': 0,
                'tickers_missing': tickers,
                'available_tickers': []
            }
    
    # Add total market cap
    sector_caps["Total"] = total_market_cap
    
    # Calculate weight percentages
    for sector in sector_caps.columns:
        if sector != "Total":
            weight_col = f"{sector}_weight_pct"
            sector_caps[weight_col] = (sector_caps[sector] / sector_caps["Total"]) * 100
    
    # Save coverage report
    if report_file:
        try:
            with open(report_file, 'w') as f:
                json.dump(coverage_report, f, indent=2)
            logging.info(f"Saved sector coverage report to {report_file}")
        except Exception as e:
            logging.error(f"Error saving coverage report: {e}")
    
    return sector_caps, coverage_report

def run_collection(api_key, days=30, verbose=True):
    """Run the full market cap collection process"""
    # Initialize Polygon client
    client = ImprovedPolygonClient(api_key)
    
    # Calculate date range
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days + 5)  # Add buffer for weekends/holidays
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    
    if verbose:
        logging.info(f"Collecting market cap data from {start_str} to {end_str}")
    
    # Get all unique tickers
    all_tickers = get_all_tickers()
    logging.info(f"Found {len(all_tickers)} unique tickers across {len(SECTOR_TICKERS)} sectors")
    
    # Get shares outstanding
    shares_dict = get_shares_outstanding(client, all_tickers, verbose)
    logging.info(f"Collected shares outstanding data for {len(shares_dict)} tickers")
    
    # Get historical prices
    price_df = get_historical_prices(client, all_tickers, start_str, end_str, verbose)
    if not price_df.empty:
        logging.info(f"Collected price data for {len(price_df.columns)} tickers over {len(price_df)} days")
    else:
        logging.error("Failed to collect any price data")
        return None, None
    
    # Calculate market caps
    market_caps = calculate_market_caps(price_df, shares_dict)
    logging.info(f"Calculated market caps for {len(market_caps.columns)} tickers")
    
    # Calculate sector market caps
    report_file = os.path.join(DATA_DIR, "sector_coverage_report.json")
    sector_caps, coverage_report = calculate_sector_market_caps(market_caps, report_file)
    logging.info(f"Calculated market caps for {len(sector_caps.columns) - 1} sectors")  # -1 for Total column
    
    # Save results
    try:
        # Save as Parquet (efficient storage)
        parquet_file = os.path.join(DATA_DIR, "sector_market_caps.parquet")
        sector_caps.to_parquet(parquet_file)
        logging.info(f"Saved sector market caps to {parquet_file}")
        
        # Also save as CSV for compatibility
        csv_file = os.path.join(DATA_DIR, "sector_market_caps.csv")
        sector_caps.to_csv(csv_file)
        logging.info(f"Saved sector market caps to {csv_file}")
    except Exception as e:
        logging.error(f"Error saving sector market caps: {e}")
    
    return sector_caps, coverage_report

def analyze_results(sector_caps, coverage_report):
    """Analyze and print sector market cap results"""
    if sector_caps is None or sector_caps.empty:
        logging.error("No data to analyze")
        return
    
    # Get latest date's data
    latest_date = sector_caps.index.max()
    latest_data = sector_caps.loc[latest_date]
    
    # Convert to billions for readability
    latest_data_billions = latest_data / 1_000_000_000
    
    # Print summary
    print("\nSector Market Caps (as of {})".format(latest_date.strftime('%Y-%m-%d')))
    print("=" * 50)
    
    # Select only sector columns (not weight columns)
    sector_columns = [col for col in latest_data_billions.index if not col.endswith('_weight_pct')]
    
    # Sort by market cap in descending order
    sorted_sectors = sorted(
        [(sector, latest_data_billions[sector]) for sector in sector_columns if sector != "Total"],
        key=lambda x: x[1],
        reverse=True
    )
    
    # Print sorted sectors
    total_market_cap = latest_data_billions["Total"]
    print(f"Total Market Cap: ${total_market_cap:.2f} billion")
    print("\nSector Breakdown:")
    for sector, market_cap in sorted_sectors:
        if sector != "Total":
            weight = (market_cap / total_market_cap) * 100
            print(f"{sector}: ${market_cap:.2f} billion ({weight:.2f}%)")
    
    # Print coverage summary
    print("\nCoverage Summary:")
    total_tickers = sum(info['tickers_total'] for info in coverage_report.values())
    available_tickers = sum(info['tickers_available'] for info in coverage_report.values())
    duplicated_tickers = total_tickers - len(get_all_tickers())
    
    print(f"Total unique tickers: {len(get_all_tickers())}")
    print(f"Total sector tickers (including duplicates): {total_tickers}")
    print(f"Ticker duplication: {duplicated_tickers} tickers appear in multiple sectors")
    print(f"Tickers with data: {available_tickers} of {total_tickers} ({available_tickers/total_tickers*100:.1f}%)")
    
    # Print sectors with less than 100% coverage
    incomplete_sectors = [sector for sector, info in coverage_report.items() if info['coverage_pct'] < 100]
    if incomplete_sectors:
        print("\nSectors with incomplete coverage:")
        for sector in incomplete_sectors:
            info = coverage_report[sector]
            print(f"{sector}: {info['coverage_pct']:.1f}% coverage ({info['tickers_available']}/{info['tickers_total']} tickers)")
            print(f"  Missing tickers: {', '.join(info['tickers_missing'])}")

def verify_specific_sector(sector_caps, coverage_report, sector_name):
    """Verify and print details for a specific sector"""
    if sector_caps is None or sector_caps.empty:
        logging.error("No data to analyze")
        return
    
    if sector_name not in coverage_report:
        logging.error(f"Sector '{sector_name}' not found in coverage report")
        return
    
    # Get sector info
    sector_info = coverage_report[sector_name]
    
    # Get latest date's data
    latest_date = sector_caps.index.max()
    latest_value = sector_caps.loc[latest_date, sector_name] / 1_000_000_000  # convert to billions
    
    print(f"\nVerification for {sector_name} Sector")
    print("=" * 50)
    print(f"Latest market cap (as of {latest_date.strftime('%Y-%m-%d')}): ${latest_value:.2f} billion")
    print(f"Coverage: {sector_info['coverage_pct']:.1f}% ({sector_info['tickers_available']}/{sector_info['tickers_total']} tickers)")
    
    # Load ticker data
    price_cache = load_price_cache()
    shares_cache = load_shares_outstanding_cache()
    
    # Print tickers
    print("\nTicker Breakdown:")
    total_market_cap = 0
    
    for ticker in SECTOR_TICKERS[sector_name]:
        if ticker in price_cache and ticker in shares_cache:
            latest_price = price_cache[ticker].iloc[-1] if len(price_cache[ticker]) > 0 else None
            shares = shares_cache[ticker]
            
            if latest_price is not None:
                market_cap = latest_price * shares
                market_cap_billions = market_cap / 1_000_000_000
                total_market_cap += market_cap
                
                print(f"{ticker}: ${market_cap_billions:.2f} billion")
                print(f"  - Price: ${latest_price:.2f}")
                print(f"  - Shares outstanding: {shares:,}")
        else:
            missing = []
            if ticker not in price_cache:
                missing.append("price data")
            if ticker not in shares_cache:
                missing.append("shares outstanding")
            print(f"{ticker}: Missing {', '.join(missing)}")
    
    total_market_cap_billions = total_market_cap / 1_000_000_000
    print(f"\nTotal {sector_name} Market Cap (calculated): ${total_market_cap_billions:.2f} billion")
    
    # Compare with sector cap
    if abs(latest_value - total_market_cap_billions) > 0.01:
        print(f"WARNING: Discrepancy between sector cap (${latest_value:.2f} billion) and sum of tickers (${total_market_cap_billions:.2f} billion)")
    else:
        print("Verification successful: Sector cap matches sum of individual tickers")

def main():
    """Main function to run improved market cap collection"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Improved Sector Market Cap Calculator")
    parser.add_argument("--days", type=int, default=30, help="Number of days of history to fetch")
    parser.add_argument("--verbose", action="store_true", help="Print verbose output")
    parser.add_argument("--verify-sector", type=str, help="Verify a specific sector")
    parser.add_argument("--analyze", action="store_true", help="Analyze existing data without new collection")
    args = parser.parse_args()
    
    # Get API key from environment variable
    api_key = os.environ.get("POLYGON_API_KEY")
    if not api_key:
        logging.error("POLYGON_API_KEY environment variable not set")
        return
    
    if args.analyze:
        # Load existing data
        parquet_file = os.path.join(DATA_DIR, "sector_market_caps.parquet")
        report_file = os.path.join(DATA_DIR, "sector_coverage_report.json")
        
        if os.path.exists(parquet_file) and os.path.exists(report_file):
            sector_caps = pd.read_parquet(parquet_file)
            with open(report_file, 'r') as f:
                coverage_report = json.load(f)
            
            if args.verify_sector:
                verify_specific_sector(sector_caps, coverage_report, args.verify_sector)
            else:
                analyze_results(sector_caps, coverage_report)
        else:
            logging.error("No existing data found, run without --analyze to collect data")
    else:
        # Run collection
        sector_caps, coverage_report = run_collection(api_key, args.days, args.verbose)
        
        if args.verify_sector:
            verify_specific_sector(sector_caps, coverage_report, args.verify_sector)
        else:
            analyze_results(sector_caps, coverage_report)

if __name__ == "__main__":
    main()