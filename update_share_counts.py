"""
Update Share Counts with Accurate Data

This script updates the shares outstanding data with more accurate values
provided by the user, particularly for high-market-cap companies that have
a major impact on sector weightings.
"""
import os
import json
import logging
import pandas as pd

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Define directory for data and cache
DATA_DIR = "data"
CACHE_DIR = os.path.join(DATA_DIR, "cache")

# Define accurate share counts
ACCURATE_SHARE_COUNTS = {
    # AdTech companies
    "GOOGL": 12_291_000_000,  # Updated from user-provided data
    "META": 2_590_000_000,    # Updated from user-provided data
    
    # Other major companies with potential share count discrepancies
    # Add more as needed
}

def update_share_counts():
    """Update share counts in the cache with accurate values"""
    # Load existing shares outstanding data
    cache_file = os.path.join(CACHE_DIR, "shares_outstanding.json")
    
    if not os.path.exists(cache_file):
        logging.error(f"Shares outstanding cache file not found: {cache_file}")
        return False
    
    try:
        # Load current data
        with open(cache_file, 'r') as f:
            shares_dict = json.load(f)
        
        # Create backup
        backup_file = f"{cache_file}.bak"
        with open(backup_file, 'w') as f:
            json.dump(shares_dict, f)
        logging.info(f"Created backup of shares outstanding data at {backup_file}")
        
        # Print current values for key stocks
        print("Current share counts:")
        for ticker, accurate_count in ACCURATE_SHARE_COUNTS.items():
            current_count = shares_dict.get(ticker, "Not found")
            if current_count != "Not found":
                print(f"  {ticker}: {current_count:,} (will update to {accurate_count:,})")
            else:
                print(f"  {ticker}: Not found in cache (will add {accurate_count:,})")
        
        # Update with accurate values
        for ticker, count in ACCURATE_SHARE_COUNTS.items():
            shares_dict[ticker] = count
        
        # Save updated data
        with open(cache_file, 'w') as f:
            json.dump(shares_dict, f)
        
        logging.info(f"Updated shares outstanding data with accurate values")
        return True
        
    except Exception as e:
        logging.error(f"Error updating shares outstanding data: {e}")
        return False

def recalculate_sector_market_caps():
    """Recalculate sector market caps with updated share counts"""
    # First update share counts
    if not update_share_counts():
        return
    
    # Load shares outstanding and price data
    cache_file = os.path.join(CACHE_DIR, "shares_outstanding.json")
    price_cache = os.path.join(CACHE_DIR, "historical_prices.pkl")
    
    if not os.path.exists(price_cache):
        logging.error(f"Price cache file not found: {price_cache}")
        return
    
    try:
        # Load data
        with open(cache_file, 'r') as f:
            shares_dict = json.load(f)
        
        price_dict = pd.read_pickle(price_cache)
        
        # Verify we have data for key stocks
        for ticker in ACCURATE_SHARE_COUNTS.keys():
            if ticker not in price_dict:
                logging.warning(f"No price data found for {ticker}")
            if ticker not in shares_dict:
                logging.warning(f"No shares outstanding data found for {ticker}")
        
        # Create price DataFrame
        price_df = pd.DataFrame(price_dict)
        price_df.index = pd.to_datetime(price_df.index)
        price_df = price_df.sort_index()
        
        # Calculate market caps
        market_caps = pd.DataFrame(index=price_df.index)
        for ticker in price_df.columns:
            if ticker in shares_dict:
                market_caps[ticker] = price_df[ticker] * shares_dict[ticker]
        
        # Define sector tickers
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
        
        # Calculate sector market caps
        sector_caps = pd.DataFrame(index=market_caps.index)
        total_market_cap = pd.Series(0, index=market_caps.index)
        
        for sector, tickers in sector_tickers.items():
            available_tickers = [t for t in tickers if t in market_caps.columns]
            if available_tickers:
                sector_caps[sector] = market_caps[available_tickers].sum(axis=1)
                total_market_cap += sector_caps[sector]
            else:
                sector_caps[sector] = pd.Series(0, index=market_caps.index)
        
        # Add total
        sector_caps["Total"] = total_market_cap
        
        # Calculate weight percentages
        for sector in sector_tickers.keys():
            weight_col = f"{sector}_weight_pct"
            sector_caps[weight_col] = (sector_caps[sector] / sector_caps["Total"]) * 100
        
        # Save results
        parquet_file = os.path.join(DATA_DIR, "sector_market_caps.parquet")
        csv_file = os.path.join(DATA_DIR, "sector_market_caps.csv")
        
        # Create backup of existing files
        if os.path.exists(parquet_file):
            os.rename(parquet_file, f"{parquet_file}.bak")
        if os.path.exists(csv_file):
            os.rename(csv_file, f"{csv_file}.bak")
        
        # Save new files
        sector_caps.to_parquet(parquet_file)
        sector_caps.to_csv(csv_file)
        
        logging.info(f"Recalculated sector market caps with updated share counts")
        
        # Print AdTech sector market cap
        latest_date = sector_caps.index.max()
        adtech_value = sector_caps.loc[latest_date, "AdTech"] / 1_000_000_000  # convert to billions
        
        print(f"\nUpdated AdTech market cap (as of {latest_date.strftime('%Y-%m-%d')}): ${adtech_value:.2f} billion")
        
        # Show breakdown of key stocks
        print("\nKey stocks in AdTech sector:")
        for ticker in ["GOOGL", "META", "APP", "TTD"]:
            if ticker in market_caps.columns:
                latest_mcap = market_caps.loc[latest_date, ticker] / 1_000_000_000
                print(f"  {ticker}: ${latest_mcap:.2f} billion")
        
    except Exception as e:
        logging.error(f"Error recalculating sector market caps: {e}")

def main():
    """Main function"""
    # Ensure cache dir exists
    os.makedirs(CACHE_DIR, exist_ok=True)
    
    # Recalculate sector market caps
    recalculate_sector_market_caps()

if __name__ == "__main__":
    main()