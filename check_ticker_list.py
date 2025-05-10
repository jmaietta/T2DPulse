"""
Verify that we're using all the tickers in the sector definitions
"""
import os
import pandas as pd
import json

def load_sector_tickers():
    """Load sector definitions and their assigned tickers"""
    try:
        # First try to load from config.py
        with open('config.py', 'r') as f:
            config_text = f.read()
            
        # Extract the sector ticker definitions
        start_marker = "SECTOR_TICKERS = {"
        end_marker = "}"
        if start_marker in config_text:
            start_idx = config_text.find(start_marker)
            end_idx = config_text.find(end_marker, start_idx) + 1
            sector_tickers_str = config_text[start_idx:end_idx]
            
            # Convert to dictionary
            sector_tickers_str = sector_tickers_str.replace("SECTOR_TICKERS = ", "")
            
            # Replace single quotes with double quotes for JSON parsing
            sector_tickers_str = sector_tickers_str.replace("'", '"')
            
            # Parse the JSON
            sector_tickers = json.loads(sector_tickers_str)
            print("Loaded sector tickers from config.py")
            return sector_tickers
        else:
            print("Could not find SECTOR_TICKERS in config.py")
            return None
    except Exception as e:
        print(f"Error loading from config.py: {e}")
        
        # Try to load from data folder
        try:
            sector_file = os.path.join("data", "sector_tickers.json")
            if os.path.exists(sector_file):
                with open(sector_file, 'r') as f:
                    sector_tickers = json.load(f)
                    print(f"Loaded sector tickers from {sector_file}")
                    return sector_tickers
            else:
                print(f"Sector ticker file not found: {sector_file}")
                return None
        except Exception as e2:
            print(f"Error loading from data folder: {e2}")
            return None

def load_market_caps():
    """Load actual market cap data and check which tickers are included"""
    try:
        market_cap_file = "T2D_Pulse_93_tickers_coverage.csv"
        if not os.path.exists(market_cap_file):
            market_cap_file = os.path.join("data", "ticker_coverage.csv")
            if not os.path.exists(market_cap_file):
                print(f"Market cap file not found")
                return None
                
        df = pd.read_csv(market_cap_file)
        print(f"Loaded market cap data from {market_cap_file} with {len(df)} rows")
        
        # Extract the tickers that are actually being used
        ticker_column = None
        for col in df.columns:
            if 'ticker' in col.lower():
                ticker_column = col
                break
        
        if ticker_column:
            tickers_in_use = set(df[ticker_column].dropna().tolist())
            print(f"Found {len(tickers_in_use)} tickers in use")
            return tickers_in_use
        else:
            print("Could not find ticker column in the data")
            return None
    except Exception as e:
        print(f"Error loading market caps: {e}")
        return None

def check_background_collector():
    """Check the background collector code to see what tickers it's using"""
    try:
        # Look for background collector file
        collector_files = [
            "background_data_collector.py",
            "batch_ticker_collector.py",
            "finnhub_data_collector.py"
        ]
        
        found_tickers = set()
        
        for file in collector_files:
            if os.path.exists(file):
                with open(file, 'r') as f:
                    code = f.read()
                    print(f"Checking {file} for ticker definitions")
                    
                    # Look for ticker definitions
                    if "tickers =" in code or "TICKERS =" in code:
                        print(f"Found ticker definition in {file}")
                        # Extract total number of tickers from log lines
                        if "unique tickers" in code:
                            lines = code.split('\n')
                            for line in lines:
                                if "unique tickers" in line and "INFO" in line:
                                    print(f"Found ticker count in log: {line}")
        
        return found_tickers
    except Exception as e:
        print(f"Error checking background collector: {e}")
        return set()

def analyze_ticker_coverage():
    """Compare the sector tickers definition with actual tickers in use"""
    # Load sector ticker definitions
    sector_tickers = load_sector_tickers()
    
    # Load tickers actually in use
    tickers_in_use = load_market_caps()
    
    # Check background collector
    check_background_collector()
    
    if not sector_tickers or not tickers_in_use:
        print("Could not analyze ticker coverage due to missing data")
        return
    
    # Count how many tickers should be in each sector
    total_tickers = 0
    print("\nSector Ticker Coverage:")
    print("----------------------")
    
    for sector, tickers in sector_tickers.items():
        ticker_count = len(tickers)
        total_tickers += ticker_count
        
        # Check how many are actually in use
        sector_tickers_set = set(tickers)
        covered_tickers = sector_tickers_set.intersection(tickers_in_use)
        coverage_pct = len(covered_tickers) / ticker_count * 100 if ticker_count > 0 else 0
        
        print(f"{sector}: {len(covered_tickers)}/{ticker_count} tickers ({coverage_pct:.1f}%)")
        
        # Show missing tickers
        missing_tickers = sector_tickers_set - tickers_in_use
        if missing_tickers:
            print(f"  Missing: {', '.join(missing_tickers)}")
    
    # Overall coverage
    overall_coverage = len(tickers_in_use) / total_tickers * 100 if total_tickers > 0 else 0
    print(f"\nOverall Coverage: {len(tickers_in_use)}/{total_tickers} tickers ({overall_coverage:.1f}%)")
    
    # Check for extra tickers not in the sector definitions
    all_defined_tickers = set()
    for tickers in sector_tickers.values():
        all_defined_tickers.update(tickers)
    
    extra_tickers = tickers_in_use - all_defined_tickers
    if extra_tickers:
        print(f"\nExtra tickers in use (not in sector definitions): {', '.join(extra_tickers)}")

if __name__ == "__main__":
    print("Checking T2D Pulse ticker coverage...\n")
    analyze_ticker_coverage()