"""
Check Market Caps

This script reads and displays the latest market cap data for all sectors.
"""
import pandas as pd
import os

def load_market_caps(filepath="data/sector_market_caps.parquet", fallback_csv="data/sector_market_caps.csv"):
    """
    Load market cap data from parquet or CSV file
    """
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

def get_latest_market_caps(df):
    """
    Get the latest market cap values for each sector
    """
    if df is None or df.empty:
        return None
    
    # Get the latest date in the dataframe
    latest_date = df.index.max()
    print(f"Latest market cap data from: {latest_date}")
    
    # Get the row for the latest date
    latest_row = df.loc[latest_date]
    
    # Convert to billions for readability
    market_caps_billions = latest_row / 1_000_000_000
    
    # Sort by market cap in descending order
    sorted_caps = market_caps_billions.sort_values(ascending=False)
    
    return sorted_caps

def main():
    """Main function"""
    # Load market cap data
    market_cap_df = load_market_caps()
    
    if market_cap_df is not None:
        # Get latest market caps
        latest_caps = get_latest_market_caps(market_cap_df)
        
        if latest_caps is not None:
            print("\nSector Market Caps (Billions USD):")
            print("=================================")
            
            total_market_cap = latest_caps.sum()
            
            for sector, market_cap in latest_caps.items():
                weight = (market_cap / total_market_cap) * 100
                print(f"{sector}: ${market_cap:.2f} billion ({weight:.2f}%)")
            
            print("\nTotal Market Cap: ${:.2f} billion".format(total_market_cap))
        else:
            print("Could not get latest market caps")
    else:
        print("Failed to load market cap data")

if __name__ == "__main__":
    main()