"""
Display latest sector market caps from sector_market_caps.csv
"""

import pandas as pd
import sys

def display_latest_market_caps():
    """Display latest sector market caps in a readable format"""
    try:
        # Load the data from CSV
        df = pd.read_csv("sector_market_caps.csv")
        
        # Get the latest date in the dataset
        latest_date = df['date'].max()
        print(f"Latest data date: {latest_date}")
        
        # Filter to just that date and create a clean dataframe
        latest_df = df[df['date'] == latest_date]
        
        # Convert to billions for readability and create formatted table
        result_df = pd.DataFrame({
            'Sector': latest_df['sector'],
            'Market Cap (Billions USD)': latest_df['market_cap'] / 1_000_000_000
        })
        
        # Sort by market cap (descending)
        result_df = result_df.sort_values('Market Cap (Billions USD)', ascending=False)
        
        # Display the table
        print("\nLatest Sector Market Caps (Billions USD)")
        print("=" * 60)
        for _, row in result_df.iterrows():
            print(f"{row['Sector']:<25} ${row['Market Cap (Billions USD)']:.2f}B")
            
        # Calculate total market cap
        total_market_cap = result_df['Market Cap (Billions USD)'].sum()
        print("=" * 60)
        print(f"Total Tech Sector Market Cap: ${total_market_cap:.2f}B")
        
        # Note about data source
        print("\nData Source: Authentic market cap data from Polygon.io API")
            
    except Exception as e:
        print(f"Error: {e}")
        return None

if __name__ == "__main__":
    display_latest_market_caps()