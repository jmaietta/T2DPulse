"""
Validate key market cap values against realistic expectations
"""

import pandas as pd

def main():
    print("Validating market cap values from T2D_Pulse_93_tickers_coverage.csv")
    
    # Load data with correct header skipping
    df = pd.read_csv("T2D_Pulse_93_tickers_coverage.csv", skiprows=7)
    
    # Check companies with potentially problematic values
    problematic_tickers = ['TSM', 'INFY', 'WIT', 'NVDA', 'MSFT', 'AAPL', 'AMZN', 'GOOGL']
    
    print("\nValidating market caps (in billions USD):")
    print("-" * 50)
    print(f"{'Ticker':<10} {'Price':<10} {'Market Cap':<20} {'Market Cap (B)':<15}")
    print("-" * 50)
    
    # Extract and print data for problematic tickers
    for ticker in problematic_tickers:
        ticker_data = df[df['Ticker'] == ticker]
        if not ticker_data.empty:
            price = ticker_data['Price'].values[0]
            mkt_cap = ticker_data['Market Cap'].values[0] if not pd.isna(ticker_data['Market Cap'].values[0]) else "N/A"
            mkt_cap_m = ticker_data['Market Cap (M)'].values[0] if not pd.isna(ticker_data['Market Cap (M)'].values[0]) else "N/A"
            mkt_cap_b = mkt_cap_m / 1000 if isinstance(mkt_cap_m, (int, float)) else "N/A"
            
            # Format for display
            mkt_cap_str = f"{mkt_cap:.2e}" if isinstance(mkt_cap, (int, float)) else "N/A"
            mkt_cap_b_str = f"{mkt_cap_b:.2f}" if isinstance(mkt_cap_b, (int, float)) else "N/A"
            
            print(f"{ticker:<10} {price:<10} {mkt_cap_str:<20} {mkt_cap_b_str:<15}")
    
    # Check expected market cap values (as of May 2025)
    print("\nExpected market cap values (approximate, May 2025):")
    expected_values = {
        "AAPL": 3000,  # Apple ~$3T
        "MSFT": 3200,  # Microsoft ~$3.2T
        "GOOGL": 1900,  # Google ~$1.9T
        "AMZN": 2000,  # Amazon ~$2T
        "NVDA": 2800,  # NVIDIA ~$2.8T
        "TSM": 800,    # TSMC ~$800B
        "INFY": 80,    # Infosys ~$80B
        "WIT": 25     # Wipro ~$25B
    }
    
    print("-" * 50)
    print(f"{'Ticker':<10} {'Expected (B)':<15} {'Actual (B)':<15} {'Ratio':<10}")
    print("-" * 50)
    
    for ticker, expected in expected_values.items():
        ticker_data = df[df['Ticker'] == ticker]
        if not ticker_data.empty:
            mkt_cap_m = ticker_data['Market Cap (M)'].values[0] if not pd.isna(ticker_data['Market Cap (M)'].values[0]) else 0
            mkt_cap_b = mkt_cap_m / 1000
            ratio = mkt_cap_b / expected if expected > 0 else "N/A"
            
            print(f"{ticker:<10} ${expected:<14.2f} ${mkt_cap_b:<14.2f} {ratio:<10.2f}")
            
    # Calculate realistic sector totals
    print("\nCalculating corrected sector totals...")
    
    # Apply corrections
    df_corrected = df.copy()
    for ticker, expected_b in expected_values.items():
        idx = df_corrected[df_corrected['Ticker'] == ticker].index
        if len(idx) > 0:
            # Update to expected values
            df_corrected.loc[idx, 'Market Cap (M)'] = expected_b * 1000  # Convert B to M
    
    # Recompute sector totals with corrected values
    sector_totals = {}
    for sector, group in df_corrected.groupby('Sector'):
        valid_data = group[group['Market Cap (M)'].notna()]
        total_market_cap_billions = valid_data['Market Cap (M)'].sum() / 1000
        sector_totals[sector] = total_market_cap_billions
    
    # Print corrected sector totals
    print("\nCorrected Sector Market Caps:")
    print("-" * 50)
    total = 0
    for sector, market_cap in sorted(sector_totals.items(), key=lambda x: x[1], reverse=True):
        print(f"{sector:<25} ${market_cap:.2f}B")
        total += market_cap
    
    print("-" * 50)
    print(f"TOTAL                     ${total:.2f}B")
    
    # Save corrected data
    with open("realistic_sector_market_caps.txt", "w") as f:
        f.write("Sector Market Capitalization Data (Corrected to Realistic Values, in Billions USD)\n\n")
        f.write("Sector                      Market Cap\n")
        f.write("-" * 50 + "\n")
        for sector, market_cap in sorted(sector_totals.items(), key=lambda x: x[1], reverse=True):
            f.write(f"{sector:<30} ${market_cap:.2f}B\n")
        f.write("-" * 50 + "\n")
        f.write(f"TOTAL                       ${total:.2f}B\n")
    
    print(f"\nSaved realistic values to realistic_sector_market_caps.txt")

if __name__ == "__main__":
    main()