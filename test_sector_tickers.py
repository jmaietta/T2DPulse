"""Test script for sector tickers functionality

This script tests the ticker basket and sector index functionality
to verify that we can create market-cap weighted baskets for each sector.
"""

import sector_tickers
import sector_market_indices
import sys

def test_one_sector(sector_name):
    """Test basket functionality for a single sector
    
    Args:
        sector_name (str): The sector name to test
    """
    print(f"\nTesting sector: {sector_name}")
    tickers = sector_tickers.get_sector_tickers(sector_name)
    print(f"Tickers: {tickers}")
    
    # Get market caps
    market_caps = sector_tickers.get_market_caps(tickers)
    print(f"\nMarket Caps:")
    for ticker, cap in market_caps.items():
        print(f"{ticker}: ${cap:,}")
    
    # Calculate weights
    weights = sector_tickers.calculate_weights(market_caps)
    print(f"\nMarket Cap Weights:")
    for ticker, weight in weights.items():
        print(f"{ticker}: {weight:.2%}")
    
    # Get index with EMA
    index_df, _ = sector_tickers.get_sector_index_with_ema(sector_name, days=30)
    if not index_df.empty:
        print(f"\nIndex Data: {len(index_df)} rows")
        print(index_df.head())
        
        # Get the latest momentum
        if 'gap_pct' in index_df.columns:
            latest_gap = index_df['gap_pct'].iloc[-1]
            print(f"\nLatest momentum (gap %): {latest_gap:.2f}%")
    
    # Try momentum through sector_market_indices
    momentum = sector_market_indices.get_sector_momentum(sector_name)
    print(f"Momentum via sector_market_indices: {momentum:.2f}%")
    
    return momentum

def main():
    """Main test function"""
    print("Testing sector ticker baskets functionality")
    
    # Test sector tickers functionality
    if len(sys.argv) > 1:
        # Test specific sector if provided as command line argument
        sector_name = sys.argv[1]
        if sector_name in sector_tickers.SECTOR_TICKERS:
            test_one_sector(sector_name)
        else:
            print(f"Error: Unknown sector '{sector_name}'")
            print(f"Available sectors: {list(sector_tickers.SECTOR_TICKERS.keys())}")
    else:
        # Test all sectors
        momentums = {}
        for sector_name in sector_tickers.SECTOR_TICKERS.keys():
            momentum = test_one_sector(sector_name)
            momentums[sector_name] = momentum
        
        print("\nAll sector momentums:")
        for sector, momentum in momentums.items():
            print(f"{sector}: {momentum:.2f}%")

if __name__ == "__main__":
    main()
