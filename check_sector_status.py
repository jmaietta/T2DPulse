#!/usr/bin/env python3
# check_sector_status.py
# -----------------------------------------------------------
# Check the status of sector data in the dashboard

import os
import pandas as pd
import config

def check_sector_scores():
    """Check the current sector scores and pulse score"""
    # Load authentic sector history
    sector_history_file = os.path.join('data', 'authentic_sector_history.csv')
    if not os.path.exists(sector_history_file):
        print(f"Sector history file not found: {sector_history_file}")
        return False
    
    try:
        sector_df = pd.read_csv(sector_history_file)
        print(f"Loaded sector history with shape {sector_df.shape}")
        
        # Print the most recent sector scores
        if 'Date' in sector_df.columns:
            sector_df['Date'] = pd.to_datetime(sector_df['Date'])
            sector_df = sector_df.sort_values('Date', ascending=False)
            
            if not sector_df.empty:
                latest_row = sector_df.iloc[0]
                latest_date = latest_row['Date']
                print(f"\nLatest sector scores for {latest_date.strftime('%Y-%m-%d')}:")
                
                for sector in config.SECTORS:
                    if sector in latest_row:
                        score = latest_row[sector]
                        print(f"{sector}: {score:.1f}")
                    else:
                        print(f"{sector}: Not available")
                
                # Check if T2D Pulse score is available
                if 'T2D Pulse' in latest_row:
                    pulse_score = latest_row['T2D Pulse']
                    print(f"\nT2D Pulse score: {pulse_score:.1f}")
                else:
                    # Check for standalone pulse score file
                    pulse_file = os.path.join('data', 'current_pulse_score.txt')
                    if os.path.exists(pulse_file):
                        with open(pulse_file, 'r') as f:
                            pulse_score = float(f.read().strip())
                            print(f"\nT2D Pulse score from file: {pulse_score:.1f}")
                    else:
                        print("\nT2D Pulse score not available")
            else:
                print("No sector data found")
        else:
            print("Date column not found in sector history")
    
    except Exception as e:
        print(f"Error checking sector scores: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True

def check_authentic_sector_files():
    """Check authentic sector history files for each date"""
    data_dir = 'data'
    files = os.listdir(data_dir)
    
    # Find all authentic sector history files
    sector_files = [f for f in files if f.startswith('authentic_sector_history_') and f.endswith('.csv')]
    
    print(f"\nFound {len(sector_files)} daily sector history files:")
    for file in sorted(sector_files):
        file_path = os.path.join(data_dir, file)
        try:
            df = pd.read_csv(file_path)
            date_str = file.replace('authentic_sector_history_', '').replace('.csv', '')
            print(f"{date_str}: {df.shape[0]} rows, {df.shape[1]} columns")
        except Exception as e:
            print(f"{file}: Error loading file - {e}")
    
    return True

def check_ticker_data_coverage():
    """Check ticker data coverage"""
    price_file = os.path.join('data', 'historical_ticker_prices.csv')
    marketcap_file = os.path.join('data', 'historical_ticker_marketcap.csv')
    
    if not os.path.exists(price_file) or not os.path.exists(marketcap_file):
        print("Ticker data files not found")
        return False
    
    try:
        price_df = pd.read_csv(price_file, index_col=0)
        marketcap_df = pd.read_csv(marketcap_file, index_col=0)
        
        print(f"\nPrice data with shape {price_df.shape}")
        print(f"Market cap data with shape {marketcap_df.shape}")
        
        # Check null counts
        price_nulls = price_df.isna().sum().sum()
        marketcap_nulls = marketcap_df.isna().sum().sum()
        
        price_null_pct = price_nulls / (price_df.shape[0] * price_df.shape[1]) * 100
        marketcap_null_pct = marketcap_nulls / (marketcap_df.shape[0] * marketcap_df.shape[1]) * 100
        
        print(f"\nPrice data missing values: {price_nulls} ({price_null_pct:.1f}%)")
        print(f"Market cap data missing values: {marketcap_nulls} ({marketcap_null_pct:.1f}%)")
        
        # Check today's data
        if price_df.shape[0] > 0 and marketcap_df.shape[0] > 0:
            latest_date = price_df.index[-1]
            print(f"\nLatest ticker data date: {latest_date}")
            
            # Count non-null values for today
            today_price_values = price_df.loc[latest_date].notna().sum()
            today_marketcap_values = marketcap_df.loc[latest_date].notna().sum()
            
            print(f"Tickers with price data for today: {today_price_values}/{price_df.shape[1]} ({today_price_values/price_df.shape[1]*100:.1f}%)")
            print(f"Tickers with market cap data for today: {today_marketcap_values}/{marketcap_df.shape[1]} ({today_marketcap_values/marketcap_df.shape[1]*100:.1f}%)")
            
            # Check sector coverage
            print("\nSector ticker coverage:")
            for sector, tickers in config.SECTORS.items():
                sector_price_count = 0
                sector_marketcap_count = 0
                
                for ticker in tickers:
                    if ticker in price_df.columns:
                        if not pd.isna(price_df.loc[latest_date, ticker]):
                            sector_price_count += 1
                    
                    if ticker in marketcap_df.columns:
                        if not pd.isna(marketcap_df.loc[latest_date, ticker]):
                            sector_marketcap_count += 1
                
                print(f"{sector}: {sector_price_count}/{len(tickers)} price, {sector_marketcap_count}/{len(tickers)} market cap")
    
    except Exception as e:
        print(f"Error checking ticker data coverage: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True

if __name__ == "__main__":
    print("Checking sector and ticker data status...")
    check_sector_scores()
    check_authentic_sector_files()
    check_ticker_data_coverage()