"""
Verify we're using all 93 tickers across the sectors
"""
import os
import pandas as pd

def check_ticker_coverage():
    try:
        # Load the ticker coverage report
        file_path = "T2D_Pulse_93_tickers_coverage.csv"
        
        # Skip the header rows to get to the data
        df = pd.read_csv(file_path, skiprows=7)
        
        # Count tickers by sector
        sector_counts = df.groupby('Sector').size().reset_index(name='Ticker Count')
        
        # Calculate total tickers
        total_tickers = len(df)
        
        # Print summary
        print(f"\nTotal tickers in use: {total_tickers}")
        print("\nTickers by sector:")
        print("------------------")
        
        # Sort sectors by ticker count (descending)
        sector_counts = sector_counts.sort_values(by='Ticker Count', ascending=False)
        
        for _, row in sector_counts.iterrows():
            sector = row['Sector']
            count = row['Ticker Count']
            tickers = df[df['Sector'] == sector]['Ticker'].tolist()
            print(f"{sector}: {count} tickers")
            print(f"  {', '.join(tickers)}")
        
        # Check for any missing data
        missing_data = df[df['Data Status'] != 'Complete']
        if len(missing_data) > 0:
            print("\nWarning: Some tickers have incomplete data:")
            for _, row in missing_data.iterrows():
                print(f"  {row['Ticker']} ({row['Sector']}): {row['Data Status']}")
        
        # Check the AdTech sector specifically
        adtech_tickers = df[df['Sector'] == 'AdTech']['Ticker'].tolist()
        print("\nAdTech sector tickers:")
        print(f"  {', '.join(adtech_tickers)}")
        
        # Check if META and GOOGL are in the list
        if 'META' in adtech_tickers and 'GOOGL' in adtech_tickers:
            print("\nConfirmed: META and GOOGL are included in the AdTech sector")
        else:
            missing = []
            if 'META' not in adtech_tickers:
                missing.append('META')
            if 'GOOGL' not in adtech_tickers:
                missing.append('GOOGL')
            print(f"\nWarning: {', '.join(missing)} not found in AdTech sector")
        
        return True
        
    except Exception as e:
        print(f"Error checking ticker coverage: {e}")
        return False

if __name__ == "__main__":
    print("Checking sector ticker coverage...")
    check_ticker_coverage()