"""
Analyze market cap variance between our calculated values and user-provided values.
This script will break down the differences by sector and by stock to identify 
exactly where the discrepancies are coming from.
"""

import pandas as pd
import os
import logging
from collections import defaultdict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Load sector-ticker mappings
def load_sector_tickers():
    """
    Load the mapping of sectors to their constituent tickers
    """
    sector_tickers = {
        "AdTech": ["APP", "APPS", "CRTO", "DV", "GOOGL", "META", "MGNI", "PUBM", "TTD"],
        "Cloud Infrastructure": ["AMZN", "CRM", "CSCO", "GOOGL", "MSFT", "NET", "ORCL", "SNOW"],
        "Fintech": ["AFRM", "BILL", "COIN", "FIS", "FI", "GPN", "PYPL", "SSNC"],
        "eCommerce": ["AMZN", "BABA", "BKNG", "CHWY", "EBAY", "ETSY", "PDD", "SE", "SHOP", "WMT"],
        "Consumer Internet": ["ABNB", "BKNG", "GOOGL", "META", "NFLX", "PINS", "SNAP", "SPOT", "TRIP", "YELP"],
        "IT Services / Legacy Tech": ["ACN", "CTSH", "DXC", "HPQ", "IBM", "INFY", "PLTR", "WIT"],
        "Hardware / Devices": ["AAPL", "DELL", "HPQ", "LOGI", "PSTG", "SMCI", "SSYS", "STX", "WDC"],
        "Cybersecurity": ["CHKP", "CRWD", "CYBR", "FTNT", "NET", "OKTA", "PANW", "S", "ZS"],
        "Dev Tools / Analytics": ["DDOG", "ESTC", "GTLB", "MDB", "TEAM"],
        "AI Infrastructure": ["AMZN", "GOOGL", "IBM", "META", "MSFT", "NVDA", "ORCL"],
        "Semiconductors": ["AMAT", "AMD", "ARM", "AVGO", "INTC", "NVDA", "QCOM", "TSM"],
        "Vertical SaaS": ["CCCS", "CPRT", "CSGP", "GWRE", "ICE", "PCOR", "SSNC", "TTAN"],
        "Enterprise SaaS": ["ADSK", "AMZN", "CRM", "IBM", "MSFT", "NOW", "ORCL", "SAP", "WDAY"],
        "SMB SaaS": ["ADBE", "BILL", "GOOGL", "HUBS", "INTU", "META"]
    }
    return sector_tickers

def load_ticker_market_caps():
    """
    Load the calculated market cap data for each ticker
    """
    # Try to find the most detailed file with individual ticker data
    if os.path.exists('corrected_sector_market_caps_detailed.csv'):
        df = pd.read_csv('corrected_sector_market_caps_detailed.csv')
        logging.info("Loaded detailed market cap data from corrected_sector_market_caps_detailed.csv")
        return df
    else:
        logging.error("Could not find detailed market cap data file")
        return None

def load_sector_market_caps():
    """
    Load the calculated sector market caps and user-provided sector market caps
    """
    calculated = pd.read_csv('corrected_sector_market_caps.csv')
    user_provided = pd.read_csv('user_provided_sector_market_caps.csv')
    
    merged = pd.merge(calculated, user_provided, on='Sector', suffixes=('_Calculated', '_User'))
    merged['Difference'] = merged['Market Cap (Billions USD)_User'] - merged['Market Cap (Billions USD)_Calculated']
    merged['Percentage_Difference'] = (merged['Difference'] / merged['Market Cap (Billions USD)_User']) * 100
    
    return merged

def analyze_by_sector():
    """
    Analyze the variance by sector
    """
    sector_data = load_sector_market_caps()
    
    print("===== SECTOR VARIANCE ANALYSIS =====")
    print(f"{'Sector':<25} {'Our Value ($B)':<15} {'Your Value ($B)':<15} {'Diff ($B)':<15} {'% Diff':<10}")
    print("-" * 80)
    
    # Sort by percentage difference
    sector_data = sector_data.sort_values('Percentage_Difference', ascending=False)
    
    for _, row in sector_data.iterrows():
        sector = row['Sector']
        calculated = row['Market Cap (Billions USD)_Calculated']
        user = row['Market Cap (Billions USD)_User']
        diff = row['Difference']
        pct_diff = row['Percentage_Difference']
        
        print(f"{sector:<25} ${calculated:<14.2f} ${user:<14.2f} ${diff:<14.2f} {pct_diff:<9.2f}%")
    
    print("-" * 80)
    total_calculated = sector_data['Market Cap (Billions USD)_Calculated'].sum()
    total_user = sector_data['Market Cap (Billions USD)_User'].sum()
    total_diff = total_user - total_calculated
    total_pct = (total_diff / total_user) * 100
    print(f"{'TOTAL':<25} ${total_calculated:<14.2f} ${total_user:<14.2f} ${total_diff:<14.2f} {total_pct:<9.2f}%")
    
    return sector_data

def analyze_potential_causes():
    """
    Analyze potential causes of the discrepancies
    """
    sector_tickers = load_sector_tickers()
    sector_data = load_sector_market_caps()
    
    # Check 1: How many companies are in each sector
    print("\n===== SECTOR COMPOSITION ANALYSIS =====")
    print(f"{'Sector':<25} {'# Tickers':<10} {'% Diff':<10}")
    print("-" * 50)
    
    for _, row in sector_data.iterrows():
        sector = row['Sector']
        pct_diff = row['Percentage_Difference']
        num_tickers = len(sector_tickers.get(sector, []))
        
        print(f"{sector:<25} {num_tickers:<10} {pct_diff:<9.2f}%")
    
    # Check 2: Cross-assignment of major companies
    print("\n===== MULTI-SECTOR COMPANY ANALYSIS =====")
    ticker_sectors = defaultdict(list)
    for sector, tickers in sector_tickers.items():
        for ticker in tickers:
            ticker_sectors[ticker].append(sector)
    
    multi_sector_tickers = {ticker: sectors for ticker, sectors in ticker_sectors.items() if len(sectors) > 1}
    
    print(f"Found {len(multi_sector_tickers)} tickers assigned to multiple sectors:")
    for ticker, sectors in multi_sector_tickers.items():
        print(f"{ticker}: {', '.join(sectors)}")
    
    # Check 3: Major companies with significant market cap
    detailed_data = load_ticker_market_caps()
    if detailed_data is not None and 'Ticker' in detailed_data.columns and 'Market Cap (Billions USD)' in detailed_data.columns:
        print("\n===== TOP COMPANIES BY MARKET CAP =====")
        print(f"{'Ticker':<10} {'Market Cap ($B)':<15} {'Sectors':<50}")
        print("-" * 80)
        
        # Sort by market cap
        top_companies = detailed_data.sort_values('Market Cap (Billions USD)', ascending=False).head(20)
        
        for _, row in top_companies.iterrows():
            ticker = row['Ticker']
            market_cap = row['Market Cap (Billions USD)']
            sectors = ', '.join(ticker_sectors.get(ticker, []))
            
            print(f"{ticker:<10} ${market_cap:<14.2f} {sectors:<50}")

def main():
    """Main analysis function"""
    print("\n========== MARKET CAP VARIANCE ANALYSIS ==========\n")
    
    # 1. Analyze by sector
    sector_analysis = analyze_by_sector()
    
    # 2. Analyze potential causes
    analyze_potential_causes()
    
    # 3. Generate a report on why the IT Services / Legacy Tech sector has such a large difference
    print("\n===== SPECIAL ANALYSIS: IT SERVICES / LEGACY TECH =====")
    it_variance = sector_analysis[sector_analysis['Sector'] == 'IT Services / Legacy Tech']
    if not it_variance.empty:
        row = it_variance.iloc[0]
        print(f"The IT Services / Legacy Tech sector shows a {row['Percentage_Difference']:.2f}% difference")
        print(f"Our calculation: ${row['Market Cap (Billions USD)_Calculated']:.2f}B")
        print(f"Your value: ${row['Market Cap (Billions USD)_User']:.2f}B")
        print(f"Absolute difference: ${row['Difference']:.2f}B")
        print("\nPotential reasons for this large difference:")
        print("1. We may be missing major IT Services companies in our ticker list")
        print("2. Share count data for companies like Infosys (INFY) may be incorrect")
        print("3. Companies with dual-class shares may have incorrect calculations")
        print("4. Our ticker list for this sector may not align with industry standards")
    
    # 4. Provide recommendations
    print("\n===== RECOMMENDATIONS =====")
    print("1. Review and validate share count data for all tickers")
    print("2. Ensure complete coverage of all companies in each sector")
    print("3. Apply appropriate handling for multi-sector companies to avoid double-counting or undercounting")
    print("4. Implement a calibration factor for sectors with persistent differences")
    print("5. Update ticker lists to ensure they represent complete sector coverage")

if __name__ == "__main__":
    main()