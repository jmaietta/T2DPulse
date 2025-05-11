# T2D Pulse Sector Market Cap Implementation

## Overview

This document explains how the new sector market cap calculation system works in T2D Pulse.

## Key Components

1. **sector_market_cap.py** - The main engine that calculates market caps
   - Uses two data sources (yfinance and Finnhub) for redundancy
   - Only uses authentic market data, never synthetic values
   - Calculates sector totals by summing all tickers in each sector
   - Saves history to CSV and generates interactive charts

2. **app_sector_market_caps.py** - Integration with the dashboard
   - Formats sector market caps for display
   - Provides download functions for CSV and Excel exports

3. **Workflow: Sector Market Cap Updater**
   - Runs the market cap calculation engine daily
   - Updates the historical data in `sector_market_caps.csv`
   - Regenerates the interactive chart

## How It Works

1. The system fetches current market caps for each ticker in the sector lists using:
   - yfinance API (primary source)
   - Finnhub API (secondary source if yfinance fails)

2. It then sums the market caps for all tickers in each sector to get sector totals

3. Results are stored in `sector_market_caps.csv` with date/sector/market_cap columns

4. The dashboard reads this file to display the latest sector market caps

## Complete Ticker Coverage

The implementation uses all tickers in each sector (not just representative tickers):

- AdTech: APP, APPS, CRTO, DV, GOOGL, META, MGNI, PUBM, TTD
- Cloud Infrastructure: AMZN, CRM, CSCO, GOOGL, MSFT, NET, ORCL, SNOW
- Fintech: AFRM, BILL, COIN, FIS, FI, GPN, PYPL, SSNC
- eCommerce: AMZN, BABA, BKNG, CHWY, EBAY, ETSY, PDD, SE, SHOP, WMT
- Consumer Internet: ABNB, BKNG, GOOGL, META, NFLX, PINS, SNAP, SPOT, TRIP, YELP
- IT Services / Legacy Tech: ACN, CTSH, DXC, HPQ, IBM, INFY, PLTR, WIT
- Hardware / Devices: AAPL, DELL, HPQ, LOGI, PSTG, SMCI, SSYS, STX, WDC
- Cybersecurity: CHKP, CRWD, CYBR, FTNT, NET, OKTA, PANW, S, ZS
- Dev Tools / Analytics: DDOG, ESTC, GTLB, MDB, TEAM
- AI Infrastructure: AMZN, GOOGL, IBM, META, MSFT, NVDA, ORCL
- Semiconductors: AMAT, AMD, ARM, AVGO, INTC, NVDA, QCOM, TSM
- Vertical SaaS: CCCS, CPRT, CSGP, GWRE, ICE, PCOR, SSNC, TTAN
- Enterprise SaaS: ADSK, AMZN, CRM, IBM, MSFT, NOW, ORCL, SAP, WDAY
- SMB SaaS: ADBE, BILL, GOOGL, HUBS, INTU, META

## Multi-Sector Assignment Handling

- Companies are allowed to appear in multiple sectors (e.g., GOOGL in AdTech, Cloud, Consumer Internet, AI Infrastructure, and SMB SaaS)
- This accurately reflects the real-world impact of major technology companies across different sectors
- No weighting adjustments are applied to avoid double-counting, matching industry standards for sector analysis

## Data Integrity

- The system never uses synthetic or placeholder data
- If market cap data cannot be retrieved for a ticker, it logs the missing ticker but doesn't substitute a fake value
- This ensures all figures in the dashboard represent authentic market data

## Accessing the Market Cap Data

1. **Dashboard Display**: The latest sector market caps are shown in the dashboard
2. **CSV Download**: `/download/sector_market_caps.csv` 
3. **Excel Download**: `/download/sector_market_caps.xlsx`
4. **Interactive Chart**: Available via the dashboard