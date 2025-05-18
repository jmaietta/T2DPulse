# refresh_market_caps.py
# ETL script: fetch historical and current market caps and store in Postgres

import os
import datetime as dt

import pandas as pd
import yfinance as yf
import finnhub
import sqlalchemy

# --- Configuration: list your tickers and their sectors here ---
TICKERS = {
    # 'TICKER': 'Sector Name',
    'AAPL': 'Large-Cap Software',
    'MSFT': 'Large-Cap Software',
    'GOOGL': 'AdTech',
    'AMZN': 'Cloud Infra',
    'META': 'AdTech',
    # add more tickers as needed
}

# Number of days of history to fetch
DAYS_BACK = 30

# 1) Setup connections
db_url = os.getenv('DATABASE_URL')
if not db_url:
    raise RuntimeError('DATABASE_URL environment variable is missing')
engine = sqlalchemy.create_engine(db_url)

# 2) Setup Finnhub client for share counts
finn = finnhub.Client(api_key=os.getenv('FINNHUB_KEY'))

# 3) Define date range for historical data
end_date = dt.date.today() + dt.timedelta(days=1)
start_date = end_date - dt.timedelta(days=DAYS_BACK)

records = []
for ticker, sector in TICKERS.items():
    # Fetch history of close prices
    hist = yf.download(
        ticker,
        start=start_date.isoformat(),
        end=end_date.isoformat(),
        progress=False,
        auto_adjust=False
    )
    if hist.empty:
        print(f"⚠️  No price data for {ticker}")
        continue

    # Fetch share count
    metrics = finn.company_basic_financials(ticker, 'all')
    shares = metrics.get('metric', {}).get('shareOutstanding')
    if not shares:
        print(f"⚠️  No share count for {ticker}")
        continue

    # Build records
    hist = hist.reset_index()[['Date', 'Close']]
    hist['ticker'] = ticker
    hist['sector'] = sector
    hist['shares_outstanding'] = int(shares)
    hist['market_cap'] = hist['Close'] * shares
    for _, row in hist.iterrows():
        records.append({
            'date': row['Date'].date(),
            'ticker': row['ticker'],
            'sector': row['sector'],
            'close_price': float(row['Close']),
            'shares_outstanding': row['shares_outstanding'],
            'market_cap': float(row['market_cap'])
        })

# 4) Load into Postgres
if not records:
    print('❌ No market cap records to insert')
    raise SystemExit

df = pd.DataFrame(records)
# Append or update existing rows
# Assumes market_caps table exists with primary key (date, ticker)
df.to_sql('market_caps', engine, if_exists='append', index=False, method='multi')

print(f"✅ Inserted {len(df)} market cap rows into market_caps table")
