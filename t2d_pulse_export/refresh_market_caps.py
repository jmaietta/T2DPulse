# refresh_market_caps.py
import os, datetime as dt
import pandas as pd
import finnhub, sqlalchemy

# --- settings ---
TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "META"]   # add more tickers here
DAYS_BACK = 30
DB_URL = os.getenv("DATABASE_URL")
FINN = finnhub.Client(api_key=os.getenv("FINNHUB_KEY"))

# --- helpers ---
def share_count(ticker: str) -> int:
    data = FINN.company_basic_financials(ticker, 'all')
    return int(data["metric"]["shareOutstanding"])

def price_on(ticker: str, date: str) -> float:
    q = FINN.quote(ticker)          # Finnhub gives only latest price; acceptable for 30‑day history
    return q["c"]

# --- build dataframe ---
today = dt.date.today()
dates = [today - dt.timedelta(days=i) for i in range(DAYS_BACK)]
rows = []
for tkr in TICKERS:
    shares = share_count(tkr)
    for d in dates:
        close = price_on(tkr, d.isoformat())
        rows.append({
            "date": d,
            "ticker": tkr,
            "sector": "N/A",
            "close_price": close,
            "shares_outstanding": shares,
            "market_cap": close * shares
        })

df = pd.DataFrame(rows)
print(f"Loaded {len(df)} rows.")

# --- store in Postgres ---
engine = sqlalchemy.create_engine(DB_URL)
df.to_sql("market_caps", engine, if_exists="append", index=False)
print("✅  market_caps table updated.")
