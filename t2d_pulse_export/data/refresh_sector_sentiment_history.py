import os
import json
import pandas as pd
from sqlalchemy import create_engine, text

def main():
    engine = create_engine(os.getenv("DATABASE_URL"))

    # 1) Load last 30 days of stock-level EMAs
    stock_sql = """
    SELECT date, ticker, raw_sentiment_score
      FROM stock_sentiment_history
     WHERE date >= (CURRENT_DATE - INTERVAL '29 days')
    ORDER BY ticker, date
    """
    stock_df = pd.read_sql(stock_sql, engine)
    stock_df['date'] = pd.to_datetime(stock_df['date'])

    # 2) Map tickers to sectors
    with open("sector_ticker_mapping.json", "r") as f:
        sector_to_tickers = json.load(f)
    # build ticker→sector lookup
    ticker_to_sector = {
        ticker: sector
        for sector, tickers in sector_to_tickers.items()
        for ticker in tickers
    }
    stock_df['sector'] = stock_df['ticker'].map(ticker_to_sector)

    # 3) Aggregate into sector raw EMA
    sector_df = (
        stock_df
          .groupby(['date','sector'])['raw_sentiment_score']
          .mean()
          .reset_index(name='sector_raw_ema')
    )

    # 4) Truncate & write to sector_sentiment_history
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE sector_sentiment_history"))
        sector_df.to_sql(
            "sector_sentiment_history",
            conn,
            if_exists="append",
            index=False
        )
    print(f"Loaded {len(sector_df)} rows into sector_sentiment_history.")

if __name__ == "__main__":
    main()
