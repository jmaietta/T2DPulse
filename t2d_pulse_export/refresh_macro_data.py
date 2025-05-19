# refresh_macro_data.py
# ETL script: load macroeconomic CSV indicators into Postgres
environment:
#   DATABASE_URL must be set in env vars
#   CSV files are expected in the `data/` directory

import os
import glob
import pandas as pd
import sqlalchemy

# --- Configuration: mapping of CSV filenames to series identifiers ---
CSV_MAP = {
    "inflation_data.csv":       "CPIAUCSL",
    "vix_data.csv":             "VIX",
    "interest_rate_data.csv":   "FEDFUNDS",
    "consumer_sentiment_data.csv": "USACSCICP02STSAM",
    "gdp_data.csv":             "GDPC1",
    "pce_data.csv":             "PCE",
    "pcepi_data.csv":           "PCEPI",
    "treasury_yield_data.csv":  "DGS10",
    "job_postings_data.csv":    "IHLIDXUSTPSOFTDEVE",
    "sector_30day_history.csv": "SECTOR_30DAY",
    "nasdaq_data.csv":          "NASDAQ",
    "unemployment_data.csv":    "UNRATE",
    "ppi_data.csv":             "PCU511210511210",  # example PPI
    # add or adjust series here
}

# --- Main ETL ---
def main():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL environment variable not set")
    engine = sqlalchemy.create_engine(db_url)

    # 1) read each CSV and append to macro_data table
    all_rows = []
    for fname, series in CSV_MAP.items():
        path = os.path.join("data", fname)
        if not os.path.exists(path):
            print(f"⚠️  Missing {fname}, skipping")
            continue
        try:
            df = pd.read_csv(path, parse_dates=[0], header=0)
        except Exception as e:
            print(f"❌ Failed to read {fname}: {e}")
            continue
        # assume first column is date, second is value
        df.columns = ["date", "value"]
        df["series"] = series
        all_rows.append(df[["series", "date", "value"]])

    if not all_rows:
        print("❌ No macro data loaded, exiting.")
        return

    macro_df = pd.concat(all_rows, ignore_index=True)
    # 2) write to Postgres
    macro_df.to_sql(
        "macro_data", engine,
        if_exists="append", index=False, method="multi"
    )
    print(f"✅ Inserted {len(macro_df)} rows into macro_data table.")

if __name__ == "__main__":
    main()
