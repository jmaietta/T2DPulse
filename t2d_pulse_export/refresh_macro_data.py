# refresh_macro_data.py
# ETL script: load macroeconomic CSV indicators into Postgres

import os
import pandas as pd
import sqlalchemy

# 1) Connect to Postgres
DB_URL = os.getenv("DATABASE_URL")
if not DB_URL:
    raise RuntimeError("DATABASE_URL environment variable not set")
engine = sqlalchemy.create_engine(DB_URL)

# 2) Map CSV filenames to series codes
CSV_MAP = {
    "inflation_data.csv":       "CPIAUCSL",            # Consumer Price Index
    "interest_rate_data.csv":   "FEDFUNDS",            # Federal Funds Rate
    "treasury_yield_data.csv":  "DGS10",               # 10-year Treasury Yield
    "consumer_sentiment_data.csv": "USACSCICP02STSAM", # Consumer Confidence Index
    "pce_data.csv":             "PCE",                 # Personal Consumption Expenditures
    "pcepi_data.csv":           "PCEPI",               # PCE Price Index
    "gdp_data.csv":             "GDPC1",               # Real GDP
    "nasdaq_data.csv":          "NASDAQ",              # NASDAQ Composite Index
    "vix_data.csv":             "VIX",                 # CBOE VIX Index
    "job_postings_data.csv":    "IHLIDXUSTPSOFTDEVE",  # Software Job Postings Index
    "software_ppi_data.csv":    "PCU511210511210",     # Software PPI
    "data_processing_ppi_data.csv": "PCU5112105112105", # Data Processing PPI
    "unemployment_data.csv":    "UNRATE",              # Unemployment Rate
}

# 3) Process each CSV and load into macro_data table
for csv_file, series in CSV_MAP.items():
    path = os.path.join("data", csv_file)
    if not os.path.exists(path):
        print(f"⚠️  File not found: {csv_file}, skipping")
        continue

    df = pd.read_csv(path, parse_dates=["date"] if "date" in pd.read_csv(path).columns else [0])
    # ensure columns are series, date, value
    # assume CSV has columns [date, value]
    df = df.rename(columns={df.columns[0]: "date", df.columns[1]: "value"})
    df["series"] = series
    df = df[["series", "date", "value"]]

    df.to_sql(
        "macro_data",
        engine,
        if_exists="append",
        index=False,
        method="multi"
    )
    print(f"✅ Loaded {len(df)} rows for {series} from {csv_file}")
