# refresh_macro_data.py
# ETL script: load macroeconomic CSV indicators into Postgres

import os
import pandas as pd
import sqlalchemy

# 1) Connect to Postgres
db_url = os.getenv("DATABASE_URL")
if not db_url:
    raise RuntimeError("DATABASE_URL environment variable not set")
engine = sqlalchemy.create_engine(db_url)

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
    # Attempt to locate the CSV
    if os.path.exists(csv_file):
        path = csv_file
    elif os.path.exists(os.path.join("data", csv_file)):
        path = os.path.join("data", csv_file)
    else:
        print(f"⚠️  File not found: {csv_file}, skipping")
        continue

    # Load file\    
df = pd.read_csv(path)
    # Rename first two columns to 'date' and 'value'
    df.rename(columns={df.columns[0]: "date", df.columns[1]: "value"}, inplace=True)
    # Drop any duplicate columns created by rename
    df = df.loc[:, ~df.columns.duplicated()]
    # Parse 'date' column to datetime
    df["date"]  = pd.to_datetime(df["date"], errors="coerce")
    df["series"] = series
    # Reorder and select
    df = df[["series", "date", "value"]]

    # Insert into Postgres
    df.to_sql(
        "macro_data",
        engine,
        if_exists="append",
        index=False,
        method="multi"
    )
    print(f"✅ Loaded {len(df)} rows for {series} from {path}")
