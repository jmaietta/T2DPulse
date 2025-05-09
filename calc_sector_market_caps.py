#!/usr/bin/env python3
"""
calc_sector_market_caps.py
-------------------------------------------------
Download the last 30 days of daily closing prices,
multiply by shares‑outstanding to get market‑caps,
then aggregate by sector and save to Parquet.

Run:
    python calc_sector_market_caps.py \
           --sectors sectors.json \
           --out data/sector_market_caps.parquet

Required pip packages:
    pip install yfinance pandas pyarrow tqdm
"""

import argparse, json, datetime as dt, pathlib, sys, time
from functools import lru_cache
from typing import Dict, List

import pandas as pd
import yfinance as yf
from tqdm import tqdm

# --------------------------------------------------------------------------- #
# 1.  CLI & config                                                            #
# --------------------------------------------------------------------------- #

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build 30‑day sector market‑caps")
    p.add_argument("--sectors", required=True,
                   help="JSON file mapping sector names to ticker arrays")
    p.add_argument("--out", default="sector_market_caps.parquet",
                   help="Output Parquet file (default: ./sector_market_caps.parquet)")
    p.add_argument("--start", type=str, default=None,
                   help="ISO date (YYYY‑MM‑DD) to start the window; "
                        "default = 30 calendar days ago")
    p.add_argument("--verbose", action="store_true")
    return p.parse_args()


# --------------------------------------------------------------------------- #
# 2.  Data helpers                                                            #
# --------------------------------------------------------------------------- #

@lru_cache(maxsize=None)
def shares_outstanding(ticker: str) -> int | float | None:
    """Fetch shares‑outstanding once per ticker (cached)."""
    try:
        # Add delay to avoid rate limits
        time.sleep(0.5)
        return yf.Ticker(ticker).info.get("sharesOutstanding")
    except Exception as e:
        print(f"[WARN] {ticker}: failed to fetch shares outstanding – {e}", file=sys.stderr)
        return None


def download_prices_batch(ticker_batch: List[str], start_date: str) -> pd.DataFrame:
    """Download prices for a batch of tickers with rate limiting."""
    try:
        df = yf.download(
            tickers=" ".join(ticker_batch),
            start=start_date,
            progress=False,
            auto_adjust=True,
            group_by="ticker",
            threads=False,  # Disable threading to better control rate limiting
        )
        
        # Check if the DataFrame is empty or None
        if df is None or df.empty:
            print(f"[WARN] Empty data returned for batch {ticker_batch}", file=sys.stderr)
            return pd.DataFrame()
        
        if len(ticker_batch) == 1:
            # Special case for single ticker
            if "Close" in df.columns:
                result = pd.DataFrame(df["Close"])
                result.columns = ticker_batch
                return result
            else:
                print(f"[WARN] No Close prices for {ticker_batch[0]}", file=sys.stderr)
                return pd.DataFrame()
        else:
            # Extract just the Close prices from multi-index DataFrame
            if "Close" in df.columns:
                return df["Close"]
            else:
                print(f"[WARN] No Close prices for batch", file=sys.stderr)
                return pd.DataFrame()
    except Exception as e:
        print(f"[ERROR] Batch download failed: {e}", file=sys.stderr)
        return pd.DataFrame()


def download_prices(tickers: List[str], start_date: str) -> pd.DataFrame:
    """
    Returns a DataFrame with ticker closing prices, with rate limiting
    to handle Yahoo Finance API restrictions
    """
    # Split tickers into smaller batches
    batch_size = 5
    batches = [tickers[i:i+batch_size] for i in range(0, len(tickers), batch_size)]
    
    # Create an empty DataFrame to store results
    result_df = pd.DataFrame()
    
    # Process each batch with delays between requests
    for i, batch in enumerate(batches):
        print(f"[INFO] Processing batch {i+1}/{len(batches)} with {len(batch)} tickers", file=sys.stderr)
        
        # Add delay between batches to avoid rate limiting
        if i > 0:
            time.sleep(2.0)
        
        batch_df = download_prices_batch(batch, start_date)
        
        # Merge with the main result DataFrame
        if result_df.empty and not batch_df.empty:
            result_df = batch_df
        elif not batch_df.empty:
            # Join on index (date)
            result_df = pd.concat([result_df, batch_df], axis=1)
    
    if result_df.empty:
        raise RuntimeError("Yahoo returned no data; check tickers or date range.")
        
    return result_df


# --------------------------------------------------------------------------- #
# 3.  Main logic                                                              #
# --------------------------------------------------------------------------- #

def build_sector_caps(sector_map: Dict[str, List[str]],
                      price_df: pd.DataFrame,
                      verbose: bool = False) -> pd.DataFrame:
    """
    Return DataFrame indexed by date with one column per sector:
        Date | AdTech | Cloud | … | Cybersecurity
    """
    all_caps = {}
    dates = price_df.index

    # Build ticker → shares dict up‑front
    so = {t: shares_outstanding(t) for t in price_df.columns}
    missing_so = [t for t, v in so.items() if not v]
    if missing_so:
        print(f"[WARN] Missing sharesOutstanding for: {', '.join(missing_so)}", file=sys.stderr)

    # Compute per‑ticker market‑cap matrix
    cap_df = pd.DataFrame(index=dates)
    for t in price_df.columns:
        if so[t]:
            cap_df[t] = price_df[t] * so[t]

    # Aggregate by sector
    for sector, tickers in sector_map.items():
        cols = [t for t in tickers if t in cap_df]
        if not cols:
            print(f"[WARN] Sector '{sector}' has no price data – skipped", file=sys.stderr)
            continue
        all_caps[sector] = cap_df[cols].sum(axis=1)
        if verbose:
            print(f"[INFO] {sector:<15}  tickers={len(cols)}")

    sector_df = pd.DataFrame(all_caps)
    sector_df.sort_index(inplace=True)
    return sector_df


def main() -> None:
    args = parse_args()

    # ------------------------------------------------------------------ #
    # 3.1  load sector dictionary                                        #
    # ------------------------------------------------------------------ #
    sector_json = pathlib.Path(args.sectors)
    if not sector_json.exists():
        raise FileNotFoundError(f"Sector map file not found: {sector_json}")
    sector_map: Dict[str, List[str]] = json.loads(sector_json.read_text())

    # Flatten tickers list
    tickers = sorted({t for tl in sector_map.values() for t in tl})

    # ------------------------------------------------------------------ #
    # 3.2  date window                                                   #
    # ------------------------------------------------------------------ #
    if args.start:
        start = args.start
    else:
        start = (dt.date.today() - dt.timedelta(days=30)).isoformat()

    if args.verbose:
        print(f"[+] Downloading prices for {len(tickers)} tickers, start = {start}")

    # ------------------------------------------------------------------ #
    # 3.3  download & compute                                            #
    # ------------------------------------------------------------------ #
    prices = download_prices(tickers, start)

    if args.verbose:
        print(f"[+] Prices shape = {prices.shape} (days × tickers)")

    sector_caps = build_sector_caps(sector_map, prices, verbose=args.verbose)

    # ------------------------------------------------------------------ #
    # 3.4  output                                                        #
    # ------------------------------------------------------------------ #
    out_path = pathlib.Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    sector_caps.to_parquet(out_path, compression="zstd")
    print(f"[✓] Wrote {len(sector_caps):,} rows × {len(sector_caps.columns)} sectors → {out_path}")


if __name__ == "__main__":
    t0 = time.perf_counter()
    main()
    print(f"[✓] Finished in {time.perf_counter() - t0:0.1f}s")