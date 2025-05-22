#!/usr/bin/env bash
set -e  # exit on any error

# move into the data folder
cd "$(dirname "$0")"

echo "Starting market-cap load…"
python ../market_cap_loader.py

echo "Computing stock-level sentiment…"
python ../compute_stock_sentiment.py

echo "Aggregating to sector sentiment…"
python refresh_sector_sentiment_history.py

echo "All done!"
