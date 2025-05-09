#!/usr/bin/env python3
# force_update_today.py
# -----------------------------------------------------------
# Force an immediate update of all ticker data for today

import sys
from collect_complete_ticker_data import collect_complete_ticker_data
from check_missing_ticker_data_revised import check_ticker_data

# First check current data coverage
print("Checking current ticker data coverage...")
check_ticker_data()

# Now run the comprehensive data collection
print("\n=== Starting forced data collection for all tickers ===\n")
success = collect_complete_ticker_data()

# Verify the results
print("\n=== Verifying results after data collection ===\n")
check_ticker_data()

if success:
    print("\nSUCCESS: All ticker data has been collected successfully!")
    sys.exit(0)
else:
    print("\nWARNING: Some tickers may still be missing data.")
    print("Please check the output above for details.")
    sys.exit(1)