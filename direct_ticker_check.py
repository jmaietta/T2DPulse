#!/usr/bin/env python3
# direct_ticker_check.py
# -----------------------------------------------------------
# Simple script to directly check ticker data from Yahoo Finance

import yfinance as yf
import pandas as pd
import sys

def check_ticker(ticker):
    print(f"Trying {ticker}...")
    try:
        # Get price
        data = yf.Ticker(ticker).history(period='1d')
        price = data['Close'].iloc[-1] if not data.empty else None
        print(f"  Price: {price}")
        
        # Get market cap
        info = yf.Ticker(ticker).info
        mcap = info.get('marketCap')
        if mcap:
            print(f"  Market Cap: {mcap:,}")
        else:
            print("  Market Cap: None")
            
        return True
    except Exception as e:
        print(f"  Error: {e}")
        return False

if __name__ == "__main__":
    tickers = sys.argv[1:] if len(sys.argv) > 1 else ['YELP', 'FISV', 'SQ', 'IAD']
    for ticker in tickers:
        check_ticker(ticker)