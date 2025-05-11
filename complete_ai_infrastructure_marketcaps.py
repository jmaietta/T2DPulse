"""
Add missing market cap data for the AI Infrastructure sector.

This script will fetch market cap data for all AI Infrastructure tickers from the Polygon API
and add the data to the sector_market_caps table for May 9, 2025.
"""
import os
import sqlite3
import requests
import traceback
from datetime import datetime

# Configure API keys
POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY")

# AI Infrastructure tickers
AI_INFRA_TICKERS = ["AMZN", "GOOGL", "IBM", "META", "MSFT", "NVDA", "ORCL"]

def get_polygon_market_cap(ticker):
    """Get market cap data from Polygon API."""
    if not POLYGON_API_KEY:
        print("Polygon API key not found in environment variables.")
        return None
    
    url = f"https://api.polygon.io/v3/reference/tickers/{ticker}?apiKey={POLYGON_API_KEY}"
    
    try:
        response = requests.get(url, timeout=10)
        if response.status_code != 200:
            print(f"Polygon API error for {ticker}: {response.status_code}")
            return None
            
        data = response.json()
        
        if data.get("results"):
            # Get market cap from Polygon data
            market_cap = data["results"].get("market_cap")
            if market_cap:
                return float(market_cap)
            
            # If no direct market cap, try calculating from shares * price
            shares = data["results"].get("share_class_shares_outstanding")
            price = data["results"].get("price")
            if not price:
                # If price not in ticker details, try quotes endpoint
                quote_url = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/{ticker}?apiKey={POLYGON_API_KEY}"
                quote_response = requests.get(quote_url, timeout=10)
                if quote_response.status_code == 200:
                    quote_data = quote_response.json()
                    if quote_data.get("ticker") and quote_data.get("ticker", {}).get("day"):
                        price = quote_data["ticker"]["day"].get("c")  # close price
                        
            if shares and price:
                return float(shares) * float(price)
                
        return None
    except Exception as e:
        print(f"Polygon API error for {ticker}: {e}")
        return None

def get_ticker_id(conn, ticker_symbol):
    """Get the ticker ID from the database."""
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM tickers WHERE symbol = ?", (ticker_symbol,))
    result = cursor.fetchone()
    return result[0] if result else None

def add_ticker_market_cap(conn, ticker_id, date, market_cap):
    """Add market cap data for a ticker to the database."""
    cursor = conn.cursor()
    
    # Check if we already have this data
    cursor.execute(
        "SELECT id FROM ticker_market_caps WHERE ticker_id = ? AND date = ?",
        (ticker_id, date)
    )
    
    existing = cursor.fetchone()
    
    if existing:
        # Update existing record
        cursor.execute(
            "UPDATE ticker_market_caps SET market_cap = ? WHERE ticker_id = ? AND date = ?",
            (market_cap, ticker_id, date)
        )
        print(f"Updated market cap for ticker_id={ticker_id} on {date}: ${market_cap/1_000_000_000:.2f}B")
    else:
        # Insert new record
        cursor.execute(
            "INSERT INTO ticker_market_caps (ticker_id, date, market_cap, data_source) VALUES (?, ?, ?, ?)",
            (ticker_id, date, market_cap, "Polygon")
        )
        print(f"Added market cap for ticker_id={ticker_id} on {date}: ${market_cap/1_000_000_000:.2f}B")
    
    conn.commit()

def recalculate_sector_market_cap(conn, sector_id, date):
    """Recalculate and update the sector market cap based on ticker data."""
    cursor = conn.cursor()
    
    # Get all tickers in this sector
    cursor.execute(
        """
        SELECT t.id 
        FROM tickers t
        JOIN ticker_sectors ts ON t.id = ts.ticker_id
        WHERE ts.sector_id = ?
        """,
        (sector_id,)
    )
    
    ticker_ids = [row[0] for row in cursor.fetchall()]
    
    if not ticker_ids:
        print(f"No tickers found for sector_id={sector_id}")
        return
    
    # Sum market caps for these tickers on the given date
    placeholders = ','.join(['?'] * len(ticker_ids))
    cursor.execute(
        f"""
        SELECT SUM(market_cap) 
        FROM ticker_market_caps 
        WHERE ticker_id IN ({placeholders}) AND date = ?
        """,
        ticker_ids + [date]
    )
    
    result = cursor.fetchone()
    total_market_cap = result[0] if result and result[0] else 0
    
    # Update sector market cap
    cursor.execute(
        "SELECT id FROM sector_market_caps WHERE sector_id = ? AND date = ?",
        (sector_id, date)
    )
    
    existing = cursor.fetchone()
    
    if existing:
        cursor.execute(
            "UPDATE sector_market_caps SET market_cap = ? WHERE sector_id = ? AND date = ?",
            (total_market_cap, sector_id, date)
        )
    else:
        cursor.execute(
            "INSERT INTO sector_market_caps (sector_id, date, market_cap) VALUES (?, ?, ?)",
            (sector_id, date, total_market_cap)
        )
    
    print(f"Updated sector_id={sector_id} market cap for {date}: ${total_market_cap/1_000_000_000:.2f}B")
    conn.commit()

def main():
    """Main function to complete market cap data for AI Infrastructure sector."""
    try:
        print("Starting script execution...")
        
        if not POLYGON_API_KEY:
            print("ERROR: POLYGON_API_KEY environment variable is not set. Please set it and try again.")
            return
            
        print(f"Using Polygon API Key: {POLYGON_API_KEY[:4]}...{POLYGON_API_KEY[-4:] if len(POLYGON_API_KEY) > 8 else ''}")
        
        conn = sqlite3.connect("market_cap_data.db")
        print("Connected to database successfully")
        
        # Get the sector ID for AI Infrastructure
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM sectors WHERE name = 'AI Infrastructure'")
        result = cursor.fetchone()
        
        if not result:
            print("ERROR: AI Infrastructure sector not found in the database.")
            conn.close()
            return
            
        sector_id = result[0]
        print(f"Found AI Infrastructure sector with ID: {sector_id}")
        
        # Target date
        date = "2025-05-09"
        
        # Process each ticker
        print(f"Fetching market cap data for AI Infrastructure tickers on {date}...")
        
        for ticker in AI_INFRA_TICKERS:
            try:
                ticker_id = get_ticker_id(conn, ticker)
                if not ticker_id:
                    print(f"Ticker {ticker} not found in the database.")
                    continue
                
                print(f"Processing ticker {ticker} (ID: {ticker_id})...")
                
                # Check if we already have data for this ticker and date
                cursor.execute(
                    "SELECT market_cap FROM ticker_market_caps WHERE ticker_id = ? AND date = ?",
                    (ticker_id, date)
                )
                
                existing = cursor.fetchone()
                if existing and existing[0]:
                    print(f"{ticker} already has market cap data for {date}: ${existing[0]/1_000_000_000:.2f}B")
                    continue
                
                # Fetch market cap from Polygon
                print(f"Fetching market cap for {ticker} from Polygon API...")
                market_cap = get_polygon_market_cap(ticker)
                if market_cap:
                    add_ticker_market_cap(conn, ticker_id, date, market_cap)
                else:
                    print(f"Failed to get market cap for {ticker}")
            except Exception as e:
                print(f"Error processing ticker {ticker}: {str(e)}")
                traceback.print_exc()
        
        # Recalculate sector market cap
        print("Recalculating sector market cap...")
        recalculate_sector_market_cap(conn, sector_id, date)
        
        conn.close()
        print("Market cap data update complete.")
        
    except Exception as e:
        print(f"ERROR in main function: {str(e)}")
        traceback.print_exc()

if __name__ == "__main__":
    main()