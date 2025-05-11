#!/usr/bin/env python3
"""
Update the AI Infrastructure sector with the correct list of tickers.

The AI Infrastructure sector should include: AMZN, GOOGL, IBM, META, MSFT, NVDA, and ORCL.
This script will add the missing tickers to the AI Infrastructure sector.
"""

import sqlite3

def get_sector_id(conn, sector_name):
    """Get the sector ID for a given sector name."""
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM sectors WHERE name = ?", (sector_name,))
    result = cursor.fetchone()
    if result:
        return result[0]
    return None

def get_ticker_id(conn, ticker_symbol):
    """Get the ticker ID for a given ticker symbol."""
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM tickers WHERE symbol = ?", (ticker_symbol,))
    result = cursor.fetchone()
    if result:
        return result[0]
    return None

def get_existing_ai_tickers(conn, sector_id):
    """Get the list of tickers already in the AI Infrastructure sector."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT t.symbol 
        FROM ticker_sectors ts 
        JOIN tickers t ON ts.ticker_id = t.id 
        WHERE ts.sector_id = ?
    """, (sector_id,))
    return [row[0] for row in cursor.fetchall()]

def add_ticker_to_sector(conn, ticker_id, sector_id):
    """Add a ticker to a sector."""
    cursor = conn.cursor()
    
    # Check if ticker-sector relationship already exists
    cursor.execute("""
        SELECT ticker_id, sector_id FROM ticker_sectors 
        WHERE ticker_id = ? AND sector_id = ?
    """, (ticker_id, sector_id))
    
    if cursor.fetchone():
        return False  # Relationship already exists
    
    # Add the relationship
    cursor.execute("""
        INSERT INTO ticker_sectors (ticker_id, sector_id)
        VALUES (?, ?)
    """, (ticker_id, sector_id))
    
    return True

def main():
    """Main function to update the AI Infrastructure sector."""
    conn = sqlite3.connect('market_cap_data.db')
    
    try:
        # Get the sector ID for AI Infrastructure
        sector_id = get_sector_id(conn, 'AI Infrastructure')
        if not sector_id:
            print("Error: AI Infrastructure sector not found in the database.")
            return
        
        print(f"AI Infrastructure sector ID: {sector_id}")
        
        # Define the tickers that should be in the AI Infrastructure sector
        ai_tickers = ['AMZN', 'GOOGL', 'IBM', 'META', 'MSFT', 'NVDA', 'ORCL']
        
        # Get the list of tickers already in the AI Infrastructure sector
        existing_tickers = get_existing_ai_tickers(conn, sector_id)
        print(f"Existing AI Infrastructure tickers: {existing_tickers}")
        
        # Add the missing tickers to the AI Infrastructure sector
        missing_tickers = [ticker for ticker in ai_tickers if ticker not in existing_tickers]
        added_count = 0
        
        for ticker in missing_tickers:
            ticker_id = get_ticker_id(conn, ticker)
            if not ticker_id:
                print(f"Warning: Ticker {ticker} not found in the database.")
                continue
            
            if add_ticker_to_sector(conn, ticker_id, sector_id):
                added_count += 1
                print(f"Added {ticker} to AI Infrastructure sector.")
        
        conn.commit()
        print(f"Added {added_count} new tickers to the AI Infrastructure sector.")
        
        # Get the updated list of tickers in the AI Infrastructure sector
        updated_tickers = get_existing_ai_tickers(conn, sector_id)
        print(f"Updated AI Infrastructure tickers: {updated_tickers}")
        
    finally:
        conn.close()

if __name__ == "__main__":
    main()