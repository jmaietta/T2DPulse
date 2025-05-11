#!/usr/bin/env python3
"""
Display all sectors and their associated tickers from the database.
"""

import sqlite3

def get_all_sectors(conn):
    """Get all sectors from the database."""
    cursor = conn.cursor()
    cursor.execute("SELECT id, name FROM sectors ORDER BY name")
    return cursor.fetchall()

def get_tickers_for_sector(conn, sector_id):
    """Get all tickers for a given sector."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT t.symbol 
        FROM ticker_sectors ts 
        JOIN tickers t ON ts.ticker_id = t.id 
        WHERE ts.sector_id = ?
        ORDER BY t.symbol
    """, (sector_id,))
    return [row[0] for row in cursor.fetchall()]

def main():
    """Main function to display all sectors and their tickers."""
    conn = sqlite3.connect('market_cap_data.db')
    
    try:
        sectors = get_all_sectors(conn)
        
        print("\nSECTOR TICKER ASSIGNMENTS")
        print("=========================\n")
        
        total_tickers = 0
        ticker_counts = {}
        
        for sector_id, sector_name in sectors:
            tickers = get_tickers_for_sector(conn, sector_id)
            ticker_counts[sector_name] = len(tickers)
            total_tickers += len(tickers)
            
            ticker_str = ", ".join(tickers)
            print(f"{sector_name} ({len(tickers)} tickers):")
            print(f"  {ticker_str}")
            print()
        
        # Get unique tickers
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(DISTINCT symbol) FROM tickers")
        unique_ticker_count = cursor.fetchone()[0]
        
        print("\nSUMMARY")
        print("=======")
        print(f"Total sectors: {len(sectors)}")
        print(f"Unique tickers: {unique_ticker_count}")
        print(f"Total ticker-sector assignments: {total_tickers}")
        print(f"Average sectors per ticker: {total_tickers / unique_ticker_count:.2f}")
        
        # Sort sectors by ticker count
        sorted_sectors = sorted(ticker_counts.items(), key=lambda x: x[1], reverse=True)
        print("\nSectors by ticker count:")
        for sector, count in sorted_sectors:
            print(f"  {sector}: {count} tickers")
        
    finally:
        conn.close()

if __name__ == "__main__":
    main()