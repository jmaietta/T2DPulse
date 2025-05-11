"""
Verify market cap data in the database.

This script will:
1. Count the number of ticker market cap entries
2. Count the number of sector market cap entries
3. Show the latest sector market caps
"""
import sqlite3
import pandas as pd

def get_db_connection():
    """Connect to the database."""
    return sqlite3.connect("market_cap_data.db")

def check_market_cap_status(conn):
    """Check market cap data status."""
    cursor = conn.cursor()
    
    # Count ticker market caps
    cursor.execute("SELECT COUNT(*) FROM ticker_market_caps")
    ticker_count = cursor.fetchone()[0]
    
    # Count sector market caps
    cursor.execute("SELECT COUNT(*) FROM sector_market_caps")
    sector_count = cursor.fetchone()[0]
    
    # Count distinct tickers
    cursor.execute("SELECT COUNT(DISTINCT ticker_id) FROM ticker_market_caps")
    distinct_tickers = cursor.fetchone()[0]
    
    # Count distinct sectors
    cursor.execute("SELECT COUNT(DISTINCT sector_id) FROM sector_market_caps")
    distinct_sectors = cursor.fetchone()[0]
    
    # Count distinct dates in ticker market caps
    cursor.execute("SELECT COUNT(DISTINCT date) FROM ticker_market_caps")
    ticker_dates = cursor.fetchone()[0]
    
    # Count distinct dates in sector market caps
    cursor.execute("SELECT COUNT(DISTINCT date) FROM sector_market_caps")
    sector_dates = cursor.fetchone()[0]
    
    print("Market Cap Data Status:")
    print("=" * 50)
    print(f"Ticker Market Caps:    {ticker_count} entries")
    print(f"Distinct Tickers:      {distinct_tickers} tickers")
    print(f"Ticker Date Coverage:  {ticker_dates} dates")
    print("-" * 50)
    print(f"Sector Market Caps:    {sector_count} entries")
    print(f"Distinct Sectors:      {distinct_sectors} sectors")
    print(f"Sector Date Coverage:  {sector_dates} dates")
    
    return ticker_count, sector_count

def display_latest_sector_market_caps(conn):
    """Display the latest sector market caps."""
    # Get the latest date
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(date) FROM sector_market_caps")
    latest_date = cursor.fetchone()[0]
    
    # Get sector market caps for the latest date
    query = """
    SELECT s.name, smc.market_cap
    FROM sector_market_caps smc
    JOIN sectors s ON smc.sector_id = s.id
    WHERE smc.date = ?
    ORDER BY smc.market_cap DESC
    """
    
    df = pd.read_sql(query, conn, params=(latest_date,))
    
    # Calculate totals
    df['market_cap_billions'] = df['market_cap'] / 1_000_000_000
    total_market_cap = df['market_cap'].sum() / 1_000_000_000
    
    # Calculate percentages
    df['percentage'] = (df['market_cap'] / df['market_cap'].sum()) * 100
    
    print(f"\nSector Market Caps for {latest_date} (Billions USD)")
    print("=" * 60)
    
    for _, row in df.iterrows():
        print(f"{row['name']:<30} ${row['market_cap_billions']:.2f}B ({row['percentage']:.2f}%)")
    
    print("-" * 60)
    print(f"TOTAL: ${total_market_cap:.2f}B")
    
    return latest_date, total_market_cap

def list_missing_sectors(conn):
    """List sectors with no market cap data."""
    cursor = conn.cursor()
    
    # Get sectors with no market cap data
    query = """
    SELECT s.name
    FROM sectors s
    LEFT JOIN (
        SELECT DISTINCT sector_id FROM sector_market_caps
    ) smc ON s.id = smc.sector_id
    WHERE smc.sector_id IS NULL
    """
    
    cursor.execute(query)
    missing_sectors = [row[0] for row in cursor.fetchall()]
    
    if missing_sectors:
        print("\nSectors with no market cap data:")
        for sector in missing_sectors:
            print(f"  - {sector}")
    else:
        print("\nAll sectors have market cap data.")
    
    return missing_sectors

def list_ticker_coverage(conn):
    """List tickers with market cap data."""
    cursor = conn.cursor()
    
    # Get tickers with market cap data
    query = """
    SELECT t.symbol, COUNT(tmc.date) as date_count
    FROM tickers t
    JOIN ticker_market_caps tmc ON t.id = tmc.ticker_id
    GROUP BY t.symbol
    ORDER BY date_count DESC
    """
    
    cursor.execute(query)
    ticker_coverage = cursor.fetchall()
    
    print("\nTickers with market cap data:")
    print("=" * 50)
    print(f"{'Ticker':<10} {'Date Count':<15}")
    print("-" * 50)
    
    for ticker, date_count in ticker_coverage:
        print(f"{ticker:<10} {date_count:<15}")
    
    return ticker_coverage

def main():
    """Main function."""
    print("Verifying market cap data...")
    
    conn = get_db_connection()
    
    try:
        # Check market cap status
        ticker_count, sector_count = check_market_cap_status(conn)
        
        # Display the latest sector market caps
        latest_date, total_market_cap = display_latest_sector_market_caps(conn)
        
        # List missing sectors
        missing_sectors = list_missing_sectors(conn)
        
        # List ticker coverage
        ticker_coverage = list_ticker_coverage(conn)
        
        # Print summary
        print("\nSummary:")
        print("=" * 50)
        print(f"Total Ticker Market Cap Entries: {ticker_count}")
        print(f"Total Sector Market Cap Entries: {sector_count}")
        print(f"Latest Market Cap Date: {latest_date}")
        print(f"Total Market Cap: ${total_market_cap:.2f}B")
        print(f"Sectors missing market cap data: {len(missing_sectors)}")
        print(f"Tickers with market cap data: {len(ticker_coverage)}")
        
    finally:
        conn.close()
    
    print("\nVerification complete.")

if __name__ == "__main__":
    main()