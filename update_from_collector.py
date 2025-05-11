"""
Update market cap data from the Polygon 30-Day History Collector workflow.

This script:
1. Reads the latest data from T2D_Pulse_Full_Ticker_History.csv
2. Updates the ticker_market_caps table with new data
3. Recalculates sector_market_caps based on the updated ticker data
4. Generates a report of the updated data
"""
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

def get_db_connection():
    """Connect to the database."""
    return sqlite3.connect("market_cap_data.db")

def load_latest_ticker_data():
    """Load the latest ticker data from the CSV file."""
    try:
        df = pd.read_csv("T2D_Pulse_Full_Ticker_History.csv")
        print(f"Loaded {len(df)} records from T2D_Pulse_Full_Ticker_History.csv")
        return df
    except Exception as e:
        print(f"Error loading CSV file: {e}")
        return None

def update_ticker_market_caps(conn, ticker_data_df):
    """Update ticker market cap data in the database."""
    cursor = conn.cursor()
    
    # Get ticker IDs for each ticker
    cursor.execute("SELECT id, symbol FROM tickers")
    ticker_id_map = {row[1]: row[0] for row in cursor.fetchall()}
    
    # Get existing ticker market cap entries
    cursor.execute("SELECT COUNT(*) FROM ticker_market_caps")
    existing_count = cursor.fetchone()[0]
    print(f"Found {existing_count} existing ticker market cap entries")
    
    # Prepare rows to insert
    ticker_rows = []
    for _, row in ticker_data_df.iterrows():
        ticker = row['ticker']
        if ticker in ticker_id_map:
            ticker_id = ticker_id_map[ticker]
            date = row['date']
            market_cap = row['market_cap']
            ticker_rows.append((ticker_id, date, market_cap))
    
    # Insert in batches using INSERT OR REPLACE
    batch_size = 100
    inserted_count = 0
    for i in range(0, len(ticker_rows), batch_size):
        batch = ticker_rows[i:i+batch_size]
        cursor.executemany(
            """
            INSERT OR REPLACE INTO ticker_market_caps 
            (ticker_id, date, market_cap) VALUES (?, ?, ?)
            """,
            batch
        )
        inserted_count += len(batch)
        conn.commit()
    
    cursor.execute("SELECT COUNT(*) FROM ticker_market_caps")
    new_count = cursor.fetchone()[0]
    
    print(f"Processed {inserted_count} ticker market cap entries")
    print(f"Ticker market cap entries: {existing_count} → {new_count}")
    
    # Get stats on ticker coverage
    cursor.execute("""
    SELECT COUNT(DISTINCT ticker_id) FROM ticker_market_caps
    """)
    distinct_tickers = cursor.fetchone()[0]
    
    # Get stats on date coverage
    cursor.execute("""
    SELECT COUNT(DISTINCT date) FROM ticker_market_caps
    """)
    distinct_dates = cursor.fetchone()[0]
    
    print(f"Now have data for {distinct_tickers} tickers across {distinct_dates} dates")
    
    return distinct_tickers, distinct_dates

def recalculate_sector_market_caps(conn):
    """Recalculate sector market caps based on ticker data."""
    cursor = conn.cursor()
    
    # Clear existing sector market caps
    cursor.execute("DELETE FROM sector_market_caps")
    conn.commit()
    print("Cleared existing sector market cap data")
    
    # Calculate new sector market caps
    cursor.execute("""
    INSERT INTO sector_market_caps (sector_id, date, market_cap)
    SELECT ts.sector_id, tmc.date, SUM(tmc.market_cap)
    FROM ticker_market_caps tmc
    JOIN ticker_sectors ts ON tmc.ticker_id = ts.ticker_id
    GROUP BY ts.sector_id, tmc.date
    """)
    conn.commit()
    
    # Count the new sector market cap entries
    cursor.execute("SELECT COUNT(*) FROM sector_market_caps")
    sector_count = cursor.fetchone()[0]
    
    # Get stats on sector coverage
    cursor.execute("""
    SELECT COUNT(DISTINCT sector_id) FROM sector_market_caps
    """)
    distinct_sectors = cursor.fetchone()[0]
    
    print(f"Created {sector_count} sector market cap entries for {distinct_sectors} sectors")
    
    return sector_count, distinct_sectors

def generate_sector_summary(conn):
    """Generate a summary of sector market caps."""
    # Get the latest date
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(date) FROM sector_market_caps")
    latest_date = cursor.fetchone()[0]
    
    if not latest_date:
        print("No sector market cap data found")
        return None
    
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
    
    # Export to Excel
    sector_history_query = """
    SELECT s.name as sector, smc.date, smc.market_cap / 1000000000 as market_cap_billions
    FROM sector_market_caps smc
    JOIN sectors s ON smc.sector_id = s.id
    ORDER BY smc.date, s.name
    """
    
    history_df = pd.read_sql(sector_history_query, conn)
    
    # Create pivot table
    pivot_df = history_df.pivot(index='date', columns='sector', values='market_cap_billions')
    
    # Export to Excel
    pivot_df.to_excel("sector_market_caps_history.xlsx")
    print("Exported sector market cap history to sector_market_caps_history.xlsx")
    
    # Create chart
    plt.figure(figsize=(12, 8))
    
    # Get top 5 sectors by latest market cap
    top_sectors = list(df.sort_values('market_cap', ascending=False).head(5)['name'])
    
    for sector in top_sectors:
        if sector in pivot_df.columns:
            plt.plot(pivot_df.index, pivot_df[sector], marker='o', linewidth=2, label=sector)
    
    plt.title('Top 5 Sectors Market Cap (30-Day History)', fontsize=16)
    plt.xlabel('Date', fontsize=14)
    plt.ylabel('Market Cap (Billions USD)', fontsize=14)
    plt.grid(True, alpha=0.3)
    plt.legend(fontsize=12)
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    # Save chart
    plt.savefig("sector_market_caps_chart.png")
    print("Created sector market cap chart: sector_market_caps_chart.png")
    
    return latest_date, total_market_cap, df

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

def main():
    """Main function."""
    print(f"Starting market cap update at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Load latest ticker data
    ticker_data_df = load_latest_ticker_data()
    if ticker_data_df is None:
        print("Failed to load ticker data. Exiting.")
        return
    
    # Connect to database
    conn = get_db_connection()
    
    try:
        # Update ticker market caps
        distinct_tickers, distinct_dates = update_ticker_market_caps(conn, ticker_data_df)
        
        # Recalculate sector market caps
        sector_count, distinct_sectors = recalculate_sector_market_caps(conn)
        
        # Generate sector summary
        latest_date, total_market_cap, _ = generate_sector_summary(conn)
        
        # List missing sectors
        missing_sectors = list_missing_sectors(conn)
        
        # Print summary
        print("\nUpdate Summary:")
        print("=" * 60)
        print(f"Ticker Market Caps:    {distinct_tickers} tickers across {distinct_dates} dates")
        print(f"Sector Market Caps:    {distinct_sectors} sectors across {distinct_dates} dates")
        print(f"Total Market Cap:      ${total_market_cap:.2f}B as of {latest_date}")
        print(f"Sectors still missing: {len(missing_sectors)} sectors")
        print(f"Update completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
    finally:
        conn.close()

if __name__ == "__main__":
    main()