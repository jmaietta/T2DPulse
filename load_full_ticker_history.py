"""
Load full ticker history from the CSV file into the database.

This script will:
1. Read the CSV file containing all ticker market caps from Polygon API
2. Insert the data into the ticker_market_caps table
3. Recalculate all sector market caps based on the updated ticker data
"""
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import matplotlib.pyplot as plt

def get_db_connection():
    """Connect to the database."""
    return sqlite3.connect("market_cap_data.db")

def read_ticker_history_csv():
    """Read the CSV file containing ticker market cap history."""
    df = pd.read_csv("T2D_Pulse_Full_Ticker_History.csv")
    print(f"Read {len(df)} rows from T2D_Pulse_Full_Ticker_History.csv")
    print(f"Found data for {df['ticker'].nunique()} tickers across {df['date'].nunique()} dates")
    return df

def get_ticker_id_mapping(conn):
    """Get mapping from ticker symbols to database IDs."""
    query = "SELECT id, symbol FROM tickers"
    df = pd.read_sql_query(query, conn)
    ticker_mapping = dict(zip(df['symbol'], df['id']))
    return ticker_mapping

def insert_ticker_market_caps(conn, ticker_data, ticker_mapping):
    """Insert ticker market cap data into the database."""
    cursor = conn.cursor()
    
    # Clear existing data first
    cursor.execute("DELETE FROM ticker_market_caps")
    conn.commit()
    
    # Prepare data for insertion
    insert_data = []
    missing_tickers = set()
    
    for _, row in ticker_data.iterrows():
        ticker = row['ticker']
        if ticker in ticker_mapping:
            ticker_id = ticker_mapping[ticker]
            date = row['date']
            market_cap = row['market_cap']
            insert_data.append((ticker_id, date, market_cap))
        else:
            missing_tickers.add(ticker)
    
    if missing_tickers:
        print(f"WARNING: The following tickers are in the CSV but not in the database: {missing_tickers}")
    
    # Insert in batches
    batch_size = 1000
    for i in range(0, len(insert_data), batch_size):
        batch = insert_data[i:i+batch_size]
        cursor.executemany(
            """
            INSERT INTO ticker_market_caps (ticker_id, date, market_cap)
            VALUES (?, ?, ?)
            """,
            batch
        )
        conn.commit()
    
    print(f"Inserted {len(insert_data)} ticker market cap entries into the database")

def recalculate_sector_market_caps(conn):
    """Recalculate all sector market caps based on ticker data."""
    # Get the dates from ticker_market_caps
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT date FROM ticker_market_caps ORDER BY date")
    dates = [row[0] for row in cursor.fetchall()]
    
    # Clear existing sector market cap data
    cursor.execute("DELETE FROM sector_market_caps")
    conn.commit()
    
    # For each date, calculate sector market caps
    sector_data = []
    for date in dates:
        query = """
        SELECT ts.sector_id, SUM(tmc.market_cap) as sector_market_cap
        FROM ticker_market_caps tmc
        JOIN ticker_sectors ts ON tmc.ticker_id = ts.ticker_id
        WHERE tmc.date = ?
        GROUP BY ts.sector_id
        """
        cursor.execute(query, (date,))
        for sector_id, market_cap in cursor.fetchall():
            sector_data.append((sector_id, date, market_cap))
    
    # Insert all sector market caps
    cursor.executemany(
        """
        INSERT INTO sector_market_caps (sector_id, date, market_cap)
        VALUES (?, ?, ?)
        """,
        sector_data
    )
    conn.commit()
    
    print(f"Recalculated and inserted {len(sector_data)} sector market cap entries")

def print_sector_summary(conn):
    """Print a summary of sector market caps for the most recent date."""
    query = """
    SELECT s.name as sector_name, smc.market_cap, smc.date
    FROM sector_market_caps smc
    JOIN sectors s ON smc.sector_id = s.id
    WHERE smc.date = (SELECT MAX(date) FROM sector_market_caps)
    ORDER BY smc.market_cap DESC
    """
    
    df = pd.read_sql_query(query, conn)
    
    if df.empty:
        print("No sector market cap data found")
        return
    
    latest_date = df['date'].iloc[0]
    total_market_cap = df['market_cap'].sum()
    
    # Format for display
    df['market_cap_b'] = df['market_cap'] / 1_000_000_000
    df['percentage'] = df['market_cap'] * 100 / total_market_cap
    
    print(f"\nSector Market Caps for {latest_date} (Billions USD)")
    print("=" * 60)
    
    for i, row in df.iterrows():
        print(f"{row['sector_name']:<25} ${row['market_cap_b']:.2f}B ({row['percentage']:.2f}%)")
    
    print("-" * 60)
    print(f"TOTAL: ${total_market_cap/1_000_000_000:.2f}B")

def plot_sector_history(conn, top_n=5):
    """Create a plot of the top N sectors over time."""
    query = """
    SELECT s.name as sector_name, smc.market_cap, smc.date
    FROM sector_market_caps smc
    JOIN sectors s ON smc.sector_id = s.id
    ORDER BY smc.date
    """
    
    df = pd.read_sql_query(query, conn)
    
    if df.empty:
        print("No sector market cap data found for plotting")
        return
    
    # Get the latest date
    latest_date = df['date'].max()
    
    # Get the top N sectors by market cap on the latest date
    top_sectors = df[df['date'] == latest_date].sort_values('market_cap', ascending=False)['sector_name'].head(top_n).tolist()
    
    # Filter data for top sectors
    plot_data = df[df['sector_name'].isin(top_sectors)]
    
    # Convert to billions for better display
    plot_data['market_cap'] = plot_data['market_cap'] / 1_000_000_000
    
    # Pivot for plotting
    pivot_data = plot_data.pivot(index='date', columns='sector_name', values='market_cap')
    
    # Plot
    plt.figure(figsize=(15, 8))
    
    for sector in top_sectors:
        plt.plot(pivot_data.index, pivot_data[sector], marker='o', linewidth=2, label=sector)
    
    plt.title(f'Top {top_n} Sectors by Market Cap', fontsize=16)
    plt.xlabel('Date', fontsize=14)
    plt.ylabel('Market Cap (Billions USD)', fontsize=14)
    plt.grid(True, alpha=0.3)
    plt.legend(fontsize=12)
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    # Save the plot
    plt.savefig('authentic_sector_history.png')
    print(f"\nSaved chart of top {top_n} sectors to authentic_sector_history.png")

def export_to_excel(conn):
    """Export sector market cap data to Excel."""
    query = """
    SELECT s.name as sector_name, smc.market_cap, smc.date
    FROM sector_market_caps smc
    JOIN sectors s ON smc.sector_id = s.id
    ORDER BY smc.date, s.name
    """
    
    df = pd.read_sql_query(query, conn)
    
    if df.empty:
        print("No sector market cap data found for export")
        return
    
    # Convert to billions for better readability
    df['market_cap'] = df['market_cap'] / 1_000_000_000
    
    # Pivot the data for a tabular view
    pivot_data = df.pivot(index='date', columns='sector_name', values='market_cap')
    
    # Add a total column
    pivot_data['TOTAL'] = pivot_data.sum(axis=1)
    
    # Sort by date ascending
    pivot_data = pivot_data.sort_index()
    
    # Export to Excel
    pivot_data.to_excel('authentic_all_sector_market_caps.xlsx')
    print("\nExported all sector market cap data to authentic_all_sector_market_caps.xlsx")

def main():
    """Main function to load ticker history and recalculate sector market caps."""
    print("Starting to load full ticker history...")
    
    # Read the CSV file
    ticker_data = read_ticker_history_csv()
    
    # Connect to the database
    conn = get_db_connection()
    
    # Get ticker ID mapping
    ticker_mapping = get_ticker_id_mapping(conn)
    print(f"Found {len(ticker_mapping)} tickers in the database")
    
    # Insert ticker market caps
    insert_ticker_market_caps(conn, ticker_data, ticker_mapping)
    
    # Recalculate sector market caps
    recalculate_sector_market_caps(conn)
    
    # Print summary
    print_sector_summary(conn)
    
    # Create plot
    plot_sector_history(conn)
    
    # Export to Excel
    export_to_excel(conn)
    
    conn.close()
    print("\nComplete! All market cap data has been loaded and sector market caps have been recalculated.")

if __name__ == "__main__":
    main()