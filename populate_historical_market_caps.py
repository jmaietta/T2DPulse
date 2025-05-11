"""
Populate historical sector market cap data using authentic data from Polygon API.

This script will:
1. Read the ticker-to-sector assignments from the database
2. Read the historical ticker market cap data from the database
3. Calculate sector market caps for each date based on ticker data
4. Update the sector_market_caps table with authentic historical values
"""
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
from tabulate import tabulate

def get_db_connection():
    """Connect to the database."""
    return sqlite3.connect("market_cap_data.db")

def get_ticker_sector_mapping(conn):
    """Get the mapping of tickers to sectors from the database."""
    query = """
    SELECT t.id as ticker_id, t.symbol as ticker_symbol, 
           s.id as sector_id, s.name as sector_name
    FROM tickers t
    JOIN ticker_sectors ts ON t.id = ts.ticker_id
    JOIN sectors s ON ts.sector_id = s.id
    """
    
    return pd.read_sql_query(query, conn)

def get_historical_ticker_market_caps(conn, start_date=None):
    """Get all historical ticker market cap data from the database."""
    if not start_date:
        # Default to 30 days ago
        start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    
    query = """
    SELECT tmc.ticker_id, t.symbol as ticker_symbol, tmc.date, tmc.market_cap
    FROM ticker_market_caps tmc
    JOIN tickers t ON tmc.ticker_id = t.id
    WHERE tmc.date >= ?
    ORDER BY tmc.date, t.symbol
    """
    
    return pd.read_sql_query(query, conn, params=(start_date,))

def calculate_sector_market_caps(ticker_data, ticker_sector_mapping):
    """Calculate sector market caps for each date based on ticker data."""
    # Merge ticker market cap data with sector mapping
    merged_data = pd.merge(
        ticker_data, 
        ticker_sector_mapping, 
        on=['ticker_id', 'ticker_symbol']
    )
    
    # Group by date and sector, sum market caps
    sector_data = merged_data.groupby(['date', 'sector_id', 'sector_name'])['market_cap'].sum().reset_index()
    
    return sector_data

def update_sector_market_caps(conn, sector_data):
    """Update the sector_market_caps table with calculated values."""
    cursor = conn.cursor()
    
    # First, check which entries already exist
    dates = sector_data['date'].unique()
    for date in dates:
        cursor.execute("DELETE FROM sector_market_caps WHERE date = ?", (date,))
    
    # Now insert the new values
    for _, row in sector_data.iterrows():
        cursor.execute(
            """
            INSERT INTO sector_market_caps (sector_id, date, market_cap)
            VALUES (?, ?, ?)
            """,
            (row['sector_id'], row['date'], row['market_cap'])
        )
    
    conn.commit()
    print(f"Updated {len(sector_data)} sector market cap entries")

def print_sector_summary(sector_data):
    """Print a summary of the sector market caps for the most recent date."""
    latest_date = sector_data['date'].max()
    latest_data = sector_data[sector_data['date'] == latest_date]
    
    # Total market cap
    total_market_cap = latest_data['market_cap'].sum()
    
    # Format for display
    summary_data = latest_data.copy()
    summary_data['market_cap'] = summary_data['market_cap'] / 1_000_000_000
    summary_data['percentage'] = summary_data['market_cap'] * 100 / (total_market_cap / 1_000_000_000)
    
    summary_data = summary_data.sort_values('market_cap', ascending=False)
    
    print(f"\nSector Market Caps for {latest_date} (Billions USD)")
    print("=" * 60)
    print(tabulate(
        summary_data[['sector_name', 'market_cap', 'percentage']],
        headers=['Sector', 'Market Cap ($B)', 'Percentage (%)'],
        tablefmt='psql',
        floatfmt=('.2f', '.2f')
    ))
    
    print(f"\nTotal Market Cap: ${total_market_cap/1_000_000_000:.2f}B")

def plot_sector_history(sector_data, top_n=5):
    """Create a line chart of sector market caps over time."""
    # Calculate total market cap by date
    total_by_date = sector_data.groupby('date')['market_cap'].sum().reset_index()
    total_by_date['sector_name'] = 'TOTAL'
    
    # Convert market cap to billions for display
    sector_data['market_cap'] = sector_data['market_cap'] / 1_000_000_000
    total_by_date['market_cap'] = total_by_date['market_cap'] / 1_000_000_000
    
    # Get top N sectors by last date
    latest_date = sector_data['date'].max()
    latest_data = sector_data[sector_data['date'] == latest_date]
    top_sectors = latest_data.sort_values('market_cap', ascending=False).head(top_n)['sector_name'].tolist()
    
    # Filter data for the top sectors
    plot_data = sector_data[sector_data['sector_name'].isin(top_sectors)]
    
    # Pivot the data for plotting
    pivot_data = plot_data.pivot(index='date', columns='sector_name', values='market_cap')
    
    # Plot
    plt.figure(figsize=(15, 8))
    
    for sector in top_sectors:
        plt.plot(pivot_data.index, pivot_data[sector], marker='o', linewidth=2, label=sector)
    
    # Also plot the total
    plt.plot(total_by_date['date'], total_by_date['market_cap'], 'k--', linewidth=2, label='TOTAL')
    
    plt.title(f'Top {top_n} Sectors by Market Cap', fontsize=16)
    plt.xlabel('Date', fontsize=14)
    plt.ylabel('Market Cap (Billions USD)', fontsize=14)
    plt.grid(True, alpha=0.3)
    plt.legend(fontsize=12)
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    # Save the plot
    plt.savefig('sector_history_chart.png')
    print(f"\nSaved chart of top {top_n} sectors to sector_history_chart.png")

def export_to_excel(sector_data):
    """Export sector market cap data to Excel."""
    # Convert to billions for better readability
    export_data = sector_data.copy()
    export_data['market_cap'] = export_data['market_cap'] / 1_000_000_000
    
    # Pivot the data for a tabular view
    pivot_data = export_data.pivot(index='date', columns='sector_name', values='market_cap')
    
    # Add a total column
    pivot_data['TOTAL'] = pivot_data.sum(axis=1)
    
    # Sort by date ascending
    pivot_data = pivot_data.sort_index()
    
    # Export to Excel
    pivot_data.to_excel('authentic_sector_market_caps.xlsx')
    print("\nExported all sector market cap data to authentic_sector_market_caps.xlsx")

def main():
    """Main function to populate historical sector market cap data."""
    conn = get_db_connection()
    
    print("Reading ticker-sector mapping...")
    ticker_sector_mapping = get_ticker_sector_mapping(conn)
    
    print(f"Found {len(ticker_sector_mapping)} ticker-sector assignments across {ticker_sector_mapping['sector_name'].nunique()} sectors")
    
    print("\nReading historical ticker market cap data...")
    ticker_data = get_historical_ticker_market_caps(conn)
    
    print(f"Found market cap data for {ticker_data['ticker_symbol'].nunique()} tickers across {ticker_data['date'].nunique()} dates")
    
    print("\nCalculating sector market caps...")
    sector_data = calculate_sector_market_caps(ticker_data, ticker_sector_mapping)
    
    print(f"Calculated {len(sector_data)} sector market cap entries")
    
    print("\nUpdating sector_market_caps table...")
    update_sector_market_caps(conn, sector_data)
    
    # Print summary and create visualizations
    print_sector_summary(sector_data)
    plot_sector_history(sector_data)
    export_to_excel(sector_data)
    
    conn.close()
    print("\nHistorical sector market cap population complete.")

if __name__ == "__main__":
    main()