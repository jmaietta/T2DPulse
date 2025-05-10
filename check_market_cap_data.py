#!/usr/bin/env python3
"""
Check Market Cap Data Quality and Display Results
"""
import os
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
from datetime import date, datetime, timedelta

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "t2d_pulse.db")

def get_db():
    """Get a database connection with Row factory"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def query(sql, params=()):
    """Execute a SQL query and return all results"""
    db = get_db()
    cursor = db.cursor()
    cursor.execute(sql, params)
    results = cursor.fetchall()
    db.close()
    return results

def check_database_exists():
    """Check if the database exists and contains required tables"""
    if not os.path.exists(DB_PATH):
        print(f"Database not found at {DB_PATH}")
        return False
    
    try:
        tables = query("SELECT name FROM sqlite_master WHERE type='table'")
        table_names = [row['name'] for row in tables]
        
        required_tables = [
            'ticker_prices',
            'share_counts',
            'ticker_market_caps',
            'sector_market_caps',
            'sector_tickers',
            'data_quality'
        ]
        
        missing = [t for t in required_tables if t not in table_names]
        
        if missing:
            print(f"Missing required tables: {', '.join(missing)}")
            return False
        
        print(f"Database structure OK. Found all required tables: {', '.join(required_tables)}")
        return True
    
    except Exception as e:
        print(f"Error checking database: {e}")
        return False

def check_sectors():
    """Check sector configuration"""
    sectors = query("SELECT DISTINCT sector FROM sector_tickers")
    sector_names = [row['sector'] for row in sectors]
    
    if not sector_names:
        print("No sectors found in the database")
        return False
    
    print(f"Found {len(sector_names)} sectors: {', '.join(sector_names)}")
    
    # Check ticker counts by sector
    results = query("""
        SELECT sector, COUNT(*) as ticker_count
        FROM sector_tickers
        GROUP BY sector
        ORDER BY ticker_count DESC
    """)
    
    print("\nTickers per sector:")
    for row in results:
        print(f"  {row['sector']}: {row['ticker_count']} tickers")
    
    return True

def check_share_counts():
    """Check share count data"""
    counts = query("SELECT COUNT(*) as count FROM share_counts")
    total = counts[0]['count'] if counts else 0
    
    if total == 0:
        print("No share counts found in the database")
        return False
    
    print(f"Found {total} share counts in the database")
    
    # Check some sample share counts
    samples = query("""
        SELECT ticker, count, updated_at
        FROM share_counts
        ORDER BY updated_at DESC
        LIMIT 10
    """)
    
    print("\nRecent share count examples:")
    for row in samples:
        print(f"  {row['ticker']}: {row['count']:,} shares (updated: {row['updated_at']})")
    
    return True

def check_prices():
    """Check price data"""
    # Get date range
    date_range = query("""
        SELECT MIN(date) as min_date, MAX(date) as max_date
        FROM ticker_prices
    """)
    
    if not date_range or not date_range[0]['min_date']:
        print("No price data found in the database")
        return False
    
    min_date = date_range[0]['min_date']
    max_date = date_range[0]['max_date']
    
    print(f"Price data available from {min_date} to {max_date}")
    
    # Check coverage by date
    results = query("""
        SELECT date, COUNT(*) as ticker_count
        FROM ticker_prices
        GROUP BY date
        ORDER BY date DESC
        LIMIT 10
    """)
    
    print("\nRecent price coverage:")
    for row in results:
        print(f"  {row['date']}: {row['ticker_count']} tickers")
    
    # Check sample prices
    samples = query("""
        SELECT ticker, date, price
        FROM ticker_prices
        WHERE date = (SELECT MAX(date) FROM ticker_prices)
        ORDER BY price DESC
        LIMIT 10
    """)
    
    print("\nSample prices for most recent date:")
    for row in samples:
        print(f"  {row['ticker']}: ${row['price']:.2f} on {row['date']}")
    
    return True

def check_market_caps():
    """Check market cap data"""
    # Get date range
    date_range = query("""
        SELECT MIN(date) as min_date, MAX(date) as max_date
        FROM ticker_market_caps
    """)
    
    if not date_range or not date_range[0]['min_date']:
        print("No market cap data found in the database")
        return False
    
    min_date = date_range[0]['min_date']
    max_date = date_range[0]['max_date']
    
    print(f"Market cap data available from {min_date} to {max_date}")
    
    # Check coverage by date
    results = query("""
        SELECT date, COUNT(*) as ticker_count, SUM(market_cap)/1e12 as total_cap
        FROM ticker_market_caps
        GROUP BY date
        ORDER BY date DESC
        LIMIT 10
    """)
    
    print("\nRecent market cap coverage:")
    for row in results:
        print(f"  {row['date']}: {row['ticker_count']} tickers, ${row['total_cap']:.2f}T total")
    
    # Check sample market caps
    samples = query("""
        SELECT ticker, date, market_cap/1e9 as market_cap_billions
        FROM ticker_market_caps
        WHERE date = (SELECT MAX(date) FROM ticker_market_caps)
        ORDER BY market_cap DESC
        LIMIT 10
    """)
    
    print("\nTop market caps for most recent date:")
    for row in samples:
        print(f"  {row['ticker']}: ${row['market_cap_billions']:.2f}B on {row['date']}")
    
    return True

def check_sector_market_caps():
    """Check sector market cap data"""
    # Get date range
    date_range = query("""
        SELECT MIN(date) as min_date, MAX(date) as max_date
        FROM sector_market_caps
    """)
    
    if not date_range or not date_range[0]['min_date']:
        print("No sector market cap data found in the database")
        return False
    
    min_date = date_range[0]['min_date']
    max_date = date_range[0]['max_date']
    
    print(f"Sector market cap data available from {min_date} to {max_date}")
    
    # Check coverage by date
    results = query("""
        SELECT date, COUNT(*) as sector_count, SUM(market_cap)/1e12 as total_cap
        FROM sector_market_caps
        GROUP BY date
        ORDER BY date DESC
        LIMIT 10
    """)
    
    print("\nRecent sector market cap coverage:")
    for row in results:
        print(f"  {row['date']}: {row['sector_count']} sectors, ${row['total_cap']:.2f}T total")
    
    # Check most recent sector market caps
    samples = query("""
        SELECT sector, market_cap/1e12 as market_cap_trillions
        FROM sector_market_caps
        WHERE date = (SELECT MAX(date) FROM sector_market_caps)
        ORDER BY market_cap DESC
    """)
    
    print("\nSector market caps for most recent date:")
    for row in samples:
        print(f"  {row['sector']}: ${row['market_cap_trillions']:.2f}T")
    
    return True

def check_data_quality():
    """Check data quality metrics"""
    # Get date range
    date_range = query("""
        SELECT MIN(date) as min_date, MAX(date) as max_date
        FROM data_quality
    """)
    
    if not date_range or not date_range[0]['min_date']:
        print("No data quality metrics found in the database")
        return False
    
    min_date = date_range[0]['min_date']
    max_date = date_range[0]['max_date']
    
    print(f"Data quality metrics available from {min_date} to {max_date}")
    
    # Check most recent quality metrics
    samples = query("""
        SELECT date, total_tickers, covered_tickers, coverage_pct, status, message
        FROM data_quality
        ORDER BY date DESC
        LIMIT 10
    """)
    
    print("\nRecent data quality metrics:")
    for row in samples:
        print(f"  {row['date']}: {row['status']} - {row['covered_tickers']}/{row['total_tickers']} tickers ({row['coverage_pct']:.1f}%)")
        print(f"    Message: {row['message']}")
    
    return True

def export_to_dataframe():
    """Export sector market caps to DataFrame and print 30-day table"""
    # Get all sector market caps for the last 30 days
    today = date.today()
    start_date = (today - timedelta(days=30)).isoformat()
    
    results = query("""
        SELECT sector, date, market_cap
        FROM sector_market_caps
        WHERE date >= ?
        ORDER BY date, sector
    """, (start_date,))
    
    if not results:
        print("No sector market cap data available for the last 30 days")
        return None
    
    # Group by date and sector
    data = {}
    for row in results:
        date_str = row['date']
        sector = row['sector']
        market_cap = row['market_cap']
        
        if date_str not in data:
            data[date_str] = {}
        
        data[date_str][sector] = market_cap
    
    # Convert to DataFrame
    df = pd.DataFrame.from_dict(data, orient='index')
    
    # Print table with market caps in trillions
    print("\n30-day sector market cap table ($ trillions):")
    df_trillions = df.copy() / 1e12
    print(df_trillions.round(2).to_string())
    
    return df

def plot_sector_trends(df):
    """Plot market cap trends for each sector"""
    if df is None or df.empty:
        print("No data available for plotting")
        return
    
    try:
        # Convert index to datetime
        df.index = pd.to_datetime(df.index)
        
        # Plot each sector trend
        plt.figure(figsize=(12, 8))
        
        for column in df.columns:
            plt.plot(df.index, df[column]/1e12, label=column)
        
        plt.title('Sector Market Cap Trends ($ Trillions)')
        plt.xlabel('Date')
        plt.ylabel('Market Cap ($ Trillions)')
        plt.legend(loc='upper left', bbox_to_anchor=(1, 1))
        plt.grid(True)
        plt.tight_layout()
        
        # Save to file
        plt.savefig('sector_market_cap_trends.png')
        print("\nSector market cap trend plot saved to sector_market_cap_trends.png")
    
    except Exception as e:
        print(f"Error plotting sector trends: {e}")

def main():
    """Main function to check market cap data"""
    print("=== Market Cap Data Quality Check ===\n")
    
    # Check database structure
    if not check_database_exists():
        print("\nDatabase check failed. Please run market_cap_ingest.py first.")
        return
    
    print("\n=== Sector Configuration ===")
    check_sectors()
    
    print("\n=== Share Count Data ===")
    check_share_counts()
    
    print("\n=== Price Data ===")
    check_prices()
    
    print("\n=== Market Cap Data ===")
    check_market_caps()
    
    print("\n=== Sector Market Cap Data ===")
    check_sector_market_caps()
    
    print("\n=== Data Quality Metrics ===")
    check_data_quality()
    
    print("\n=== 30-Day Market Cap Table ===")
    df = export_to_dataframe()
    
    # Plot sector trends
    plot_sector_trends(df)
    
    print("\n=== Check completed successfully ===")

if __name__ == "__main__":
    main()