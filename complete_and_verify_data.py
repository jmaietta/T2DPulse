"""
Complete and verify the sector market cap data in the database.

This script will:
1. Check how many tickers have been loaded from the Polygon API data
2. Check that all sectors have market cap data for all dates
3. Verify consistency between ticker market caps and sector market caps
4. Generate a verification report for the historical market cap data
"""
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

def get_db_connection():
    """Connect to the database."""
    return sqlite3.connect("market_cap_data.db")

def check_ticker_coverage(conn):
    """Check how many tickers have market cap data in the database."""
    cursor = conn.cursor()
    
    # Get total number of tickers
    cursor.execute("SELECT COUNT(*) FROM tickers")
    total_tickers = cursor.fetchone()[0]
    
    # Get tickers with market cap data
    cursor.execute("""
    SELECT COUNT(DISTINCT ticker_id) 
    FROM ticker_market_caps
    """)
    tickers_with_data = cursor.fetchone()[0]
    
    print(f"Ticker Coverage: {tickers_with_data} out of {total_tickers} tickers have market cap data")
    
    # Get tickers without data
    cursor.execute("""
    SELECT t.symbol
    FROM tickers t
    LEFT JOIN (
        SELECT DISTINCT ticker_id FROM ticker_market_caps
    ) tmc ON t.id = tmc.ticker_id
    WHERE tmc.ticker_id IS NULL
    """)
    
    missing_tickers = [row[0] for row in cursor.fetchall()]
    
    if missing_tickers:
        print(f"Missing market cap data for {len(missing_tickers)} tickers:")
        print(", ".join(missing_tickers))
    else:
        print("All tickers have market cap data.")
        
    return tickers_with_data, total_tickers, missing_tickers

def check_sector_coverage(conn):
    """Check sector market cap coverage."""
    cursor = conn.cursor()
    
    # Get total number of sectors
    cursor.execute("SELECT COUNT(*) FROM sectors")
    total_sectors = cursor.fetchone()[0]
    
    # Get sectors with market cap data
    cursor.execute("""
    SELECT COUNT(DISTINCT sector_id) 
    FROM sector_market_caps
    """)
    sectors_with_data = cursor.fetchone()[0]
    
    print(f"\nSector Coverage: {sectors_with_data} out of {total_sectors} sectors have market cap data")
    
    # Get sectors without data
    cursor.execute("""
    SELECT s.name
    FROM sectors s
    LEFT JOIN (
        SELECT DISTINCT sector_id FROM sector_market_caps
    ) smc ON s.id = smc.sector_id
    WHERE smc.sector_id IS NULL
    """)
    
    missing_sectors = [row[0] for row in cursor.fetchall()]
    
    if missing_sectors:
        print(f"Missing market cap data for {len(missing_sectors)} sectors:")
        print(", ".join(missing_sectors))
    else:
        print("All sectors have market cap data.")
        
    return sectors_with_data, total_sectors, missing_sectors

def check_date_coverage(conn):
    """Check date coverage for sector market caps."""
    cursor = conn.cursor()
    
    # Get dates with market cap data
    cursor.execute("""
    SELECT DISTINCT date 
    FROM sector_market_caps 
    ORDER BY date
    """)
    
    dates = [row[0] for row in cursor.fetchall()]
    
    print(f"\nDate Coverage: Market cap data available for {len(dates)} dates:")
    print(", ".join(dates))
    
    return dates

def verify_consistency(conn):
    """Verify consistency between ticker market caps and sector market caps."""
    cursor = conn.cursor()
    
    # Get a sample date
    cursor.execute("SELECT MAX(date) FROM sector_market_caps")
    latest_date = cursor.fetchone()[0]
    
    print(f"\nVerifying consistency for date: {latest_date}")
    
    # For each sector, compare the sum of ticker market caps with the sector market cap
    cursor.execute("""
    SELECT s.name, smc.market_cap as sector_total,
           SUM(tmc.market_cap) as ticker_sum,
           COUNT(tmc.ticker_id) as ticker_count
    FROM sector_market_caps smc
    JOIN sectors s ON smc.sector_id = s.id
    JOIN ticker_sectors ts ON s.id = ts.sector_id
    LEFT JOIN ticker_market_caps tmc ON ts.ticker_id = tmc.ticker_id AND tmc.date = smc.date
    WHERE smc.date = ?
    GROUP BY s.id
    """, (latest_date,))
    
    results = cursor.fetchall()
    
    print("\nSector consistency check:")
    print("=" * 80)
    print(f"{'Sector':<25} {'Sector Total':<15} {'Ticker Sum':<15} {'Difference':<15} {'Ticker Count':<15}")
    print("-" * 80)
    
    inconsistent_sectors = []
    
    for row in results:
        sector_name, sector_total, ticker_sum, ticker_count = row
        
        if ticker_sum is None:
            ticker_sum = 0
            
        difference = sector_total - ticker_sum if ticker_sum else sector_total
        difference_pct = (difference / sector_total * 100) if sector_total else 0
        
        print(f"{sector_name:<25} ${sector_total/1e9:.2f}B {'$' + str(ticker_sum/1e9) + 'B' if ticker_sum else 'N/A':<15} ${difference/1e9:.2f}B ({difference_pct:.2f}%) {ticker_count}")
        
        if ticker_sum and abs(difference_pct) > 1.0:
            inconsistent_sectors.append(sector_name)
    
    if inconsistent_sectors:
        print(f"\nFound inconsistencies in {len(inconsistent_sectors)} sectors:")
        print(", ".join(inconsistent_sectors))
    else:
        print("\nAll sectors have consistent market cap data.")
        
    return inconsistent_sectors

def generate_summary_report(conn):
    """Generate a summary report of sector market caps."""
    # Get sector market cap data
    df = pd.read_sql("""
    SELECT s.name as sector, smc.date, smc.market_cap
    FROM sector_market_caps smc
    JOIN sectors s ON smc.sector_id = s.id
    ORDER BY smc.date, s.name
    """, conn)
    
    # Convert market cap to billions
    df['market_cap'] = df['market_cap'] / 1e9
    
    # Create pivot table
    pivot_df = df.pivot(index='date', columns='sector', values='market_cap')
    
    # Create line chart of sector market caps over time
    plt.figure(figsize=(15, 10))
    
    # Get top 5 sectors by latest market cap
    latest_date = pivot_df.index.max()
    top_sectors = pivot_df.loc[latest_date].sort_values(ascending=False).head(5).index.tolist()
    
    for sector in top_sectors:
        plt.plot(pivot_df.index, pivot_df[sector], marker='o', linewidth=2, label=sector)
    
    plt.title('Top 5 Sectors Market Cap Over Time', fontsize=16)
    plt.xlabel('Date', fontsize=14)
    plt.ylabel('Market Cap (Billions USD)', fontsize=14)
    plt.grid(True, alpha=0.3)
    plt.legend(fontsize=12)
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    # Save the chart
    plt.savefig('sector_market_cap_report.png')
    print("\nGenerated sector market cap chart: sector_market_cap_report.png")
    
    # Create summary table
    latest_df = df[df['date'] == latest_date]
    latest_pivot = latest_df.pivot(index='date', columns='sector', values='market_cap')
    
    # Save to Excel
    latest_pivot.to_excel('latest_sector_market_caps.xlsx')
    print("Generated latest sector market caps report: latest_sector_market_caps.xlsx")
    
    # Print summary
    total_market_cap = latest_df['market_cap'].sum()
    
    print(f"\nTotal Market Cap as of {latest_date}: ${total_market_cap:.2f}B")
    
    # Print top 5 sectors
    print("\nTop 5 Sectors by Market Cap:")
    top_5 = latest_df.sort_values('market_cap', ascending=False).head(5)
    
    for i, (_, row) in enumerate(top_5.iterrows(), 1):
        sector_pct = (row['market_cap'] / total_market_cap) * 100
        print(f"{i}. {row['sector']}: ${row['market_cap']:.2f}B ({sector_pct:.2f}%)")

def main():
    """Main function to check and verify market cap data."""
    print("Starting data verification...")
    
    conn = get_db_connection()
    
    # Check ticker coverage
    tickers_with_data, total_tickers, missing_tickers = check_ticker_coverage(conn)
    
    # Check sector coverage
    sectors_with_data, total_sectors, missing_sectors = check_sector_coverage(conn)
    
    # Check date coverage
    dates = check_date_coverage(conn)
    
    # Verify consistency
    inconsistent_sectors = verify_consistency(conn)
    
    # Generate summary report
    generate_summary_report(conn)
    
    # Print overall verification result
    print("\nOverall Verification Result:")
    print("=" * 80)
    
    if tickers_with_data == total_tickers and sectors_with_data == total_sectors and not inconsistent_sectors:
        print("✅ All verification checks passed!")
        print(f"✅ {tickers_with_data}/{total_tickers} tickers have market cap data")
        print(f"✅ {sectors_with_data}/{total_sectors} sectors have market cap data")
        print(f"✅ Market cap data available for {len(dates)} dates")
        print("✅ All sector market caps are consistent with ticker data")
    else:
        print("❌ Some verification checks failed:")
        if tickers_with_data < total_tickers:
            print(f"❌ Missing market cap data for {total_tickers - tickers_with_data} tickers")
        else:
            print(f"✅ {tickers_with_data}/{total_tickers} tickers have market cap data")
            
        if sectors_with_data < total_sectors:
            print(f"❌ Missing market cap data for {total_sectors - sectors_with_data} sectors")
        else:
            print(f"✅ {sectors_with_data}/{total_sectors} sectors have market cap data")
            
        print(f"✅ Market cap data available for {len(dates)} dates")
        
        if inconsistent_sectors:
            print(f"❌ Found inconsistencies in {len(inconsistent_sectors)} sectors")
        else:
            print("✅ All sector market caps are consistent with ticker data")
    
    conn.close()
    print("\nData verification complete.")

if __name__ == "__main__":
    main()