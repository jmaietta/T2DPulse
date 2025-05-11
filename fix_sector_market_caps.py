"""
Fix sector market caps by using the authentic ticker data from the Polygon API.

This script will:
1. Read the CSV file containing all Polygon API data
2. Load the ticker to sector mapping from the database
3. Calculate sector market caps for each date
4. Update the sector_market_caps table with accurate values
"""
import sqlite3
import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt

# Database connection function
def get_db_connection():
    """Connect to the database."""
    return sqlite3.connect("market_cap_data.db")

# Read the CSV file with ticker market cap history
def load_ticker_market_caps():
    """Read the CSV file with ticker market cap data."""
    try:
        df = pd.read_csv("T2D_Pulse_Full_Ticker_History.csv")
        print(f"Loaded {len(df)} records from T2D_Pulse_Full_Ticker_History.csv")
        return df
    except Exception as e:
        print(f"Error loading CSV file: {e}")
        return None

# Get sector information from database
def load_sector_info(conn):
    """Load sector information from the database."""
    try:
        sectors_df = pd.read_sql("SELECT id, name FROM sectors", conn)
        print(f"Loaded {len(sectors_df)} sectors from database")
        return sectors_df
    except Exception as e:
        print(f"Error loading sectors: {e}")
        return None

# Get ticker to sector mapping from database
def load_ticker_sector_mapping(conn):
    """Load the mapping of tickers to sectors from database."""
    try:
        query = """
        SELECT t.symbol as ticker, s.id as sector_id, s.name as sector_name
        FROM tickers t
        JOIN ticker_sectors ts ON t.id = ts.ticker_id
        JOIN sectors s ON ts.sector_id = s.id
        """
        mapping_df = pd.read_sql(query, conn)
        print(f"Loaded {len(mapping_df)} ticker-sector mappings from database")
        return mapping_df
    except Exception as e:
        print(f"Error loading ticker-sector mapping: {e}")
        return None

# Calculate sector market caps
def calculate_sector_market_caps(ticker_market_caps_df, ticker_sector_mapping_df):
    """Calculate sector market caps based on ticker data."""
    try:
        # Merge ticker market caps with sector mapping
        merged_df = pd.merge(
            ticker_market_caps_df,
            ticker_sector_mapping_df,
            on='ticker',
            how='inner'
        )
        
        # Group by date and sector to calculate market caps
        sector_market_caps = merged_df.groupby(['date', 'sector_id', 'sector_name'])['market_cap'].sum().reset_index()
        
        print(f"Calculated {len(sector_market_caps)} sector market cap entries")
        return sector_market_caps
    except Exception as e:
        print(f"Error calculating sector market caps: {e}")
        return None

# Clear and update sector market caps in database
def update_sector_market_caps(conn, sector_market_caps_df):
    """Update sector market caps in the database."""
    try:
        cursor = conn.cursor()
        
        # Delete existing sector market caps
        cursor.execute("DELETE FROM sector_market_caps")
        conn.commit()
        print("Cleared existing sector market cap data")
        
        # Insert new sector market caps
        rows = []
        for _, row in sector_market_caps_df.iterrows():
            rows.append((row['sector_id'], row['date'], row['market_cap']))
        
        cursor.executemany(
            "INSERT INTO sector_market_caps (sector_id, date, market_cap) VALUES (?, ?, ?)",
            rows
        )
        conn.commit()
        
        print(f"Inserted {len(rows)} new sector market cap entries")
        return True
    except Exception as e:
        print(f"Error updating sector market caps: {e}")
        return False

# Create a summary of sector market caps
def print_sector_market_cap_summary(sector_market_caps_df):
    """Print a summary of sector market caps for the latest date."""
    try:
        # Get the latest date
        latest_date = sector_market_caps_df['date'].max()
        
        # Filter for latest date
        latest_df = sector_market_caps_df[sector_market_caps_df['date'] == latest_date]
        
        # Calculate total market cap
        total_market_cap = latest_df['market_cap'].sum()
        
        # Print summary
        print(f"\nSector Market Caps for {latest_date} (Billions USD)")
        print("=" * 60)
        
        # Sort by market cap descending
        for _, row in latest_df.sort_values('market_cap', ascending=False).iterrows():
            market_cap_billions = row['market_cap'] / 1_000_000_000
            percentage = (row['market_cap'] / total_market_cap) * 100
            print(f"{row['sector_name']:<30} ${market_cap_billions:.2f}B ({percentage:.2f}%)")
        
        # Print total
        print("-" * 60)
        print(f"TOTAL: ${total_market_cap/1_000_000_000:.2f}B")
        
    except Exception as e:
        print(f"Error printing sector market cap summary: {e}")

# Export sector market caps to Excel
def export_to_excel(sector_market_caps_df):
    """Export sector market caps to Excel for analysis."""
    try:
        # Pivot the data for easier viewing
        pivot_df = sector_market_caps_df.pivot(
            index='date',
            columns='sector_name',
            values='market_cap'
        )
        
        # Convert to billions for readability
        pivot_df = pivot_df / 1_000_000_000
        
        # Save to Excel
        pivot_df.to_excel("authentic_sector_market_caps.xlsx")
        print("\nExported sector market caps to authentic_sector_market_caps.xlsx")
        
    except Exception as e:
        print(f"Error exporting to Excel: {e}")

# Main function
def main():
    """Main function to fix sector market caps."""
    print("Starting to fix sector market caps...")
    
    # Connect to database
    conn = get_db_connection()
    
    # Load ticker market caps from CSV
    ticker_market_caps_df = load_ticker_market_caps()
    if ticker_market_caps_df is None:
        print("Failed to load ticker market caps. Exiting.")
        conn.close()
        return
    
    # Load sector information
    sectors_df = load_sector_info(conn)
    if sectors_df is None:
        print("Failed to load sector information. Exiting.")
        conn.close()
        return
    
    # Load ticker to sector mapping
    ticker_sector_mapping_df = load_ticker_sector_mapping(conn)
    if ticker_sector_mapping_df is None:
        print("Failed to load ticker-sector mapping. Exiting.")
        conn.close()
        return
    
    # Calculate sector market caps
    sector_market_caps_df = calculate_sector_market_caps(ticker_market_caps_df, ticker_sector_mapping_df)
    if sector_market_caps_df is None:
        print("Failed to calculate sector market caps. Exiting.")
        conn.close()
        return
    
    # First, insert ticker market caps into database
    # Get ticker IDs for each ticker
    cursor = conn.cursor()
    cursor.execute("SELECT id, symbol FROM tickers")
    ticker_id_map = {row[1]: row[0] for row in cursor.fetchall()}
    
    # Clear existing ticker market caps
    cursor.execute("DELETE FROM ticker_market_caps")
    conn.commit()
    print("Cleared existing ticker market cap data")
    
    # Insert ticker market caps
    ticker_rows = []
    for _, row in ticker_market_caps_df.iterrows():
        ticker = row['ticker']
        if ticker in ticker_id_map:
            ticker_id = ticker_id_map[ticker]
            date = row['date']
            market_cap = row['market_cap']
            ticker_rows.append((ticker_id, date, market_cap))
    
    # Insert in batches
    batch_size = 1000
    for i in range(0, len(ticker_rows), batch_size):
        batch = ticker_rows[i:i+batch_size]
        cursor.executemany(
            "INSERT INTO ticker_market_caps (ticker_id, date, market_cap) VALUES (?, ?, ?)",
            batch
        )
        conn.commit()
    
    print(f"Inserted {len(ticker_rows)} ticker market cap entries into database")
    
    # Update sector market caps in database
    success = update_sector_market_caps(conn, sector_market_caps_df)
    if not success:
        print("Failed to update sector market caps. Exiting.")
        conn.close()
        return
    
    # Print summary
    print_sector_market_cap_summary(sector_market_caps_df)
    
    # Export to Excel
    export_to_excel(sector_market_caps_df)
    
    # Close database connection
    conn.close()
    
    print("\nSector market caps fixed successfully!")

if __name__ == "__main__":
    main()