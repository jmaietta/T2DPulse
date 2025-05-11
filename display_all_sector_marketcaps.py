"""
Display historical market cap data for all sectors in the database.

This script retrieves and displays market cap data for all sectors,
organized by date to help visualize sector performance over time.
"""
import sqlite3
import pandas as pd
from tabulate import tabulate
import matplotlib.pyplot as plt
from datetime import datetime

def get_db_connection():
    """Connect to the database."""
    return sqlite3.connect("market_cap_data.db")

def get_all_sector_market_caps(conn):
    """Get all market cap data for all sectors in the database."""
    query = """
    SELECT s.name as sector, smc.date, smc.market_cap
    FROM sector_market_caps smc
    JOIN sectors s ON smc.sector_id = s.id
    ORDER BY smc.date, s.name
    """
    
    return pd.read_sql_query(query, conn)

def format_market_caps(df):
    """Format market cap values for display."""
    # Convert to billions for display
    df['market_cap'] = df['market_cap'] / 1_000_000_000
    
    # Pivot the data for a tabular display
    pivot_df = df.pivot(index='date', columns='sector', values='market_cap')
    
    # Add total row
    pivot_df.loc['TOTAL'] = pivot_df.sum()
    
    # Format the values to 2 decimal places
    formatted_df = pivot_df.round(2)
    
    return formatted_df

def print_tabular_data(df):
    """Print data in a tabular format."""
    print("\nHistorical Market Cap Data for All Sectors (Billions USD)")
    print("=" * 80)
    print(tabulate(df, headers='keys', tablefmt='psql', floatfmt='.2f'))

def plot_sector_market_caps(df, top_n=5):
    """Create a line chart of sector market caps over time."""
    # Remove the TOTAL row
    if 'TOTAL' in df.index:
        df = df.drop('TOTAL')
    
    # Convert date index to datetime
    df.index = pd.to_datetime(df.index)
    
    # Sort dates
    df = df.sort_index()
    
    # Get the top N sectors by last date value
    last_date = df.index[-1]
    top_sectors = df.loc[last_date].sort_values(ascending=False).head(top_n).index.tolist()
    
    # Plot the top sectors
    plt.figure(figsize=(15, 8))
    for sector in top_sectors:
        plt.plot(df.index, df[sector], marker='o', linewidth=2, label=sector)
    
    plt.title(f'Top {top_n} Sectors by Market Cap', fontsize=16)
    plt.xlabel('Date', fontsize=14)
    plt.ylabel('Market Cap (Billions USD)', fontsize=14)
    plt.grid(True, alpha=0.3)
    plt.legend(fontsize=12)
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    # Save the plot
    plt.savefig('top_sectors_market_caps.png')
    print(f"\nSaved chart of top {top_n} sectors to top_sectors_market_caps.png")

def main():
    """Main function to display all sector market cap data."""
    conn = get_db_connection()
    
    # Get all sector market cap data
    df = get_all_sector_market_caps(conn)
    
    if df.empty:
        print("No market cap data found in the database.")
        conn.close()
        return
    
    # Format the data
    formatted_df = format_market_caps(df)
    
    # Print the data
    print_tabular_data(formatted_df)
    
    # Plot the top sectors
    plot_sector_market_caps(formatted_df)
    
    # Export to Excel
    formatted_df.to_excel('sector_market_caps_history.xlsx')
    print("\nExported all sector market cap data to sector_market_caps_history.xlsx")
    
    conn.close()

if __name__ == "__main__":
    main()