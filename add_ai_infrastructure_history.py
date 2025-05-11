#!/usr/bin/env python3
"""
Add historical data for the AI Infrastructure sector to the database.

The AI Infrastructure sector consists of AMZN, GOOGL, IBM, META, MSFT, NVDA, and ORCL tickers.
This script will calculate the historical sector market cap from these tickers
and add it to the sector_market_caps table for all the dates between April 11 and May 9, 2025.
"""

import sqlite3
import pandas as pd
from datetime import datetime

def get_sector_id(conn, sector_name):
    """Get the sector ID for a given sector name."""
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM sectors WHERE name = ?", (sector_name,))
    result = cursor.fetchone()
    if result:
        return result[0]
    return None

def calculate_ai_infrastructure_market_caps(conn, sector_id):
    """Calculate AI Infrastructure market caps for historical dates."""
    # Get the tickers in the AI Infrastructure sector
    cursor = conn.cursor()
    cursor.execute("""
        SELECT t.id, t.symbol 
        FROM ticker_sectors ts 
        JOIN tickers t ON ts.ticker_id = t.id 
        WHERE ts.sector_id = ?
    """, (sector_id,))
    tickers = cursor.fetchall()
    ticker_ids = [t[0] for t in tickers]
    ticker_symbols = [t[1] for t in tickers]
    
    print(f"AI Infrastructure Tickers: {ticker_symbols}")
    
    # Get historical market cap data for these tickers
    placeholders = ', '.join(['?'] * len(ticker_ids))
    cursor.execute(f"""
        SELECT date, SUM(market_cap) as total_market_cap
        FROM ticker_market_caps
        WHERE ticker_id IN ({placeholders})
        GROUP BY date
        ORDER BY date
    """, ticker_ids)
    
    market_cap_data = []
    for row in cursor.fetchall():
        date, market_cap = row
        market_cap_data.append({
            'date': date,
            'sector_id': sector_id,
            'market_cap': market_cap,
            'sentiment_score': None  # Set to None as we don't have sentiment data
        })
    
    return market_cap_data

def insert_historical_market_caps(conn, market_cap_data):
    """Insert the calculated market caps into the sector_market_caps table."""
    cursor = conn.cursor()
    
    # Check if entries already exist
    existing_dates = []
    for entry in market_cap_data:
        cursor.execute("""
            SELECT id FROM sector_market_caps 
            WHERE sector_id = ? AND date = ?
        """, (entry['sector_id'], entry['date']))
        if cursor.fetchone():
            existing_dates.append(entry['date'])
    
    # Filter out existing entries
    market_cap_data = [entry for entry in market_cap_data if entry['date'] not in existing_dates]
    
    if not market_cap_data:
        print("No new market cap data to add.")
        return 0
    
    # Insert the new entries
    for entry in market_cap_data:
        cursor.execute("""
            INSERT INTO sector_market_caps 
            (sector_id, date, market_cap, sentiment_score) 
            VALUES (?, ?, ?, ?)
        """, (
            entry['sector_id'],
            entry['date'],
            entry['market_cap'],
            entry['sentiment_score']
        ))
    
    conn.commit()
    return len(market_cap_data)

def generate_market_cap_table(conn):
    """Generate a formatted table of sector market caps for all dates."""
    cursor = conn.cursor()
    
    # Get all sectors
    cursor.execute("SELECT id, name FROM sectors ORDER BY name")
    sectors = cursor.fetchall()
    
    # Get all dates
    cursor.execute("SELECT DISTINCT date FROM sector_market_caps ORDER BY date")
    dates = [row[0] for row in cursor.fetchall()]
    
    # Create a DataFrame to hold the data
    data = []
    
    # For each date, get all sector market caps
    for date in dates:
        row_data = {'Date': date}
        
        for sector_id, sector_name in sectors:
            cursor.execute("""
                SELECT market_cap/1e12 FROM sector_market_caps 
                WHERE sector_id = ? AND date = ?
            """, (sector_id, date))
            result = cursor.fetchone()
            if result:
                row_data[sector_name] = f"{result[0]:.2f}T"
            else:
                row_data[sector_name] = "N/A"
        
        data.append(row_data)
    
    # Create a DataFrame and display the table
    df = pd.DataFrame(data)
    return df

def main():
    """Main function to add AI Infrastructure sector historical data."""
    conn = sqlite3.connect('market_cap_data.db')
    
    try:
        # Get the sector ID for AI Infrastructure
        sector_id = get_sector_id(conn, 'AI Infrastructure')
        if not sector_id:
            print("Error: AI Infrastructure sector not found in the database.")
            return
        
        print(f"AI Infrastructure sector ID: {sector_id}")
        
        # Calculate market caps for historical dates
        market_cap_data = calculate_ai_infrastructure_market_caps(conn, sector_id)
        
        # Insert the data into the database
        inserted_count = insert_historical_market_caps(conn, market_cap_data)
        print(f"Inserted {inserted_count} historical market cap records for AI Infrastructure.")
        
        # Generate a table of all sector market caps for all dates
        print("\nHistorical Market Cap Data (in Trillions):")
        df = generate_market_cap_table(conn)
        
        # Print AI Infrastructure column first, then sort the rest
        cols = ['Date', 'AI Infrastructure'] + sorted([c for c in df.columns if c not in ['Date', 'AI Infrastructure']])
        df = df[cols]
        
        # Display the table
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', 1000)
        print(df)
        
        # Save the table to a CSV file
        df.to_csv('sector_market_cap_history.csv', index=False)
        print("Saved market cap history to sector_market_cap_history.csv")
        
    finally:
        conn.close()

if __name__ == "__main__":
    main()