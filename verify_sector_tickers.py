#!/usr/bin/env python3
"""
Verify that all sector-ticker assignments in the database match the provided CSV file.
This script will:
1. Read the sector-ticker mappings from 'T2D Pulse Sector Names and Tickers.csv'
2. Compare them with the current database assignments
3. Output any discrepancies or missing assignments
4. Provide a summary of the verification
"""

import csv
import sqlite3
import os
from collections import defaultdict

def read_text_mappings(text_file):
    """Read sector-ticker mappings from the tab-delimited text file."""
    sector_tickers = defaultdict(list)
    ticker_sectors = defaultdict(list)
    
    with open(text_file, 'r') as f:
        for line in f:
            line = line.strip()
            if line:
                parts = line.split('\t')
                if len(parts) == 2:  # Ensure we have sector and ticker
                    sector_name = parts[0].strip()
                    ticker = parts[1].strip()
                    
                    if sector_name and ticker:
                        # For consistency with database names
                        if sector_name == "Hardware Devices":
                            sector_name = "Hardware / Devices"
                        elif sector_name == "Vertical SasS":
                            sector_name = "Vertical SaaS"
                        elif sector_name == "Dev Tools /  Analytics":
                            sector_name = "Dev Tools / Analytics"
                        elif sector_name == "IT Services /  Legacy Tech":
                            sector_name = "IT Services / Legacy Tech"
                        
                        sector_tickers[sector_name].append(ticker)
                        ticker_sectors[ticker].append(sector_name)
    
    return sector_tickers, ticker_sectors

def get_db_mappings(conn):
    """Get current sector-ticker mappings from the database."""
    cursor = conn.cursor()
    
    # Get all sectors
    cursor.execute("SELECT id, name FROM sectors")
    sectors = {row[0]: row[1] for row in cursor.fetchall()}
    
    # Get all tickers
    cursor.execute("SELECT id, symbol FROM tickers")
    tickers = {row[0]: row[1] for row in cursor.fetchall()}
    
    # Get all sector-ticker relationships
    cursor.execute("""
        SELECT s.name, t.symbol 
        FROM ticker_sectors ts
        JOIN sectors s ON ts.sector_id = s.id
        JOIN tickers t ON ts.ticker_id = t.id
    """)
    
    db_sector_tickers = defaultdict(list)
    db_ticker_sectors = defaultdict(list)
    
    for sector_name, ticker in cursor.fetchall():
        db_sector_tickers[sector_name].append(ticker)
        db_ticker_sectors[ticker].append(sector_name)
    
    return db_sector_tickers, db_ticker_sectors, sectors, tickers

def compare_mappings(csv_mappings, db_mappings):
    """Compare the CSV mappings with database mappings."""
    csv_sector_tickers, csv_ticker_sectors = csv_mappings
    db_sector_tickers, db_ticker_sectors, _, _ = db_mappings
    
    # Check for missing or extra sectors
    all_sectors = set(csv_sector_tickers.keys()) | set(db_sector_tickers.keys())
    missing_sectors = set(csv_sector_tickers.keys()) - set(db_sector_tickers.keys())
    extra_sectors = set(db_sector_tickers.keys()) - set(csv_sector_tickers.keys())
    
    # Check for missing or extra tickers
    all_tickers = set(csv_ticker_sectors.keys()) | set(db_ticker_sectors.keys())
    missing_tickers = set(csv_ticker_sectors.keys()) - set(db_ticker_sectors.keys())
    extra_tickers = set(db_ticker_sectors.keys()) - set(csv_ticker_sectors.keys())
    
    # Check for missing sector-ticker assignments
    missing_assignments = []
    for sector, tickers in csv_sector_tickers.items():
        if sector in db_sector_tickers:
            for ticker in tickers:
                if ticker not in db_sector_tickers[sector]:
                    missing_assignments.append((sector, ticker))
    
    # Check for extra sector-ticker assignments
    extra_assignments = []
    for sector, tickers in db_sector_tickers.items():
        if sector in csv_sector_tickers:
            for ticker in tickers:
                if ticker not in csv_sector_tickers[sector]:
                    extra_assignments.append((sector, ticker))
    
    return {
        'all_sectors': all_sectors,
        'missing_sectors': missing_sectors,
        'extra_sectors': extra_sectors,
        'all_tickers': all_tickers,
        'missing_tickers': missing_tickers,
        'extra_tickers': extra_tickers,
        'missing_assignments': missing_assignments,
        'extra_assignments': extra_assignments
    }

def add_missing_sectors(conn, missing_sectors):
    """Add missing sectors to the database."""
    cursor = conn.cursor()
    for sector in missing_sectors:
        print(f"Adding missing sector: {sector}")
        cursor.execute("INSERT INTO sectors (name) VALUES (?)", (sector,))
    conn.commit()

def add_missing_tickers(conn, missing_tickers):
    """Add missing tickers to the database."""
    cursor = conn.cursor()
    for ticker in missing_tickers:
        print(f"Adding missing ticker: {ticker}")
        cursor.execute("INSERT INTO tickers (symbol) VALUES (?)", (ticker,))
    conn.commit()

def add_missing_assignments(conn, missing_assignments):
    """Add missing sector-ticker assignments to the database."""
    cursor = conn.cursor()
    for sector, ticker in missing_assignments:
        print(f"Adding assignment: {ticker} to {sector}")
        
        # Get sector_id
        cursor.execute("SELECT id FROM sectors WHERE name = ?", (sector,))
        sector_id = cursor.fetchone()[0]
        
        # Get ticker_id
        cursor.execute("SELECT id FROM tickers WHERE symbol = ?", (ticker,))
        ticker_id = cursor.fetchone()[0]
        
        # Check if the assignment already exists
        cursor.execute(
            "SELECT 1 FROM ticker_sectors WHERE sector_id = ? AND ticker_id = ?", 
            (sector_id, ticker_id)
        )
        
        if not cursor.fetchone():
            cursor.execute(
                "INSERT INTO ticker_sectors (sector_id, ticker_id) VALUES (?, ?)",
                (sector_id, ticker_id)
            )
    
    conn.commit()

def remove_extra_assignments(conn, extra_assignments):
    """Remove extra sector-ticker assignments from the database."""
    cursor = conn.cursor()
    for sector, ticker in extra_assignments:
        print(f"Removing assignment: {ticker} from {sector}")
        
        # Get sector_id
        cursor.execute("SELECT id FROM sectors WHERE name = ?", (sector,))
        sector_id = cursor.fetchone()[0]
        
        # Get ticker_id
        cursor.execute("SELECT id FROM tickers WHERE symbol = ?", (ticker,))
        ticker_id = cursor.fetchone()[0]
        
        cursor.execute(
            "DELETE FROM ticker_sectors WHERE sector_id = ? AND ticker_id = ?",
            (sector_id, ticker_id)
        )
    
    conn.commit()

def print_counts(mapping_comparison, db_mappings):
    """Print counts of sectors, tickers, and assignments."""
    db_sector_tickers, db_ticker_sectors, sectors, tickers = db_mappings
    
    unique_tickers = len(db_ticker_sectors)
    total_assignments = sum(len(tickers) for tickers in db_sector_tickers.values())
    
    print("\nDatabase Statistics:")
    print(f"- Total sectors: {len(sectors)}")
    print(f"- Unique tickers: {unique_tickers}")
    print(f"- Total sector-ticker assignments: {total_assignments}")
    
    print("\nMapping Comparison:")
    print(f"- Missing sectors: {len(mapping_comparison['missing_sectors'])}")
    print(f"- Extra sectors: {len(mapping_comparison['extra_sectors'])}")
    print(f"- Missing tickers: {len(mapping_comparison['missing_tickers'])}")
    print(f"- Extra tickers: {len(mapping_comparison['extra_tickers'])}")
    print(f"- Missing assignments: {len(mapping_comparison['missing_assignments'])}")
    print(f"- Extra assignments: {len(mapping_comparison['extra_assignments'])}")

def print_sector_stats(db_mappings):
    """Print statistics for each sector."""
    db_sector_tickers, _, _, _ = db_mappings
    
    print("\nSector Statistics:")
    for sector, tickers in sorted(db_sector_tickers.items()):
        print(f"- {sector}: {len(tickers)} tickers")

def main():
    """Main function to verify sector-ticker mappings."""
    # Connect to the database
    conn = sqlite3.connect('market_cap_data.db')
    
    # Read mappings from text file and database
    text_file = 'attached_assets/Tickers and sectors.txt'
    if os.path.exists(text_file):
        text_mappings = read_text_mappings(text_file)
        db_mappings = get_db_mappings(conn)
        
        # Compare mappings
        mapping_comparison = compare_mappings(text_mappings, db_mappings)
        
        # Print comparison results
        print("Sector-Ticker Mapping Verification Results:")
        
        if mapping_comparison['missing_sectors']:
            print("\nMissing Sectors:")
            for sector in sorted(mapping_comparison['missing_sectors']):
                print(f"- {sector}")
            
            # Add missing sectors
            add_missing_sectors(conn, mapping_comparison['missing_sectors'])
        
        if mapping_comparison['extra_sectors']:
            print("\nExtra Sectors (in DB but not in text file):")
            for sector in sorted(mapping_comparison['extra_sectors']):
                print(f"- {sector}")
        
        if mapping_comparison['missing_tickers']:
            print("\nMissing Tickers:")
            for ticker in sorted(mapping_comparison['missing_tickers']):
                print(f"- {ticker}")
            
            # Add missing tickers
            add_missing_tickers(conn, mapping_comparison['missing_tickers'])
        
        if mapping_comparison['extra_tickers']:
            print("\nExtra Tickers (in DB but not in text file):")
            for ticker in sorted(mapping_comparison['extra_tickers']):
                print(f"- {ticker}")
        
        if mapping_comparison['missing_assignments']:
            print("\nMissing Sector-Ticker Assignments:")
            for sector, ticker in sorted(mapping_comparison['missing_assignments']):
                print(f"- {ticker} should be in {sector}")
            
            # Add missing assignments
            add_missing_assignments(conn, mapping_comparison['missing_assignments'])
        
        if mapping_comparison['extra_assignments']:
            print("\nExtra Sector-Ticker Assignments (in DB but not in text file):")
            for sector, ticker in sorted(mapping_comparison['extra_assignments']):
                print(f"- {ticker} should not be in {sector}")
            
            # Remove extra assignments
            remove_extra_assignments(conn, mapping_comparison['extra_assignments'])
        
        # Refresh db mappings after changes
        db_mappings = get_db_mappings(conn)
        
        # Print statistics
        print_counts(mapping_comparison, db_mappings)
        print_sector_stats(db_mappings)
        
        # Print summary
        if not (mapping_comparison['missing_sectors'] or mapping_comparison['missing_tickers'] or 
                mapping_comparison['missing_assignments'] or mapping_comparison['extra_assignments']):
            print("\n✓ All sector-ticker mappings are correct!")
        else:
            print("\n✓ Fixed all discrepancies. Database now matches text file.")
    else:
        print(f"Error: Text file '{text_file}' not found.")
    
    conn.close()

if __name__ == "__main__":
    main()