#!/usr/bin/env python3
"""
Display Authentic Market Caps

This script generates a comprehensive CSV and text report of the authentic market cap data
from Polygon API for all sectors, with proper formatting for display and analysis.
"""

import os
import sys
import pandas as pd
import logging
from datetime import datetime
from tabulate import tabulate

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("display_authentic_marketcaps.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

def load_sector_market_caps():
    """Load the most recent market cap data from the sector_market_caps.csv file"""
    try:
        df = pd.read_csv('sector_market_caps.csv')
        
        # Get the latest date
        latest_date = df['date'].max()
        latest_data = df[df['date'] == latest_date]
        
        # Create a dictionary of sector -> market cap
        sector_market_caps = {}
        for _, row in latest_data.iterrows():
            sector_market_caps[row['sector']] = row['market_cap']
        
        return sector_market_caps, latest_date
    except Exception as e:
        logging.error(f"Error loading latest market caps: {e}")
        return None, None

def generate_market_cap_report():
    """Generate a comprehensive market cap report with proper formatting"""
    sector_market_caps, latest_date = load_sector_market_caps()
    if not sector_market_caps or not latest_date:
        logging.error("No market cap data available")
        return False
    
    # Convert market caps to billions for readability
    data = []
    total_market_cap = 0
    for sector, market_cap in sorted(sector_market_caps.items(), key=lambda x: x[1], reverse=True):
        market_cap_billions = market_cap / 1e9
        total_market_cap += market_cap
        data.append({
            'Sector': sector,
            'Market Cap (Billions USD)': market_cap_billions,
            'Market Cap (USD)': market_cap,
            'Percentage of Total': 0  # Will calculate after we have the total
        })
    
    # Calculate percentage of total
    total_market_cap_billions = total_market_cap / 1e9
    for i in range(len(data)):
        data[i]['Percentage of Total'] = (data[i]['Market Cap (USD)'] / total_market_cap) * 100
    
    # Create DataFrame
    df = pd.DataFrame(data)
    
    # Save as CSV
    df.to_csv('authentic_sector_market_caps.csv', index=False)
    
    # Create a formatted table for display
    table_data = []
    for row in data:
        table_data.append([
            row['Sector'],
            f"${row['Market Cap (Billions USD)']:.2f}B",
            f"{row['Percentage of Total']:.2f}%"
        ])
    
    # Add total row
    table_data.append([
        "TOTAL",
        f"${total_market_cap_billions:.2f}B",
        "100.00%"
    ])
    
    # Format as table
    table = tabulate(
        table_data,
        headers=["Sector", "Market Cap", "% of Total"],
        tablefmt="grid"
    )
    
    # Save as text file
    with open('authentic_sector_market_caps.txt', 'w') as f:
        f.write(f"T2D Pulse Authentic Sector Market Caps as of {latest_date}\n")
        f.write("Source: Polygon API (100% Authentic Data)\n")
        f.write("=" * 70 + "\n\n")
        f.write(table)
    
    # Print to console
    print(f"T2D Pulse Authentic Sector Market Caps as of {latest_date}")
    print("Source: Polygon API (100% Authentic Data)")
    print("=" * 70 + "\n")
    print(table)
    
    logging.info(f"Market cap report generated successfully for {latest_date}")
    return True

if __name__ == "__main__":
    logging.info("Generating authentic market cap report...")
    success = generate_market_cap_report()
    if success:
        logging.info("Authentic market cap report generated successfully!")
    else:
        logging.error("Failed to generate authentic market cap report")