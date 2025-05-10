"""
Generate a formatted 30-day market cap table with dates on the left and sectors across the top.
This script creates both HTML and markdown formatted tables.
"""
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import tabulate

# Define directories
DATA_DIR = "data"

def load_sector_market_caps():
    """Load sector market cap data from parquet file"""
    market_cap_file = os.path.join(DATA_DIR, "sector_market_caps.parquet")
    
    if os.path.exists(market_cap_file):
        try:
            sector_caps = pd.read_parquet(market_cap_file)
            print(f"Loaded sector market caps from {market_cap_file}")
            return sector_caps
        except Exception as e:
            print(f"Error loading sector market caps: {e}")
            return None
    else:
        print(f"Sector market cap file not found: {market_cap_file}")
        return None

def create_formatted_tables():
    """Create formatted tables for the past 30 days of sector market caps"""
    # Load sector market caps
    sector_caps = load_sector_market_caps()
    if sector_caps is None:
        return False
    
    # Get all sector columns (excluding weight columns and Total)
    sector_columns = [col for col in sector_caps.columns 
                     if not col.endswith('_weight_pct') and col != 'Total']
    
    # Sort sectors by most recent market cap value (descending)
    latest_date = sector_caps.index.max()
    latest_values = sector_caps.loc[latest_date]
    sorted_sectors = sorted(sector_columns, key=lambda x: latest_values[x], reverse=True)
    
    # Convert to trillions for readability
    sector_caps_trillions = sector_caps[sorted_sectors] / 1_000_000_000_000
    
    # Filter to last 30 days and sort by date (descending)
    end_date = sector_caps.index.max()
    start_date = end_date - timedelta(days=30)
    recent_caps = sector_caps_trillions[(sector_caps_trillions.index >= start_date)]
    recent_caps = recent_caps.sort_index(ascending=False)
    
    # Add currency formatting
    formatted_df = recent_caps.applymap(lambda x: f"${x:.2f}T")
    
    # Create markdown table manually
    md_table = "# Sector Market Caps - 30 Day History\n\n"
    md_table += "| Date | " + " | ".join(sorted_sectors) + " |\n"
    md_table += "|-----|" + "|".join(["---"] * len(sorted_sectors)) + "|\n"
    
    for date in recent_caps.index:
        date_str = date.strftime("%Y-%m-%d")
        values = [formatted_df.loc[date, sector] for sector in sorted_sectors]
        md_table += f"| {date_str} | " + " | ".join(values) + " |\n"
    
    # Save markdown table
    md_file = os.path.join(DATA_DIR, "sector_marketcap_table.md")
    with open(md_file, 'w') as f:
        f.write(md_table)
    
    print(f"Saved markdown table to {md_file}")
    
    # Create HTML table with styling
    html_table = "<h1>Sector Market Caps - 30 Day History (Trillions USD)</h1>\n"
    html_table += "<style>\n"
    html_table += "table { border-collapse: collapse; width: 100%; font-family: Arial, sans-serif; }\n"
    html_table += "th, td { padding: 8px; text-align: right; }\n"
    html_table += "th { background-color: #f2f2f2; position: sticky; top: 0; }\n"
    html_table += "th:first-child, td:first-child { text-align: left; font-weight: bold; position: sticky; left: 0; background-color: #f9f9f9; }\n"
    html_table += "tr:nth-child(even) { background-color: #f9f9f9; }\n"
    html_table += "tr:hover { background-color: #e9f5ff; }\n"
    html_table += "</style>\n"
    
    html_table += formatted_df.to_html()
    
    # Save HTML table
    html_file = os.path.join(DATA_DIR, "sector_marketcap_table.html")
    with open(html_file, 'w') as f:
        f.write(html_table)
    
    print(f"Saved HTML table to {html_file}")
    
    # Print the markdown table for terminal display
    print("\n" + md_table)
    
    # Print a condensed version (every 3rd day) for easier viewing
    print("\nCondensed 30-Day Market Cap Table (every 3rd day):")
    
    # Create condensed table manually
    condensed_dates = list(recent_caps.index)[::3]
    
    print("| Date | " + " | ".join(sorted_sectors) + " |")
    print("|-----|" + "|".join(["---"] * len(sorted_sectors)) + "|")
    
    for date in condensed_dates:
        date_str = date.strftime("%Y-%m-%d")
        values = [formatted_df.loc[date, sector] for sector in sorted_sectors]
        print(f"| {date_str} | " + " | ".join(values) + " |")
    
    return True

if __name__ == "__main__":
    create_formatted_tables()