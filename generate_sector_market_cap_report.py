#!/usr/bin/env python3
"""
Generate a comprehensive 30-day sector market cap history report
"""

import pandas as pd
import os
from datetime import datetime

def format_marketcap(value):
    """Format market cap value in trillions or billions as appropriate"""
    if value >= 1e12:
        return f"${value/1e12:.2f}T"
    else:
        return f"${value/1e9:.2f}B"

def main():
    # Load sector market cap data
    df = pd.read_csv('sector_market_caps.csv')
    
    # Create pivot table with date on rows and sectors on columns
    # This gives us exactly what the user wants: date on left, sectors at top, values in middle
    pivot_df = df.pivot(index='date', columns='sector', values='market_cap')
    
    # Format pivot table for display
    formatted_pivot = pivot_df.copy()
    for col in formatted_pivot.columns:
        formatted_pivot[col] = formatted_pivot[col].apply(lambda x: format_marketcap(x) if pd.notnull(x) else 'N/A')
    
    # Save to Excel (dates as rows, sectors as columns)
    formatted_pivot.to_excel('30day_sector_marketcap_analysis.xlsx')
    print(f"Saved Excel report to 30day_sector_marketcap_analysis.xlsx")
    
    # Print the table with date on left, sectors on top
    # First get list of sectors and dates
    sectors = sorted(df['sector'].unique())
    dates = sorted(df['date'].unique())
    
    # Print header row with sector names
    print(f"{'DATE':<12}", end="")
    for sector in sectors:
        # Abbreviate sector names to fit more columns
        abbrev = sector[:12]
        print(f"{abbrev:>12}", end="")
    print()
    
    # Print a separator line
    print("-" * 12, end="")
    for _ in sectors:
        print("-" * 12, end="")
    print()
    
    # Print each date with its sector values
    for date in dates:
        print(f"{date:<12}", end="")
        date_data = df[df['date'] == date]
        for sector in sectors:
            sector_data = date_data[date_data['sector'] == sector]
            if len(sector_data) > 0:
                value = sector_data['market_cap'].values[0]
                if value >= 1e12:
                    print(f"${value/1e12:>11.2f}T", end="")
                else:
                    print(f"${value/1e9:>11.2f}B", end="")
            else:
                print(f"{'N/A':>12}", end="")
        print()
    
    # Also save as plain text format
    with open('30day_sector_marketcap_table.txt', 'w') as f:
        # Write header
        f.write(f"{'DATE':<12}")
        for sector in sectors:
            abbrev = sector[:12]
            f.write(f"{abbrev:>12}")
        f.write("\n")
        
        # Write separator
        f.write("-" * 12)
        for _ in sectors:
            f.write("-" * 12)
        f.write("\n")
        
        # Write data rows
        for date in dates:
            f.write(f"{date:<12}")
            date_data = df[df['date'] == date]
            for sector in sectors:
                sector_data = date_data[date_data['sector'] == sector]
                if len(sector_data) > 0:
                    value = sector_data['market_cap'].values[0]
                    if value >= 1e12:
                        f.write(f"${value/1e12:>11.2f}T")
                    else:
                        f.write(f"${value/1e9:>11.2f}B")
                else:
                    f.write(f"{'N/A':>12}")
            f.write("\n")
    
    print(f"Saved text report to 30day_sector_marketcap_table.txt")
    
    # Print AI Infrastructure data specifically to verify correctness
    print("\nAI INFRASTRUCTURE MARKET CAP HISTORY:\n")
    ai_infra = df[df['sector'] == 'AI Infrastructure'].sort_values('date')
    for _, row in ai_infra.iterrows():
        print(f"{row['date']}: {format_marketcap(row['market_cap'])}")

if __name__ == "__main__":
    main()