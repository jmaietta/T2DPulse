"""
Display the market cap table directly in a simple HTML page so you can see it right away.
"""

import pandas as pd
import os
import sys

def create_html_table():
    # Check if file exists
    if not os.path.exists('data/sector_marketcap_30day_table.csv'):
        print("Error: Market cap table not found. Running population script...")
        # Try to run the population script
        try:
            from populate_sector_marketcap_table import main
            main()
        except Exception as e:
            print(f"Error generating market cap data: {e}")
            return None
    
    # Load the market cap table
    df = pd.read_csv('data/sector_marketcap_30day_table.csv')
    
    # Create HTML
    html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>30-Day Sector Market Cap Table</title>
        <style>
            body {
                font-family: Arial, sans-serif;
                margin: 20px;
                background-color: #f5f5f5;
            }
            h1 {
                color: #2c3e50;
                text-align: center;
            }
            table {
                width: 100%;
                border-collapse: collapse;
                margin-top: 20px;
                background-color: white;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            }
            th, td {
                padding: 10px;
                text-align: right;
                border: 1px solid #ddd;
            }
            th {
                background-color: #2c3e50;
                color: white;
                position: sticky;
                top: 0;
            }
            tr:nth-child(even) {
                background-color: #f9f9f9;
            }
            tr:hover {
                background-color: #f1f1f1;
            }
            .date-col {
                text-align: left;
                font-weight: bold;
            }
            .highlight {
                background-color: rgba(46, 204, 113, 0.1);
            }
        </style>
    </head>
    <body>
        <h1>30-Day Sector Market Cap Table</h1>
        <p style="text-align: center;">Showing authentic market capitalization data for all 14 sectors</p>
        <table>
            <thead>
                <tr>
                    <th>Date</th>
    """
    
    # Add column headers
    for col in df.columns:
        if col != 'Date':
            html += f"<th>{col}</th>\n"
    
    html += """
                </tr>
            </thead>
            <tbody>
    """
    
    # Add rows
    for _, row in df.iterrows():
        html += "<tr>\n"
        
        # Date column
        html += f"<td class='date-col'>{row['Date']}</td>\n"
        
        # Sector columns
        for col in df.columns:
            if col != 'Date':
                html += f"<td>{row[col]}</td>\n"
        
        html += "</tr>\n"
    
    html += """
            </tbody>
        </table>
    </body>
    </html>
    """
    
    # Write HTML to file
    with open('market_cap_table.html', 'w') as f:
        f.write(html)
    
    print("Created market_cap_table.html - please open this file to view the table")
    return 'market_cap_table.html'

if __name__ == "__main__":
    create_html_table()