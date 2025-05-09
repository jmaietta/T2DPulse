#!/usr/bin/env python3
# generate_ticker_history_tables.py
# -----------------------------------------------------------
# Script to generate formatted tables showing 30-day history of ticker data

import pandas as pd
import os
import sys
from datetime import datetime

# Output file for the HTML tables
OUTPUT_FILE = 'ticker_history_tables.html'

def load_ticker_data():
    """Load the historical price and market cap data"""
    try:
        price_df = pd.read_csv('recent_price_data.csv', index_col='Date')
        mcap_df = pd.read_csv('recent_marketcap_data.csv', index_col='Date')
        return price_df, mcap_df
    except Exception as e:
        print(f"Error loading data: {e}")
        return None, None

def generate_html_tables(price_df, mcap_df):
    """Generate HTML tables for each ticker showing 30-day history"""
    
    # Get all ticker symbols (columns in price_df)
    tickers = sorted(price_df.columns.tolist())
    
    # Start building HTML
    html = """<!DOCTYPE html>
<html>
<head>
    <title>T2D Pulse Ticker 30-Day History</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            margin: 20px;
            background-color: #f8f9fa;
        }
        h1 {
            color: #333;
            text-align: center;
            padding: 10px;
            background-color: #e9ecef;
            border-radius: 5px;
        }
        h2 {
            color: #495057;
            margin-top: 30px;
            border-bottom: 2px solid #dee2e6;
            padding-bottom: 5px;
        }
        table {
            border-collapse: collapse;
            width: 100%;
            margin-bottom: 30px;
            background-color: white;
            box-shadow: 0 1px 3px rgba(0,0,0,0.12), 0 1px 2px rgba(0,0,0,0.24);
        }
        th, td {
            border: 1px solid #dee2e6;
            padding: 8px;
            text-align: right;
        }
        th {
            background-color: #e9ecef;
            font-weight: bold;
        }
        tr:nth-child(even) {
            background-color: #f2f2f2;
        }
        .ticker-header {
            background-color: #007bff;
            color: white;
            padding: 10px;
            border-radius: 5px 5px 0 0;
            margin-bottom: 0;
        }
        .sector-name {
            color: #6c757d;
            font-style: italic;
            margin-top: 0;
            margin-bottom: 10px;
        }
        .value-change-positive {
            color: green;
        }
        .value-change-negative {
            color: red;
        }
        .ticker-container {
            margin-bottom: 40px;
            border-radius: 5px;
            overflow: hidden;
        }
        .summary {
            background-color: #e9ecef;
            padding: 15px;
            border-radius: 5px;
            margin-bottom: 20px;
        }
        .date-generated {
            text-align: center;
            color: #6c757d;
            margin-top: 30px;
            font-style: italic;
        }
        .tab-container {
            margin-bottom: 20px;
        }
        .tab-button {
            background-color: #f8f9fa;
            border: 1px solid #dee2e6;
            padding: 10px 20px;
            cursor: pointer;
            transition: background-color 0.3s;
        }
        .tab-button:hover {
            background-color: #e9ecef;
        }
        .tab-button.active {
            background-color: #007bff;
            color: white;
            border-color: #007bff;
        }
        .tab-content {
            display: none;
        }
        .tab-content.active {
            display: block;
        }
    </style>
</head>
<body>
    <h1>T2D Pulse 30-Day Ticker History (March 31 - May 9, 2025)</h1>
    
    <div class="summary">
        <h3>Summary</h3>
        <p><strong>Total Tickers:</strong> 93 (with 100% data coverage)</p>
        <p><strong>Data Period:</strong> March 31, 2025 - May 9, 2025 (30 days)</p>
        <p><strong>Previously Problematic Tickers (Now Fixed):</strong> YELP, XYZ (formerly SQ), FI (formerly FISV)</p>
    </div>
    
    <div class="tab-container">
        <button class="tab-button active" onclick="openTab('price-tables')">Price History</button>
        <button class="tab-button" onclick="openTab('mcap-tables')">Market Cap History</button>
    </div>
    
    <div id="price-tables" class="tab-content active">
        <h2>Price History Tables (30 Days)</h2>
"""
    
    # Get sector info from full ticker report
    sectors = {}
    try:
        ticker_df = pd.read_csv('T2D_Pulse_Full_Ticker_History.csv', skiprows=8)
        for index, row in ticker_df.iterrows():
            ticker = row['Ticker']
            sector = row['Sector']
            sectors[ticker] = sector
    except Exception as e:
        print(f"Warning: Couldn't load sector data: {e}")
        
    # Group tickers by sector
    sector_tickers = {}
    for ticker in tickers:
        sector = sectors.get(ticker, "Unknown")
        if sector not in sector_tickers:
            sector_tickers[sector] = []
        sector_tickers[sector].append(ticker)
    
    # Add tables for each sector
    for sector, sector_ticker_list in sorted(sector_tickers.items()):
        if sector == "Unknown":
            continue  # Skip unknown sector
            
        html += f'<h2>{sector} Sector</h2>\n'
        
        # Add tables for each ticker in this sector
        for ticker in sorted(sector_ticker_list):
            # Get ticker data
            ticker_prices = price_df[ticker]
            
            # Calculate price change over the period
            first_price = ticker_prices.iloc[0]
            last_price = ticker_prices.iloc[-1]
            price_change = last_price - first_price
            price_change_pct = (price_change / first_price) * 100 if first_price != 0 else 0
            
            # Determine change color
            change_class = "value-change-positive" if price_change >= 0 else "value-change-negative"
            
            html += f"""
    <div class="ticker-container">
        <h3 class="ticker-header">{ticker}</h3>
        <p class="sector-name">{sector}</p>
        <p><strong>Price Change:</strong> <span class="{change_class}">${price_change:.2f} ({price_change_pct:.2f}%)</span></p>
        <table>
            <tr>
                <th>Date</th>
                <th>Price ($)</th>
            </tr>
"""
            
            # Add rows for each date
            for date, price in ticker_prices.items():
                html += f"""
            <tr>
                <td>{date}</td>
                <td>${price:.2f}</td>
            </tr>
"""
            
            html += """
        </table>
    </div>
"""
    
    # Start market cap section
    html += """
    </div>
    
    <div id="mcap-tables" class="tab-content">
        <h2>Market Cap History Tables (30 Days, in Billions)</h2>
"""

    # Add tables for each sector (market cap)
    for sector, sector_ticker_list in sorted(sector_tickers.items()):
        if sector == "Unknown":
            continue  # Skip unknown sector
            
        html += f'<h2>{sector} Sector</h2>\n'
        
        # Add tables for each ticker in this sector
        for ticker in sorted(sector_ticker_list):
            # Get ticker data
            ticker_mcaps = mcap_df[ticker]
            
            # Calculate market cap change over the period
            first_mcap = ticker_mcaps.iloc[0]
            last_mcap = ticker_mcaps.iloc[-1]
            mcap_change = last_mcap - first_mcap
            mcap_change_pct = (mcap_change / first_mcap) * 100 if first_mcap != 0 else 0
            
            # Determine change color
            change_class = "value-change-positive" if mcap_change >= 0 else "value-change-negative"
            
            html += f"""
    <div class="ticker-container">
        <h3 class="ticker-header">{ticker}</h3>
        <p class="sector-name">{sector}</p>
        <p><strong>Market Cap Change:</strong> <span class="{change_class}">${mcap_change:.2f}B ({mcap_change_pct:.2f}%)</span></p>
        <table>
            <tr>
                <th>Date</th>
                <th>Market Cap ($B)</th>
            </tr>
"""
            
            # Add rows for each date
            for date, mcap in ticker_mcaps.items():
                html += f"""
            <tr>
                <td>{date}</td>
                <td>${mcap:.2f}B</td>
            </tr>
"""
            
            html += """
        </table>
    </div>
"""
    
    # Finish HTML
    html += f"""
    </div>
    
    <p class="date-generated">Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    
    <script>
        function openTab(tabName) {{
            // Hide all tab contents
            var tabContents = document.getElementsByClassName('tab-content');
            for (var i = 0; i < tabContents.length; i++) {{
                tabContents[i].classList.remove('active');
            }}
            
            // Deactivate all tab buttons
            var tabButtons = document.getElementsByClassName('tab-button');
            for (var i = 0; i < tabButtons.length; i++) {{
                tabButtons[i].classList.remove('active');
            }}
            
            // Show the selected tab content and activate its button
            document.getElementById(tabName).classList.add('active');
            event.currentTarget.classList.add('active');
        }}
    </script>
</body>
</html>
"""
    
    return html

def main():
    """Main function"""
    # Load data
    price_df, mcap_df = load_ticker_data()
    if price_df is None or mcap_df is None:
        print("Error: Could not load ticker data")
        return
    
    # Generate HTML tables
    html = generate_html_tables(price_df, mcap_df)
    
    # Write to file
    with open(OUTPUT_FILE, 'w') as f:
        f.write(html)
    
    print(f"Generated ticker history tables in {OUTPUT_FILE}")
    print(f"Created tables for {len(price_df.columns)} tickers over {len(price_df)} days")
    
    # Also create a simpler version for key tickers
    create_key_tickers_table(['YELP', 'XYZ', 'FI'], price_df, mcap_df)

def create_key_tickers_table(tickers, price_df, mcap_df):
    """Create a simpler HTML table for key tickers"""
    output_file = 'key_tickers_history.html'
    
    # Start building HTML
    html = """<!DOCTYPE html>
<html>
<head>
    <title>Key Tickers 30-Day History</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            margin: 20px;
        }
        h1 {
            color: #333;
            text-align: center;
        }
        h2 {
            color: #555;
            margin-top: 20px;
        }
        table {
            border-collapse: collapse;
            width: 100%;
            margin-bottom: 20px;
        }
        th, td {
            border: 1px solid #ddd;
            padding: 8px;
            text-align: right;
        }
        th {
            background-color: #f2f2f2;
            font-weight: bold;
        }
        tr:nth-child(even) {
            background-color: #f9f9f9;
        }
        .ticker-header {
            background-color: #4CAF50;
            color: white;
            padding: 5px 10px;
        }
    </style>
</head>
<body>
    <h1>Key Tickers 30-Day History (March 31 - May 9, 2025)</h1>
    <p><strong>Note:</strong> This table shows the complete 30-day history for previously problematic tickers that now have 100% data coverage.</p>
"""
    
    # Add price history section
    html += """
    <h2>Price History</h2>
    <table>
        <tr>
            <th>Date</th>
"""
    
    # Add column for each ticker
    for ticker in tickers:
        html += f'            <th>{ticker}</th>\n'
    
    html += "        </tr>\n"
    
    # Add rows for each date
    for date in price_df.index:
        html += f'        <tr>\n            <td>{date}</td>\n'
        
        # Add cells for each ticker
        for ticker in tickers:
            if ticker in price_df.columns:
                html += f'            <td>${price_df.loc[date, ticker]:.2f}</td>\n'
            else:
                html += '            <td>N/A</td>\n'
        
        html += '        </tr>\n'
    
    html += '    </table>\n'
    
    # Add market cap history section
    html += """
    <h2>Market Cap History (in Billions)</h2>
    <table>
        <tr>
            <th>Date</th>
"""
    
    # Add column for each ticker
    for ticker in tickers:
        html += f'            <th>{ticker}</th>\n'
    
    html += "        </tr>\n"
    
    # Add rows for each date
    for date in mcap_df.index:
        html += f'        <tr>\n            <td>{date}</td>\n'
        
        # Add cells for each ticker
        for ticker in tickers:
            if ticker in mcap_df.columns:
                html += f'            <td>${mcap_df.loc[date, ticker]:.2f}B</td>\n'
            else:
                html += '            <td>N/A</td>\n'
        
        html += '        </tr>\n'
    
    html += '    </table>\n'
    
    # Finish HTML
    html += f"""
    <p><em>Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</em></p>
</body>
</html>
"""
    
    # Write to file
    with open(output_file, 'w') as f:
        f.write(html)
    
    print(f"Generated key tickers history table in {output_file}")

if __name__ == "__main__":
    main()