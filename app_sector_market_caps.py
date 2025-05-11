"""
Integrate sector_market_cap.py with app.py

This module serves as a bridge between the new sector market cap calculation
engine and the dashboard application. It provides functions for:

1. Loading the most recent sector market caps
2. Formatting them for display
3. Creating download endpoints for the data
"""

import os
import pandas as pd
from pathlib import Path
import flask

# Define constants
SECTOR_CAPS_CSV = Path("sector_market_caps.csv")
SECTOR_CAPS_CHART = Path("sector_caps_chart.html")

def get_latest_sector_caps():
    """
    Get the latest sector market caps from the CSV file
    
    Returns:
        DataFrame: DataFrame containing the latest sector market caps
    """
    if not SECTOR_CAPS_CSV.exists():
        print(f"Warning: {SECTOR_CAPS_CSV} not found")
        return pd.DataFrame()
    
    try:
        # Load the CSV file
        df = pd.read_csv(SECTOR_CAPS_CSV)
        if df.empty:
            return pd.DataFrame()
        
        # Get the latest date
        latest_date = df['date'].max()
        
        # Filter to just that date's data
        latest_df = df[df['date'] == latest_date]
        
        return latest_df
    except Exception as e:
        print(f"Error loading sector market caps: {e}")
        return pd.DataFrame()

def format_sector_caps_for_display(billions=True):
    """
    Format the sector market caps for display in the dashboard
    
    Args:
        billions (bool): Whether to convert to billions (True) or trillions (False)
        
    Returns:
        DataFrame: Formatted DataFrame for display
    """
    latest_df = get_latest_sector_caps()
    if latest_df.empty:
        return pd.DataFrame()
    
    # Create a new DataFrame with just sector and market cap
    display_df = pd.DataFrame({
        'Sector': latest_df['sector'],
        'Market Cap': latest_df['market_cap']
    })
    
    # Convert to billions or trillions
    if billions:
        display_df['Market Cap (Billions USD)'] = display_df['Market Cap'] / 1_000_000_000
        display_df = display_df.drop('Market Cap', axis=1)
    else:
        display_df['Market Cap (Trillions USD)'] = display_df['Market Cap'] / 1_000_000_000_000
        display_df = display_df.drop('Market Cap', axis=1)
    
    # Sort by market cap (descending)
    return display_df.sort_values('Market Cap (Billions USD)' if billions else 'Market Cap (Trillions USD)', 
                                ascending=False).reset_index(drop=True)

def download_sector_marketcap(app):
    """
    Create a download endpoint for the sector market caps
    
    Args:
        app: Flask app instance
        
    Returns:
        Function: Route function for downloading the sector market caps
    """
    @app.route('/download/sector_market_caps.csv')
    def download_sector_market_caps_csv():
        # Get the formatted data
        df = format_sector_caps_for_display()
        if df.empty:
            return "No sector market cap data available", 404
        
        # Save to a temporary file
        temp_file = "temp_sector_market_caps.csv"
        df.to_csv(temp_file, index=False)
        
        # Send the file
        return flask.send_file(temp_file, as_attachment=True, download_name="sector_market_caps.csv")
    
    return download_sector_market_caps_csv

def download_sector_marketcap_excel(app):
    """
    Create a download endpoint for the sector market caps in Excel format
    
    Args:
        app: Flask app instance
        
    Returns:
        Function: Route function for downloading the sector market caps in Excel
    """
    @app.route('/download/sector_market_caps.xlsx')
    def download_sector_market_caps_excel():
        # Get the formatted data
        df = format_sector_caps_for_display()
        if df.empty:
            return "No sector market cap data available", 404
        
        # Save to a temporary Excel file
        temp_file = "temp_sector_market_caps.xlsx"
        df.to_excel(temp_file, index=False)
        
        # Send the file
        return flask.send_file(temp_file, as_attachment=True, download_name="sector_market_caps.xlsx")
    
    return download_sector_market_caps_excel

def get_sector_market_cap_chart():
    """
    Get the HTML content of the sector market cap chart
    
    Returns:
        str: HTML content of the chart, or empty string if not available
    """
    if not SECTOR_CAPS_CHART.exists():
        return ""
    
    try:
        with open(SECTOR_CAPS_CHART, 'r') as f:
            return f.read()
    except Exception as e:
        print(f"Error loading sector market cap chart: {e}")
        return ""

def initialize_app(app):
    """
    Initialize the Flask app with sector market cap routes
    
    Args:
        app: Flask app instance
    """
    # Register download endpoints
    download_sector_marketcap(app)
    download_sector_marketcap_excel(app)
    
    print("Initialized sector market cap integration with app.py")

if __name__ == "__main__":
    # Test the functions
    latest_caps = get_latest_sector_caps()
    if not latest_caps.empty:
        print(f"Latest sector market caps from {latest_caps['date'].iloc[0]}:")
        formatted = format_sector_caps_for_display()
        print(formatted)
    else:
        print("No sector market cap data available")