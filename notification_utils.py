#!/usr/bin/env python3
# notification_utils.py
# -----------------------------------------------------------
# Utilities for sending notifications when data issues are detected

import os
import sys
import pandas as pd
from datetime import datetime
import pytz
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

def get_sendgrid_key():
    """Get SendGrid API key from environment variables"""
    return os.environ.get('SENDGRID_API_KEY')

def send_missing_data_alert(missing_tickers, admin_email="admin@example.com", from_email="t2dpulse@example.com"):
    """
    Send an email alert about missing ticker data
    
    Args:
        missing_tickers (list): List of tickers with missing data
        admin_email (str): Email address to send the alert to
        from_email (str): Email address to send the alert from
    
    Returns:
        bool: True if the email was sent successfully, False otherwise
    """
    if not missing_tickers:
        print("No missing tickers to report")
        return True
    
    # Get SendGrid API key
    sendgrid_key = get_sendgrid_key()
    if not sendgrid_key:
        print("SendGrid API key not found. Cannot send email alert.")
        return False
    
    # Get current date in Eastern time (US market timezone)
    eastern = pytz.timezone('US/Eastern')
    today = datetime.now(eastern).strftime('%Y-%m-%d')
    
    # Prepare email content
    subject = f"T2D Pulse: Missing Ticker Data Alert - {today}"
    
    # Convert the missing tickers list to HTML format
    tickers_html = "<ul>"
    for ticker in missing_tickers:
        tickers_html += f"<li>{ticker}</li>"
    tickers_html += "</ul>"
    
    # Create the HTML content
    html_content = f"""
    <h1>T2D Pulse: Missing Ticker Data Alert</h1>
    <p><strong>Date:</strong> {today}</p>
    <p>The system has detected missing price or market cap data for the following tickers:</p>
    {tickers_html}
    <p>Please take action to ensure 100% ticker coverage as required for accurate sector sentiment calculation.</p>
    <p>This is an automated alert from the T2D Pulse system.</p>
    """
    
    # Create the email message
    message = Mail(
        from_email=from_email,
        to_emails=admin_email,
        subject=subject,
        html_content=html_content
    )
    
    # Send the email
    try:
        sg = SendGridAPIClient(sendgrid_key)
        response = sg.send(message)
        print(f"Email alert sent with status code: {response.status_code}")
        return response.status_code == 202
    except Exception as e:
        print(f"Error sending email alert: {e}")
        return False

def check_data_and_send_alerts(admin_email="admin@example.com", from_email="t2dpulse@example.com"):
    """
    Check for missing ticker data and send an alert if needed
    
    Args:
        admin_email (str): Email address to send the alert to
        from_email (str): Email address to send the alert from
    
    Returns:
        bool: True if there is no missing data or if the alert was sent successfully,
              False if there is missing data and the alert failed to send
    """
    # Import the check_ticker_data function
    try:
        from check_missing_ticker_data_revised import get_all_tickers
    except ImportError:
        print("Error importing check_missing_ticker_data_revised.py")
        return False
    
    # Get all tickers
    all_tickers = get_all_tickers()
    
    # File paths
    price_file = "data/historical_ticker_prices.csv"
    marketcap_file = "data/historical_ticker_marketcap.csv"
    
    # Get the current date in Eastern time (US market timezone)
    eastern = pytz.timezone('US/Eastern')
    today = datetime.now(eastern).strftime('%Y-%m-%d')
    
    # Check if files exist
    if not os.path.exists(price_file) or not os.path.exists(marketcap_file):
        print("Missing data files, need to collect data")
        # Send alert about completely missing data files
        return send_missing_data_alert(all_tickers, admin_email, from_email)
    
    # Load data files
    try:
        price_df = pd.read_csv(price_file, index_col=0)
        marketcap_df = pd.read_csv(marketcap_file, index_col=0)
        
        # Check if today's data exists
        if today not in price_df.index or today not in marketcap_df.index:
            print(f"No data for today ({today}) in one or both files")
            # Send alert about missing today's data
            return send_missing_data_alert(all_tickers, admin_email, from_email)
        
        # Check coverage for each ticker
        missing_tickers = []
        
        for ticker in all_tickers:
            has_price = ticker in price_df.columns and not pd.isna(price_df.loc[today, ticker])
            has_marketcap = ticker in marketcap_df.columns and not pd.isna(marketcap_df.loc[today, ticker])
            
            if not has_price or not has_marketcap:
                missing_tickers.append(ticker)
        
        if missing_tickers:
            print(f"Missing data for {len(missing_tickers)} tickers")
            # Send alert about missing ticker data
            return send_missing_data_alert(missing_tickers, admin_email, from_email)
        
        # If we got here, we have 100% coverage
        print(f"Verified 100% data coverage for all {len(all_tickers)} tickers on {today}")
        return True
        
    except Exception as e:
        print(f"Error checking data coverage: {e}")
        # Send alert about error checking data coverage
        return send_missing_data_alert(all_tickers, admin_email, from_email)

if __name__ == "__main__":
    # Example usage
    admin_email = sys.argv[1] if len(sys.argv) > 1 else "admin@example.com"
    from_email = sys.argv[2] if len(sys.argv) > 2 else "t2dpulse@example.com"
    
    success = check_data_and_send_alerts(admin_email, from_email)
    if success:
        print("Data check successful or alert sent")
        sys.exit(0)
    else:
        print("Data check failed and alert failed to send")
        sys.exit(1)