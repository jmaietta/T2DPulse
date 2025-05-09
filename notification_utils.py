#!/usr/bin/env python3
# notification_utils.py
# -----------------------------------------------------------
# Utilities for sending notifications about data issues

import os
import sys
from datetime import datetime
import pytz
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, To, Content

# Ensure we have the SendGrid API key
SENDGRID_API_KEY = os.environ.get('SENDGRID_API_KEY')

def get_eastern_time():
    """Get current time in US Eastern timezone"""
    eastern = pytz.timezone('US/Eastern')
    return datetime.now(eastern)

def send_data_issue_notification(subject, message, to_email='josh@techtwodegrees.com'):
    """
    Send a notification email for data issues
    
    Args:
        subject (str): Email subject line
        message (str): Email body content
        to_email (str): Recipient email address
    
    Returns:
        bool: True if email was sent successfully, False otherwise
    """
    if not SENDGRID_API_KEY:
        print("WARNING: SendGrid API key not set, cannot send notification")
        return False
    
    # Create the email
    email = Mail(
        from_email=Email('t2dpulse@notifications.replit.app'),
        to_emails=To(to_email),
        subject=subject,
        html_content=Content("text/html", message)
    )
    
    try:
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        response = sg.send(email)
        print(f"Notification email sent to {to_email}, status code: {response.status_code}")
        return True
    except Exception as e:
        print(f"ERROR: Failed to send notification email: {e}")
        return False

def send_market_cap_alert(missing_tickers, sector_data):
    """
    Send alert for missing market cap data
    
    Args:
        missing_tickers (dict): Dictionary of tickers with missing data by sector
        sector_data (dict): Dictionary of sector data with available tickers
    
    Returns:
        bool: True if email was sent successfully, False otherwise
    """
    # Format the current time
    eastern_time = get_eastern_time()
    time_str = eastern_time.strftime('%Y-%m-%d %H:%M:%S %Z')
    
    # Create the email content
    subject = f"T2D Pulse Alert: Missing Market Cap Data - {time_str}"
    
    # Build the HTML message
    message = f"""
    <html>
    <body>
        <h2>T2D Pulse Data Alert</h2>
        <p>The following tickers have missing market cap data as of {time_str}:</p>
        <table border="1" cellpadding="5" cellspacing="0">
            <tr>
                <th>Sector</th>
                <th>Missing Tickers</th>
                <th>Available Tickers</th>
                <th>Completion %</th>
            </tr>
    """
    
    # Add info for each sector with missing tickers
    for sector, tickers in missing_tickers.items():
        if sector in sector_data:
            total_tickers = sector_data[sector].get('total_tickers', 0)
            tickers_with_data = sector_data[sector].get('tickers_with_data', 0)
            
            if total_tickers > 0:
                completion_pct = (tickers_with_data / total_tickers) * 100
            else:
                completion_pct = 0
            
            message += f"""
            <tr>
                <td>{sector}</td>
                <td>{', '.join(tickers)}</td>
                <td>{tickers_with_data} / {total_tickers}</td>
                <td>{completion_pct:.1f}%</td>
            </tr>
            """
    
    message += """
        </table>
        <p>Please check API limits and data sources to ensure complete data collection.</p>
        <p>This is an automated notification from the T2D Pulse economic dashboard.</p>
    </body>
    </html>
    """
    
    return send_data_issue_notification(subject, message)

if __name__ == "__main__":
    # Test sending an alert
    test_missing_tickers = {
        "IT Services / Legacy Tech": ["IBM", "ORCL", "DXC"],
        "Cybersecurity": ["CRWD", "PANW"]
    }
    
    test_sector_data = {
        "IT Services / Legacy Tech": {
            "tickers_with_data": 2,
            "total_tickers": 5,
            "market_cap": 1000000000,
            "momentum": 1.5
        },
        "Cybersecurity": {
            "tickers_with_data": 3, 
            "total_tickers": 5,
            "market_cap": 2000000000,
            "momentum": -0.5
        }
    }
    
    success = send_market_cap_alert(test_missing_tickers, test_sector_data)
    print(f"Test email sent: {success}")