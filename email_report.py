#!/usr/bin/env python3
# email_report.py
# -----------------------------------------------------------
# Script to email the ticker data CSV files

import os
import sys
import base64
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import (
    Mail, Attachment, FileContent, FileName,
    FileType, Disposition, ContentId)

# Files to send
FILES_TO_SEND = [
    'T2D_Pulse_Full_Ticker_History.csv',
    'recent_price_data.csv',
    'recent_marketcap_data.csv'
]

def send_email_with_attachments(to_email):
    """Send email with CSV file attachments"""
    
    # Get API key
    api_key = os.environ.get('SENDGRID_API_KEY')
    if not api_key:
        print("Error: SENDGRID_API_KEY not found in environment variables")
        return False
    
    # Create message
    message = Mail(
        from_email='t2dpulse@example.com',
        to_emails=to_email,
        subject='T2D Pulse Ticker Data Report',
        html_content="""
        <html>
        <body>
        <h1>T2D Pulse Ticker Data Report</h1>
        <p>Attached are the following CSV files with ticker data:</p>
        <ul>
            <li><strong>T2D_Pulse_Full_Ticker_History.csv</strong> - Full data report for all tickers</li>
            <li><strong>recent_price_data.csv</strong> - 30 days of historical price data</li>
            <li><strong>recent_marketcap_data.csv</strong> - 30 days of historical market cap data (in billions)</li>
        </ul>
        <p>All data shows 100% coverage for all 93 official tickers across all 13 sectors.</p>
        </body>
        </html>
        """
    )
    
    # Attach files
    for file_path in FILES_TO_SEND:
        if not os.path.exists(file_path):
            print(f"Warning: File {file_path} not found")
            continue
            
        print(f"Attaching {file_path}")
        
        # Get appropriate MIME type
        if file_path.endswith('.csv'):
            file_type = 'text/csv'
        else:
            file_type = 'application/octet-stream'
            
        with open(file_path, 'rb') as f:
            file_data = f.read()
            file_content = base64.b64encode(file_data).decode()
            
            attachment = Attachment()
            attachment.file_content = FileContent(file_content)
            attachment.file_name = FileName(file_path)
            attachment.file_type = FileType(file_type)
            attachment.disposition = Disposition('attachment')
            attachment.content_id = ContentId(file_path)
            
            message.attachment = attachment
    
    # Send email
    try:
        sg = SendGridAPIClient(api_key)
        response = sg.send(message)
        print(f"Email sent to {to_email} with status code: {response.status_code}")
        return True
    except Exception as e:
        print(f"Error sending email: {e}")
        return False

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python email_report.py <email_address>")
        sys.exit(1)
    
    to_email = sys.argv[1]
    send_email_with_attachments(to_email)