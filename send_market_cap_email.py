"""
This script sends the market cap data file to a specified email
using the SendGrid API.
"""

import os
import base64
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Attachment, FileContent, FileName, FileType, Disposition, ContentId

# Check if SendGrid API key is available
SENDGRID_API_KEY = os.environ.get('SENDGRID_API_KEY')
if not SENDGRID_API_KEY:
    print("Error: SENDGRID_API_KEY environment variable not found.")
    exit(1)

# Email parameters
FROM_EMAIL = "t2d.pulse.report@example.com"  # This is just a placeholder
TO_EMAIL = "jonathanmaietta@gmail.com"
SUBJECT = "T2D Pulse 30-Day Market Cap Table"
CONTENT = """
Hello,

Attached is the 30-day historical market cap table you requested, showing data for all 14 technology sectors.

The data is available in both text format and Excel format.

Best regards,
T2D Pulse Data Service
"""

# Prepare Email
message = Mail(
    from_email=FROM_EMAIL,
    to_emails=TO_EMAIL,
    subject=SUBJECT,
    plain_text_content=CONTENT
)

# Attach the text file
try:
    with open('30day_sector_marketcap_table.txt', 'rb') as f:
        file_content = f.read()
        
    encoded_file = base64.b64encode(file_content).decode()
    
    attachment = Attachment()
    attachment.file_content = FileContent(encoded_file)
    attachment.file_name = FileName('30day_sector_marketcap_table.txt')
    attachment.file_type = FileType('text/plain')
    attachment.disposition = Disposition('attachment')
    attachment.content_id = ContentId('Text Table')
    
    message.add_attachment(attachment)
    print("Added text file attachment")
except Exception as e:
    print(f"Error attaching text file: {e}")

# Attach the CSV file
try:
    with open('sector_marketcap_table.csv', 'rb') as f:
        file_content = f.read()
        
    encoded_file = base64.b64encode(file_content).decode()
    
    attachment = Attachment()
    attachment.file_content = FileContent(encoded_file)
    attachment.file_name = FileName('sector_marketcap_table.csv')
    attachment.file_type = FileType('text/csv')
    attachment.disposition = Disposition('attachment')
    attachment.content_id = ContentId('CSV Data')
    
    message.add_attachment(attachment)
    print("Added CSV file attachment")
except Exception as e:
    print(f"Error attaching CSV file: {e}")

# Attach the JSON file for complete data
try:
    with open('complete_market_cap_data.json', 'rb') as f:
        file_content = f.read()
        
    encoded_file = base64.b64encode(file_content).decode()
    
    attachment = Attachment()
    attachment.file_content = FileContent(encoded_file)
    attachment.file_name = FileName('complete_market_cap_data.json')
    attachment.file_type = FileType('application/json')
    attachment.disposition = Disposition('attachment')
    attachment.content_id = ContentId('JSON Data')
    
    message.add_attachment(attachment)
    print("Added JSON file attachment")
except Exception as e:
    print(f"Error attaching JSON file: {e}")

# Send the email
try:
    sg = SendGridAPIClient(SENDGRID_API_KEY)
    response = sg.send(message)
    print(f"Email sent with status code {response.status_code}")
    print(f"Response body: {response.body}")
    print(f"Response headers: {response.headers}")
except Exception as e:
    print(f"Error sending email: {e}")