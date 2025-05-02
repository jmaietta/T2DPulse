"""Utility script to update the Google Sheet URL for sector data.

This script sets the SECTOR_GOOGLE_SHEET_URL environment variable
which is used by the T2D Pulse dashboard to fetch sector data.
"""

import os
import sys
import json

def set_google_sheet_url(url):
    """Set the Google Sheet URL environment variable.
    
    Args:
        url (str): The published Google Sheet URL
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Save the URL to a JSON file
        config_path = os.path.join('data', 'config.json')
        os.makedirs('data', exist_ok=True)
        
        # Read existing config if it exists
        config = {}
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                try:
                    config = json.load(f)
                except json.JSONDecodeError:
                    config = {}
        
        # Update the URL
        config['SECTOR_GOOGLE_SHEET_URL'] = url
        
        # Write back to file
        with open(config_path, 'w') as f:
            json.dump(config, f, indent=2)
            
        # Set in current environment for immediate use
        os.environ['SECTOR_GOOGLE_SHEET_URL'] = url
        
        print(f"Google Sheet URL updated successfully: {url}")
        return True
    except Exception as e:
        print(f"Error updating Google Sheet URL: {str(e)}")
        return False

def get_google_sheet_url():
    """Get the current Google Sheet URL.
    
    Returns:
        str: The current URL or empty string if not set
    """
    # Check environment first
    url = os.environ.get('SECTOR_GOOGLE_SHEET_URL', '')
    if url:
        return url
    
    # Check config file if not in environment
    config_path = os.path.join('data', 'config.json')
    if os.path.exists(config_path):
        with open(config_path, 'r') as f:
            try:
                config = json.load(f)
                return config.get('SECTOR_GOOGLE_SHEET_URL', '')
            except:
                pass
    
    return ''

# Script mode: Allow setting URL from command line
if __name__ == '__main__':
    if len(sys.argv) > 1:
        url = sys.argv[1]
        if url.startswith('https://docs.google.com/'):
            set_google_sheet_url(url)
        else:
            print("Error: URL must start with 'https://docs.google.com/'")
            print("Usage: python update_google_sheet_url.py <google_sheet_url>")
    else:
        current_url = get_google_sheet_url()
        if current_url:
            print(f"Current Google Sheet URL: {current_url}")
        else:
            print("No Google Sheet URL is currently set.")
            print("Usage: python update_google_sheet_url.py <google_sheet_url>")
