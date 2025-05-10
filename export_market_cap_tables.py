"""
Add export functionality for the market cap tables to the dashboard.
This script creates functions to export the market cap data and adds 
the download functionality to the T2D Pulse dashboard.
"""

import os
import pandas as pd
import shutil
from datetime import datetime

def create_market_cap_export_files():
    """Create market cap table export files in Excel and CSV formats"""
    
    # Check if we have the market cap table file
    if not os.path.exists('data/sector_marketcap_30day_table.csv'):
        print("Error: sector_marketcap_30day_table.csv not found")
        return False
    
    # Ensure export directories exist
    os.makedirs('data', exist_ok=True)
    
    # Read the market cap table
    df = pd.read_csv('data/sector_marketcap_30day_table.csv')
    
    # Create todays date string
    today = datetime.now().strftime('%Y-%m-%d')
    
    # Export files with todays date
    csv_export = f'data/sector_marketcap_export_{today}.csv'
    excel_export = f'data/sector_marketcap_export_{today}.xlsx'
    
    # Export to standard filenames (without date)
    std_csv_export = 'data/sector_marketcap_export.csv'
    std_excel_export = 'data/sector_marketcap_export.xlsx'
    
    # Save exports
    df.to_csv(csv_export, index=False)
    df.to_csv(std_csv_export, index=False)
    df.to_excel(excel_export, index=False)
    df.to_excel(std_excel_export, index=False)
    
    print(f"Created market cap exports:")
    print(f"  - {csv_export}")
    print(f"  - {excel_export}")
    print(f"  - {std_csv_export}")
    print(f"  - {std_excel_export}")
    
    return True

def fix_download_function_in_app():
    """Fix the download_file function in app.py to include market cap exports"""
    
    # Check if app.py exists
    if not os.path.exists('app.py'):
        print("Error: app.py not found")
        return False
    
    # Read the app.py file
    with open('app.py', 'r') as f:
        content = f.read()
    
    # Check if we need to update the download_file function
    if "sector_marketcap_export" not in content:
        # Find the download_file function
        download_function_start = "def download_file(filename):"
        
        # Find the function body with return statement
        download_function_body = """def download_file(filename):
    \"\"\"
    Serve files from the data directory for download
    This is used to provide downloadable Excel exports of sector sentiment history
    \"\"\"
    # Check if the file exists
    if filename is None:
        # Default to sector sentiment history Excel
        filename = "sector_sentiment_history.xlsx"
        
    # Handle special case for market cap export Excel
    if filename == "sector_marketcap_export.xlsx":
        # Ensure export exists and is up to date
        if not os.path.exists("data/sector_marketcap_export.xlsx"):
            # Generate the export if it doesn't exist
            from export_market_cap_tables import create_market_cap_export_files
            create_market_cap_export_files()
            
    # Handle special case for market cap export CSV
    if filename == "sector_marketcap_export.csv":
        # Ensure export exists and is up to date
        if not os.path.exists("data/sector_marketcap_export.csv"):
            # Generate the export if it doesn't exist
            from export_market_cap_tables import create_market_cap_export_files
            create_market_cap_export_files()
            
    # Path to the file
    file_path = os.path.join("data", filename)
    
    # Return the file for download
    return flask.send_file(file_path, as_attachment=True)"""
        
        # Replace the function in the content
        if download_function_start in content:
            # Find the beginning and end of the function
            start_index = content.find(download_function_start)
            end_index = content.find("\n\n", start_index)
            
            # Replace the function
            updated_content = content[:start_index] + download_function_body + content[end_index:]
            
            # Write the updated content back to the file
            with open('app.py', 'w') as f:
                f.write(updated_content)
            
            print("Updated download_file function in app.py")
            return True
        else:
            print("Error: download_file function not found in app.py")
            return False
    else:
        print("Market cap export functionality already added to app.py")
        return True

def create_market_cap_download_buttons():
    """Add market cap download buttons to the dashboard layout"""
    
    # Check if app.py exists
    if not os.path.exists('app.py'):
        print("Error: app.py not found")
        return False
    
    # Read the app.py file
    with open('app.py', 'r') as f:
        content = f.read()
    
    # Check if we need to add the download buttons
    if "Download Market Cap Excel" not in content:
        # Find the download buttons section
        download_section_marker = 'id="sector-download-excel"'
        
        # Define the market cap download buttons to add
        market_cap_buttons = """
                    # Spacer
                    html.Div(style={'height': '10px'}),
                    
                    # Market Cap History Downloads
                    html.Div([
                        html.H5("Market Cap Export", style={'marginBottom': '10px'}),
                        
                        # Excel download
                        html.Div([
                            html.A([
                                html.Button(
                                    "Download Market Cap Excel",
                                    id="marketcap-download-excel",
                                    className="download-button",
                                    style={
                                        'backgroundColor': '#2c3e50',
                                        'color': 'white',
                                        'border': 'none',
                                        'padding': '10px',
                                        'borderRadius': '5px',
                                        'cursor': 'pointer',
                                        'width': '100%',
                                        'textAlign': 'center',
                                        'marginBottom': '10px'
                                    }
                                )
                            ], href="/download/sector_marketcap_export.xlsx")
                        ]),
                        
                        # CSV download
                        html.Div([
                            html.A([
                                html.Button(
                                    "Download Market Cap CSV",
                                    id="marketcap-download-csv",
                                    className="download-button",
                                    style={
                                        'backgroundColor': '#2c3e50',
                                        'color': 'white',
                                        'border': 'none',
                                        'padding': '10px',
                                        'borderRadius': '5px',
                                        'cursor': 'pointer',
                                        'width': '100%',
                                        'textAlign': 'center'
                                    }
                                )
                            ], href="/download/sector_marketcap_export.csv")
                        ])
                    ], style={'marginBottom': '20px'}),"""
        
        # Add the market cap buttons after the sector download buttons
        if download_section_marker in content:
            # Find the end of the download section
            marker_index = content.find(download_section_marker)
            section_end = content.find("])"), marker_index)
            
            # Add our buttons before the end of the section
            insertion_point = content.rfind("]),", marker_index, section_end) + 3
            
            # Create the updated content
            updated_content = content[:insertion_point] + market_cap_buttons + content[insertion_point:]
            
            # Write the updated content back to the file
            with open('app.py', 'w') as f:
                f.write(updated_content)
            
            print("Added market cap download buttons to app.py")
            return True
        else:
            print("Error: Could not find download section in app.py")
            return False
    else:
        print("Market cap download buttons already added to app.py")
        return True

def main():
    """Main function to add market cap export functionality"""
    
    # Create market cap export files
    create_market_cap_export_files()
    
    # Fix download function in app.py
    fix_download_function_in_app()
    
    # Add market cap download buttons
    create_market_cap_download_buttons()
    
    print("\nMarket cap export functionality added successfully!")

if __name__ == "__main__":
    main()