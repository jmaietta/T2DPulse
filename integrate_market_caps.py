"""
Integrate the new sector_market_cap.py engine with app.py
This script updates app.py to use the new market cap calculation system.
"""

import os
import sys
import datetime as dt
from pathlib import Path
import shutil
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Check if sector_market_cap.py exists
if not os.path.exists('sector_market_cap.py'):
    logging.error("sector_market_cap.py not found. Please create it first.")
    sys.exit(1)

# Create backup of app.py
app_backup = f"app_before_marketcap_integration_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.py"
if os.path.exists('app.py'):
    shutil.copy2('app.py', app_backup)
    logging.info(f"Created backup of app.py at {app_backup}")
else:
    logging.error("app.py not found")
    sys.exit(1)

# Add the import for sector_market_cap
with open('app.py', 'r') as f:
    app_content = f.read()

# Check if the import already exists
if 'from sector_market_cap import' not in app_content:
    # Find the last import statement
    import_lines = [line for line in app_content.split('\n') if line.startswith('import') or line.startswith('from')]
    if import_lines:
        last_import = import_lines[-1]
        last_import_pos = app_content.find(last_import) + len(last_import)
        
        # Add our new import after the last import
        new_imports = "\n\n# Import new market cap calculation engine\nfrom sector_market_cap import get_latest_sector_caps, format_sector_caps_for_display"
        app_content = app_content[:last_import_pos] + new_imports + app_content[last_import_pos:]
        
        logging.info("Added sector_market_cap imports to app.py")
    else:
        logging.warning("Could not find import statements in app.py")

# Replace market cap download functions with new implementation
def replace_download_sector_marketcap_function():
    """Replace the download_sector_marketcap function with a new implementation"""
    old_func_signature = "def download_sector_marketcap():"
    if old_func_signature in app_content:
        func_start = app_content.find(old_func_signature)
        func_end = app_content.find('\n\n', func_start)
        
        new_function = """def download_sector_marketcap():
    \"\"\"Create a direct download route for the sector market cap data\"\"\"
    # Ensure we have the latest data
    if os.path.exists('sector_market_caps.csv'):
        df = format_sector_caps_for_display()
        if not df.empty:
            # Save to a downloadable CSV
            output_path = os.path.join('data', 'sector_marketcap_export.csv')
            os.makedirs('data', exist_ok=True)
            df.to_csv(output_path, index=False)
            return flask.send_file(output_path, as_attachment=True, download_name="sector_market_caps.csv")
    
    # Fallback to a blank CSV if no data is available
    return "No market cap data available", 404"""
        
        return app_content[:func_start] + new_function + app_content[func_end:]
    
    return app_content

# Create function to update the sector market cap display
def add_sector_marketcap_display_function():
    """Add or update the function to display sector market caps in the dashboard"""
    new_function = """
def update_sector_market_cap_display(n):
    \"\"\"Update the sector market cap display with the latest data\"\"\"
    df = format_sector_caps_for_display()
    if df.empty:
        return dash.no_update
    
    # Create a styled table for the dashboard
    table = html.Div([
        html.H4("Sector Market Caps", className="card-title"),
        html.Table([
            html.Thead(
                html.Tr([
                    html.Th("Sector"),
                    html.Th("Market Cap (Billions USD)")
                ])
            ),
            html.Tbody([
                html.Tr([
                    html.Td(row["Sector"]),
                    html.Td(f"${row['Market Cap (Billions USD)']:.2f}B")
                ]) for _, row in df.iterrows()
            ])
        ], className="table table-striped")
    ])
    
    return table
"""
    
    # Check if the function already exists
    if "def update_sector_market_cap_display" in app_content:
        return app_content
    
    # Find a good insertion point - before the first callback
    insertion_point = app_content.find("@app.callback")
    if insertion_point == -1:
        insertion_point = app_content.find("@server.route")
    
    if insertion_point == -1:
        logging.warning("Could not find insertion point for market cap display function")
        return app_content
    
    # Insert the new function
    return app_content[:insertion_point] + new_function + app_content[insertion_point:]

# Apply the changes
app_content = replace_download_sector_marketcap_function()
app_content = add_sector_marketcap_display_function()

# Write the updated app.py
with open('app.py', 'w') as f:
    f.write(app_content)

logging.info("Updated app.py to use the new market cap calculation engine")

# Now run the sector_market_cap.py once to generate initial data
logging.info("Running sector_market_cap.py to generate initial data...")
try:
    os.system("python sector_market_cap.py")
    logging.info("Successfully generated initial market cap data")
except Exception as e:
    logging.error(f"Error running sector_market_cap.py: {e}")

print("\n")
print("=" * 80)
print("MARKET CAP INTEGRATION COMPLETE")
print("=" * 80)
print(f"✓ Created backup of app.py at {app_backup}")
print("✓ Updated app.py to use the new market cap calculation engine")
print("✓ Added integration functions for downloading and displaying market caps")
print("\nNext steps:")
print("1. Restart the Economic Dashboard Server to apply changes")
print("2. Configure a daily schedule to run sector_market_cap.py at market close")
print("   (e.g., 16:00 ET / 4:00pm Eastern Time)")
print("=" * 80)