#!/usr/bin/env python3
# display_sector_charts.py
"""
A simple standalone script to add HTML chart displays to each sector card in the dashboard.
"""

import os
import sys
import logging
import re

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# App.py file path
APP_PY_PATH = 'app.py'

def read_file(file_path):
    """Read file content"""
    try:
        with open(file_path, 'r') as f:
            return f.read()
    except Exception as e:
        logger.error(f"Error reading file {file_path}: {e}")
        return None

def write_file(file_path, content):
    """Write content to file"""
    try:
        with open(file_path, 'w') as f:
            f.write(content)
        return True
    except Exception as e:
        logger.error(f"Error writing file {file_path}: {e}")
        return False

def add_chart_import():
    """Add necessary imports for chart display"""
    app_content = read_file(APP_PY_PATH)
    if app_content is None:
        return False
    
    # Check if the import is already there
    if "import html.iframe" in app_content.lower():
        logger.info("Import already present")
        return True
    
    # Add the import
    modified_content = app_content.replace(
        "import os",
        "import os\nimport base64\nfrom pathlib import Path"
    )
    
    return write_file(APP_PY_PATH, modified_content)

def add_chart_to_cards():
    """Add chart display to sector cards"""
    app_content = read_file(APP_PY_PATH)
    if app_content is None:
        return False
    
    # Check if charts are already added
    if "sector-chart-container" in app_content:
        logger.info("Charts already added to sector cards")
        return True
    
    # Find the drivers list section
    drivers_pattern = r"(\s+# Drivers list\s+html\.Ul\(\[\s+html\.Li\(driver\) for driver in drivers\s+\], className=\"drivers-list\"\),)"
    
    # Check if the pattern is found
    if not re.search(drivers_pattern, app_content):
        logger.error("Could not find drivers list pattern in app.py")
        return False
    
    # Prepare the chart HTML
    chart_html = """
                    # Sector chart display
                    html.Div([
                        html.Iframe(
                            srcDoc=read_file(f"data/sector_chart_{sector.replace(' ', '_').replace('/', '_')}.html") if os.path.exists(f"data/sector_chart_{sector.replace(' ', '_').replace('/', '_')}.html") else "",
                            style={
                                'width': '100%',
                                'height': '40px',
                                'border': 'none',
                                'padding': '0',
                                'margin': '5px 0',
                                'overflow': 'hidden',
                            }
                        )
                    ], className="sector-chart-container", style={"marginBottom": "10px"}),
"""
    
    # Add a read_file function
    read_file_func = """
def read_file(file_path):
    \"\"\"Read file content\"\"\"
    try:
        with open(file_path, 'r') as f:
            return f.read()
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        return ""
"""
    
    # Add the read_file function if not already present
    if "def read_file(file_path):" not in app_content:
        # Find a good location for the function
        idx = app_content.find("def get_eastern_date():")
        if idx > 0:
            app_content = app_content[:idx] + read_file_func + app_content[idx:]
    
    # Replace the drivers list with drivers list + chart
    modified_content = re.sub(
        drivers_pattern,
        r"\1" + chart_html,
        app_content
    )
    
    # Check if the replacement was done
    if modified_content == app_content:
        logger.error("Failed to insert chart HTML into app.py")
        return False
    
    return write_file(APP_PY_PATH, modified_content)

def simplify_chart_code():
    """Create a simpler version that directly adds iframe tags to each sector card"""
    try:
        # Create data directory if it doesn't exist
        os.makedirs('data', exist_ok=True)
        
        # Save a simple HTML template for each sector
        sectors = [
            "SMB SaaS", "Enterprise SaaS", "Cloud Infrastructure", 
            "AdTech", "Fintech", "Consumer Internet", "eCommerce",
            "Cybersecurity", "Dev Tools / Analytics", "Semiconductors",
            "AI Infrastructure", "Vertical SaaS", "IT Services / Legacy Tech",
            "Hardware / Devices"
        ]
        
        for sector in sectors:
            # Create file name with underscores
            file_name = f"data/sector_chart_{sector.replace(' ', '_').replace('/', '_')}.html"
            
            # Simple SVG sparkline (a basic placeholder)
            html_content = f"""
            <svg width="100%" height="40" xmlns="http://www.w3.org/2000/svg">
                <path d="M0,20 L15,18 L30,22 L45,15 L60,25 L75,10 L90,15 L105,20 L120,18 L135,22 L150,20" 
                    stroke="#3498db" stroke-width="2" fill="none" />
            </svg>
            """
            
            # Write the file
            with open(file_name, 'w') as f:
                f.write(html_content)
            
            logger.info(f"Created chart HTML file for {sector}")
        
        return True
    except Exception as e:
        logger.error(f"Error creating chart HTML files: {e}")
        return False

def main():
    """Main function"""
    logger.info("Starting sector chart display fix")
    
    # Simple approach: just create the HTML files
    if simplify_chart_code():
        logger.info("Successfully created chart HTML files")
    else:
        logger.error("Failed to create chart HTML files")
        return False
    
    # Add import statements if needed
    if add_chart_import():
        logger.info("Successfully added imports")
    else:
        logger.error("Failed to add imports")
        return False
    
    # Add charts to sector cards
    if add_chart_to_cards():
        logger.info("Successfully added charts to sector cards")
    else:
        logger.error("Failed to add charts to sector cards")
        return False
    
    logger.info("Sector chart display fix completed successfully!")
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)