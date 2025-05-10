#!/usr/bin/env python3
# fix_sector_chart_display.py
"""
Fix sector chart display by generating authentic charts from sector sentiment history
"""

import os
import sys
import pandas as pd
import json
import logging
from datetime import datetime
import pytz
import re

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Define file paths
DATA_DIR = 'data'
AUTHENTIC_HISTORY_JSON = os.path.join(DATA_DIR, 'authentic_sector_history.json')
AUTHENTIC_HISTORY_CSV = os.path.join(DATA_DIR, 'authentic_sector_history.csv')
APP_PY_PATH = 'app.py'

def get_eastern_date():
    """Get the current date in US Eastern Time"""
    eastern = pytz.timezone('US/Eastern')
    today = datetime.now(eastern)
    return today.strftime('%Y-%m-%d')

def create_directory_if_needed(directory):
    """Create directory if it doesn't exist"""
    os.makedirs(directory, exist_ok=True)
    logger.info(f"Ensured directory exists: {directory}")

def load_sector_history():
    """Load sector history data from JSON file"""
    if os.path.exists(AUTHENTIC_HISTORY_JSON):
        try:
            with open(AUTHENTIC_HISTORY_JSON, 'r') as f:
                data = json.load(f)
            logger.info(f"Loaded sector history from JSON: {len(data)} records")
            return data
        except Exception as e:
            logger.error(f"Error loading sector history from JSON: {e}")
    
    if os.path.exists(AUTHENTIC_HISTORY_CSV):
        try:
            df = pd.read_csv(AUTHENTIC_HISTORY_CSV)
            data = df.to_dict('records')
            logger.info(f"Loaded sector history from CSV: {len(data)} records")
            return data
        except Exception as e:
            logger.error(f"Error loading sector history from CSV: {e}")
    
    logger.error("No sector history data found")
    return None

def create_sector_sparkline(sector_name, sector_data, dates, min_height=40):
    """Create a simple sparkline for a sector"""
    # Get the most recent data points (up to 30 days)
    if not sector_data or len(sector_data) < 2:
        logger.warning(f"Not enough data for sector {sector_name}, using placeholder")
        return f"""
        <svg width="100%" height="{min_height}" xmlns="http://www.w3.org/2000/svg">
            <text x="50%" y="50%" text-anchor="middle" font-size="10" fill="#999">No data</text>
        </svg>
        """
    
    # Convert from -1/+1 scale to 0-100 scale for display
    normalized_data = [(d + 1) * 50 for d in sector_data]
    
    # Calculate min and max for scaling
    min_value = min(normalized_data)
    max_value = max(normalized_data)
    value_range = max(max_value - min_value, 10)  # Ensure some vertical range
    
    # Normalize to SVG coordinates
    height = max(min_height, 40)  # Minimum height of 40px
    width = 150  # Fixed width for consistency
    
    # Calculate points with even spacing on x-axis
    num_points = len(normalized_data)
    x_step = width / (num_points - 1) if num_points > 1 else width
    
    # Create path data
    points = []
    for i, value in enumerate(normalized_data):
        x = i * x_step
        # Map value to SVG y-coordinate (invert because SVG y-axis goes down)
        y = height - ((value - min_value) / value_range * height * 0.8 + height * 0.1)
        points.append(f"{x},{y}")
    
    path_data = " ".join([f"L{p}" for p in points])
    path_data = "M" + path_data[1:]  # Replace first L with M
    
    # Get color based on latest score
    latest_score = normalized_data[-1]
    if latest_score >= 60:
        stroke_color = "#27ae60"  # Green
    elif latest_score >= 40:
        stroke_color = "#f39c12"  # Yellow/Orange
    else:
        stroke_color = "#e74c3c"  # Red
    
    # Create SVG with path
    svg = f"""
    <svg width="100%" height="{height}" xmlns="http://www.w3.org/2000/svg" preserveAspectRatio="none">
        <!-- Background -->
        <rect width="100%" height="100%" fill="transparent" />
        
        <!-- Sparkline -->
        <path d="{path_data}" stroke="{stroke_color}" stroke-width="2" fill="none" vector-effect="non-scaling-stroke" />
        
        <!-- Point for most recent value -->
        <circle cx="{width}" cy="{points[-1].split(',')[1]}" r="3" fill="{stroke_color}" />
    </svg>
    """
    
    return svg

def create_sector_charts():
    """Create sparklines for all sectors"""
    # Create data directory if it doesn't exist
    create_directory_if_needed(DATA_DIR)
    
    # Load sector history data
    sector_data = load_sector_history()
    if not sector_data:
        logger.error("No sector data found, charts cannot be created")
        return False
    
    # Extract the most recent date
    dates = sorted(list({item['date'] for item in sector_data}))
    logger.info(f"Found data for {len(dates)} dates: {dates}")
    
    # Group data by sector
    sector_values = {}
    for item in sector_data:
        for key, value in item.items():
            if key != 'date':
                if key not in sector_values:
                    sector_values[key] = []
                sector_values[key].append(value)
    
    # Create chart for each sector
    for sector, values in sector_values.items():
        # Create file name with underscores
        file_name = f"data/sector_chart_{sector.replace(' ', '_').replace('/', '_')}.html"
        
        # Create sparkline SVG
        svg_content = create_sector_sparkline(sector, values, dates)
        
        # Write to file
        with open(file_name, 'w') as f:
            f.write(svg_content)
        
        logger.info(f"Created chart for sector: {sector}")
    
    return True

def modify_sector_cards_code():
    """Add code to app.py to display the sector charts"""
    # Read app.py
    try:
        with open(APP_PY_PATH, 'r') as f:
            content = f.read()
    except Exception as e:
        logger.error(f"Error reading app.py: {e}")
        return False
    
    # Check if sector charts are already included
    if 'sector-chart-container' in content:
        logger.info("Sector charts are already in app.py")
        return True
    
    # Find the right place to insert the chart code
    pattern = r'(\s+# Tickers with label\s+html\.Div\(\[\s+html\.Div\(\s+html\.Span\("Representative Tickers:", style=\{"fontSize": "13px", "marginBottom": "5px", "display": "block"\}\),\s+style=\{"marginBottom": "3px"\}\s+\),)'
    
    chart_code = """
                    # Sector chart
                    html.Div([
                        html.Iframe(
                            srcDoc=open(f"data/sector_chart_{sector.replace(' ', '_').replace('/', '_')}.html", 'r').read() if os.path.exists(f"data/sector_chart_{sector.replace(' ', '_').replace('/', '_')}.html") else "",
                            style={
                                'width': '100%',
                                'height': '40px',
                                'border': 'none',
                                'padding': '0',
                                'margin': '0 0 10px 0',
                                'overflow': 'hidden',
                            }
                        )
                    ], className="sector-chart-container"),
                    """
    
    # Replace with chart code + original pattern
    if re.search(pattern, content):
        modified_content = re.sub(pattern, chart_code + r"\1", content)
        
        # Write back to app.py
        try:
            with open(APP_PY_PATH, 'w') as f:
                f.write(modified_content)
            logger.info("Successfully added sector charts to app.py")
            return True
        except Exception as e:
            logger.error(f"Error writing to app.py: {e}")
            return False
    else:
        logger.error("Could not find the right pattern to insert chart code")
        
        # Try a simpler approach instead
        simpler_pattern = '# Tickers with label'
        if simpler_pattern in content:
            # Determine the indentation level
            lines = content.split('\n')
            for i, line in enumerate(lines):
                if simpler_pattern in line:
                    indent = line[:line.find('#')]
                    
                    # Insert our chart code before this line with same indentation
                    chart_lines = chart_code.split('\n')
                    indented_chart_code = '\n'.join([indent + line.lstrip() for line in chart_lines if line.strip()])
                    
                    lines.insert(i, indented_chart_code)
                    
                    # Write back to app.py
                    try:
                        with open(APP_PY_PATH, 'w') as f:
                            f.write('\n'.join(lines))
                        logger.info("Successfully added sector charts to app.py using simple approach")
                        return True
                    except Exception as e:
                        logger.error(f"Error writing to app.py: {e}")
                        return False
        
        logger.error("Could not find a way to insert chart code")
        return False

def fix_sector_display():
    """Fix sector display functionality"""
    logger.info("Starting sector display fix")
    
    # Create sector charts
    if create_sector_charts():
        logger.info("Successfully created sector charts")
    else:
        logger.error("Failed to create sector charts")
        return False
    
    # Modify app.py to display the charts
    if modify_sector_cards_code():
        logger.info("Successfully modified app.py to display sector charts")
    else:
        logger.error("Failed to modify app.py")
        return False
    
    logger.info("Sector display fix completed successfully")
    return True

if __name__ == '__main__':
    success = fix_sector_display()
    sys.exit(0 if success else 1)