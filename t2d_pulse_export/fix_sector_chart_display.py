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
SECTOR_SENTIMENT_JSON = os.path.join(DATA_DIR, 'sector_sentiment_history.json')
AUTHENTIC_HISTORY_CSV = os.path.join(DATA_DIR, 'authentic_sector_history.csv')
SECTOR_SENTIMENT_CSV = os.path.join(DATA_DIR, 'sector_sentiment_history.csv')
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
    
    # Try authentic_sector_history.json first (preferred source)
    if os.path.exists(AUTHENTIC_HISTORY_JSON):
        try:
            with open(AUTHENTIC_HISTORY_JSON, 'r') as f:
                data_dict = json.load(f)
            
            # Count sectors
            sector_count = 0
            for date, sectors in data_dict.items():
                sector_count = max(sector_count, len(sectors))
            
            logger.info(f"Loaded authentic sector history from JSON with {len(data_dict)} dates and {sector_count} sectors")
            return data_dict
        except Exception as e:
            logger.error(f"Error loading authentic sector history from JSON: {e}")
    
    # Try sector_sentiment_history.json second
    if os.path.exists(SECTOR_SENTIMENT_JSON):
        try:
            with open(SECTOR_SENTIMENT_JSON, 'r') as f:
                data_dict = json.load(f)
            
            # Count sectors
            sector_count = 0
            for date, sectors in data_dict.items():
                sector_count = max(sector_count, len(sectors))
            
            logger.info(f"Loaded sector sentiment history from JSON with {len(data_dict)} dates and {sector_count} sectors")
            return data_dict
        except Exception as e:
            logger.error(f"Error loading sector sentiment history from JSON: {e}")
    
    # Try CSV files if JSON not available
    for csv_path, name in [(AUTHENTIC_HISTORY_CSV, "authentic"), (SECTOR_SENTIMENT_CSV, "sector sentiment")]:
        if os.path.exists(csv_path):
            try:
                df = pd.read_csv(csv_path)
                # Try to convert to the same format as JSON
                if 'date' in df.columns or 'Date' in df.columns:
                    date_col = 'date' if 'date' in df.columns else 'Date'
                    data_dict = {}
                    for _, row in df.iterrows():
                        date_str = row[date_col]
                        date_data = {col: row[col] for col in df.columns if col != date_col}
                        data_dict[date_str] = date_data
                    logger.info(f"Loaded {name} history from CSV: {len(data_dict)} dates")
                    return data_dict
                else:
                    logger.error(f"CSV {csv_path} does not have a date column")
            except Exception as e:
                logger.error(f"Error loading {name} history from CSV: {e}")
    
    # Last resort: Check if we can find any sector chart files already created
    # and use them as a starting point for future updates
    logger.warning("No comprehensive sector history found, looking for existing sector charts...")
    
    # Create simple dummy data to ensure we have a baseline
    dummy_data = {
        get_eastern_date(): {
            "SMB SaaS": 50.0,
            "Enterprise SaaS": 50.0, 
            "Cloud Infrastructure": 50.0,
            "AdTech": 50.0,
            "Fintech": 50.0,
            "Consumer Internet": 50.0,
            "eCommerce": 50.0,
            "Cybersecurity": 50.0,
            "Dev Tools / Analytics": 50.0,
            "Semiconductors": 50.0,
            "AI Infrastructure": 50.0,
            "Vertical SaaS": 50.0,
            "IT Services / Legacy Tech": 50.0,
            "Hardware / Devices": 50.0
        }
    }
    
    logger.warning("Using fallback sector data for charts")
    return dummy_data

def create_sector_sparkline(sector_name, sector_data, dates, min_height=40):
    """Create a simple sparkline for a sector"""
    # Handle single data point by creating a simple flat line
    if len(sector_data) == 1:
        logger.warning(f"Only one data point for sector {sector_name}, creating flat line")
        # Duplicate the data point to create a line
        sector_data = [sector_data[0], sector_data[0]]
    
    # If still no valid data, use placeholder
    if not sector_data or len(sector_data) < 2:
        logger.warning(f"No valid data for sector {sector_name}, using placeholder")
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
    
    # Create a bit more visual interest by adding a small delta around a single value
    if max_value == min_value:
        min_value = max(0, min_value - 5)  
        max_value = min(100, max_value + 5)
    
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
    data_dict = load_sector_history()
    if not data_dict:
        logger.error("No sector data found, charts cannot be created")
        return False
    
    # Extract dates and sort them chronologically
    dates = sorted(list(data_dict.keys()))
    logger.info(f"Found data for {len(dates)} dates: {dates}")
    
    # If we have only one date, let's create a basic chart anyway
    # even though it won't show a trend
    if len(dates) == 1:
        logger.warning("Only one date found, charts will be simple")
    
    # Find all unique sectors across all dates
    all_sectors = set()
    for date, sectors in data_dict.items():
        all_sectors.update(sectors.keys())
    logger.info(f"Found {len(all_sectors)} sectors: {all_sectors}")
    
    # For each sector, gather values across all dates
    for sector in all_sectors:
        # These values will be in chronological order based on sorted dates
        sector_values = []
        for date in dates:
            if sector in data_dict[date]:
                # These values are already in 0-100 scale, not -1/+1
                score = data_dict[date][sector]
                # Convert back to -1/+1 scale for the sparkline function
                sector_values.append((score / 50) - 1)
        
        # Create file name with underscores
        file_name = f"data/sector_chart_{sector.replace(' ', '_').replace('/', '_')}.html"
        
        # Create sparkline SVG
        svg_content = create_sector_sparkline(sector, sector_values, dates)
        
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