#!/usr/bin/env python3
# fix_sector_chart_display.py
# -----------------------------------------------------------
# Create small historical sparklines for sector cards

import os
import sys
import json
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import logging
from datetime import datetime, timedelta
import pytz

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_eastern_date():
    """Get the current date in US Eastern Time"""
    eastern = pytz.timezone('US/Eastern')
    return datetime.now(eastern)

def create_directory_if_needed(directory):
    """Create directory if it doesn't exist"""
    if not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)
        logger.info(f"Created directory: {directory}")

def load_sector_history():
    """Load sector history data from JSON file"""
    json_file = "data/sector_history.json"
    
    if not os.path.exists(json_file):
        logger.error(f"Error: {json_file} not found")
        return None
    
    with open(json_file, 'r') as f:
        data = json.load(f)
    
    return data

def create_sector_sparkline(sector_name, sector_data, dates, min_height=40):
    """Create a simple sparkline for a sector"""
    try:
        # Create a Plotly figure with transparent background
        fig = go.Figure()
        
        # Add the line trace
        fig.add_trace(go.Scatter(
            x=dates,
            y=sector_data,
            mode='lines',
            line=dict(
                color='rgba(0, 128, 255, 0.8)',  # Semi-transparent blue
                width=2,
            ),
            hoverinfo='none',  # Disable hover tooltip
        ))
        
        # Get min and max values for the y-axis range with a little padding
        y_min = max(0, min(sector_data) - 2)
        y_max = min(100, max(sector_data) + 2)
        
        # Set layout with no axes, grid, or background
        fig.update_layout(
            showlegend=False,
            autosize=True,
            margin=dict(l=0, r=0, t=0, b=0),
            height=min_height,
            plot_bgcolor='rgba(0,0,0,0)',  # Transparent background
            paper_bgcolor='rgba(0,0,0,0)',  # Transparent background
            xaxis=dict(
                showgrid=False,
                showticklabels=False,
                zeroline=False,
                visible=False,
            ),
            yaxis=dict(
                showgrid=False,
                showticklabels=False,
                zeroline=False,
                visible=False,
                range=[y_min, y_max],  # Set consistent y-axis range
            ),
        )
        
        # Save the chart as HTML to include in the sector card
        chart_file = f"data/sector_chart_{sector_name.replace(' ', '_').replace('/', '_')}.html"
        fig.write_html(chart_file, include_plotlyjs='cdn', full_html=False, config={'displayModeBar': False})
        
        logger.info(f"Created sparkline for {sector_name}")
        return chart_file
    except Exception as e:
        logger.error(f"Error creating sparkline for {sector_name}: {e}")
        return None

def create_sector_charts():
    """Create sparklines for all sectors"""
    logger.info("Creating sector sparklines...")
    
    # Create data directory if needed
    create_directory_if_needed('data')
    
    # Load sector history
    history_data = load_sector_history()
    if history_data is None:
        logger.error("Failed to load sector history data")
        return False
    
    dates = history_data.get('dates', [])
    sectors_data = history_data.get('sectors', {})
    
    if not dates or not sectors_data:
        logger.error("Sector history data is empty or invalid")
        return False
    
    # Count how many sparklines we created
    chart_count = 0
    
    # Generate sparklines for each sector
    for sector_name, sector_data in sectors_data.items():
        chart_file = create_sector_sparkline(sector_name, sector_data, dates)
        if chart_file:
            chart_count += 1
    
    logger.info(f"Created {chart_count} sector sparklines")
    return chart_count > 0

def modify_sector_cards_code():
    """Add code to app.py to display the sector charts"""
    try:
        with open('app.py', 'r') as f:
            app_code = f.read()
        
        # Look for the sector card creation section
        if "Create a sector card without an image" not in app_code:
            logger.error("Couldn't find sector card creation code in app.py")
            return False
        
        # Check if our fix is already applied
        if "sector_chart_html = None" in app_code:
            logger.info("Sector chart code already present in app.py")
            return True
        
        # Find the sector card creation code
        sector_card_code = """
        # Create a sector card without an image
        def create_sector_card(sector_data):
            sector = sector_data["sector"]
            score = sector_data["normalized_score"]
            stance = sector_data["stance"]
            drivers = sector_data.get("drivers", [])
            tickers = sector_data.get("tickers", [])
            
            # Determine color based on score
            if score >= 60:
                sentiment_color = "#28A745"  # Green
            elif score <= 30:
                sentiment_color = "#DC3545"  # Red
            else:
                sentiment_color = "#FFC107"  # Yellow
"""
        
        # Create updated code with sparkline chart
        updated_card_code = """
        # Create a sector card without an image
        def create_sector_card(sector_data):
            sector = sector_data["sector"]
            score = sector_data["normalized_score"]
            stance = sector_data["stance"]
            drivers = sector_data.get("drivers", [])
            tickers = sector_data.get("tickers", [])
            
            # Determine color based on score
            if score >= 60:
                sentiment_color = "#28A745"  # Green
            elif score <= 30:
                sentiment_color = "#DC3545"  # Red
            else:
                sentiment_color = "#FFC107"  # Yellow
                
            # Try to load sector chart HTML
            sector_chart_html = None
            chart_file = f"data/sector_chart_{sector.replace(' ', '_').replace('/', '_')}.html"
            if os.path.exists(chart_file):
                try:
                    with open(chart_file, 'r') as f:
                        sector_chart_html = f.read()
                except Exception as e:
                    print(f"Error loading sector chart for {sector}: {e}")
"""
        
        # Replace the code
        updated_app_code = app_code.replace(sector_card_code, updated_card_code)
        
        # Now find where the sector card content is defined
        card_content_code = """
            # Content of the card
            card_content = [
                # Title and score row
                html.Div([
                    # Title
                    html.Div([
                        html.H5(sector, className="sector-title"),
                    ], className="sector-title-column"),
                    
                    # Score
                    html.Div([
                        html.P([
                            html.Span(f"{score}", className="sector-score"),
                        ], className="sector-score-container")
                    ], className="sector-score-column"),
                ], className="sector-header-row"),
"""
        
        # Updated card content with sparkline chart
        updated_content_code = """
            # Content of the card
            card_content = [
                # Title and score row
                html.Div([
                    # Title
                    html.Div([
                        html.H5(sector, className="sector-title"),
                    ], className="sector-title-column"),
                    
                    # Score
                    html.Div([
                        html.P([
                            html.Span(f"{score}", className="sector-score"),
                        ], className="sector-score-container")
                    ], className="sector-score-column"),
                ], className="sector-header-row"),
                
                # Sparkline chart (if available)
                html.Div([
                    html.Div([
                        html.Iframe(
                            srcDoc=sector_chart_html,
                            style={
                                'width': '100%',
                                'height': '40px',
                                'border': 'none',
                                'padding': '0',
                                'margin': '0 0 8px 0',
                            }
                        ) if sector_chart_html else None
                    ], className="sector-chart-container"),
                ], className="sector-chart-row") if sector_chart_html else None,
"""
        
        # Replace the card content code
        updated_app_code = updated_app_code.replace(card_content_code, updated_content_code)
        
        # Write the updated code back to app.py
        with open('app.py', 'w') as f:
            f.write(updated_app_code)
        
        logger.info("Successfully updated app.py to display sector charts")
        return True
    except Exception as e:
        logger.error(f"Error modifying app.py: {e}")
        return False

if __name__ == "__main__":
    # Create sparklines for all sectors
    if create_sector_charts():
        logger.info("Successfully created sector charts")
    else:
        logger.error("Failed to create sector charts")
        sys.exit(1)
    
    # Update app.py to display the charts
    if modify_sector_cards_code():
        logger.info("Successfully updated app.py")
    else:
        logger.error("Failed to update app.py")
        sys.exit(1)
    
    logger.info("Sector chart fix completed successfully!")
    sys.exit(0)