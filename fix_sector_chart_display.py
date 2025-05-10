"""
Generate and display sector charts for the T2D Pulse dashboard.

This script generates HTML files with embedded Plotly charts for each sector,
which can be directly included in the dashboard.
"""

import os
import pandas as pd
import plotly.graph_objects as go
from plotly.offline import plot
import json
import numpy as np
from datetime import datetime, timedelta

def normalize_score(score):
    """Convert -1 to +1 score to 0-100 scale"""
    return ((score + 1) / 2) * 100

def get_sector_history():
    """Get sector sentiment history from available sources"""
    try:
        # First try to load the JSON data
        if os.path.exists('data/sector_sentiment_history.json'):
            with open('data/sector_sentiment_history.json', 'r') as f:
                return json.load(f)
        
        # Then try CSV as fallback
        if os.path.exists('data/sector_sentiment_history.csv'):
            df = pd.read_csv('data/sector_sentiment_history.csv')
            
            # Group by date and create a list of sector data for each date
            grouped = df.groupby('date')
            
            history = []
            for date, group in grouped:
                day_data = {"date": date, "sectors": []}
                for _, row in group.iterrows():
                    sector_data = {
                        "sector": row['sector'],
                        "score": row['score'],
                        "normalized_score": normalize_score(row['score']),
                        "stance": row['stance'],
                        "takeaway": row.get('takeaway', ""),
                    }
                    day_data["sectors"].append(sector_data)
                history.append(day_data)
            
            return history
    except Exception as e:
        print(f"Error loading sector history: {e}")
        return []

def create_sector_chart_html(sector_name, history, output_dir='assets/sector_charts'):
    """Create an HTML file with an embedded Plotly chart for a sector"""
    # Create directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Filter data for this sector
    sector_data = []
    for day in history:
        date = day["date"]
        for sector in day["sectors"]:
            if sector["sector"] == sector_name:
                # Convert any normalized_score that might be missing
                if "normalized_score" not in sector:
                    sector["normalized_score"] = normalize_score(sector["score"])
                
                # Add to our dataset
                sector_data.append({
                    "date": date,
                    "score": sector["normalized_score"]  # Use normalized score (0-100)
                })
    
    # Convert to dataframe and ensure dates are sorted
    if not sector_data:
        print(f"No data found for sector: {sector_name}")
        # Create single-point fallback data
        today = datetime.now().strftime('%Y-%m-%d')
        sector_data = [{"date": today, "score": 50}]  # Neutral fallback
    
    df = pd.DataFrame(sector_data)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')
    
    # If only one data point, create a simple dot chart
    if len(df) == 1:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=[df['date'].iloc[0]],
            y=[df['score'].iloc[0]],
            mode='markers',
            marker=dict(size=10, color='#2c3e50'),
            name=sector_name
        ))
        fig.update_layout(
            margin=dict(l=0, r=0, t=0, b=0),
            height=80,
            xaxis=dict(
                showticklabels=False,
                showgrid=False,
                zeroline=False,
            ),
            yaxis=dict(
                showticklabels=False,
                showgrid=False,
                zeroline=False,
                range=[0, 100]
            ),
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            showlegend=False
        )
    else:
        # Create a sparkline chart
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df['date'],
            y=df['score'],
            mode='lines',
            line=dict(width=2, color='#2c3e50'),
            name=sector_name
        ))
        
        # Add the latest value as a point
        fig.add_trace(go.Scatter(
            x=[df['date'].iloc[-1]],
            y=[df['score'].iloc[-1]],
            mode='markers',
            marker=dict(size=8, color='#2c3e50'),
            showlegend=False
        ))
        
        # Format the chart as a clean sparkline
        fig.update_layout(
            margin=dict(l=0, r=0, t=0, b=0),
            height=80,
            xaxis=dict(
                showticklabels=False,
                showgrid=False,
                zeroline=False,
            ),
            yaxis=dict(
                showticklabels=False,
                showgrid=False,
                zeroline=False,
                range=[0, 100]
            ),
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            showlegend=False
        )
    
    # Add horizontal reference lines for bearish/neutral/bullish zones
    fig.add_shape(
        type="line",
        x0=df['date'].min(),
        y0=30,
        x1=df['date'].max(),
        y1=30,
        line=dict(color="#e74c3c", width=1, dash="dot"),
    )
    
    fig.add_shape(
        type="line",
        x0=df['date'].min(),
        y0=60,
        x1=df['date'].max(),
        y1=60,
        line=dict(color="#2ecc71", width=1, dash="dot"),
    )
    
    # Create the HTML file
    html_file = f"{output_dir}/{sector_name.replace(' ', '_').lower()}.html"
    
    # Generate the HTML with the plot
    plot_div = plot(fig, output_type='div', include_plotlyjs='cdn')
    
    # Create a minimal HTML file with just the chart
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>{sector_name} Chart</title>
        <style>
            body, html {{
                margin: 0;
                padding: 0;
                overflow: hidden;
                height: 100%;
            }}
            .chart-container {{
                width: 100%;
                height: 100%;
            }}
        </style>
    </head>
    <body>
        <div class="chart-container">
            {plot_div}
        </div>
    </body>
    </html>
    """
    
    with open(html_file, 'w') as f:
        f.write(html_content)
    
    print(f"Created chart for {sector_name} at {html_file}")
    return html_file

def update_sector_cards_in_app():
    """Update sector cards in app.py to display the charts"""
    try:
        with open('app.py', 'r') as f:
            content = f.read()
        
        # Find the sector chart container section
        sector_chart_container = '                    ], className="sector-chart-container"),'
        
        # Replace with the iframe-based chart
        iframe_chart = '''                    ], className="sector-chart-container"),
                    
                    # Sector chart from generated HTML file
                    html.Div([
                        html.Iframe(
                            src=f"/assets/sector_charts/{sector.replace(' ', '_').lower()}.html",
                            style={
                                'width': '100%',
                                'height': '80px',
                                'border': 'none',
                                'padding': '0',
                                'margin': '5px 0',
                                'overflow': 'hidden',
                            }
                        )
                    ], className="sector-sparkline-container"),'''
        
        # Replace the content
        updated_content = content.replace(sector_chart_container, iframe_chart)
        
        # Write back to the file
        with open('app.py', 'w') as f:
            f.write(updated_content)
        
        print("Updated app.py with iframe-based sector charts")
        return True
    except Exception as e:
        print(f"Error updating app.py: {e}")
        return False

def create_directory_for_assets():
    """Create the sector_charts directory in assets"""
    assets_dir = 'assets'
    sector_charts_dir = os.path.join(assets_dir, 'sector_charts')
    
    # Create assets directory if it doesn't exist
    if not os.path.exists(assets_dir):
        os.makedirs(assets_dir)
        print(f"Created assets directory: {assets_dir}")
    
    # Create sector_charts directory if it doesn't exist
    if not os.path.exists(sector_charts_dir):
        os.makedirs(sector_charts_dir)
        print(f"Created sector charts directory: {sector_charts_dir}")
    
    return sector_charts_dir

def main():
    """Main function to generate all sector charts"""
    # Create the output directory
    output_dir = create_directory_for_assets()
    
    # Get sector history
    history = get_sector_history()
    
    if not history:
        print("No sector history data found")
        return
    
    # Get unique sectors
    all_sectors = set()
    for day in history:
        for sector in day["sectors"]:
            all_sectors.add(sector["sector"])
    
    # Create charts for each sector
    for sector in all_sectors:
        if sector != "T2D Pulse":  # Skip the overall T2D Pulse
            create_sector_chart_html(sector, history, output_dir)
    
    # Update the app.py file to display the charts
    update_sector_cards_in_app()
    
    print(f"Generated {len(all_sectors) - 1} sector charts")

if __name__ == "__main__":
    main()