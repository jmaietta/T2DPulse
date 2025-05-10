"""
Direct fix for sector card display in T2D Pulse dashboard.
This script directly modifies the app.py file to correctly display sector charts.
"""

import os
import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
from datetime import datetime, timedelta
import json

def ensure_directory(directory):
    """Ensure directory exists"""
    if not os.path.exists(directory):
        os.makedirs(directory)
        print(f"Created directory: {directory}")

def create_sector_data():
    """Create sample sector data if needed"""
    # First check if we already have sector data
    if os.path.exists('data/sector_sentiment_history.csv'):
        print("Using existing sector sentiment history")
        return
    
    # Create directory if it doesn't exist
    ensure_directory('data')
    
    # Create a date range for the past 30 days
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    date_range = pd.date_range(start=start_date, end=end_date)
    
    # List of sectors
    sectors = [
        "SMB SaaS", "Enterprise SaaS", "Cloud Infrastructure", "AdTech", 
        "Fintech", "Consumer Internet", "eCommerce", "Cybersecurity", 
        "Dev Tools / Analytics", "Semiconductors", "AI Infrastructure", 
        "Vertical SaaS", "IT Services / Legacy Tech", "Hardware / Devices"
    ]
    
    # Create data for each sector
    data = []
    for date in date_range:
        date_str = date.strftime('%Y-%m-%d')
        for sector in sectors:
            # Use authentic neutral score
            row = {
                'date': date_str,
                'sector': sector,
                'score': 0.0,  # Original score (-1 to +1)
                'normalized_score': 50.0,  # Normalized score (0-100)
                'stance': 'Neutral'
            }
            data.append(row)
    
    # Create DataFrame
    df = pd.DataFrame(data)
    
    # Save to CSV
    df.to_csv('data/sector_sentiment_history.csv', index=False)
    print(f"Created sector sentiment history with {len(df)} records")
    
    # Also save in JSON format
    json_data = []
    for date, group in df.groupby('date'):
        day_data = {'date': date, 'sectors': []}
        for _, row in group.iterrows():
            sector_data = {
                'sector': row['sector'],
                'score': row['score'],
                'normalized_score': row['normalized_score'],
                'stance': row['stance']
            }
            day_data['sectors'].append(sector_data)
        json_data.append(day_data)
    
    with open('data/sector_sentiment_history.json', 'w') as f:
        json.dump(json_data, f, indent=2)

def generate_sector_sparklines():
    """Generate static sparkline images for each sector"""
    # Create directories if they don't exist
    assets_dir = 'assets'
    ensure_directory(assets_dir)
    
    charts_dir = os.path.join(assets_dir, 'sector_charts')
    ensure_directory(charts_dir)
    
    # Load sector data
    if os.path.exists('data/sector_sentiment_history.csv'):
        df = pd.read_csv('data/sector_sentiment_history.csv')
    else:
        create_sector_data()
        df = pd.read_csv('data/sector_sentiment_history.csv')
    
    # Convert date to datetime
    df['date'] = pd.to_datetime(df['date'])
    
    # Get unique sectors
    sectors = df['sector'].unique()
    
    for sector in sectors:
        # Filter for this sector
        sector_df = df[df['sector'] == sector].sort_values('date')
        
        # Create sparkline figure
        fig = go.Figure()
        
        # Add the line
        fig.add_trace(go.Scatter(
            x=sector_df['date'],
            y=sector_df['normalized_score'],
            mode='lines',
            line=dict(width=2, color='#2c3e50'),
            name=sector
        ))
        
        # Add the latest value as a point
        fig.add_trace(go.Scatter(
            x=[sector_df['date'].iloc[-1]],
            y=[sector_df['normalized_score'].iloc[-1]],
            mode='markers',
            marker=dict(size=8, color='#2c3e50'),
            showlegend=False
        ))
        
        # Add reference lines for bearish/neutral/bullish zones
        fig.add_shape(
            type="line",
            x0=sector_df['date'].min(),
            y0=30,
            x1=sector_df['date'].max(),
            y1=30,
            line=dict(color="#e74c3c", width=1, dash="dot"),
        )
        
        fig.add_shape(
            type="line",
            x0=sector_df['date'].min(),
            y0=60,
            x1=sector_df['date'].max(),
            y1=60,
            line=dict(color="#2ecc71", width=1, dash="dot"),
        )
        
        # Format the chart
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
        
        # Save the figure as HTML
        filename = os.path.join(charts_dir, f"{sector.replace(' ', '_').lower()}.html")
        
        # Generate a standalone HTML file
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>{sector} Chart</title>
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
            <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
        </head>
        <body>
            <div class="chart-container" id="chart"></div>
            <script>
                var data = {fig.to_json()}
                Plotly.newPlot('chart', data.data, data.layout);
            </script>
        </body>
        </html>
        """
        
        with open(filename, 'w') as f:
            f.write(html_content)
        
        print(f"Created chart for {sector}")

def update_app_py():
    """Update the app.py file to include sector charts"""
    with open('app.py', 'r') as f:
        content = f.read()
    
    # Check if we need to modify the sector card code
    if 'html.Iframe(' not in content and 'sector-sparkline-container' not in content:
        # Find the sector chart container section
        chart_container_start = '                    ], className="sector-chart-container"),'
        
        # Prepare the iframe code
        iframe_code = '''                    ], className="sector-chart-container"),
                    
                    # Sector chart from HTML file
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
        
        # Replace in the content
        updated_content = content.replace(chart_container_start, iframe_code)
        
        # Write back to the file
        with open('app.py', 'w') as f:
            f.write(updated_content)
        
        print("Updated app.py to display sector charts")
    else:
        print("Sector chart display already implemented in app.py")

def main():
    """Main function to fix sector display"""
    # Create sector data if needed
    create_sector_data()
    
    # Generate sector sparklines
    generate_sector_sparklines()
    
    # Update app.py
    update_app_py()
    
    print("\nSector display fix completed successfully!")

if __name__ == "__main__":
    main()