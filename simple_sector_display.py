"""
Simple sector display generator for T2D Pulse
"""
import os
import json
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

def generate_sector_chart(sector_name, save_path=None):
    """Generate a simple line chart for a sector"""
    # Create a simple plot - will be replaced with actual data later
    fig = make_subplots(rows=1, cols=1)
    
    # Add placeholder trace
    fig.add_trace(
        go.Scatter(
            x=[1, 2, 3, 4, 5],
            y=[50, 55, 53, 57, 60],
            mode='lines',
            name=sector_name,
            line=dict(color='#0072B2', width=3)
        ),
        row=1, col=1
    )
    
    # Update layout
    fig.update_layout(
        title=None,
        showlegend=False,
        margin=dict(l=10, r=10, t=10, b=10),
        height=180,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
    )
    
    # Remove axes for clean look
    fig.update_xaxes(visible=False)
    fig.update_yaxes(visible=False)
    
    if save_path:
        fig.write_html(save_path)
    
    return fig

def ensure_sector_chart_files():
    """Ensure sector chart HTML files exist for all sectors"""
    # Define the list of sectors (fallback)
    sectors = [
        "SMB SaaS", "Enterprise SaaS", "Cloud Infrastructure", "AdTech",
        "Fintech", "Consumer Internet", "eCommerce", "Cybersecurity",
        "Dev Tools / Analytics", "Semiconductors", "AI Infrastructure",
        "Vertical SaaS", "IT Services / Legacy Tech", "Hardware / Devices"
    ]
    
    # Create data directory if it doesn't exist
    os.makedirs('data', exist_ok=True)
    
    # Generate chart for each sector
    for sector in sectors:
        sector_id = sector.replace(' ', '_').replace('/', '_')
        file_path = f'data/sector_chart_{sector_id}.html'
        
        # Only generate if the file doesn't exist
        if not os.path.exists(file_path):
            print(f"Generating chart for {sector}")
            generate_sector_chart(sector, file_path)
        else:
            print(f"Chart for {sector} already exists")

def get_sample_sector_data():
    """Return sample sector data structure"""
    sectors = [
        "SMB SaaS", "Enterprise SaaS", "Cloud Infrastructure", "AdTech",
        "Fintech", "Consumer Internet", "eCommerce", "Cybersecurity",
        "Dev Tools / Analytics", "Semiconductors", "AI Infrastructure",
        "Vertical SaaS", "IT Services / Legacy Tech", "Hardware / Devices"
    ]
    
    # Create sample data for each sector
    sector_data = []
    for sector in sectors:
        # Random score between 30 and 70
        import random
        score = round(random.uniform(30, 70), 1)
        
        sector_obj = {
            "sector": sector,
            "score": (score - 50) / 50,  # Convert to -1 to +1 range
            "normalized_score": score,  # Already in 0-100 range
            "stance": "Neutral",
            "tickers": ["AAPL", "MSFT", "GOOG"],
            "drivers": ["GDP Growth", "Interest Rates"]
        }
        sector_data.append(sector_obj)
    
    return sector_data

if __name__ == "__main__":
    ensure_sector_chart_files()
    print("Sector chart files created successfully")