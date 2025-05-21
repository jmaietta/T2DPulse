#!/usr/bin/env python3
# sector_trend_chart.py
# -----------------------------------------------------------
# Create mini trend charts for sector sentiment history using authentic data

import os
import plotly.graph_objects as go
import pandas as pd
import pytz
from datetime import datetime, timedelta
import authentic_sector_history

# Color scheme for trend charts
TREND_COLORS = {
    'line': '#2E86C1',           # Main line color (blue)
    'fill': 'rgba(46,134,193,0.2)',  # Fill color (light blue)
    'marker': '#2471A3',         # Point marker color (darker blue)
    'highlight': '#F39C12'       # Highlight color (orange)
}

# Alias function name to match what's expected in app.py
def create_sector_trend_chart(sector_name, height=50, show_axes=False, auto_range=True, show_value=False):
    """Alias for create_mini_trend_chart to match the function name used in app.py"""
    return create_mini_trend_chart(sector_name, height, show_axes, auto_range, show_value)
    
def create_mini_trend_chart(sector_name, height=50, show_axes=False, auto_range=True, show_value=False):
    """
    # Connect to Postgres
    from sqlalchemy import create_engine, text
    import pandas as pd
    import os

    engine = create_engine(os.getenv("DATABASE_URL"))

    # 1) Load the last 30 trading days for this sector
    sql = (
        "SELECT date, sector_sentiment_score "
        "FROM sector_sentiment_history "
        "WHERE sector = :sector "
        "ORDER BY date ASC "
        "LIMIT 30"
    )
    df = pd.read_sql(sql, engine, params={"sector": sector_name})
    df["date"] = pd.to_datetime(df["date"])


    # 2) If no data, return empty figure
    if df.empty:
        fig = go.Figure()
        fig.update_layout(height=height, margin=dict(l=0, r=0, t=0, b=0))
        return fig

    # 3) Build the sparkline
    y_min = df["sector_sentiment_score"].min() - 2
    y_max = df["sector_sentiment_score"].max() + 2

    fig = go.Figure(go.Scatter(
        x=df["date"], y=df["sector_sentiment_score"],
        mode="lines", line=dict(color=TREND_COLORS['line'], width=2),
        hovertemplate="<b>%{x|%b %d}</b><br>Score: %{y:.1f}<extra></extra>"
    ))
    fig.update_layout(
        height=height,
        margin=dict(l=0, r=0, t=0, b=0),
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)'
    )
    fig.update_xaxes(visible=False)
    fig.update_yaxes(range=[y_min, y_max], visible=False)

    return fig

def create_combined_sector_chart(sector_names, title=None, height=400):
    """
    Create a combined chart for multiple sectors using authentic historical data
    
    Args:
        sector_names (list): List of sector names
        title (str, optional): Chart title
        height (int, optional): Height of the chart in pixels
        
    Returns:
        dict: Plotly figure object
    """
    # Get authentic historical data
    sector_history = authentic_sector_history.get_authentic_sector_history()
    
    if not sector_history:
        # Return empty chart if no data available
        fig = go.Figure()
        fig.update_layout(height=height, title=title)
        return fig
    
    # Create figure
    fig = go.Figure()
    
    # Color palette for multiple sectors
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', 
              '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf']
    
    # Add each sector
    for i, sector_name in enumerate(sector_names):
        if sector_name not in sector_history:
            continue
            
        # Get data for this sector
        df = sector_history[sector_name]
        
        # Sort by date
        df = df.sort_index()
        
        # Reset index to handle both formats (where date might be index or column)
        df = df.reset_index()
        
        # Ensure date column is properly named (handle both 'date' and 'index' as date column)
        if 'date' not in df.columns and 'index' in df.columns:
            df = df.rename(columns={'index': 'date'})
        
        # Ensure date is datetime type
        df['date'] = pd.to_datetime(df['date'])
        
        # Filter out weekends
        df['is_weekday'] = df['date'].dt.dayofweek < 5  # Only keep weekdays (0-4)
        df = df[df['is_weekday']]
        
        # Check if we still have data after filtering
        if df.empty:
            continue
            
        # Convert columns to lists for plotting
        dates = df['date'].tolist()
        values = df['value'].tolist()
        
        # Add line or marker (handle single point case)
        if len(df) < 2:
            # For a single data point, add just a marker
            fig.add_trace(go.Scatter(
                x=dates,
                y=values,
                name=sector_name,
                mode='markers',
                marker=dict(color=colors[i % len(colors)], size=8),
                hovertemplate="%{x|%Y-%m-%d}: %{y:.1f}<extra>" + sector_name + "</extra>"
            ))
        else:
            # For multiple points, add a line
            fig.add_trace(go.Scatter(
                x=dates,
                y=values,
                name=sector_name,
                line=dict(color=colors[i % len(colors)], width=2),
                mode='lines+markers',
                marker=dict(size=6),
                hovertemplate="%{x|%Y-%m-%d}: %{y:.1f}<extra>" + sector_name + "</extra>"
            ))
    
    # Set layout
    fig.update_layout(
        height=height,
        title=title,
        margin=dict(l=40, r=40, t=40, b=40),
        plot_bgcolor='rgba(240,240,240,0.8)',
        paper_bgcolor='white',
        hovermode='closest',
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    # Format axes
    fig.update_xaxes(
        title='Date',
        showgrid=True,
        gridcolor='rgba(200,200,200,0.4)'
    )
    
    fig.update_yaxes(
        title='Sentiment Score',
        showgrid=True,
        gridcolor='rgba(200,200,200,0.4)',
        range=[0, 100]
    )
    
    return fig

if __name__ == "__main__":
    # Test the trend chart
    import dash
    import dash_bootstrap_components as dbc
    from dash import html, dcc
    
    app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
    
    # Create layout
    app.layout = dbc.Container([
        html.H1("Sector Trend Charts"),
        
        # Mini trend charts
        html.H2("Mini Trend Charts"),
        dbc.Row([
            dbc.Col([
                html.H4("Enterprise SaaS"),
                dcc.Graph(figure=create_mini_trend_chart("Enterprise SaaS"))
            ], width=4),
            dbc.Col([
                html.H4("AdTech"),
                dcc.Graph(figure=create_mini_trend_chart("AdTech"))
            ], width=4),
            dbc.Col([
                html.H4("Cybersecurity"),
                dcc.Graph(figure=create_mini_trend_chart("Cybersecurity"))
            ], width=4)
        ]),
        
        # Combined chart
        html.H2("Combined Chart"),
        dcc.Graph(figure=create_combined_sector_chart(
            ["Enterprise SaaS", "AdTech", "Cybersecurity", "AI Infrastructure"],
            title="Sector Comparison"
        ))
    ])
    
    # Run the app
    app.run_server(host='0.0.0.0', port=5002, debug=True)
