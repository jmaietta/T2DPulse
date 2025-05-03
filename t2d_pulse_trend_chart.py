#!/usr/bin/env python3
# t2d_pulse_trend_chart.py
# -----------------------------------------------------------
# Create mini trend charts for T2D Pulse score history using authentic data

import os
import plotly.graph_objects as go
import pandas as pd
import pytz
from datetime import datetime, timedelta

# Color scheme for trend charts
TREND_COLORS = {
    'line': '#2E86C1',           # Main line color (blue)
    'fill': 'rgba(46,134,193,0.2)',  # Fill color (light blue)
    'marker': '#2471A3',         # Point marker color (darker blue)
    'highlight': '#F39C12'       # Highlight color (orange)
}

def create_t2d_pulse_chart(pulse_color, height=80, show_axes=False, auto_range=True, show_value=False):
    """
    Create a mini trend chart for T2D Pulse using authentic historical data
    
    Args:
        pulse_color (str): Color for the pulse trend line
        height (int, optional): Height of the chart in pixels
        show_axes (bool, optional): Whether to show axes
        auto_range (bool, optional): Whether to auto-range the y-axis
        show_value (bool, optional): Whether to show the latest value annotation
        
    Returns:
        dict: Plotly figure object
    """
    # Read the authentic T2D Pulse history file
    history_file = "data/t2d_pulse_history.csv"
    
    if not os.path.exists(history_file):
        # Return empty chart if no data available
        fig = go.Figure()
        fig.update_layout(height=height, margin=dict(l=0, r=0, t=0, b=0))
        return fig
    
    try:
        # Read the history file
        df = pd.read_csv(history_file)
        
        # Check if we have the required columns
        if 'date' in df.columns and ('T2D Pulse Score' in df.columns or 'pulse_score' in df.columns):
            # Convert date column to datetime
            df['date'] = pd.to_datetime(df['date'])
            
            # Determine which column has the data (pulse_score or T2D Pulse Score)
            score_column = 'T2D Pulse Score' if 'T2D Pulse Score' in df.columns and not df['T2D Pulse Score'].isna().all() else 'pulse_score'
            
            # Drop rows with missing scores
            df = df.dropna(subset=[score_column])
            
            # Sort by date ascending
            df = df.sort_values('date')
            
            # Get the last 30 days of data or all data if less than 30 days
            if len(df) > 30:
                df = df.tail(30)
                
            # Convert columns to lists for plotting
            dates = df['date'].tolist()
            values = df[score_column].tolist()
            
            # Create figure
            fig = go.Figure()
            
            # Add area fill
            fig.add_trace(go.Scatter(
                x=dates,
                y=values,
                fill='tozeroy',
                fillcolor=f"rgba(188, 140, 0, 0.2)",  # Light orange with opacity
                line=dict(color=pulse_color, width=2),
                mode='lines',
                hoverinfo='none',
                showlegend=False
            ))
            
            # Add markers for each point (for hover info)
            fig.add_trace(go.Scatter(
                x=dates,
                y=values,
                mode='markers',
                marker=dict(color=pulse_color, size=4, opacity=0),  # Invisible markers for hover
                hoverinfo='text',
                hovertext=[f"Date: {d.strftime('%Y-%m-%d')}<br>Score: {v:.1f}" 
                           for d, v in zip(dates, values)],
                showlegend=False
            ))
            
            # Set layout
            fig.update_layout(
                height=height,
                autosize=True,                    # Make chart automatically size to container
                margin=dict(l=5, r=5, t=0, b=20),
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                hovermode='closest',
                transition_duration=0,            # No transition animation
                uirevision=True                   # Preserve UI state on updates
            )
            
            # Hide axes if requested
            if not show_axes:
                fig.update_xaxes(
                    showgrid=False,
                    zeroline=False,
                    showticklabels=False,
                    showline=False
                )
                fig.update_yaxes(
                    showgrid=False,
                    zeroline=False,
                    showticklabels=False,
                    showline=False
                )
            
            # Set y-axis range to focus on differences
            if auto_range:
                # Let Plotly handle auto-ranging but with padding
                y_min = min(values) - 10 if values else 0
                y_max = max(values) + 10 if values else 100
                fig.update_yaxes(range=[max(0, y_min), min(100, y_max)])
            else:
                # Fixed range from 0-100 for consistent visualization
                fig.update_yaxes(range=[0, 100])
            
            return fig
        else:
            raise ValueError("History file missing required columns")
            
    except Exception as e:
        print(f"Error creating T2D Pulse trend chart: {e}")
        # Return empty chart if there's an error
        fig = go.Figure()
        fig.update_layout(height=height, margin=dict(l=0, r=0, t=0, b=0))
        return fig

if __name__ == "__main__":
    # Test the trend chart
    import dash
    import dash_bootstrap_components as dbc
    from dash import html, dcc
    
    app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
    
    # Create layout
    app.layout = dbc.Container([
        html.H1("T2D Pulse Trend Chart"),
        
        # Mini trend chart
        dbc.Row([
            dbc.Col([
                html.H4("T2D Pulse Score"),
                dcc.Graph(figure=create_t2d_pulse_chart("#f39c12"))
            ], width=6),
        ]),
    ])
    
    # Run the app
    app.run_server(host='0.0.0.0', port=5050, debug=True)
