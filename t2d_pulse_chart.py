#!/usr/bin/env python3
# t2d_pulse_chart.py
# -----------------------------------------------------------
# Creates time series charts for T2D Pulse score trends

import plotly.graph_objects as go
import pandas as pd
import os
from datetime import datetime, timedelta

# Check if we can import authentic historical data
try:
    import authentic_historical_data
    USE_AUTHENTIC_DATA = True
except ImportError:
    USE_AUTHENTIC_DATA = False

def create_t2d_pulse_trend_chart(days=14, height=80, width=None):
    """
    Create a mini time series chart for T2D Pulse score trend
    
    Args:
        days (int): Number of days of history to show
        height (int): Height of the chart in pixels
        width (int): Width of the chart in pixels (optional)
    
    Returns:
        plotly.graph_objects.Figure: A plotly figure object
    """
    # Try to get authentic historical data first
    if USE_AUTHENTIC_DATA and os.path.exists("data/authentic_t2d_pulse_history.json"):
        try:
            df = authentic_historical_data.get_t2d_pulse_history_dataframe(days)
            if not df.empty:
                print(f"Using authentic historical data for T2D Pulse trend chart")
        except Exception as e:
            print(f"Error using authentic data for T2D Pulse: {e}")
            df = pd.DataFrame(columns=['date', 'score'])
    else:
        df = pd.DataFrame(columns=['date', 'score'])
    
    # If we don't have authentic data, create a fallback DataFrame
    if df.empty:
        # Get current T2D Pulse score from existing dashboard calculation
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        
        # Create a fallback with just today's score
        try:
            from app import current_t2d_pulse_score
            if hasattr(app, 'current_t2d_pulse_score'):
                today_score = current_t2d_pulse_score
                df = pd.DataFrame([{'date': today, 'score': today_score}])
            else:
                print("Current T2D Pulse score not available")
                return create_empty_chart(height, width)
        except (ImportError, AttributeError):
            print("Could not access current T2D Pulse score")
            return create_empty_chart(height, width)
    
    # If we still don't have data, show empty chart
    if df.empty:
        return create_empty_chart(height, width)
    
    # Get the last score to determine line color
    latest_score = df['score'].iloc[-1]
    
    # Determine color based on sentiment level
    if latest_score >= 60:
        line_color = "#2ecc71"  # Green for Bullish
        area_color = "rgba(46, 204, 113, 0.15)"  # Light green with transparency
    elif latest_score <= 30:
        line_color = "#e74c3c"  # Red for Bearish
        area_color = "rgba(231, 76, 60, 0.15)"  # Light red with transparency
    else:
        line_color = "#f39c12"  # Orange for Neutral
        area_color = "rgba(243, 156, 18, 0.15)"  # Light orange with transparency
    
    # Create sentiment zones
    bullish_zone = go.Scatter(
        x=[df['date'].min(), df['date'].max()],
        y=[60, 60],
        mode='lines',
        line=dict(color='rgba(46, 204, 113, 0.3)', width=1, dash='dash'),
        showlegend=False,
        hoverinfo='skip'
    )
    
    bearish_zone = go.Scatter(
        x=[df['date'].min(), df['date'].max()],
        y=[30, 30],
        mode='lines',
        line=dict(color='rgba(231, 76, 60, 0.3)', width=1, dash='dash'),
        showlegend=False,
        hoverinfo='skip'
    )
    
    # Create line trace for score
    line_trace = go.Scatter(
        x=df['date'],
        y=df['score'],
        mode='lines',
        line=dict(color=line_color, width=2),
        fill='tozeroy',
        fillcolor=area_color,
        showlegend=False,
        hovertemplate='%{y:.1f}<extra></extra>'
    )
    
    # Add marker for latest point
    marker_trace = go.Scatter(
        x=[df['date'].iloc[-1]],
        y=[df['score'].iloc[-1]],
        mode='markers',
        marker=dict(color=line_color, size=6),
        showlegend=False,
        hovertemplate='%{y:.1f}<extra></extra>'
    )
    
    # Create figure
    fig = go.Figure()
    fig.add_trace(bullish_zone)
    fig.add_trace(bearish_zone)
    fig.add_trace(line_trace)
    fig.add_trace(marker_trace)
    
    # Update layout to be compact
    fig.update_layout(
        height=height,
        width=width,
        margin=dict(l=0, r=0, t=5, b=0, pad=0),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        hovermode="x unified",
        xaxis=dict(
            showticklabels=False,
            showgrid=False,
            zeroline=False
        ),
        yaxis=dict(
            range=[0, 100],
            showticklabels=False,
            showgrid=False,
            zeroline=False
        )
    )
    
    return fig

def create_empty_chart(height, width):
    """Create an empty placeholder chart"""
    fig = go.Figure()
    fig.update_layout(
        height=height,
        width=width,
        margin=dict(l=0, r=0, t=0, b=0, pad=0),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        showlegend=False,
        annotations=[
            dict(
                text="T2D Pulse trend data will appear here",
                showarrow=False,
                xref="paper",
                yref="paper",
                x=0.5,
                y=0.5,
                font=dict(size=10, color="#999999")
            )
        ],
        xaxis=dict(showticklabels=False, showgrid=False, zeroline=False),
        yaxis=dict(showticklabels=False, showgrid=False, zeroline=False)
    )
    return fig