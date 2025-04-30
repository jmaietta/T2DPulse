#!/usr/bin/env python3
# sector_trend_chart.py
# -----------------------------------------------------------
# Creates mini time series charts for sector sentiment trends

import plotly.graph_objects as go
import sector_sentiment_history
import pandas as pd

def create_sector_trend_chart(sector_name, days=14, height=80, width=None):
    """
    Create a mini time series chart for sector sentiment trend
    
    Args:
        sector_name (str): Name of the sector
        days (int): Number of days of history to show
        height (int): Height of the chart in pixels
        width (int): Width of the chart in pixels (optional)
    
    Returns:
        plotly.graph_objects.Figure: A plotly figure object
    """
    # Get historical data for the sector
    df = sector_sentiment_history.get_sector_history_dataframe(sector_name, days)
    
    # If we have no real data yet, return an empty placeholder
    if df.empty or df['score'].nunique() <= 1:
        # Create a placeholder figure
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
                    text="Trend data will appear here",
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