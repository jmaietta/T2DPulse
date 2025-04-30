#!/usr/bin/env python3
# sector_trend_chart.py
# -----------------------------------------------------------
# Generate mini time series charts for historical sector sentiment

import plotly.graph_objects as go
import pandas as pd
from datetime import datetime, timedelta
from authentic_sector_history import get_sector_history_dataframe

def get_chart_color(score_value):
    """Get the appropriate color based on the sentiment score"""
    if score_value >= 60:
        return "#2ecc71"  # Green for bullish (60-100)
    elif score_value >= 30:
        return "#f39c12"  # Orange for neutral (30-60)
    else:
        return "#e74c3c"  # Red for bearish (0-30)

def create_sector_trend_chart(sector_name, days=10, height=85, width=200):
    """
    Create a mini time series chart for a specific sector using authentic historical data
    
    Args:
        sector_name (str): Name of the sector
        days (int): Number of days to include in the chart (defaults to 10 for better performance)
        height (int): Height of the chart in pixels
        width (int): Width of the chart in pixels
        
    Returns:
        go.Figure: Plotly figure with the trend chart
    """
    try:
        # Get authentic historical data for this sector
        df = get_sector_history_dataframe(sector_name, days)
        
        # If no data, try to get it from historical_sector_scores as a fallback
        if df.empty:
            try:
                import historical_sector_scores
                print(f"No authentic data for {sector_name}, trying legacy historical data")
                df = historical_sector_scores.get_historical_scores(sector_name, days)
            except Exception as e:
                print(f"Error getting fallback historical data for {sector_name}: {e}")
        
        # If still no data, return an empty chart with a message
        if df.empty:
            fig = go.Figure()
            fig.update_layout(
                margin=dict(l=0, r=0, t=0, b=0),
                height=height,
                width=width,
                annotations=[
                    dict(
                        text="Historical data loading...",
                        xref="paper",
                        yref="paper",
                        x=0.5,
                        y=0.5,
                        showarrow=False,
                        font=dict(size=10, color="#777777")
                    )
                ]
            )
            return fig
    except Exception as e:
        print(f"Error in create_sector_trend_chart for {sector_name}: {e}")
        fig = go.Figure()
        fig.update_layout(
            margin=dict(l=0, r=0, t=0, b=0),
            height=height,
            width=width,
            annotations=[
                dict(
                    text="Chart loading...",
                    xref="paper",
                    yref="paper",
                    x=0.5,
                    y=0.5,
                    showarrow=False,
                    font=dict(size=10, color="#777777")
                )
            ]
        )
        return fig
    
    # Get the most recent score to determine line color
    latest_score = df["score"].iloc[-1] if not df.empty else 50
    line_color = get_chart_color(latest_score)
    
    # Create the line chart
    fig = go.Figure()
    
    # Add the score line
    fig.add_trace(
        go.Scatter(
            x=df["date"],
            y=df["score"],
            mode="lines",
            line=dict(width=2, color=line_color),
            hovertemplate="%{y:.1f}",
        )
    )
    
    # Add a reference line at 50 (the midpoint)
    fig.add_shape(
        type="line",
        x0=df["date"].min(),
        x1=df["date"].max(),
        y0=50,
        y1=50,
        line=dict(color="#cccccc", width=1, dash="dot"),
    )
    
    # Format the chart
    fig.update_layout(
        margin=dict(l=0, r=0, t=0, b=0),
        height=height,
        width=width,
        showlegend=False,
        xaxis=dict(visible=False, showgrid=False),
        yaxis=dict(visible=False, showgrid=False, range=[0, 100]),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        hovermode="x unified",
    )
    
    return fig

def create_sector_trend_div(sector_name, sector_score):
    """
    Create a div with the sector name, score, and trend chart
    
    Args:
        sector_name (str): Name of the sector
        sector_score (float): Current sentiment score for the sector
        
    Returns:
        html.Div: Div with the sector card
    """
    from dash import html, dcc
    import dash_bootstrap_components as dbc
    
    # Convert score to 0-100 scale if it's in -1 to 1 range
    if -1 <= sector_score <= 1:
        normalized_score = ((sector_score + 1) / 2) * 100
    else:
        normalized_score = sector_score
    
    # Get color based on the score
    score_color = get_chart_color(normalized_score)
    
    # Create the card
    return dbc.Card(
        [
            dbc.CardHeader(
                [
                    html.Div(
                        sector_name,
                        style={"fontWeight": "bold", "fontSize": "0.9rem"}
                    ),
                    html.Div(
                        f"{normalized_score:.1f}",
                        style={
                            "fontWeight": "bold",
                            "color": score_color,
                            "fontSize": "1.1rem"
                        }
                    )
                ],
                style={"display": "flex", "justifyContent": "space-between", "alignItems": "center"}
            ),
            dbc.CardBody(
                [
                    dcc.Graph(
                        figure=create_sector_trend_chart(sector_name),
                        config={"displayModeBar": False},
                        style={"height": "85px", "width": "100%"}
                    )
                ],
                style={"padding": "0.5rem"}
            ),
        ],
        style={"width": "200px", "margin": "5px", "height": "150px"}
    )

# Example usage
if __name__ == "__main__":
    # Test the chart creation
    fig = create_sector_trend_chart("AdTech")
    fig.show()