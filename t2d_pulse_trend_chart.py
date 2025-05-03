#!/usr/bin/env python3
# t2d_pulse_trend_chart.py
# -----------------------------------------------------------
# Creates and updates the 30-day T2D Pulse trend chart for the dashboard

import plotly.graph_objs as go
from t2d_pulse_history import get_t2d_pulse_history

def create_t2d_pulse_chart(days=30):
    """
    Create a 30-day T2D Pulse trend chart
    
    Args:
        days (int): Number of days of history to include
        
    Returns:
        Figure: Plotly figure object for the chart
    """
    # Get historical data
    pulse_data = get_t2d_pulse_history(days)
    
    if pulse_data.empty:
        # Return empty figure with message if no data
        fig = go.Figure()
        fig.update_layout(
            title="No T2D Pulse history data available",
            height=300,
            xaxis_title="Date",
            yaxis_title="Pulse Score"
        )
        return fig
    
    # Create trace for the line chart
    trace = go.Scatter(
        x=pulse_data['date'],
        y=pulse_data['pulse_score'],
        mode='lines+markers',
        name='T2D Pulse Score',
        line=dict(
            color='#2c3e50',
            width=3,
            shape='spline'  # Smoothed line
        ),
        marker=dict(
            size=6,
            color='#2c3e50'
        ),
        hovertemplate='<b>%{x|%b %d, %Y}</b><br>T2D Pulse: %{y:.1f}<extra></extra>'
    )
    
    # Create layout with colored background regions for Bearish, Neutral, Bullish
    layout = go.Layout(
        title={
            'text': 'T2D Pulse Score - 30 Day Trend',
            'font': {
                'size': 16,
                'color': '#333'
            },
            'x': 0.01,  # Title aligned to left
            'xanchor': 'left'
        },
        height=300,
        margin=dict(l=50, r=30, t=60, b=50),
        xaxis=dict(
            title='Date',
            tickformat='%b %d',
            gridcolor='#f5f5f5'
        ),
        yaxis=dict(
            title='Pulse Score',
            range=[0, 100],
            tickvals=[0, 30, 60, 100],
            gridcolor='#f5f5f5'
        ),
        shapes=[
            # Bearish region (0-30)
            dict(
                type='rect',
                xref='paper', yref='y',
                x0=0, x1=1,
                y0=0, y1=30,
                fillcolor='rgba(231, 76, 60, 0.15)',
                line=dict(width=0)
            ),
            # Neutral region (30-60)
            dict(
                type='rect',
                xref='paper', yref='y',
                x0=0, x1=1,
                y0=30, y1=60,
                fillcolor='rgba(243, 156, 18, 0.15)',
                line=dict(width=0)
            ),
            # Bullish region (60-100)
            dict(
                type='rect',
                xref='paper', yref='y',
                x0=0, x1=1,
                y0=60, y1=100,
                fillcolor='rgba(46, 204, 113, 0.15)',
                line=dict(width=0)
            )
        ],
        annotations=[
            # Bearish label
            dict(
                x=0.01, y=15,
                xref='paper', yref='y',
                text='Bearish',
                showarrow=False,
                font=dict(
                    color='rgba(192, 57, 43, 0.7)',
                    size=12
                )
            ),
            # Neutral label
            dict(
                x=0.01, y=45,
                xref='paper', yref='y',
                text='Neutral',
                showarrow=False,
                font=dict(
                    color='rgba(211, 84, 0, 0.7)',
                    size=12
                )
            ),
            # Bullish label
            dict(
                x=0.01, y=80,
                xref='paper', yref='y',
                text='Bullish',
                showarrow=False,
                font=dict(
                    color='rgba(39, 174, 96, 0.7)',
                    size=12
                )
            )
        ],
        hovermode='x unified',
        legend=dict(
            orientation='h',
            yanchor='bottom',
            y=1.02,
            xanchor='right',
            x=1
        )
    )
    
    fig = go.Figure(data=[trace], layout=layout)
    return fig
