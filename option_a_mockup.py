#!/usr/bin/env python3
# option_a_mockup.py - Side by Side Layout
# -----------------------------------------------------------
# Option A: Side-by-side layout with pulse circle on left, trend chart on right

import dash
from dash import html, dcc
import dash_bootstrap_components as dbc
import plotly.graph_objs as go
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import base64
import os

# Import the glowing circle component
from simple_glow_mockup import create_pulse_glow_circle

# T2D logo path
T2D_LOGO_PATH = "attached_assets/T2D Pulse logo.png"

# Initialize the app
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# Generate sample pulse data for testing
def generate_sample_pulse_data(days=30):
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    # Generate dates
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    
    # Generate scores using a sine wave pattern with noise
    baseline = 55.7  # Current authentic score
    x = np.linspace(0, 3*np.pi, len(date_range))
    trend = 5 * np.sin(x) + np.random.normal(0, 2, len(date_range))
    scores = baseline + trend
    
    # Ensure scores stay within 0-100 range
    scores = np.clip(scores, 0, 100)
    
    # Create DataFrame
    df = pd.DataFrame({
        'date': date_range,
        'pulse_score': scores
    })
    
    # Filter out weekends
    df['weekday'] = df['date'].dt.dayofweek
    df = df[df['weekday'] < 5].drop('weekday', axis=1)
    
    return df

# Create the trend chart
def create_trend_chart(pulse_data):
    # Create trace for the line chart
    trace = go.Scatter(
        x=pulse_data['date'],
        y=pulse_data['pulse_score'],
        mode='lines',
        line=dict(
            color='#f39c12',  # Orange color
            width=3,
            shape='spline'  # Smoothed line
        ),
        fill='tozeroy',
        fillcolor='rgba(243, 156, 18, 0.2)',  # Light orange fill
        hovertemplate='<b>%{x|%b %d, %Y}</b><br>T2D Pulse: %{y:.1f}<extra></extra>'
    )
    
    # Create layout with colored background regions
    layout = go.Layout(
        height=280,
        margin=dict(l=30, r=20, t=20, b=30),
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        xaxis=dict(
            title='',
            showgrid=False,
            showline=True,
            linecolor='#ddd',
            tickformat='%b %d',
            tickangle=-45
        ),
        yaxis=dict(
            title='',
            range=[0, 100],
            showgrid=True,
            gridcolor='#eee',
            tickvals=[0, 30, 60, 100],
            showline=True,
            linecolor='#ddd'
        ),
        shapes=[
            # Bearish region (0-30)
            dict(
                type='rect',
                xref='paper', yref='y',
                x0=0, x1=1,
                y0=0, y1=30,
                fillcolor='rgba(231, 76, 60, 0.1)',  # Light red
                line=dict(width=0)
            ),
            # Neutral region (30-60)
            dict(
                type='rect',
                xref='paper', yref='y',
                x0=0, x1=1,
                y0=30, y1=60,
                fillcolor='rgba(243, 156, 18, 0.1)',  # Light orange
                line=dict(width=0)
            ),
            # Bullish region (60-100)
            dict(
                type='rect',
                xref='paper', yref='y',
                x0=0, x1=1,
                y0=60, y1=100,
                fillcolor='rgba(46, 204, 113, 0.1)',  # Light green
                line=dict(width=0)
            )
        ],
        annotations=[
            # Bearish label
            dict(
                x=0.02, y=15,
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
                x=0.02, y=45,
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
                x=0.02, y=80,
                xref='paper', yref='y',
                text='Bullish',
                showarrow=False,
                font=dict(
                    color='rgba(39, 174, 96, 0.7)',
                    size=12
                )
            )
        ],
        hovermode='x unified'
    )
    
    fig = go.Figure(data=[trace], layout=layout)
    return fig

# Encode T2D logo in base64 if it exists
def encode_t2d_logo():
    if os.path.exists(T2D_LOGO_PATH):
        with open(T2D_LOGO_PATH, "rb") as image_file:
            encoded_image = base64.b64encode(image_file.read()).decode('utf-8')
            return f"data:image/png;base64,{encoded_image}"
    return None

# Define the layout
app.layout = html.Div([
    html.Div([
        # T2D Pulse header with logo
        html.Div([
            html.Img(src=encode_t2d_logo() or '', 
                   style={'height': '40px', 'marginRight': '10px'}),
            html.H4('T2D PULSE', style={'margin': '0', 'color': '#e74c3c'})
        ], style={
            'display': 'flex',
            'alignItems': 'center',
            'justifyContent': 'center',
            'marginBottom': '15px',
            'borderBottom': '1px solid #eee',
            'paddingBottom': '10px'
        }),
        
        # Option A: Side-by-side layout with circle on left, chart on right
        html.Div([
            html.Div([
                # Left side - Pulse Circle
                html.Div([
                    # Using the glowing circle component
                    create_pulse_glow_circle(55.7, size=240)  # Authentic T2D Pulse score
                ], style={
                    'flex': '0 0 auto',
                    'marginRight': '20px',
                    'display': 'flex',
                    'alignItems': 'center',
                    'justifyContent': 'center'
                }),
                
                # Right side - Trend Chart
                html.Div([
                    html.Div("30-Day Trend", style={
                        'fontSize': '16px',
                        'fontWeight': '500',
                        'marginBottom': '5px',
                        'textAlign': 'center',
                        'color': '#555'
                    }),
                    dcc.Graph(
                        id='trend-chart',
                        figure=create_trend_chart(generate_sample_pulse_data()),
                        config={'displayModeBar': False}
                    )
                ], style={
                    'flex': '1',
                    'minWidth': '0',  # Allows the flex item to shrink below content size
                    'height': '280px',
                    'border': '1px solid #eee',
                    'borderRadius': '5px',
                    'padding': '10px',
                    'backgroundColor': '#fff'
                })
            ], style={
                'display': 'flex',
                'flexDirection': 'row',
                'alignItems': 'center',
                'justifyContent': 'space-between',
                'width': '100%',
                'marginBottom': '15px'
            })
        ]),
        
        # Last updated text
        html.Div([
            f"Data refreshed on {datetime.now().strftime('%b %d, %Y')}"
        ], style={
            'textAlign': 'center',
            'color': '#888',
            'fontSize': '12px',
            'marginTop': '10px'
        })
    ], style={
        'maxWidth': '1200px',
        'margin': '0 auto',
        'padding': '20px',
        'backgroundColor': '#f8f9fa',
        'borderRadius': '8px',
        'boxShadow': '0 2px 4px rgba(0, 0, 0, 0.05)'
    })
])

# Run the app
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
