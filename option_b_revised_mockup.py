#!/usr/bin/env python3
# option_b_revised_mockup.py - Expanded Circle with Integrated Chart
# -----------------------------------------------------------
# Option B: Enlarged circle with integrated mini chart at the bottom

import dash
from dash import html, dcc
import dash_bootstrap_components as dbc
import plotly.graph_objs as go
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import base64
import os

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
    
    # Generate scores with trend around the authentic value of 55.7
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

# Create a mini trend chart for the circle
def create_mini_trend_chart(pulse_data):
    trace = go.Scatter(
        x=pulse_data['date'],
        y=pulse_data['pulse_score'],
        mode='lines',
        line=dict(
            color='#f39c12',  # Orange color (matching Neutral)
            width=2,
            shape='spline'
        ),
        fill='tozeroy',
        fillcolor='rgba(243, 156, 18, 0.2)',
        hoverinfo='none'
    )
    
    layout = go.Layout(
        height=80,
        margin=dict(l=0, r=0, t=0, b=0),
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        xaxis=dict(
            showgrid=False,
            zeroline=False,
            showticklabels=False,
            showline=False
        ),
        yaxis=dict(
            showgrid=False,
            zeroline=False,
            showticklabels=False,
            showline=False,
            range=[0, 100]
        )
    )
    
    fig = go.Figure(data=[trace], layout=layout)
    return fig

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
        height=180,  # Shorter height for this option
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
        # Title with explanation
        html.H3("Option B: Enlarged Circle with Integrated Chart", 
                style={'textAlign': 'center', 'marginBottom': '20px'}),
        
        html.P("This layout features an enlarged pulse circle with the T2D logo and sentiment score, "
               "plus an integrated mini-chart at the bottom of the circle. A full-width 30-day trend "
               "chart appears below the circle for detailed historical context.",
               style={'textAlign': 'center', 'marginBottom': '30px', 'maxWidth': '800px', 'margin': '0 auto'}),
        
        # Main content
        html.Div([
            # Header with T2D logo
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
            
            # Option B: Enlarged circle with integrated chart at bottom
            html.Div([
                # Center-aligned enlarged pulse circle
                html.Div([
                    # Container for the circle
                    html.Div([
                        # T2D Pulse Logo
                        html.Img(src=encode_t2d_logo() or '',
                                 style={
                                     'width': '120px',
                                     'margin': '0 auto 15px auto',
                                     'display': 'block'
                                 }),
                        
                        # Score and status
                        html.Div([
                            html.Div("55.7", style={
                                'fontSize': '70px',
                                'fontWeight': '600',
                                'color': '#f39c12',  # Neutral orange
                                'textAlign': 'center',
                                'lineHeight': '1'
                            }),
                            html.Div("Neutral", style={
                                'fontSize': '24px',
                                'fontWeight': '400',
                                'color': '#f39c12',
                                'textAlign': 'center',
                                'marginBottom': '20px'
                            }),
                            
                            # Mini trend chart inside the circle
                            html.Div([
                                dcc.Graph(
                                    id='mini-chart',
                                    figure=create_mini_trend_chart(generate_sample_pulse_data()),
                                    config={'displayModeBar': False},
                                    style={'height': '80px'}
                                )
                            ], style={
                                'width': '90%',
                                'margin': '0 auto'
                            })
                        ], style={
                            'display': 'flex',
                            'flexDirection': 'column',
                            'justifyContent': 'center'
                        })
                    ], style={
                        'width': '320px',
                        'height': '320px',
                        'borderRadius': '50%',
                        'backgroundColor': 'white',
                        'boxShadow': '0 0 20px #f39c12',
                        'border': '3px solid #f39c12',
                        'display': 'flex',
                        'flexDirection': 'column',
                        'justifyContent': 'center',
                        'padding': '20px',
                        'margin': '0 auto'
                    })
                ], style={
                    'marginBottom': '20px',
                    'display': 'flex',
                    'justifyContent': 'center'
                }),
                
                # Full width trend chart below the circle
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
                    'border': '1px solid #eee',
                    'borderRadius': '5px',
                    'padding': '10px',
                    'backgroundColor': '#fff',
                    'width': '100%'
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
            'maxWidth': '1000px',
            'margin': '0 auto',
            'padding': '20px',
            'backgroundColor': '#fff',
            'borderRadius': '8px',
            'boxShadow': '0 2px 4px rgba(0, 0, 0, 0.05)'
        })
    ], style={
        'padding': '20px',
        'backgroundColor': '#f8f9fa',
        'minHeight': '100vh'
    })
])

# Run the app
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5002)
