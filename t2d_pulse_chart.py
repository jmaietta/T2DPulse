#!/usr/bin/env python3
# t2d_pulse_chart.py
# -----------------------------------------------------------
# A mockup visualization of the 30-day T2D Pulse chart 

import dash
from dash import html, dcc
import dash_bootstrap_components as dbc
import plotly.graph_objs as go
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Create a Dash app for the mockup
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# Generate sample T2D Pulse score data for the last 30 days
def generate_sample_pulse_data(days=30):
    # Start date (30 days ago)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    # Generate dates
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    
    # We'll use a sine wave with some noise and trend to simulate realistic pulse score movement
    # Define a baseline score
    baseline = 60.0
    
    # Generate scores with some trend and randomness 
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
    
    # Filter out weekends (just for realism)
    df['weekday'] = df['date'].dt.dayofweek
    df = df[df['weekday'] < 5].drop('weekday', axis=1)
    
    return df

# Create the T2D Pulse chart
def create_pulse_chart(pulse_data):
    # Create trace for the line chart
    trace = go.Scatter(
        x=pulse_data['date'],
        y=pulse_data['pulse_score'],
        mode='lines+markers',
        name='T2D Pulse Score',
        line=dict(
            color='#2c3e50',
            width=3
        ),
        marker=dict(
            size=6,
            color='#2c3e50'
        )
    )
    
    # Create layout with colored background regions for Bearish, Neutral, Bullish
    layout = go.Layout(
        title={
            'text': 'T2D Pulse Score - 30 Day Trend',
            'font': {
                'size': 18,
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

# Create a mock T2D Pulse gauge for demonstration
def create_mock_pulse_gauge(score=60.8):
    # Determine color based on score
    if score >= 60:
        color = '#2ecc71'  # Green for Bullish
        status = 'Bullish'
    elif score <= 30:
        color = '#e74c3c'  # Red for Bearish
        status = 'Bearish'
    else:
        color = '#f39c12'  # Orange for Neutral
        status = 'Neutral'
    
    # Create the mock gauge
    gauge = html.Div([
        # Header with T2D logo and title
        html.Div([
            html.Img(src='https://cdn.replit.com/attached_assets/T2D%20Pulse%20logo.png', 
                    style={'height': '40px', 'marginRight': '10px'}),
            html.H4('T2D Pulse', style={'display': 'inline-block', 'marginBottom': '0px', 'verticalAlign': 'middle'})
        ], style={'display': 'flex', 'alignItems': 'center', 'justifyContent': 'center', 'marginBottom': '15px'}),
        html.Div([
            html.Div([
                html.H2(f"{score:.1f}", style={
                    'color': color,
                    'textAlign': 'center',
                    'margin': '0',
                    'fontSize': '48px',
                    'fontWeight': 'bold'
                })
            ], style={
                'border': f'6px solid {color}',
                'borderRadius': '50%',
                'width': '150px',
                'height': '150px',
                'display': 'flex',
                'alignItems': 'center',
                'justifyContent': 'center',
                'margin': '0 auto 10px auto',
                'backgroundColor': '#fff',
                'boxShadow': f'0 0 15px {color}'
            }),
            html.Div(status, style={
                'textAlign': 'center',
                'color': color,
                'fontWeight': 'bold',
                'fontSize': '18px'
            })
        ])
    ], style={
        'padding': '20px',
        'backgroundColor': '#f9f9f9',
        'borderRadius': '8px',
        'marginBottom': '20px'
    })
    
    return gauge

# Define the app layout
def create_mockup_layout():
    # Generate sample data
    pulse_data = generate_sample_pulse_data(30)
    
    # Create chart figure
    pulse_chart = create_pulse_chart(pulse_data)
    
    # Latest score (last value in our sample data)
    latest_score = pulse_data['pulse_score'].iloc[-1]
    
    # Create the layout
    layout = dbc.Container([
        html.H2("T2D Pulse Economic Dashboard - Mockup", className="my-4"),
        
        # Explanation text
        html.Div([
            html.P("This is a mockup showing the recommended placement of the 30-day T2D Pulse chart below the main gauge",
                  className="lead")
        ], className="mb-4"),
        
        # Top section with T2D Pulse gauge
        dbc.Row([
            dbc.Col([
                create_mock_pulse_gauge(latest_score)
            ], width=12)
        ]),
        
        # Chart section right below the gauge
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        dcc.Graph(
                            id='pulse-chart',
                            figure=pulse_chart,
                            config={'displayModeBar': False}
                        )
                    ])
                ])
            ], width=12)
        ]),
        
        # Other dashboard elements would follow
        dbc.Row([
            dbc.Col([
                html.Div([
                    html.H4("Sector Cards Would Follow Here", className="text-center mt-4"),
                    html.P("The sector cards and other dashboard elements would be positioned below the T2D Pulse chart.",
                          className="text-center text-muted")
                ])
            ], width=12)
        ])
    ], fluid=True)
    
    return layout

# Set up the app layout
app.layout = create_mockup_layout()

# Run the app
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5050)
