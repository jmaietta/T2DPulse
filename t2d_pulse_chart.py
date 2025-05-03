#!/usr/bin/env python3
# Import the new simple glow mockup
from simple_glow_mockup import create_pulse_glow_circle
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

# Initialize the Dash app with T2D Pulse styling
app = dash.Dash(
    __name__, 
    external_stylesheets=[
        dbc.themes.BOOTSTRAP,
        # Custom font to match dashboard
        'https://fonts.googleapis.com/css2?family=Roboto:wght@300;400;500;700&display=swap'
    ],
    # Define custom CSS to exactly match the main dashboard
    meta_tags=[
        {"name": "viewport", "content": "width=device-width, initial-scale=1"},
    ]
)

# Add custom CSS to match the main dashboard look and feel
app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>T2D Pulse Chart Mockup</title>
        {%favicon%}
        {%css%}
        <style>
            body {
                font-family: 'Roboto', sans-serif;
                background-color: #f8f9fa;
            }
            .dashboard-container {
                max-width: 1400px;
                margin: 0 auto;
                background-color: white;
                border-radius: 8px;
                box-shadow: 0 2px 6px rgba(0,0,0,0.05);
            }
            .header-row {
                border-bottom: 1px solid #eaeaea;
            }
            .shadow-sm {
                box-shadow: 0 .125rem .25rem rgba(0,0,0,.075);
            }
            .border-dashed {
                border: 2px dashed #dee2e6;
                border-radius: 6px;
            }
            /* Additional styling for T2D Pulse Gauge */
            .t2d-pulse-card {
                border-radius: 8px;
                overflow: hidden;
            }
            /* Footer styling */
            .footer {
                color: #6c757d;
                font-size: 14px;
                margin-top: 30px;
                text-align: center;
            }
        </style>
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>
'''

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
            html.Img(src='/assets/T2D Pulse logo.png', 
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
    
    # Create a realistic mockup showing the integration with the main dashboard
    layout = dbc.Container([
        # Dashboard Header
        dbc.Row([
            dbc.Col([
                html.Div([
                    html.Img(src='/assets/T2D Pulse logo.png', height='60px', className="me-3"),
                    html.H2('Economic Dashboard', className='d-inline align-middle')
                ], className="d-flex align-items-center py-3")
            ])
        ], className="header-row border-bottom mb-4"),
        
        # Mockup Explanation Text
        dbc.Row([
            dbc.Col([
                html.Div([
                    html.H4("30-Day T2D Pulse Chart - Mockup", className="text-primary mb-3"),
                    html.P(
                        "This visualization shows how the 30-day T2D Pulse chart could be integrated "
                        "directly below the main gauge for better trend visualization. The chart displays "
                        "the daily T2D Pulse score over the last 30 market days with colored zones for "
                        "Bearish, Neutral, and Bullish regions.",
                        className="lead mb-4"
                    )
                ])
            ])
        ]),
        
        # Main Dashboard Section with Two Panels for Demo
        dbc.Row([
            # Left Panel with T2D Pulse Score and Chart
            dbc.Col([
                # Dashboard Title for Demo
                html.H5("Main Gauge & 30-Day Chart Integration", 
                        className="mb-4 text-center text-secondary"),
                
                # T2D Pulse Gauge Panel
                dbc.Card([
                    dbc.CardBody([
                        # Main T2D Pulse Gauge using the new glowing circle design
                        create_pulse_glow_circle(55.7)  # Using authentic value
                    ])
                ], className="mb-4 shadow-sm"),
                
                # 30-Day T2D Pulse Chart Panel - THIS IS THE MAIN ADDITION
                dbc.Card([
                    dbc.CardBody([
                        dcc.Graph(
                            id='pulse-chart',
                            figure=pulse_chart,
                            config={'displayModeBar': False}
                        )
                    ])
                ], className="mb-4 shadow-sm")
            ], width=9),
            
            # Right Panel with Note/Legend
            dbc.Col([
                # Dashboard title for Demo
                html.H5("Implementation Notes", className="mb-4 text-center text-secondary"),
                
                # Notes Card
                dbc.Card([
                    dbc.CardHeader("Key Features", className="fw-bold"),
                    dbc.CardBody([
                        html.Ul([
                            html.Li("Displays T2D Pulse score trend over 30 market days"),
                            html.Li("Color-coded zones match T2D Pulse categories"),
                            html.Li("Interactive hover information"),
                            html.Li("Automatically filters out weekends"),
                            html.Li("Handles gaps in data gracefully"),
                            html.Li("Visually consistent with dashboard style")
                        ])
                    ])
                ], className="mb-4 shadow-sm"),
                
                # Implementation Card
                dbc.Card([
                    dbc.CardHeader("Integration", className="fw-bold"),
                    dbc.CardBody([
                        html.P("The chart would be positioned immediately below the T2D Pulse gauge "
                             "in the main dashboard layout, providing context and historical "
                             "perspective for the current sentiment value.")
                    ])
                ], className="mb-4 shadow-sm")
            ], width=3)
        ]),
        
        # Bottom Row showing where sector cards would follow
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardBody([
                        html.H5("Sector Cards (Existing)", className="text-muted text-center"),
                        html.Div([
                            html.P("The existing sector cards would follow below this new chart component, "
                                 "maintaining the current dashboard organization.", 
                                 className="text-center text-muted")
                        ])
                    ])
                ], className="border-dashed")
            ])
        ], className="mt-3")
    ], fluid=True, className="dashboard-container py-3")
    
    return layout

# Set up the app layout
app.layout = create_mockup_layout()

# Run the app
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5050)
