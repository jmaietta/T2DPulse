#!/usr/bin/env python3
# option_c_mockup.py - Card Layout with Sectors Integration
# -----------------------------------------------------------
# Option C: Horizontal card layout with sectors preview

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

# Generate sample sector data
def generate_sector_data():
    sectors = [
        {"name": "Cloud Computing", "score": 68.2, "trend": "rising"},
        {"name": "Software", "score": 62.1, "trend": "steady"},
        {"name": "Hardware", "score": 44.5, "trend": "falling"},
        {"name": "Semiconductors", "score": 59.8, "trend": "rising"},
        {"name": "Cybersecurity", "score": 74.3, "trend": "rising"},
        {"name": "AI & Machine Learning", "score": 82.7, "trend": "rising"},
    ]
    return sectors

# Create the trend chart
def create_trend_chart(pulse_data, height=220):
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
        height=height,
        margin=dict(l=20, r=20, t=10, b=30),
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

# Create a mini trend line for sector cards
def create_mini_trend(width=60):
    # Generate a simple uptrend or downtrend line
    x = np.linspace(0, 10, 30)
    
    # Choose random trend type
    trend_type = np.random.choice(['up', 'down', 'stable'])
    
    if trend_type == 'up':
        y = np.linspace(40, 60, 30) + np.random.normal(0, 2, 30)
        color = '#2ecc71'  # Green for uptrend
    elif trend_type == 'down':
        y = np.linspace(60, 40, 30) + np.random.normal(0, 2, 30)
        color = '#e74c3c'  # Red for downtrend
    else:
        y = np.linspace(50, 50, 30) + np.random.normal(0, 3, 30)
        color = '#f39c12'  # Orange for stable
    
    # Create figure
    fig = go.Figure()
    
    # Add the line
    fig.add_trace(go.Scatter(
        x=x, y=y,
        mode='lines',
        line=dict(
            color=color,
            width=2
        ),
        hoverinfo='none'
    ))
    
    # Set minimal layout
    fig.update_layout(
        width=width,
        height=28,
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
            showline=False
        )
    )
    
    return fig

# Create sector preview cards
def create_sector_preview():
    sectors = generate_sector_data()
    sector_rows = []
    
    for sector in sectors:
        # Determine color based on score
        if sector['score'] >= 60:
            color = '#2ecc71'  # Green for bullish
            status = 'Bullish'
        elif sector['score'] <= 30:
            color = '#e74c3c'  # Red for bearish
            status = 'Bearish'
        else:
            color = '#f39c12'  # Orange for neutral
            status = 'Neutral'
        
        # Create a sector row
        sector_row = html.Div([
            # Sector name
            html.Div(sector['name'], style={
                'flex': '1',
                'whiteSpace': 'nowrap',
                'overflow': 'hidden',
                'textOverflow': 'ellipsis',
                'paddingRight': '5px'
            }),
            
            # Score
            html.Div(f"{sector['score']:.1f}", style={
                'fontWeight': '600',
                'color': color,
                'width': '40px',
                'textAlign': 'right'
            }),
            
            # Mini trend line
            html.Div([
                dcc.Graph(
                    figure=create_mini_trend(),
                    config={'displayModeBar': False},
                    style={'height': '28px'}
                )
            ], style={
                'width': '60px',
                'marginLeft': '8px'
            })
        ], style={
            'display': 'flex',
            'alignItems': 'center',
            'padding': '4px 0',
            'borderBottom': '1px solid #f0f0f0'
        })
        
        sector_rows.append(sector_row)
    
    return html.Div(sector_rows, style={
        'maxHeight': '200px',
        'overflowY': 'auto'
    })

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
        
        # Option C: Horizontal card layout with sectors preview
        html.Div([
            # Card with pulse score circle, trend chart, and sector preview
            dbc.Card([
                dbc.CardBody([
                    # Main content row
                    html.Div([
                        # Left column - Pulse Circle with title
                        html.Div([
                            # Section title
                            html.Div("Current Sentiment", style={
                                'fontSize': '16px',
                                'fontWeight': '500',
                                'marginBottom': '10px',
                                'color': '#555',
                                'textAlign': 'center'
                            }),
                            
                            # Circle
                            html.Div([
                                create_pulse_glow_circle(55.7, size=200)
                            ], style={
                                'display': 'flex',
                                'justifyContent': 'center'
                            })
                        ], style={
                            'width': '200px',
                            'marginRight': '20px'
                        }),
                        
                        # Middle column - Trend Chart
                        html.Div([
                            # Section title
                            html.Div("30-Day Trend", style={
                                'fontSize': '16px',
                                'fontWeight': '500',
                                'marginBottom': '10px',
                                'color': '#555'
                            }),
                            
                            # Chart
                            dcc.Graph(
                                id='trend-chart',
                                figure=create_trend_chart(generate_sample_pulse_data()),
                                config={'displayModeBar': False}
                            )
                        ], style={
                            'flex': '1',
                            'minWidth': '400px'
                        }),
                        
                        # Right column - Top Sectors Preview
                        html.Div([
                            # Section title
                            html.Div("Top Sectors", style={
                                'fontSize': '16px',
                                'fontWeight': '500',
                                'marginBottom': '10px',
                                'color': '#555'
                            }),
                            
                            # Sector preview list
                            create_sector_preview(),
                            
                            # View all link
                            html.Div([
                                html.A("View All Sectors", href="#", style={
                                    'fontSize': '13px',
                                    'color': '#3498db',
                                    'textDecoration': 'none'
                                })
                            ], style={
                                'textAlign': 'right',
                                'marginTop': '8px',
                                'paddingRight': '5px'
                            })
                        ], style={
                            'width': '250px',
                            'marginLeft': '20px',
                            'borderLeft': '1px solid #eee',
                            'paddingLeft': '15px'
                        })
                    ], style={
                        'display': 'flex',
                        'flexWrap': 'wrap'
                    })
                ])
            ], className="shadow-sm"),
        ]),
        
        # Last updated text
        html.Div([
            f"Data refreshed on {datetime.now().strftime('%b %d, %Y')}"
        ], style={
            'textAlign': 'center',
            'color': '#888',
            'fontSize': '12px',
            'marginTop': '15px'
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
    app.run_server(debug=True, host='0.0.0.0', port=5052)
