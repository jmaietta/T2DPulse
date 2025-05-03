#!/usr/bin/env python3
# option_d_revised_mockup.py - Dashboard Integration with Mini-Indicators
# -----------------------------------------------------------
# Option D: Dashboard integration with mini indicator cards

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

# Create pulse circle
def create_pulse_circle(score=55.7, size=180):
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
    
    # Create the circle component
    circle = html.Div([
        html.Div([
            html.Div(f"{score:.1f}", style={
                'fontSize': '42px',
                'fontWeight': '600',
                'color': color
            }),
            html.Div(status, style={
                'fontSize': '18px',
                'color': color
            })
        ], style={
            'display': 'flex',
            'flexDirection': 'column',
            'alignItems': 'center',
            'justifyContent': 'center',
            'width': f"{size}px",
            'height': f"{size}px",
            'borderRadius': '50%',
            'border': f'3px solid {color}',
            'boxShadow': f'0 0 15px {color}',
            'backgroundColor': 'white'
        })
    ])
    
    return circle

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

# Create sparkline chart for key indicators
def create_sparkline(trend_type='up', width=100, height=40, color=None):
    # Generate sample data points
    x = list(range(20))
    
    # Generate y values based on trend type
    if trend_type == 'up':
        y = [30 + i + np.random.normal(0, 1) for i in range(20)]
        color = color or '#2ecc71'  # Green for rising
    elif trend_type == 'down':
        y = [50 - i + np.random.normal(0, 1) for i in range(20)]
        color = color or '#e74c3c'  # Red for falling
    else:  # stable
        y = [40 + np.random.normal(0, 2) for _ in range(20)]
        color = color or '#f39c12'  # Orange for stable
    
    # Create figure
    fig = go.Figure(
        go.Scatter(
            x=x, y=y,
            mode='lines',
            line=dict(
                color=color,
                width=2,
                shape='spline'
            ),
            fill='tozeroy',
            fillcolor='rgba(243, 156, 18, 0.1)',
            hoverinfo='none'
        )
    )
    
    # Update layout for minimal appearance
    fig.update_layout(
        width=width,
        height=height,
        margin=dict(l=0, r=0, t=0, b=0),
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        xaxis=dict(
            showgrid=False,
            zeroline=False,
            showticklabels=False,
            showline=False,
            range=[0, 19]
        ),
        yaxis=dict(
            showgrid=False,
            zeroline=False,
            showticklabels=False,
            showline=False
        )
    )
    
    return fig

# Create indicator card with sparkline
def create_indicator_card(title, value, trend, trend_type='up'):
    # Determine color based on trend type
    if trend_type == 'up':
        color = '#2ecc71'  # Green
        trend_icon = '\u2197'  # Up arrow
    elif trend_type == 'down':
        color = '#e74c3c'  # Red
        trend_icon = '\u2198'  # Down arrow
    else:
        color = '#f39c12'  # Orange
        trend_icon = '\u2192'  # Right arrow
    
    # Create card
    card = html.Div([
        # Title
        html.Div(title, style={
            'fontSize': '12px',
            'color': '#777',
            'whiteSpace': 'nowrap',
            'overflow': 'hidden',
            'textOverflow': 'ellipsis'
        }),
        
        # Value with trend indicator
        html.Div([
            html.Span(value, style={
                'fontSize': '16px',
                'fontWeight': '600',
                'color': '#333'
            }),
            html.Span(f" {trend_icon} {trend}", style={
                'fontSize': '12px',
                'fontWeight': '400',
                'color': color,
                'marginLeft': '5px'
            })
        ]),
        
        # Sparkline
        dcc.Graph(
            figure=create_sparkline(trend_type, width=100, height=40, color=color),
            config={'displayModeBar': False},
            style={
                'marginTop': '5px'
            }
        )
    ], style={
        'flex': '1',
        'minWidth': '110px',
        'maxWidth': '150px',
        'padding': '10px',
        'borderRight': '1px solid #eee',
        'height': '100%'
    })
    
    return card

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
        html.H3("Option D: Dashboard Integration with Mini-Indicators", 
                style={'textAlign': 'center', 'marginBottom': '20px'}),
        
        html.P("This layout integrates the T2D Pulse score with additional key indicators in a "
               "full dashboard header experience. It includes the pulse circle, 30-day trend chart, "
               "and key economic indicators with mini-sparklines for a comprehensive view.",
               style={'textAlign': 'center', 'marginBottom': '30px', 'maxWidth': '800px', 'margin': '0 auto'}),
        
        # Main content
        html.Div([
            # Dashboard integrated header with T2D Pulse and key indicators
            html.Div([
                # Main card containing all elements
                dbc.Card([
                    # Header with logo and title
                    dbc.CardHeader([
                        html.Div([
                            # Left side: Logo and title
                            html.Div([
                                html.Img(src=encode_t2d_logo() or '', 
                                    style={'height': '40px', 'marginRight': '10px'}),
                                html.Div([
                                    html.H4('T2D PULSE', 
                                         style={'margin': '0', 'color': '#e74c3c', 'fontWeight': '600'}),
                                    html.Div('Economic Dashboard', 
                                            style={'color': '#777', 'fontSize': '14px'})
                                ])
                            ], style={
                                'display': 'flex',
                                'alignItems': 'center'
                            }),
                            
                            # Right side: Last updated info
                            html.Div([
                                f"Last updated: {datetime.now().strftime('%b %d, %Y')}"
                            ], style={
                                'color': '#888',
                                'fontSize': '12px'
                            })
                        ], style={
                            'display': 'flex',
                            'justifyContent': 'space-between',
                            'alignItems': 'center'
                        })
                    ], className="d-flex justify-content-between align-items-center"),
                    
                    # Main content area with pulse circle, chart and indicators
                    dbc.CardBody([
                        html.Div([
                            # Left panel - T2D Pulse
                            html.Div([
                                # T2D Pulse score circle
                                create_pulse_circle(55.7, 180),  # Authentic score
                                
                                # Below the circle: 7-day change
                                html.Div([
                                    html.Span("7-Day Change: ", 
                                            style={'fontSize': '14px', 'color': '#777'}),
                                    html.Span("+2.1", 
                                            style={'fontSize': '14px', 'fontWeight': '600', 'color': '#2ecc71'})
                                ], style={'textAlign': 'center', 'marginTop': '15px'})
                            ], style={
                                'width': '180px',
                                'marginRight': '20px'
                            }),
                            
                            # Middle panel - 30-day trend chart
                            html.Div([
                                # Chart title
                                html.Div("30-Day Trend", style={
                                    'fontSize': '16px',
                                    'fontWeight': '500',
                                    'marginBottom': '10px',
                                    'color': '#555'
                                }),
                                
                                # The chart
                                dcc.Graph(
                                    id='trend-chart',
                                    figure=create_trend_chart(generate_sample_pulse_data(), height=200),
                                    config={'displayModeBar': False}
                                )
                            ], style={
                                'flex': '1',
                                'minWidth': '350px',
                                'border': '1px solid #eee',
                                'borderRadius': '5px',
                                'padding': '15px',
                                'backgroundColor': '#fff'
                            }),
                            
                            # Right panel - Key indicators with sparklines
                            html.Div([
                                # Title
                                html.Div("Key Indicators", style={
                                    'fontSize': '16px',
                                    'fontWeight': '500',
                                    'marginBottom': '10px',
                                    'color': '#555',
                                    'paddingLeft': '10px'
                                }),
                                
                                # Indicators container
                                html.Div([
                                    # Indicators row
                                    html.Div([
                                        create_indicator_card("NASDAQ", "16,742", "0.5%", "up"),
                                        create_indicator_card("Fed Rate", "5.25%", "0.0%", "stable"),
                                        create_indicator_card("10Y Yield", "4.68%", "0.03%", "down")
                                    ], style={
                                        'display': 'flex',
                                        'borderBottom': '1px solid #eee'
                                    }),
                                    
                                    # Second row
                                    html.Div([
                                        create_indicator_card("CPI", "3.4%", "0.1%", "down"),
                                        create_indicator_card("VIX", "18.2", "1.2", "up"),
                                        create_indicator_card("Unemployment", "4.1%", "0.1%", "up")
                                    ], style={
                                        'display': 'flex'
                                    })
                                ], style={
                                    'border': '1px solid #eee',
                                    'borderRadius': '5px',
                                    'backgroundColor': '#fff'
                                })
                            ], style={
                                'width': '350px',
                                'marginLeft': '20px'
                            })
                        ], style={
                            'display': 'flex',
                            'flexWrap': 'wrap',
                            'alignItems': 'flex-start'
                        })
                    ])
                ], className="shadow-sm")
            ])
        ], style={
            'maxWidth': '1000px',
            'margin': '0 auto',
            'padding': '20px'
        })
    ], style={
        'padding': '20px',
        'backgroundColor': '#f8f9fa',
        'minHeight': '100vh'
    })
])

# Run the app
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5004)
