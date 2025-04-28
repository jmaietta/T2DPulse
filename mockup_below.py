"""
Simple mockup showing the Key Indicators below the sector cards layout.
"""
import dash
from dash import dcc, html
import dash_bootstrap_components as dbc

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# Sample data for the mockup
t2d_pulse_score = 46.4

app.layout = html.Div([
    html.H1("Key Indicators Below Sectors - Mockup", 
            style={'textAlign': 'center', 'margin': '20px', 'color': '#2c3e50'}),
    html.Hr(),
    
    # Main container
    html.Div([
        # Header
        html.Div([
            html.H2("T2D Pulse Economic Dashboard"),
            html.Div([
                html.Span("T2D Pulse: ", style={'fontWeight': 'bold', 'marginRight': '10px'}),
                html.Span(f"{t2d_pulse_score}", 
                         style={'color': '#f39c12', 'fontWeight': 'bold', 'fontSize': '24px',
                               'border': '1px solid #ddd', 'padding': '5px 10px',
                               'borderRadius': '5px', 'boxShadow': '0 0 10px rgba(243, 156, 18, 0.5)'})
            ], style={'display': 'flex', 'alignItems': 'center', 'marginTop': '10px'})
        ], style={'borderBottom': '1px solid #eee', 'paddingBottom': '15px'}),
        
        # Sector Sentiment section - full width
        html.Div([
            html.H3("Sector Sentiment", style={'margin': '20px 0'}),
            
            # Grid of sector cards (simplified for mockup)
            html.Div([
                # Just a few sample cards
                dbc.Card([
                    dbc.CardBody([
                        html.H5("SMB SaaS", className="card-title"),
                        html.H3("62.5", style={'color': '#2ecc71', 'fontWeight': 'bold'}),
                        dbc.Badge("Bullish", color="success", className="mr-1")
                    ])
                ], style={'borderLeft': '8px solid #2ecc71', 'margin': '10px'}),
                
                dbc.Card([
                    dbc.CardBody([
                        html.H5("Enterprise SaaS", className="card-title"),
                        html.H3("54.2", style={'color': '#f39c12', 'fontWeight': 'bold'}),
                        dbc.Badge("Neutral", color="warning", className="mr-1")
                    ])
                ], style={'borderLeft': '8px solid #f39c12', 'margin': '10px'}),
                
                dbc.Card([
                    dbc.CardBody([
                        html.H5("Cloud Infrastructure", className="card-title"),
                        html.H3("71.3", style={'color': '#2ecc71', 'fontWeight': 'bold'}),
                        dbc.Badge("Bullish", color="success", className="mr-1")
                    ])
                ], style={'borderLeft': '8px solid #2ecc71', 'margin': '10px'}),
                
                dbc.Card([
                    dbc.CardBody([
                        html.H5("AdTech", className="card-title"),
                        html.H3("48.9", style={'color': '#f39c12', 'fontWeight': 'bold'}),
                        dbc.Badge("Neutral", color="warning", className="mr-1")
                    ])
                ], style={'borderLeft': '8px solid #f39c12', 'margin': '10px'}),
                
                dbc.Card([
                    dbc.CardBody([
                        html.H5("Consumer Internet", className="card-title"),
                        html.H3("29.4", style={'color': '#e74c3c', 'fontWeight': 'bold'}),
                        dbc.Badge("Bearish", color="danger", className="mr-1")
                    ])
                ], style={'borderLeft': '8px solid #e74c3c', 'margin': '10px'}),
                
                dbc.Card([
                    dbc.CardBody([
                        html.H5("eCommerce", className="card-title"),
                        html.H3("45.2", style={'color': '#f39c12', 'fontWeight': 'bold'}),
                        dbc.Badge("Neutral", color="warning", className="mr-1")
                    ])
                ], style={'borderLeft': '8px solid #f39c12', 'margin': '10px'}),
                
                # Extra cards for grid demonstration
                dbc.Card([
                    dbc.CardBody([
                        html.H5("Cybersecurity", className="card-title"),
                        html.H3("68.7", style={'color': '#2ecc71', 'fontWeight': 'bold'}),
                        dbc.Badge("Bullish", color="success", className="mr-1")
                    ])
                ], style={'borderLeft': '8px solid #2ecc71', 'margin': '10px'}),
                
                dbc.Card([
                    dbc.CardBody([
                        html.H5("AI Infrastructure", className="card-title"),
                        html.H3("78.9", style={'color': '#2ecc71', 'fontWeight': 'bold'}),
                        dbc.Badge("Bullish", color="success", className="mr-1")
                    ])
                ], style={'borderLeft': '8px solid #2ecc71', 'margin': '10px'})
                
            ], style={'display': 'grid', 
                     'gridTemplateColumns': 'repeat(auto-fill, minmax(200px, 1fr))', 
                     'gap': '10px'})
        ]),
        
        # Key Indicators section - below, full width but more compact
        html.Div([
            dbc.Card([
                dbc.CardHeader(
                    html.H5("Key Indicators", 
                           style={'textTransform': 'uppercase', 
                                 'letterSpacing': '1px', 
                                 'color': '#7f8c8d',
                                 'textAlign': 'center',
                                 'margin': 0})
                ),
                dbc.CardBody([
                    # Indicator cards in a row
                    html.Div([
                        # Indicator 1
                        dbc.Card([
                            dbc.CardBody([
                                html.Div("10-Year Treasury Yield", style={'color': '#7f8c8d', 'fontSize': '14px'}),
                                html.Div([
                                    html.Span("4.42%", style={'fontWeight': 'bold', 'fontSize': '16px'}),
                                    html.Span("▲ 0.05", style={'color': '#2ecc71', 'marginLeft': '5px', 'fontSize': '12px'})
                                ])
                            ])
                        ], style={'backgroundColor': '#f8f9fa', 'flex': '1', 'minWidth': '150px', 'margin': '0 5px'}),
                        
                        # Indicator 2
                        dbc.Card([
                            dbc.CardBody([
                                html.Div("VIX Volatility", style={'color': '#7f8c8d', 'fontSize': '14px'}),
                                html.Div([
                                    html.Span("32.48", style={'fontWeight': 'bold', 'fontSize': '16px'}),
                                    html.Span("▲ 0.82", style={'color': '#2ecc71', 'marginLeft': '5px', 'fontSize': '12px'})
                                ])
                            ])
                        ], style={'backgroundColor': '#f8f9fa', 'flex': '1', 'minWidth': '150px', 'margin': '0 5px'}),
                        
                        # Indicator 3
                        dbc.Card([
                            dbc.CardBody([
                                html.Div("NASDAQ Trend", style={'color': '#7f8c8d', 'fontSize': '14px'}),
                                html.Div([
                                    html.Span("+3.25%", style={'fontWeight': 'bold', 'fontSize': '16px'}),
                                    html.Span("▲ 0.75", style={'color': '#2ecc71', 'marginLeft': '5px', 'fontSize': '12px'})
                                ])
                            ])
                        ], style={'backgroundColor': '#f8f9fa', 'flex': '1', 'minWidth': '150px', 'margin': '0 5px'}),
                        
                        # Indicator 4
                        dbc.Card([
                            dbc.CardBody([
                                html.Div("Consumer Sentiment", style={'color': '#7f8c8d', 'fontSize': '14px'}),
                                html.Div([
                                    html.Span("61.3", style={'fontWeight': 'bold', 'fontSize': '16px'}),
                                    html.Span("▼ 2.1", style={'color': '#e74c3c', 'marginLeft': '5px', 'fontSize': '12px'})
                                ])
                            ])
                        ], style={'backgroundColor': '#f8f9fa', 'flex': '1', 'minWidth': '150px', 'margin': '0 5px'})
                    ], style={'display': 'flex', 'flexWrap': 'wrap', 'gap': '10px'})
                ])
            ], style={'marginTop': '30px'})
        ])
    ], style={'maxWidth': '1200px', 'margin': '0 auto', 'padding': '20px'})
])

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5101, debug=True)