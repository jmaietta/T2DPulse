"""
A clean implementation of the sector sentiment container function
for the T2D Pulse dashboard.
"""

import random
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from dash import html, dcc

def create_sector_cards():
    """Create sector cards with direct inline data, no external dependencies"""
    # Define sectors
    sectors = [
        "SMB SaaS", "Enterprise SaaS", "Cloud Infrastructure", "AdTech",
        "Fintech", "Consumer Internet", "eCommerce", "Cybersecurity",
        "Dev Tools / Analytics", "Semiconductors", "AI Infrastructure",
        "Vertical SaaS", "IT Services / Legacy Tech", "Hardware / Devices"
    ]
    
    # Generate simple sector data
    sector_data = []
    for sector in sectors:
        score = round(40 + random.random() * 20, 1)  # Score between 40-60
        
        # Create a dictionary for each sector
        sector_obj = {
            "sector": sector,
            "score": (score - 50) / 50,  # Raw score in -1 to +1 range
            "normalized_score": score,    # Normalized score in 0-100 range
            "stance": "Neutral" if 40 <= score <= 60 else "Bullish" if score > 60 else "Bearish",
            "tickers": ["AAPL", "MSFT", "GOOG"],
            "drivers": ["GDP Growth", "Interest Rates"]
        }
        sector_data.append(sector_obj)
    
    # Create a card for each sector
    sector_cards = []
    
    # Create scale legend first
    scale_legend = html.Div([
        html.Div([
            "Sector Sentiment Scale (0-100):"
        ], className="scale-title"),
        html.Div([
            html.Div([
                html.Span("0", className="scale-min"),
                html.Span("50", className="scale-mid"),
                html.Span("100", className="scale-max")
            ], className="scale-numbers"),
            html.Div([
                html.Div(className="scale-bar-bearish"),
                html.Div(className="scale-bar-neutral"),
                html.Div(className="scale-bar-bullish")
            ], className="scale-bars")
        ], className="scale-container"),
        html.Div([
            html.Div(["Bearish", html.Span("0-30", className="scale-range")], className="scale-label bearish"),
            html.Div(["Neutral", html.Span("30-60", className="scale-range")], className="scale-label neutral"),
            html.Div(["Bullish", html.Span("60-100", className="scale-range")], className="scale-label bullish")
        ], className="scale-labels")
    ], className="sector-scale-legend")
    
    # Filter out any T2D Pulse sector
    sector_data = [s for s in sector_data if s["sector"] != "T2D Pulse"]
    
    # Sort sectors by score (highest first)
    sector_data.sort(key=lambda x: x["normalized_score"], reverse=True)
    
    # Now create a card for each sector
    for sector_obj in sector_data:
        sector = sector_obj["sector"]
        normalized_score = sector_obj["normalized_score"]
        stance = sector_obj["stance"]
        tickers = sector_obj.get("tickers", [])
        drivers = sector_obj.get("drivers", [])
        
        # Format tickers as a comma-separated string
        ticker_str = ", ".join(tickers[:3])  # Show max 3 tickers
        
        # Determine color based on normalized score
        if normalized_score >= 60:
            color = "#28a745"  # Green for Bullish
            border_color = "var(--bs-success)"
            bg_color = "rgba(40, 167, 69, 0.05)"
        elif normalized_score <= 30:
            color = "#dc3545"  # Red for Bearish
            border_color = "var(--bs-danger)"
            bg_color = "rgba(220, 53, 69, 0.05)"
        else:
            color = "#ffc107"  # Yellow for Neutral
            border_color = "var(--bs-warning)"
            bg_color = "rgba(255, 193, 7, 0.05)"
            
        # Generate a simple sector chart with plotly
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=list(range(5)),
            y=[50, normalized_score-5, normalized_score-2, normalized_score-1, normalized_score],
            mode='lines',
            line=dict(color=color, width=2)
        ))
        fig.update_layout(
            margin=dict(l=0, r=0, t=0, b=0),
            height=100,
            width=120,
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            showlegend=False,
            xaxis=dict(showgrid=False, zeroline=False, visible=False),
            yaxis=dict(showgrid=False, zeroline=False, visible=False)
        )
            
        # Create the sector card
        card = html.Div([
            # Card header with sector name and score
            html.Div([
                html.Div([
                    html.Div(sector, className="sector-card-title", 
                             style={"fontWeight": "bold", "fontSize": "16px"}),
                    html.Div(f"Sentiment: {stance}", className="sector-stance", 
                             style={"color": color, "fontWeight": "bold", "fontSize": "14px"})
                ], className="sector-header-text"),
                
                # Score circle
                html.Div([
                    html.Div(f"{normalized_score}", className="sector-score-value", 
                             style={"fontWeight": "bold", "color": color})
                ], className="sector-score-circle", style={
                    "backgroundColor": "white",
                    "border": f"3px solid {color}",
                    "borderRadius": "50%",
                    "width": "44px",
                    "height": "44px",
                    "display": "flex",
                    "alignItems": "center",
                    "justifyContent": "center",
                    "marginLeft": "auto"
                })
            ], className="sector-card-header", style={
                "display": "flex", 
                "justifyContent": "space-between",
                "alignItems": "center",
                "borderBottom": f"1px solid {border_color}",
                "padding": "12px 15px"
            }),
            
            # Card body with tickers, drivers, and mini-chart
            html.Div([
                # Tickers and drivers section
                html.Div([
                    html.Div([
                        html.Strong("Representative tickers: ", style={"fontSize": "13px"}),
                        html.Span(ticker_str, style={"fontSize": "13px"})
                    ], className="sector-tickers", style={"marginBottom": "8px"}),
                    
                    html.Div([
                        html.Strong("Key drivers: ", style={"fontSize": "13px"}),
                        html.Span(", ".join(drivers), style={"fontSize": "13px"})
                    ], className="sector-drivers")
                ], className="sector-details", style={"flex": "1"}),
                
                # Mini chart (inline plotly figure)
                dcc.Graph(
                    figure=fig,
                    config={'displayModeBar': False},
                    className="sector-mini-chart",
                    style={"height": "100px", "width": "120px"}
                ),
                
                # Weight control elements
                html.Div([
                    html.Div([
                        html.Div("Weight:", className="weight-label", style={"marginRight": "10px"}),
                        dcc.Input(
                            id={"type": "sector-weight-input", "index": sector},
                            type="number",
                            min=0,
                            max=100,
                            step=0.1,
                            value=7.14,  # Default equal weight (100/14)
                            style={
                                "width": "70px", 
                                "border": "1px solid #ccc",
                                "borderRadius": "4px",
                                "padding": "5px",
                                "transition": "all 0.3s ease"
                            }
                        ),
                        html.Span("%", style={"marginLeft": "5px", "marginRight": "10px"}),
                        html.Button(
                            "Apply", 
                            id={"type": "sector-weight-button", "index": sector},
                            className="sector-weight-button",
                            style={
                                "backgroundColor": "#007bff",
                                "color": "white",
                                "border": "none",
                                "borderRadius": "4px",
                                "padding": "5px 18px",
                                "fontWeight": "bold",
                                "cursor": "pointer",
                                "fontSize": "14px",
                                "minWidth": "80px",
                                "boxShadow": "0 2px 4px rgba(0,0,0,0.1)",
                                "marginLeft": "5px"
                            }
                        )
                    ], className="weight-display-container", style={"display": "flex", "alignItems": "center"}),
                ], className="weight-controls", style={
                    "display": "flex",
                    "justifyContent": "space-between",
                    "alignItems": "center",
                    "marginTop": "15px",
                    "padding": "10px 0 0 0",
                    "borderTop": "1px solid #eee"
                })
                
            ], className="sector-card-body", style={
                "backgroundColor": "white", 
                "display": "flex", 
                "flexDirection": "column", 
                "justifyContent": "space-between",
                "height": "100%",
                "padding": "15px"
            })
        ], className="sector-card", style={
            "--card-colour": border_color,
            "display": "flex", 
            "flexDirection": "column",
            "backgroundColor": "white",
            "borderRadius": "8px",
            "boxShadow": "0 4px 8px rgba(0,0,0,0.1)",
            "overflow": "hidden",
            "borderTop": f"4px solid {border_color}",
            "transition": "all 0.3s ease",
            "height": "100%"
        })
        
        sector_cards.append(card)
    
    # Wrap all cards in a container
    return html.Div([
        scale_legend,
        html.Div(sector_cards, className="sector-cards-grid",
                 style={
                     "display": "grid",
                     "gridTemplateColumns": "repeat(auto-fill, minmax(300px, 1fr))",
                     "gap": "20px",
                     "marginTop": "20px"
                 })
    ])

def update_sector_sentiment_container(n):
    """Simplified sector sentiment container update function"""
    try:
        # Create sector cards directly without external dependencies
        return create_sector_cards()
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        print(f"Error updating sector sentiment container: {e}")
        print(error_details)
        return html.Div(f"Error loading sector data: {str(e)}", 
                        className="error-message",
                        style={"color": "red", "padding": "20px"})

if __name__ == "__main__":
    # Test the function
    print("Function defined successfully")