"""
Market insights panel components for T2D Pulse dashboard
"""
from dash import html, dcc
import pandas as pd
from chart_styling import color_scheme, market_heuristics

def create_insights_panel(chart_type, data):
    """
    Create collapsible market insights panel for a specific chart
    
    Args:
        chart_type (str): Type of chart ('treasury_yield', 'inflation', 'vix', or 'pce')
        data (DataFrame): DataFrame containing the chart data
        
    Returns:
        dash component: The insights panel component
    """
    if chart_type not in market_heuristics:
        return html.Div()  # Return empty div if no heuristics for this chart
    
    # Get relevant heuristics for this chart type
    heuristics_list = market_heuristics[chart_type]
    
    # Create cards for each heuristic
    heuristic_cards = []
    
    for h in heuristics_list:
        # Check if condition is met
        try:
            condition_met = h["condition"](data)
        except Exception:
            condition_met = False
        
        # Set styling based on effect and condition
        effect_color = color_scheme["positive"] if h["effect"] == "positive" else color_scheme["negative"]
        border_style = "2px solid " + (effect_color if condition_met else "transparent")
        bg_color = f"rgba{tuple(int(effect_color.lstrip('#')[i:i+2], 16) for i in (0, 2, 4)) + (0.1,)}" if condition_met else "white"
        
        dot_style = {
            "backgroundColor": effect_color,
            "borderRadius": "50%",
            "display": "inline-block",
            "height": "12px",
            "width": "12px",
            "marginRight": "8px"
        }
        
        card = html.Div([
            html.Div([
                html.Div([
                    html.Span(style=dot_style),
                    html.Strong(h["trigger"]),
                    html.Span(" - Currently Active" if condition_met else "", 
                             style={"color": effect_color if condition_met else "inherit"})
                ], style={"display": "flex", "alignItems": "center"}),
                html.Div(h["market_response"], style={"fontSize": "14px", "marginTop": "5px"}),
                html.Div([
                    html.Span(h["effect_text"], style={"color": effect_color, "fontSize": "14px"})
                ], style={"marginTop": "5px"}),
                html.Div([
                    html.Span("Rationale: ", style={"fontWeight": "bold", "fontSize": "13px"}),
                    html.Span(h["rationale"], style={"fontSize": "13px"})
                ], style={"marginTop": "5px"}),
                html.Div([
                    html.Span("Confidence: ", style={"fontWeight": "bold", "fontSize": "13px"}),
                    html.Span(h["confidence"], style={"fontSize": "13px"})
                ], style={"marginTop": "5px"}),
            ], style={
                "padding": "10px",
                "border": border_style,
                "borderRadius": "5px",
                "marginBottom": "10px",
                "backgroundColor": bg_color,
                "transition": "all 0.3s ease"
            })
        ])
        
        heuristic_cards.append(card)
    
    # Create collapsible container
    insights_panel = html.Div([
        html.Div([
            html.Button([
                "Market Insights ",
                html.I(className="fas fa-chevron-down")
            ], id=f"{chart_type}-insights-button", 
               className="insights-button",
               style={
                   "backgroundColor": "white", 
                   "border": "1px solid #ddd",
                   "padding": "8px 15px",
                   "borderRadius": "4px",
                   "cursor": "pointer",
                   "marginTop": "10px",
                   "marginBottom": "10px",
                   "fontSize": "14px",
                   "fontWeight": "bold"
               }),
        ]),
        html.Div(
            heuristic_cards,
            id=f"{chart_type}-insights-content",
            className="insights-content",
            style={
                "backgroundColor": "#f9f9f9", 
                "padding": "15px",
                "borderRadius": "5px",
                "border": "1px solid #ddd",
                "marginTop": "5px",
                "marginBottom": "15px",
                "display": "none"  # Initially hidden
            }
        )
    ])
    
    return insights_panel