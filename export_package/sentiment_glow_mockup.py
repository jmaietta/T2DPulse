import dash
from dash import dcc, html
import plotly.graph_objs as go
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Define sentiment score and categories with associated colors
sentiment_scores = [
    {"score": 85, "category": "Boom", "color": "#2ECC71"},  # Green
    {"score": 65, "category": "Expansion", "color": "#F1C40F"},  # Yellow
    {"score": 45, "category": "Moderate Growth", "color": "#E67E22"},  # Orange
    {"score": 25, "category": "Slowdown", "color": "#E74C3C"},  # Light Red
    {"score": 10, "category": "Contraction", "color": "#C0392B"}  # Dark Red
]

app = dash.Dash(__name__)

app.layout = html.Div([
    html.H1("T2D Pulse Sentiment Card with Color-Matched Glow", style={"textAlign": "center", "marginBottom": "30px"}),
    
    # Create a row of all sentiment cards for comparison
    html.Div([
        # Create a card for each sentiment category
        html.Div([
            html.Div([
                # Container with vertical centering for all elements
                html.Div([
                    # Title 
                    html.H3("T2D Pulse Sentiment", 
                            style={
                                "fontSize": "22px", 
                                "fontWeight": "bold", 
                                "marginBottom": "15px", 
                                "textAlign": "center",
                                "color": "#333333"
                            }),
                    # Score value
                    html.Div([
                        html.Span(f"{item['score']}", 
                                style={
                                    "fontSize": "54px", 
                                    "fontWeight": "bold", 
                                    "color": item['color']
                                }),
                    ], style={"textAlign": "center", "marginBottom": "10px"}),
                    # Category with info icon
                    html.Div([
                        html.Span(item['category'], 
                                style={
                                    "fontSize": "22px", 
                                    "color": item['color'],
                                    "marginRight": "5px",
                                    "display": "inline-block"
                                }),
                        html.Span(
                            "ⓘ", 
                            className="info-icon",
                            style={
                                "cursor": "pointer", 
                                "fontSize": "16px", 
                                "display": "inline-block",
                                "color": "#2c3e50",
                                "verticalAlign": "text-top" 
                            }
                        )
                    ], style={
                        "textAlign": "center", 
                        "display": "flex", 
                        "alignItems": "center", 
                        "justifyContent": "center", 
                        "position": "relative",
                        "height": "30px"  # Fixed height to prevent layout shifts
                    })
                ], style={
                    "display": "flex",
                    "flexDirection": "column",
                    "justifyContent": "center",
                    "alignItems": "center",
                    "padding": "20px 0"
                })
            ], style={
                "display": "flex", 
                "alignItems": "center", 
                "justifyContent": "center",
                "height": "100%"
            }
        ], style={
                "backgroundColor": "white",
                "borderRadius": "8px",
                "padding": "20px",
                "width": "100%",
                "height": "250px",
                "boxShadow": f"0 0 15px {item['color']}",  # Color-matched glow
                "border": f"1px solid {item['color']}",     # Color-matched border
                "transition": "all 0.3s ease",
                "margin": "0 auto",
                "display": "flex",
                "alignItems": "center",
                "justifyContent": "center"
            }),
            html.H4(f"{item['category']} ({item['score']})", style={"textAlign": "center", "marginTop": "10px"})
        ], style={"width": "18%", "margin": "0 1%"})
        for item in sentiment_scores
    ], style={"display": "flex", "justifyContent": "center", "marginBottom": "50px"}),
    
    # Create a row with a single, larger mockup
    html.H2("Selected Mockup (Moderate Growth)", style={"textAlign": "center", "marginTop": "30px"}),
    html.Div([
        html.Div([
            # Container with vertical centering for all elements
            html.Div([
                # Title 
                html.H3("T2D Pulse Sentiment", 
                        style={
                            "fontSize": "26px", 
                            "fontWeight": "bold", 
                            "marginBottom": "20px", 
                            "textAlign": "center",
                            "color": "#333333"
                        }),
                # Score value
                html.Div([
                    html.Span("58.7", 
                            style={
                                "fontSize": "72px", 
                                "fontWeight": "bold", 
                                "color": "#E67E22"  # Orange for Moderate Growth
                            }),
                ], style={"textAlign": "center", "marginBottom": "15px"}),
                # Category with info icon
                html.Div([
                    html.Span("Moderate Growth", 
                            style={
                                "fontSize": "26px", 
                                "color": "#E67E22",  # Orange for Moderate Growth
                                "marginRight": "8px",
                                "display": "inline-block"
                            }),
                    html.Span(
                        "ⓘ", 
                        className="info-icon",
                        style={
                            "cursor": "pointer", 
                            "fontSize": "18px", 
                            "display": "inline-block",
                            "color": "#2c3e50",
                            "verticalAlign": "text-top" 
                        }
                    )
                ], style={
                    "textAlign": "center", 
                    "display": "flex", 
                    "alignItems": "center", 
                    "justifyContent": "center", 
                    "position": "relative",
                    "height": "40px"
                })
            ], style={
                "display": "flex",
                "flexDirection": "column",
                "justifyContent": "center",
                "alignItems": "center",
                "padding": "30px 0"
            })
        ], style={
                "display": "flex", 
                "alignItems": "center", 
                "justifyContent": "center",
                "height": "100%"
            }
        ], style={
            "backgroundColor": "white",
            "borderRadius": "10px",
            "padding": "30px",
            "width": "600px",
            "height": "300px",
            "boxShadow": "0 0 20px #E67E22",  # Orange glow for Moderate Growth
            "border": "1px solid #E67E22",    # Orange border
            "transition": "all 0.3s ease",
            "margin": "0 auto",
            "display": "flex",
            "alignItems": "center",
            "justifyContent": "center"
        })
    ], style={"display": "flex", "justifyContent": "center", "marginTop": "20px"}),
    
    # Add a section with variations of glow intensity
    html.H2("Glow Intensity Variations", style={"textAlign": "center", "marginTop": "50px"}),
    html.Div([
        # Create cards with different glow intensities
        html.Div([
            html.Div([
                html.H3("Light Glow", style={"textAlign": "center", "marginBottom": "10px"}),
                html.Div("58.7", style={
                    "fontSize": "36px",
                    "fontWeight": "bold",
                    "color": "#E67E22",
                    "textAlign": "center",
                    "padding": "30px"
                })
            ], style={
                "backgroundColor": "white",
                "borderRadius": "8px",
                "padding": "20px",
                "width": "200px",
                "height": "150px",
                "boxShadow": "0 0 10px rgba(230, 126, 34, 0.4)",  # Light glow
                "border": "1px solid #E67E22",
                "margin": "0 auto"
            })
        ], style={"width": "30%", "marginRight": "5%"}),
        
        html.Div([
            html.Div([
                html.H3("Medium Glow", style={"textAlign": "center", "marginBottom": "10px"}),
                html.Div("58.7", style={
                    "fontSize": "36px",
                    "fontWeight": "bold",
                    "color": "#E67E22",
                    "textAlign": "center",
                    "padding": "30px"
                })
            ], style={
                "backgroundColor": "white",
                "borderRadius": "8px",
                "padding": "20px",
                "width": "200px",
                "height": "150px",
                "boxShadow": "0 0 15px rgba(230, 126, 34, 0.6)",  # Medium glow
                "border": "1px solid #E67E22",
                "margin": "0 auto"
            })
        ], style={"width": "30%", "marginRight": "5%"}),
        
        html.Div([
            html.Div([
                html.H3("Strong Glow", style={"textAlign": "center", "marginBottom": "10px"}),
                html.Div("58.7", style={
                    "fontSize": "36px",
                    "fontWeight": "bold",
                    "color": "#E67E22",
                    "textAlign": "center",
                    "padding": "30px"
                })
            ], style={
                "backgroundColor": "white",
                "borderRadius": "8px",
                "padding": "20px",
                "width": "200px",
                "height": "150px",
                "boxShadow": "0 0 20px rgba(230, 126, 34, 0.8)",  # Strong glow
                "border": "1px solid #E67E22",
                "margin": "0 auto"
            })
        ], style={"width": "30%"})
    ], style={"display": "flex", "justifyContent": "center", "marginTop": "20px", "maxWidth": "800px", "margin": "0 auto"})
], style={"maxWidth": "1200px", "margin": "0 auto", "padding": "20px", "fontFamily": "Arial, sans-serif"})

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5002)