from dash import Dash, html
import plotly.graph_objs as go

# Initialize the Dash app
app = Dash(__name__)

# Sample sentiment data
sentiment_score = 58.7
sentiment_category = "Moderate Growth"
sentiment_color = "#E67E22"  # Orange for Moderate Growth

# Create the layout with just a single card
app.layout = html.Div([
    html.H2("T2D Pulse Sentiment Card - Light Glow Style", style={"textAlign": "center", "margin": "40px 0 20px"}),
    
    # Single card container
    html.Div([
        # Card with light glow
        html.Div([
            # Nested content container
            html.Div([
                html.H3("T2D Pulse Sentiment", style={"textAlign": "center", "fontWeight": "bold", "marginBottom": "10px"}),
                html.Div(f"{sentiment_score}", style={"fontSize": "48px", "textAlign": "center", "fontWeight": "bold", "color": sentiment_color}),
                html.Div(sentiment_category, style={"fontSize": "20px", "textAlign": "center", "color": sentiment_color, "marginTop": "5px"})
            ], style={
                "display": "flex",
                "flexDirection": "column",
                "justifyContent": "center",
                "alignItems": "center",
                "width": "100%",
                "height": "100%",
                "backgroundColor": "white",
                "padding": "10px 0"
            })
        ], style={
            "backgroundColor": "white",
            "borderRadius": "12px",
            "boxShadow": f"0 0 10px {sentiment_color}",  # Light transparent glow
            "border": f"2px solid {sentiment_color}",  # Matching border color
            "height": "140px",
            "width": "800px",
            "margin": "0 auto"
        })
    ], style={"width": "100%", "display": "flex", "justifyContent": "center", "marginBottom": "40px"}),
    
    # Additional information
    html.Div([
        html.P("This card features:"),
        html.Ul([
            html.Li("Square shape with clean edges"),
            html.Li("White background for clean presentation"),
            html.Li("Light orange glow around the border"),
            html.Li("2px solid orange border"),
            html.Li("Centered T2D Pulse Sentiment heading"),
            html.Li("Large, bold sentiment score (58.7)"),
            html.Li("Category label below the score (Moderate Growth)")
        ]),
        html.P("The glow and border color will change based on sentiment category:")
    ], style={"maxWidth": "800px", "margin": "20px auto", "lineHeight": "1.6"}),
    
    # Color reference
    html.Div([
        html.Div([
            html.Div(category, style={"fontWeight": "bold"}),
            html.Div(style={
                "height": "20px", 
                "backgroundColor": color, 
                "width": "100%", 
                "marginTop": "5px",
                "borderRadius": "4px"
            })
        ], style={"width": "19%", "margin": "0 0.5%"})
        for category, color in [
            ("Boom", "#2ECC71"),            # Green
            ("Expansion", "#F1C40F"),       # Yellow
            ("Moderate Growth", "#E67E22"), # Orange
            ("Slowdown", "#E74C3C"),        # Light Red
            ("Contraction", "#C0392B")      # Dark Red
        ]
    ], style={"display": "flex", "maxWidth": "800px", "margin": "20px auto"})
    
], style={"fontFamily": "Arial, sans-serif", "padding": "20px", "backgroundColor": "#f8f9fa"})

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5004)