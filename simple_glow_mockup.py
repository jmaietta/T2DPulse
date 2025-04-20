import dash
from dash import dcc, html
import plotly.graph_objs as go

# Define sentiment score categories with associated colors
sentiment_categories = [
    {"name": "Boom", "color": "#2ECC71"},  # Green
    {"name": "Expansion", "color": "#F1C40F"},  # Yellow
    {"name": "Moderate Growth", "color": "#E67E22"},  # Orange
    {"name": "Slowdown", "color": "#E74C3C"},  # Light Red
    {"name": "Contraction", "color": "#C0392B"}  # Dark Red
]

app = dash.Dash(__name__)

# Create the layout
app.layout = html.Div([
    html.H1("T2D Pulse Sentiment - Rectangular Glow Concept", style={"textAlign": "center", "margin": "20px 0"}),
    
    # Header with T2D logo and tagline (similar to the image)
    html.Div([
        html.Div([
            html.Div([
                html.Span("T2D", style={"color": "#E41E26", "fontWeight": "bold", "fontSize": "28px"}),
                html.Span(" Pulse", style={"color": "#3A4F66", "fontWeight": "bold", "fontSize": "28px"})
            ], style={"display": "inline-block"}),
            html.Div("Powering investment decisions with macro data and proprietary intelligence", 
                     style={"color": "#777", "fontSize": "14px", "marginTop": "5px"})
        ], style={
            "padding": "15px 20px",
            "backgroundColor": "white",
            "borderRadius": "8px",
            "boxShadow": "0 2px 5px rgba(0,0,0,0.1)",
            "width": "100%",
            "maxWidth": "800px",
            "margin": "0 auto 30px auto"
        })
    ]),
    
    # Mockup matching the provided image
    html.Div([
        # Main rectangular sentiment display with glow
        html.Div([
            html.H3("T2D Pulse Sentiment", style={"textAlign": "center", "fontWeight": "bold", "marginBottom": "10px"}),
            html.Div("58.7", style={"fontSize": "56px", "textAlign": "center", "fontWeight": "bold", "color": "#E67E22"}),
            html.Div("Moderate Growth", style={"fontSize": "22px", "textAlign": "center", "color": "#E67E22", "marginTop": "5px"})
        ], style={
            "padding": "20px 30px",
            "backgroundColor": "white",
            "borderRadius": "12px",
            "boxShadow": "0 0 15px rgba(230, 126, 34, 0.6)",  # Medium orange glow
            "border": "2px solid #E67E22",  # Matching border color
            "width": "800px",
            "height": "140px",
            "display": "flex",
            "flexDirection": "column",
            "justifyContent": "center",
            "margin": "0 auto 40px auto"
        })
    ]),
    
    # Different glow intensity options
    html.H2("Glow Intensity Options", style={"textAlign": "center", "margin": "30px 0 20px"}),
    html.Div([
        html.Div([
            html.H3("Light Glow", style={"textAlign": "center", "marginBottom": "10px"}),
            html.Div([
                html.H3("T2D Pulse Sentiment", style={"textAlign": "center", "fontWeight": "bold", "marginBottom": "10px"}),
                html.Div("58.7", style={"fontSize": "48px", "textAlign": "center", "fontWeight": "bold", "color": "#E67E22"}),
                html.Div("Moderate Growth", style={"fontSize": "20px", "textAlign": "center", "color": "#E67E22", "marginTop": "5px"})
            ], style={
                "padding": "20px",
                "backgroundColor": "white",
                "borderRadius": "12px",
                "boxShadow": "0 0 10px rgba(230, 126, 34, 0.4)",  # Light transparent glow
                "border": "2px solid #E67E22",  # Matching border color
                "height": "140px",
                "display": "flex",
                "flexDirection": "column",
                "justifyContent": "center"
            })
        ], style={"width": "30%", "margin": "0 1.5%"}),
        
        html.Div([
            html.H3("Medium Glow", style={"textAlign": "center", "marginBottom": "10px"}),
            html.Div([
                html.H3("T2D Pulse Sentiment", style={"textAlign": "center", "fontWeight": "bold", "marginBottom": "10px"}),
                html.Div("58.7", style={"fontSize": "48px", "textAlign": "center", "fontWeight": "bold", "color": "#E67E22"}),
                html.Div("Moderate Growth", style={"fontSize": "20px", "textAlign": "center", "color": "#E67E22", "marginTop": "5px"})
            ], style={
                "padding": "20px",
                "backgroundColor": "white",
                "borderRadius": "12px",
                "boxShadow": "0 0 15px rgba(230, 126, 34, 0.6)",  # Medium transparent glow
                "border": "2px solid #E67E22",  # Matching border color
                "height": "140px",
                "display": "flex",
                "flexDirection": "column",
                "justifyContent": "center"
            })
        ], style={"width": "30%", "margin": "0 1.5%"}),
        
        html.Div([
            html.H3("Strong Glow", style={"textAlign": "center", "marginBottom": "10px"}),
            html.Div([
                html.H3("T2D Pulse Sentiment", style={"textAlign": "center", "fontWeight": "bold", "marginBottom": "10px"}),
                html.Div("58.7", style={"fontSize": "48px", "textAlign": "center", "fontWeight": "bold", "color": "#E67E22"}),
                html.Div("Moderate Growth", style={"fontSize": "20px", "textAlign": "center", "color": "#E67E22", "marginTop": "5px"})
            ], style={
                "padding": "20px",
                "backgroundColor": "white",
                "borderRadius": "12px",
                "boxShadow": "0 0 20px rgba(230, 126, 34, 0.8)",  # Strong transparent glow
                "border": "2px solid #E67E22",  # Matching border color
                "height": "140px",
                "display": "flex",
                "flexDirection": "column",
                "justifyContent": "center"
            })
        ], style={"width": "30%", "margin": "0 1.5%"})
    ], style={"display": "flex", "marginBottom": "40px", "maxWidth": "1200px", "margin": "0 auto"}),
    
    # Show all sentiment categories with corresponding colors
    html.H2("All Sentiment Categories", style={"textAlign": "center", "margin": "40px 0 20px"}),
    html.Div([
        html.Div([
            html.Div([
                html.H3("T2D Pulse Sentiment", style={"textAlign": "center", "fontWeight": "bold", "marginBottom": "15px"}),
                html.Div(f"{70 - i*15}", style={"fontSize": "48px", "textAlign": "center", "fontWeight": "bold", "color": category["color"]}),
                html.Div(category["name"], style={"fontSize": "22px", "textAlign": "center", "color": category["color"], "marginTop": "10px"})
            ], style={
                "padding": "30px",
                "backgroundColor": "white",
                "borderRadius": "8px",
                "boxShadow": f"0 0 15px {category['color']}",  # Matching glow
                "border": f"1px solid {category['color']}",  # Matching border color
                "height": "250px",
                "display": "flex",
                "flexDirection": "column",
                "justifyContent": "center"
            })
        ], style={"width": "18%", "margin": "0 1%"})
        for i, category in enumerate(sentiment_categories)
    ], style={"display": "flex", "maxWidth": "1200px", "margin": "0 auto"})
], style={"fontFamily": "Arial, sans-serif", "padding": "20px", "backgroundColor": "#f8f9fa"})

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5003)