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
    html.H1("T2D Pulse Sentiment - Glow Border Concept", style={"textAlign": "center", "margin": "20px 0"}),
    
    # Three mockups with different glow intensities
    html.Div([
        html.Div([
            html.H3("Light Glow Effect", style={"textAlign": "center", "marginBottom": "10px"}),
            html.Div([
                html.H3("T2D Pulse Sentiment", style={"textAlign": "center", "fontWeight": "bold", "marginBottom": "15px"}),
                html.Div("58.7", style={"fontSize": "48px", "textAlign": "center", "fontWeight": "bold", "color": "#E67E22"}),
                html.Div("Moderate Growth", style={"fontSize": "22px", "textAlign": "center", "color": "#E67E22", "marginTop": "10px"})
            ], style={
                "padding": "30px",
                "backgroundColor": "white",
                "borderRadius": "8px",
                "boxShadow": "0 0 10px rgba(230, 126, 34, 0.4)",  # Light transparent glow
                "border": "1px solid #E67E22",  # Matching border color
                "height": "250px",
                "display": "flex",
                "flexDirection": "column",
                "justifyContent": "center"
            })
        ], style={"width": "30%", "margin": "0 1.5%"}),
        
        html.Div([
            html.H3("Medium Glow Effect", style={"textAlign": "center", "marginBottom": "10px"}),
            html.Div([
                html.H3("T2D Pulse Sentiment", style={"textAlign": "center", "fontWeight": "bold", "marginBottom": "15px"}),
                html.Div("58.7", style={"fontSize": "48px", "textAlign": "center", "fontWeight": "bold", "color": "#E67E22"}),
                html.Div("Moderate Growth", style={"fontSize": "22px", "textAlign": "center", "color": "#E67E22", "marginTop": "10px"})
            ], style={
                "padding": "30px",
                "backgroundColor": "white",
                "borderRadius": "8px",
                "boxShadow": "0 0 15px rgba(230, 126, 34, 0.6)",  # Medium transparent glow
                "border": "1px solid #E67E22",  # Matching border color
                "height": "250px",
                "display": "flex",
                "flexDirection": "column",
                "justifyContent": "center"
            })
        ], style={"width": "30%", "margin": "0 1.5%"}),
        
        html.Div([
            html.H3("Strong Glow Effect", style={"textAlign": "center", "marginBottom": "10px"}),
            html.Div([
                html.H3("T2D Pulse Sentiment", style={"textAlign": "center", "fontWeight": "bold", "marginBottom": "15px"}),
                html.Div("58.7", style={"fontSize": "48px", "textAlign": "center", "fontWeight": "bold", "color": "#E67E22"}),
                html.Div("Moderate Growth", style={"fontSize": "22px", "textAlign": "center", "color": "#E67E22", "marginTop": "10px"})
            ], style={
                "padding": "30px",
                "backgroundColor": "white",
                "borderRadius": "8px",
                "boxShadow": "0 0 20px rgba(230, 126, 34, 0.8)",  # Strong transparent glow
                "border": "1px solid #E67E22",  # Matching border color
                "height": "250px",
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