from dash import Dash, html, callback, Output, Input

external_stylesheets = [
    {
        "href": "https://cdnjs.cloudflare.com/ajax/libs/normalize/8.0.1/normalize.min.css",
        "rel": "stylesheet",
    }
]

app = Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = html.Div([
    # --- Sector Sentiments Section ---
    html.Div([
        html.H2("Sector Sentiments", style={"textAlign": "center", "fontSize": "2em", "marginBottom": "20px"}),
        html.Div([
            html.Div([
                html.H3("Cloud Infrastructure"),
                html.P("+0.3", style={"fontSize": "1.5em"})
            ], style={"flex": "1 1 250px", "background": "#f9f9f9", "padding": "20px", "borderRadius": "12px",
                      "boxShadow": "0 2px 5px rgba(0,0,0,0.1)", "textAlign": "center"}),

            html.Div([
                html.H3("Fintech"),
                html.P("-0.1", style={"fontSize": "1.5em"})
            ], style={"flex": "1 1 250px", "background": "#f9f9f9", "padding": "20px", "borderRadius": "12px",
                      "boxShadow": "0 2px 5px rgba(0,0,0,0.1)", "textAlign": "center"}),

        ], style={
            "display": "flex", "flexWrap": "wrap", "gap": "20px", "justifyContent": "center"
        })
    ], style={"width": "100%", "maxWidth": "1200px", "margin": "0 auto"}),

    # --- Toggle Button ---
    html.Div([
        html.Button(
            "Show Key Indicators ▼",
            id="toggle-button",
            n_clicks=0,
            style={"padding": "10px 20px", "fontSize": "1em", "borderRadius": "8px",
                   "border": "none", "backgroundColor": "#007BFF", "color": "white",
                   "cursor": "pointer", "marginTop": "40px"}
        )
    ], style={"textAlign": "center"}),

    # --- Key Indicators Section ---
    html.Div([
        html.H3("Key Indicators", style={"textAlign": "center", "fontSize": "1.5em", "color": "#666", "marginTop": "30px"}),
        html.Div([
            html.Div([
                html.Div("GDP Growth"),
                html.Div("2.3%", style={"marginTop": "6px", "fontWeight": "bold", "fontSize": "0.85em"})
            ], className="indicator-card"),

            html.Div([
                html.Div("10Y Treasury"),
                html.Div("4.15%", style={"marginTop": "6px", "fontWeight": "bold", "fontSize": "0.85em"})
            ], className="indicator-card"),

            html.Div([
                html.Div("VIX"),
                html.Div("16.2", style={"marginTop": "6px", "fontWeight": "bold", "fontSize": "0.85em"})
            ], className="indicator-card"),

            html.Div([
                html.Div("Unemployment"),
                html.Div("3.9%", style={"marginTop": "6px", "fontWeight": "bold", "fontSize": "0.85em"})
            ], className="indicator-card"),

            html.Div([
                html.Div("PCE Inflation"),
                html.Div("2.8%", style={"marginTop": "6px", "fontWeight": "bold", "fontSize": "0.85em"})
            ], className="indicator-card"),
        ], id="keyIndicatorsCards", className="key-indicators-grid", style={"marginTop": "20px"})
    ], id="key-indicators-section", style={
        "height": "0px", "overflow": "hidden", "opacity": 0,
        "transition": "height 0.6s ease, opacity 0.6s ease",
        "width": "100%", "maxWidth": "1200px", "margin": "0 auto"
    })
])

# --- Callback for toggling ---
@callback(
    Output("key-indicators-section", "style"),
    Output("toggle-button", "children"),
    Input("toggle-button", "n_clicks"),
)
def toggle_key_indicators(n_clicks):
    if n_clicks % 2 == 1:
        return {
            "height": "600px", "overflow": "hidden", "opacity": 1,
            "transition": "height 0.6s ease, opacity 0.6s ease",
            "width": "100%", "maxWidth": "1200px", "margin": "0 auto"
        }, "Hide Key Indicators ▲"
    else:
        return {
            "height": "0px", "overflow": "hidden", "opacity": 0,
            "transition": "height 0.6s ease, opacity 0.6s ease",
            "width": "100%", "maxWidth": "1200px", "margin": "0 auto"
        }, "Show Key Indicators ▼"

# --- Custom CSS for Mobile Optimization ---
app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>T2D Pulse</title>
        {%favicon%}
        {%css%}
        <style>
        .key-indicators-grid {
            display: flex;
            flex-wrap: wrap;
            gap: 10px;
            justify-content: center;
        }
        .indicator-card {
            flex: 1 1 120px;
            background: #efefef;
            padding: 12px;
            border-radius: 8px;
            text-align: center;
            font-size: 0.85em;
            color: #555;
        }
        @media (max-width: 768px) {
            .indicator-card {
                flex: 1 1 100%;
            }
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

if __name__ == '__main__':
    app.run_server(debug=True)
