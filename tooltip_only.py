"""
Minimal Working Example of CSS-only Tooltip for T2D Pulse
This focused example demonstrates the hover-based tooltip without dependencies
"""

import dash
from dash import html, dcc

app = dash.Dash(__name__)

# Create a minimal example app with just the tooltip
app.layout = html.Div([
    html.H1("T2D Pulse Tooltip Solution", style={"textAlign": "center"}),
    html.P("Hover over the info icon to see the tooltip", style={"textAlign": "center"}),
    
    html.Div([
        # Sentiment Index with info icon/tooltip
        html.Div([
            html.Div(
                "Sentiment Index",
                style={
                    "fontSize": "18px", 
                    "fontWeight": "500",
                    "marginRight": "10px",
                    "display": "inline-block"
                }
            ),
            # Tooltip container
            html.Div([
                # Info icon that triggers the tooltip
                html.Span("ⓘ", className="tooltip-icon"),
                # Tooltip content
                html.Div([
                    html.H4("Sentiment Index Categories", style={"marginTop": "0"}),
                    html.Div([
                        html.Div([
                            html.Span("Bullish (60-100): ", style={"fontWeight": "bold", "color": "#2ecc71"}),
                            "Positive outlook; favorable growth conditions for technology sector"
                        ], style={"marginBottom": "8px"}),
                        html.Div([
                            html.Span("Neutral (30-60): ", style={"fontWeight": "bold", "color": "#f39c12"}),
                            "Balanced outlook; mixed signals with both opportunities and challenges"
                        ], style={"marginBottom": "8px"}),
                        html.Div([
                            html.Span("Bearish (0-30): ", style={"fontWeight": "bold", "color": "#e74c3c"}),
                            "Negative outlook; economic headwinds likely impacting tech industry growth"
                        ])
                    ])
                ], className="tooltip-content")
            ], className="tooltip-wrapper")
        ], style={
            "textAlign": "center", 
            "display": "flex", 
            "alignItems": "center", 
            "justifyContent": "center",
            "position": "relative",
            "height": "30px",
            "margin": "30px 0 60px 0"
        }),
        
        # Explanation of how it works
        html.H2("How the CSS-only Tooltip Works:", style={"marginTop": "50px"}),
        html.Div([
            html.H3("1. HTML Structure"),
            dcc.Markdown('''
```html
<div class="tooltip-wrapper">
  <span class="tooltip-icon">ⓘ</span>
  <div class="tooltip-content">
    Tooltip content goes here
  </div>
</div>
```
            ''', style={"backgroundColor": "#f9f9f9", "padding": "15px", "borderRadius": "5px"})
        ]),
        
        html.Div([
            html.H3("2. CSS Implementation"),
            dcc.Markdown('''
```css
.tooltip-wrapper {
  position: relative;
  display: inline-block;
  cursor: pointer;
}

.tooltip-icon {
  font-size: 16px;
  color: #3498db;
}

.tooltip-content {
  visibility: hidden;
  position: absolute;
  z-index: 1000;
  width: 300px;
  background-color: white;
  color: #333;
  text-align: left;
  border-radius: 6px;
  padding: 15px;
  box-shadow: 0 5px 15px rgba(0,0,0,0.3);
  opacity: 0;
  transition: opacity 0.3s;
  
  /* Position the tooltip */
  bottom: 125%;
  left: 50%;
  margin-left: -150px; /* Half of width */
}

/* Show tooltip on hover */
.tooltip-wrapper:hover .tooltip-content {
  visibility: visible;
  opacity: 1;
}
```
            ''', style={"backgroundColor": "#f9f9f9", "padding": "15px", "borderRadius": "5px"})
        ]),
        
        html.Div([
            html.H3("3. Key Features"),
            html.Ul([
                html.Li("Uses CSS :hover pseudo-class for triggering"),
                html.Li("No JavaScript or Dash callbacks required"),
                html.Li("Smooth fade-in animation with transition property"),
                html.Li("Tooltip positioned absolutely relative to the container"),
                html.Li("Proper z-index to ensure tooltip appears above other content")
            ])
        ])
    ], style={
        "maxWidth": "800px",
        "margin": "0 auto",
        "padding": "20px",
        "fontFamily": "Arial, sans-serif"
    })
])

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5050)