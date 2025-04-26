import dash
from dash import dcc, html
import plotly.graph_objs as go
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Create sample data for the Treasury Yield
def create_sample_data():
    start_date = datetime(2019, 1, 1)
    end_date = datetime(2024, 3, 31)
    date_range = pd.date_range(start=start_date, end=end_date, freq='M')
    
    # Sample Treasury Yield data with realistic values and patterns
    np.random.seed(42)  # For reproducibility
    values = [2.1, 2.2, 2.4, 2.3, 2.1, 1.9, 1.7, 1.6, 1.5, 1.7, 1.8, 1.9,  # 2019
              1.8, 1.5, 0.7, 0.6, 0.7, 0.8, 0.7, 0.6, 0.7, 0.8, 0.9, 0.9,  # 2020
              1.1, 1.3, 1.6, 1.7, 1.6, 1.5, 1.3, 1.3, 1.4, 1.5, 1.6, 1.5,  # 2021
              1.8, 1.9, 2.3, 2.9, 3.0, 3.1, 3.0, 2.8, 3.8, 4.1, 3.9, 3.8,  # 2022
              3.7, 3.9, 4.0, 3.5, 3.6, 3.8, 4.2, 4.3, 4.5, 4.6, 4.4, 4.2,  # 2023
              4.1, 4.2, 4.3]                                                # 2024 (partial)
    
    # Create dataframe
    df = pd.DataFrame({
        'date': date_range,
        'value': values
    })
    
    return df

# Create mock data
treasury_yield_data = create_sample_data()

# Define custom template
custom_template = {
    "layout": {
        "font": {"family": "Arial, sans-serif", "size": 12, "color": "#505050"},
        "paper_bgcolor": "rgba(0,0,0,0)",
        "plot_bgcolor": "rgba(248,249,250,1)",
        "margin": {"l": 40, "r": 40, "t": 40, "b": 40},
        "xaxis": {
            "showgrid": True,
            "gridwidth": 1,
            "gridcolor": "rgba(230,230,230,0.8)",
        },
        "yaxis": {
            "showgrid": True,
            "gridwidth": 1,
            "gridcolor": "rgba(230,230,230,0.8)",
            "zeroline": True,
            "zerolinecolor": "rgba(0,0,0,0.2)",
        },
        "legend": {
            "orientation": "h",
            "yanchor": "bottom",
            "y": 1.02,
            "xanchor": "right",
            "x": 1,
            "bgcolor": "rgba(255,255,255,0.9)",
        },
        "hovermode": "x unified",
    }
}

# Standardized color scheme
color_scheme = {
    "growth": "#3366CC",      # Blue for growth indicators (GDP, etc.)
    "inflation": "#FF9900",   # Orange for inflation metrics (CPI, PCEPI)
    "employment": "#DC3912",  # Red for employment metrics
    "market": "#7030A0",      # Purple for market indicators (NASDAQ, etc.)
    "rates": "#109618",       # Green for interest rates
    "target": "#109618",      # Green for target lines (dashed)
    "risk": "#FF0000",        # Red for risk indicators
    "positive": "#109618",    # Green for positive trends
    "negative": "#DC3912",    # Red for negative trends
    "neutral": "#999999",     # Gray for neutral reference lines
}

def create_treasury_yield_graph():
    # Filter for last 5 years
    cutoff_date = datetime.now() - timedelta(days=5*365)
    filtered_data = treasury_yield_data[treasury_yield_data['date'] >= cutoff_date].copy()
    
    # Create figure
    fig = go.Figure()
    
    # Add treasury yield line with consistent color scheme
    fig.add_trace(go.Scatter(
        x=filtered_data['date'],
        y=filtered_data['value'],
        mode='lines',
        name='10-Year Treasury Yield',
        line=dict(color=color_scheme["rates"], width=2.5),
        hovertemplate="<b>%{x|%b %Y}</b><br>%{y:.2f}%<extra></extra>"
    ))
    
    # Add optimal range shading (2-4% is often considered neutral for 10-year treasuries)
    x_range = [filtered_data['date'].min(), filtered_data['date'].max()]
    
    fig.add_trace(go.Scatter(
        x=x_range + x_range[::-1],
        y=[2, 2, 4, 4],
        fill='toself',
        fillcolor='rgba(16, 150, 24, 0.1)',  # Using rates color with transparency
        line=dict(color='rgba(16, 150, 24, 0.5)'),  # Using rates color with transparency
        hoverinfo='skip',
        name='Neutral Yield Range (2-4%)',
        showlegend=True
    ))
    
    # Add a current threshold marker for 4% (based on heuristics)
    fig.add_shape(
        type="line",
        x0=filtered_data['date'].min(),
        x1=filtered_data['date'].max(),
        y0=4.0,
        y1=4.0,
        line=dict(
            color="rgba(255, 0, 0, 0.5)",
            width=2,
            dash="dash",
        ),
    )
    
    # Add annotation for threshold
    fig.add_annotation(
        x=filtered_data['date'].max(),
        y=4.0,
        text="4.0% Threshold",
        showarrow=False,
        yshift=10,
        xshift=-5,
        font=dict(size=10, color="rgba(255, 0, 0, 0.8)"),
    )
    
    # Update layout with custom template
    fig.update_layout(
        template=custom_template,
        height=400,
        title=dict(
            text="10-Year Treasury Yield",
            font=dict(size=16),
            x=0.5,
            xanchor='center'
        ),
        xaxis_title="",
        yaxis_title="Yield (%)",
        yaxis=dict(
            ticksuffix="%",
            range=[0, max(5.0, filtered_data['value'].max() * 1.1)],
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
    )
    
    # Add current value annotation
    current_value = filtered_data['value'].iloc[-1]
    previous_value = filtered_data['value'].iloc[-2]
    change = current_value - previous_value
    change_pct = (change / previous_value) * 100
    
    arrow_color = color_scheme["positive"] if change > 0 else color_scheme["negative"]
    arrow_symbol = "▲" if change > 0 else "▼"
    
    current_value_annotation = f"Current: {current_value:.2f}% {arrow_symbol} {abs(change_pct):.2f}%"
    
    fig.add_annotation(
        x=0.02,
        y=0.98,
        xref="paper",
        yref="paper",
        text=current_value_annotation,
        showarrow=False,
        font=dict(size=14, color=arrow_color),
        align="left",
        bgcolor="rgba(255, 255, 255, 0.8)",
        bordercolor=arrow_color,
        borderwidth=1,
        borderpad=4,
        opacity=0.8
    )
    
    return fig

# Create Insights Panel
def create_insights_panel():
    # Market heuristics based on the provided slides
    heuristics = [
        {
            "trigger": "10Y ↑ > 4.0%",
            "market_response": "Bond prices fall, equity valuations compressed",
            "effect": "negative",
            "effect_text": "Valuation pressure on long-duration tech (e.g., growth stocks)",
            "rationale": "Higher discount rate → lower DCF values for growth",
            "confidence": "High",
            "condition_met": True,  # Based on our sample data
        },
        {
            "trigger": "10Y ↓ < 3.0%",
            "market_response": "Risk-on rally, multiple expansion",
            "effect": "positive",
            "effect_text": "Tailwind for tech stocks",
            "rationale": "Lower discount rates boost forward valuations",
            "confidence": "High",
            "condition_met": False,  # Based on our sample data
        },
        {
            "trigger": "Fed Hike Surprise",
            "market_response": "Equities sell off, dollar strengthens",
            "effect": "negative",
            "effect_text": "Rotation out of tech",
            "rationale": "Signals hawkish Fed, rate-sensitive sectors hurt",
            "confidence": "High",
            "condition_met": False,  # Based on our sample data
        },
        {
            "trigger": "Fed Cut Surprise",
            "market_response": "Equities rally, bond yields drop",
            "effect": "positive",
            "effect_text": "Tech outperforms",
            "rationale": "Growth sectors favored when liquidity returns",
            "confidence": "High",
            "condition_met": False,  # Based on our sample data
        },
    ]
    
    # Create cards for each heuristic
    heuristic_cards = []
    
    for h in heuristics:
        # Set styling based on effect and condition
        effect_color = color_scheme["positive"] if h["effect"] == "positive" else color_scheme["negative"]
        border_style = "2px solid " + (effect_color if h["condition_met"] else "transparent")
        bg_color = f"rgba{tuple(int(effect_color.lstrip('#')[i:i+2], 16) for i in (0, 2, 4)) + (0.1,)}" if h["condition_met"] else "white"
        
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
                    html.Span(" - Currently Active" if h["condition_met"] else "", 
                             style={"color": effect_color if h["condition_met"] else "inherit"})
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
            ], id="treasury-insights-button", 
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
            id="treasury-insights-content",
            style={
                "backgroundColor": "#f9f9f9", 
                "padding": "15px",
                "borderRadius": "5px",
                "border": "1px solid #ddd",
                "marginTop": "10px",
                "display": "block"  # In real implementation, this would be "none" initially
            }
        )
    ])
    
    return insights_panel

# Create the app layout
app = dash.Dash(__name__)

app.layout = html.Div([
    html.H1("T2D Pulse - Chart Styling Mock-up", style={"textAlign": "center"}),
    html.Div([
        dcc.Graph(
            id="treasury-yield-graph",
            figure=create_treasury_yield_graph()
        ),
        create_insights_panel()
    ], style={"maxWidth": "1000px", "margin": "0 auto", "padding": "20px"})
])

# JavaScript callbacks would be added for toggling the insights panel visibility
app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
        {%css%}
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.15.3/css/all.min.css">
        <style>
            body {
                font-family: Arial, sans-serif;
                background-color: #f8f9fa;
                margin: 0;
                padding: 0;
            }
            /* Add additional styling */
            .card {
                border-radius: 5px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                margin-bottom: 15px;
                overflow: hidden;
                transition: all 0.3s ease;
            }
            .card:hover {
                box-shadow: 0 4px 8px rgba(0,0,0,0.1);
            }
            /* Would include JS for toggling in real implementation */
        </style>
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
            <script>
                // This would toggle the insights panel in a real implementation
                document.addEventListener('DOMContentLoaded', function() {
                    const insightsButton = document.getElementById('treasury-insights-button');
                    const insightsContent = document.getElementById('treasury-insights-content');
                    
                    if (insightsButton && insightsContent) {
                        insightsButton.addEventListener('click', function() {
                            if (insightsContent.style.display === 'none') {
                                insightsContent.style.display = 'block';
                                insightsButton.querySelector('i').className = 'fas fa-chevron-up';
                            } else {
                                insightsContent.style.display = 'none';
                                insightsButton.querySelector('i').className = 'fas fa-chevron-down';
                            }
                        });
                    }
                });
            </script>
        </footer>
    </body>
</html>
'''

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5001)