"""
Chart styling and market heuristics data for T2D Pulse dashboard
"""

# Define custom template for consistent chart styling
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

# Standardized color scheme for consistent indicator coloring
color_scheme = {
    "growth": "#3366CC",      # Blue for growth indicators (GDP, PCE, etc.)
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

# Market heuristics data
# This mapping connects indicators to their relevant market heuristics
market_heuristics = {
    "treasury_yield": [
        {
            "trigger": "10Y ↑ > 4.0%",
            "market_response": "Bond prices fall, equity valuations compressed",
            "effect": "negative",
            "effect_text": "Valuation pressure on long-duration tech (e.g., growth stocks)",
            "rationale": "Higher discount rate → lower DCF values for growth",
            "confidence": "High",
            "condition": lambda data: data['value'].iloc[-1] > 4.0 and data['value'].iloc[-1] > data['value'].iloc[-2],
        },
        {
            "trigger": "10Y ↓ < 3.0%",
            "market_response": "Risk-on rally, multiple expansion",
            "effect": "positive",
            "effect_text": "Tailwind for tech stocks",
            "rationale": "Lower discount rates boost forward valuations",
            "confidence": "High",
            "condition": lambda data: data['value'].iloc[-1] < 3.0 and data['value'].iloc[-1] < data['value'].iloc[-2],
        },
        {
            "trigger": "Fed Hike Surprise",
            "market_response": "Equities sell off, dollar strengthens",
            "effect": "negative",
            "effect_text": "Rotation out of tech",
            "rationale": "Signals hawkish Fed, rate-sensitive sectors hurt",
            "confidence": "High",
            "condition": lambda data: False,  # This would require external data about Fed surprises
        },
        {
            "trigger": "Fed Cut Surprise",
            "market_response": "Equities rally, bond yields drop",
            "effect": "positive",
            "effect_text": "Tech outperforms",
            "rationale": "Growth sectors favored when liquidity returns",
            "confidence": "High",
            "condition": lambda data: False,  # This would require external data about Fed surprises
        },
    ],
    "inflation": [
        {
            "trigger": "CPI > 4.0%",
            "market_response": "Yields spike, Fed expected to act",
            "effect": "negative",
            "effect_text": "Margin pressure + multiple compression",
            "rationale": "Cost inflation + rate hikes",
            "confidence": "High",
            "condition": lambda data: data['inflation'].iloc[-1] > 4.0,
        },
        {
            "trigger": "CPI < 2.5%",
            "market_response": "Easing inflation concerns",
            "effect": "positive",
            "effect_text": "Supports tech sector valuations",
            "rationale": "Lower inflation boosts real earnings growth",
            "confidence": "Medium-High",
            "condition": lambda data: data['inflation'].iloc[-1] < 2.5,
        },
        {
            "trigger": "CPI decline > 0.3%",
            "market_response": "Bonds rally, rate hike odds fall",
            "effect": "positive",
            "effect_text": "Tech sector rebounds",
            "rationale": "Repricing of future cash flows with lower discount rates",
            "confidence": "Medium-High",
            "condition": lambda data: data['inflation'].iloc[-1] < data['inflation'].iloc[-2] and (data['inflation'].iloc[-2] - data['inflation'].iloc[-1]) > 0.3,
        },
        {
            "trigger": "CPI Uptrend (3+ months)",
            "market_response": "Bond yields rise, equity risk premiums increase",
            "effect": "negative",
            "effect_text": "Headwind for tech sector growth stocks",
            "rationale": "Rising inflation trend increases discount rates on future earnings",
            "confidence": "Medium",
            "condition": lambda data: data['inflation'].iloc[-1] > data['inflation'].iloc[-2] > data['inflation'].iloc[-3] if len(data) >= 3 else False,
        },
    ],
    "vix": [
        {
            "trigger": "VIX > 25",
            "market_response": "Risk-off environment",
            "effect": "negative", 
            "effect_text": "Tech de-risked in favor of defensives",
            "rationale": "High-beta sectors like tech get hit hardest",
            "confidence": "High",
            "condition": lambda data: data['value'].iloc[-1] > 25,
        },
        {
            "trigger": "VIX < 15",
            "market_response": "Stable conditions",
            "effect": "positive",
            "effect_text": "Favorable for tech flows",
            "rationale": "Risk-on sentiment fuels growth stocks",
            "confidence": "High",
            "condition": lambda data: data['value'].iloc[-1] < 15,
        },
        {
            "trigger": "VIX Spike (>+5 pts in 1 week)",
            "market_response": "Flight to safety",
            "effect": "negative",
            "effect_text": "Short-term tech pullback likely",
            "rationale": "Hedge funds delever and rotate",
            "confidence": "Medium",
            "condition": lambda data: (data['value'].iloc[-1] - data['value'].iloc[-5]) > 5 if len(data) >= 5 else False,
        },
    ],
    "pce": [
        {
            "trigger": "PCE (Real) ↑ > 4% YoY",
            "market_response": "Signals strong demand",
            "effect": "positive",
            "effect_text": "Boost for B2C tech like eComm, AdTech",
            "rationale": "More spending = more digital activity",
            "confidence": "Medium",
            "condition": lambda data: data['value'].iloc[-1] > 4.0,
        },
    ],
    "pcepi": [
        {
            "trigger": "PCEPI > 3.5%",
            "market_response": "Fed remains hawkish",
            "effect": "negative",
            "effect_text": "Delays Fed pivot = bad for long-duration tech",
            "rationale": "Fed's preferred measure showing sustained inflation",
            "confidence": "High",
            "condition": lambda data: data['yoy_growth'].iloc[-1] > 3.5,
        },
        {
            "trigger": "PCEPI < 2.5%",
            "market_response": "Fed more dovish",
            "effect": "positive",
            "effect_text": "Tailwind for tech valuations",
            "rationale": "Inflation approaching Fed target allows for rate cuts",
            "confidence": "Medium-High",
            "condition": lambda data: data['yoy_growth'].iloc[-1] < 2.5,
        },
        {
            "trigger": "PCEPI Downtrend (3 months)",
            "market_response": "Market prices in sooner rate cuts",
            "effect": "positive",
            "effect_text": "Supports multiple expansion in tech",
            "rationale": "Declining inflation trend = falling discount rates",
            "confidence": "Medium",
            "condition": lambda data: data['yoy_growth'].iloc[-1] < data['yoy_growth'].iloc[-2] < data['yoy_growth'].iloc[-3] if len(data) >= 3 else False,
        },
    ]
}