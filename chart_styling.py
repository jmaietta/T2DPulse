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
    "consumption": "#E67300"  # Orange/amber for consumption metrics
}

# Market heuristics data
# This mapping connects indicators to their relevant market heuristics
market_heuristics = {
    "gdp": [
        {
            "trigger": "Real GDP YoY > 3.5%",
            "market_response": "Optimism on growth, rotation into cyclicals",
            "effect": "neutral",
            "effect_text": "Neutral to Slight underperformance",
            "rationale": "Strong GDP growth often rotates flows into industrials, financials, and value",
            "confidence": "Medium",
            "condition": lambda data: data['yoy_growth'].iloc[-1] > 3.5,
        },
        {
            "trigger": "Real GDP YoY 1.5% – 3.5%",
            "market_response": "Stable macro backdrop",
            "effect": "positive",
            "effect_text": "Supportive for broad tech sentiment",
            "rationale": "\"Goldilocks\" zone supports top-line growth without major policy risk",
            "confidence": "High",
            "condition": lambda data: 1.5 <= data['yoy_growth'].iloc[-1] <= 3.5,
        },
        {
            "trigger": "Real GDP YoY < 1.5%",
            "market_response": "Growth scare, potential recession concerns",
            "effect": "negative",
            "effect_text": "Risk-off, tech beta punished initially",
            "rationale": "PMs rotate out of high-beta/growth sectors during downturn fears",
            "confidence": "Medium-High",
            "condition": lambda data: data['yoy_growth'].iloc[-1] < 1.5 and data['yoy_growth'].iloc[-1] >= 0,
        },
        {
            "trigger": "Real GDP < 0% (contracting)",
            "market_response": "Recession pricing begins",
            "effect": "negative",
            "effect_text": "Underperforms at onset, may bottom before cycle",
            "rationale": "Tech sells off early but often leads recovery on easing expectations",
            "confidence": "High",
            "condition": lambda data: data['yoy_growth'].iloc[-1] < 0,
        },
    ],
    "fed_funds": [
        {
            "trigger": "Hike surprise (> expected)",
            "market_response": "Bond yields jump, equities sell off",
            "effect": "negative",
            "effect_text": "Tech derided, rotation to value",
            "rationale": "Higher cost of capital compresses long-duration valuations",
            "confidence": "High",
            "condition": lambda data: len(data) >= 2 and data['value'].iloc[-1] > data['value'].iloc[-2] and (data['value'].iloc[-1] - data['value'].iloc[-2]) >= 0.25,
        },
        {
            "trigger": "Cut surprise (< expected)",
            "market_response": "Risk assets rally, growth bid returns",
            "effect": "positive",
            "effect_text": "Tech outperforms, especially high-growth names",
            "rationale": "Dovish surprise improves liquidity & forward DCF assumptions",
            "confidence": "High",
            "condition": lambda data: len(data) >= 2 and data['value'].iloc[-1] < data['value'].iloc[-2] and (data['value'].iloc[-2] - data['value'].iloc[-1]) >= 0.25,
        },
        {
            "trigger": "> 5.25% Terminal Rate",
            "market_response": "Market cautious, rates seen as \"restrictive\"",
            "effect": "negative",
            "effect_text": "Persistent headwind for growth",
            "rationale": "High-rate regime = tightening financial conditions for longer",
            "confidence": "Medium-High",
            "condition": lambda data: data['value'].iloc[-1] > 5.25,
        },
        {
            "trigger": "4.0% – 5.25% Range",
            "market_response": "\"High but expected\" zone",
            "effect": "neutral",
            "effect_text": "Mixed tech sentiment",
            "rationale": "Tech can work selectively if earnings momentum is strong",
            "confidence": "High",
            "condition": lambda data: 4.0 <= data['value'].iloc[-1] <= 5.25,
        },
        {
            "trigger": "< 4.0%",
            "market_response": "Policy viewed as more neutral/accommodative",
            "effect": "positive",
            "effect_text": "Valuation tailwind, especially mid/large-cap tech",
            "rationale": "Lower rates = multiple expansion and return of speculative appetite",
            "confidence": "High",
            "condition": lambda data: data['value'].iloc[-1] < 4.0 and data['value'].iloc[-1] >= 2.5,
        },
        {
            "trigger": "< 2.5%",
            "market_response": "Emergency/zero-rate stimulus mode",
            "effect": "positive",
            "effect_text": "Speculative growth and long-duration tech favored",
            "rationale": "Historically fuels IPOs, VC rounds, unprofitable tech rallies",
            "confidence": "High",
            "condition": lambda data: data['value'].iloc[-1] < 2.5,
        },
    ],
    "treasury_yield": [
        {
            "trigger": "> 4.25%",
            "market_response": "Sharp valuation compression, growth-to-value rotation",
            "effect": "negative",
            "effect_text": "Pressure on long-duration tech (e.g. high-growth SaaS)",
            "rationale": "Discount rate increases → future earnings worth less",
            "confidence": "High",
            "condition": lambda data: data['value'].iloc[-1] > 4.25,
        },
        {
            "trigger": "3.75% – 4.25%",
            "market_response": "Cautious optimism, valuation cap on growth",
            "effect": "neutral",
            "effect_text": "Mixed to slightly negative for tech",
            "rationale": "Still elevated, but expectations are likely priced in",
            "confidence": "High",
            "condition": lambda data: 3.75 <= data['value'].iloc[-1] <= 4.25,
        },
        {
            "trigger": "3.0% – 3.75%",
            "market_response": "\"Neutral zone\"",
            "effect": "positive",
            "effect_text": "Valuation stabilizer for tech",
            "rationale": "Range historically tolerable for risk-on positioning",
            "confidence": "Medium-High",
            "condition": lambda data: 3.0 <= data['value'].iloc[-1] <= 3.75,
        },
        {
            "trigger": "< 3.0%",
            "market_response": "Risk-on environment, easing narrative grows",
            "effect": "positive",
            "effect_text": "Multiple expansion begins for growth sectors",
            "rationale": "Lower yields = more accommodative = stronger DCF values",
            "confidence": "High",
            "condition": lambda data: data['value'].iloc[-1] < 3.0,
        },
        {
            "trigger": "Rapid move > +50bps/month",
            "market_response": "Yield shock, market-wide repricing",
            "effect": "negative",
            "effect_text": "Short-term tech selloff",
            "rationale": "Institutions rotate to safety as DCFs reset, even if earnings are strong",
            "confidence": "High",
            "condition": lambda data: len(data) >= 30 and (data['value'].iloc[-1] - data['value'].iloc[-30]) > 0.5,
        },
        {
            "trigger": "Decline > 50bps in month",
            "market_response": "Rate relief, flight to quality, dovish shift",
            "effect": "positive",
            "effect_text": "Tech outperformance (especially mega-cap)",
            "rationale": "Lower yields often interpreted as easing cycle approaching",
            "confidence": "Medium-High",
            "condition": lambda data: len(data) >= 30 and (data['value'].iloc[-30] - data['value'].iloc[-1]) > 0.5,
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
            "trigger": "CPI surprise ↓",
            "market_response": "Bonds rally, rate hike odds fall",
            "effect": "positive",
            "effect_text": "Tech rebounds",
            "rationale": "Repricing of future cash flows",
            "confidence": "Medium-High",
            "condition": lambda data: data['inflation'].iloc[-1] < data['inflation'].iloc[-2] and (data['inflation'].iloc[-2] - data['inflation'].iloc[-1]) > 0.3,
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
            "trigger": "Real PCE YoY > 4.0%",
            "market_response": "Strong consumer demand, growth optimism",
            "effect": "positive",
            "effect_text": "Tailwind for B2C tech (eComm, AdTech, SMB SaaS)",
            "rationale": "High digital activity → higher rev growth",
            "confidence": "Medium",
            "condition": lambda data: data['yoy_growth'].iloc[-1] > 4.0,
        },
        {
            "trigger": "Real PCE YoY 2.0%–4.0%",
            "market_response": "Steady economic activity",
            "effect": "positive",
            "effect_text": "Supportive for broad tech",
            "rationale": "Signals stable demand with manageable inflation pressures",
            "confidence": "High",
            "condition": lambda data: 2.0 <= data['yoy_growth'].iloc[-1] <= 4.0,
        },
        {
            "trigger": "Real PCE YoY < 2.0%",
            "market_response": "Demand slowdown concern",
            "effect": "negative",
            "effect_text": "Drag on B2C software, digital services",
            "rationale": "Weak consumption = earnings risk for eComm/AdTech",
            "confidence": "Medium-High",
            "condition": lambda data: data['yoy_growth'].iloc[-1] < 2.0 and data['yoy_growth'].iloc[-1] >= 0,
        },
        {
            "trigger": "Real PCE < 0%",
            "market_response": "Recessionary conditions",
            "effect": "negative",
            "effect_text": "Defensive posturing, tech derisking",
            "rationale": "Signals demand contraction and potential layoffs",
            "confidence": "High",
            "condition": lambda data: data['yoy_growth'].iloc[-1] < 0,
        },
    ],
    "pcepi": [
        {
            "trigger": "PCEPI YoY > 3.5%",
            "market_response": "Hawkish Fed expectations, rate fears",
            "effect": "negative",
            "effect_text": "Tech valuation compression",
            "rationale": "Rate hikes repriced into market → hurts long-duration tech",
            "confidence": "High",
            "condition": lambda data: data['yoy_growth'].iloc[-1] > 3.5,
        },
        {
            "trigger": "PCEPI YoY 2.5%–3.5%",
            "market_response": "Inflation sticky, but manageable",
            "effect": "neutral",
            "effect_text": "Neutral to Cautious",
            "rationale": "Could delay Fed pivot or suggest prolonged margin pressures",
            "confidence": "Medium-High",
            "condition": lambda data: 2.5 <= data['yoy_growth'].iloc[-1] <= 3.5,
        },
        {
            "trigger": "PCEPI YoY < 2.5%",
            "market_response": "Disinflation narrative gains ground",
            "effect": "positive",
            "effect_text": "Tech-friendly rate environment",
            "rationale": "Eases pressure on multiples and supports rotation into growth",
            "confidence": "High",
            "condition": lambda data: data['yoy_growth'].iloc[-1] < 2.5 and data['yoy_growth'].iloc[-1] >= 2.0,
        },
        {
            "trigger": "PCEPI YoY < 2.0%",
            "market_response": "Deflation fears possible",
            "effect": "neutral",
            "effect_text": "Mixed (depends on growth data)",
            "rationale": "Could raise demand concerns even as valuations improve",
            "confidence": "Medium",
            "condition": lambda data: data['yoy_growth'].iloc[-1] < 2.0,
        },
    ]
}