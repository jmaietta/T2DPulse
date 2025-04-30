import dash
from dash import html, dcc, Input, Output, MATCH
import dash_bootstrap_components as dbc
import plotly.graph_objs as go
import pandas as pd
from datetime import datetime, timedelta
import sentiment_engine

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# Define your sectors here
sectors = [
    {"id": "tech",   "name": "Technology"},
    {"id": "fin",    "name": "Financials"},
    {"id": "health", "name": "Healthcare"},
    # Add more sectors as needed
]

def get_historical_sector_sentiment(sector_id: str, days: int) -> pd.DataFrame:
    """
    Returns a DataFrame with columns ['date','score'] for the last `days`
    calendar days (Mon–Fri only), by replaying your existing scoring routine.
    """
    today = datetime.today()
    dates = []
    curr = today
    # Collect the last `days` weekdays
    while len(dates) < days:
        if curr.weekday() < 5:  # Mon–Fri only
            dates.append(curr)
        curr -= timedelta(days=1)
    dates = sorted(dates)

    # Compute the score for each date
    scores = []
    for d in dates:
        raw_score = sentiment_engine.score_sector_on_date(sector_id, d)
        # Normalize from -1..+1 to 0..100 if needed
        norm_score = ((raw_score + 1.0) / 2.0) * 100
        scores.append(norm_score)

    return pd.DataFrame({"date": dates, "score": scores})

# Build one card per sector
cards = []
for sec in sectors:
    cards.append(
        dbc.Card(
            [
                dbc.CardHeader(sec["name"]),
                dbc.CardBody(
                    dcc.Graph(
                        id={"type": "sector-trend-chart", "index": sec["id"]},
                        figure=go.Figure(),  # start blank
                        config={"displayModeBar": False},
                        style={"height": "85px", "width": "100%"}
                    )
                ),
            ],
            style={"width": "200px", "margin": "5px"}
        )
    )

app.layout = dbc.Container(
    [
        html.Div(cards, style={"display": "flex", "flexWrap": "wrap"}),
        dcc.Interval(id="interval-component", interval=24*60*60*1000, n_intervals=0)
    ],
    fluid=True
)

@app.callback(
    Output({"type": "sector-trend-chart", "index": MATCH}, "figure"),
    [Input("interval-component", "n_intervals"),
     Input({"type": "sector-trend-chart", "index": MATCH}, "id")]
)
def update_sector_trend_chart(n_intervals, chart_id):
    sector = chart_id["index"]
    days = 30  # adjust to 10, 20, or 30 as needed
    df = get_historical_sector_sentiment(sector, days)

    fig = go.Figure(
        go.Scatter(x=df["date"], y=df["score"], mode="lines", line=dict(width=2))
    )
    fig.update_layout(
        margin=dict(l=0, r=0, t=0, b=0),
        xaxis={"visible": False},
        yaxis={"visible": False},
    )
    return fig

if __name__ == "__main__":
    app.run_server(debug=True)
