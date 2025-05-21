import os
import plotly.graph_objects as go
import pandas as pd
from sqlalchemy import create_engine

# Trend chart colors
tRENDCOL = '#2E86C1'


def create_mini_trend_chart(sector_name, height=50):
    # Connect to the database
    engine = create_engine(os.getenv('DATABASE_URL'))

    # Query last 30 trading days
    sql = (
        "SELECT date, sector_sentiment_score "
        "FROM sector_sentiment_history "
        "WHERE sector = :sector "
        "ORDER BY date ASC "
        "LIMIT 30"
    )
    df = pd.read_sql(sql, engine, params={"sector": sector_name})
    if df.empty:
        fig = go.Figure()
        fig.update_layout(height=height, margin=dict(l=0, r=0, t=0, b=0))
        return fig

    df['date'] = pd.to_datetime(df['date'])
    ymin = df['sector_sentiment_score'].min() - 2
    ymax = df['sector_sentiment_score'].max() + 2

    fig = go.Figure(go.Scatter(
        x=df['date'],
        y=df['sector_sentiment_score'],
        mode='lines',
        line=dict(color=tRENDCOL, width=2),
        hovertemplate='<b>%{x|%b %d}</b><br>Score: %{y:.1f}<extra></extra>'
    ))
    fig.update_layout(
        height=height,
        margin=dict(l=0, r=0, t=0, b=0),
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)'
    )
    fig.update_xaxes(visible=False)
    fig.update_yaxes(range=[ymin, ymax], visible=False)
    return fig


def create_combined_sector_chart(sector_names, height=400):
    engine = create_engine(os.getenv('DATABASE_URL'))
    fig = go.Figure()
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728']

    for i, sector in enumerate(sector_names):
        sql = (
            "SELECT date, sector_sentiment_score "
            "FROM sector_sentiment_history "
            "WHERE sector = :sector "
            "ORDER BY date ASC "
            "LIMIT 30"
        )
        df = pd.read_sql(sql, engine, params={"sector": sector})
        if df.empty:
            continue
        df['date'] = pd.to_datetime(df['date'])
        if len(df) < 2:
            fig.add_trace(go.Scatter(
                x=df['date'], y=df['sector_sentiment_score'],
                mode='markers', marker=dict(color=colors[i%len(colors)], size=6),
                name=sector
            ))
        else:
            fig.add_trace(go.Scatter(
                x=df['date'], y=df['sector_sentiment_score'],
                mode='lines+markers', line=dict(color=colors[i%len(colors)], width=2),
                name=sector
            ))

    fig.update_layout(
        height=height,
        margin=dict(l=40, r=40, t=40, b=40),
        plot_bgcolor='rgba(240,240,240,0.8)',
        paper_bgcolor='white',
        hovermode='closest'
    )
    fig.update_xaxes(showgrid=True, gridcolor='rgba(200,200,200,0.4)')
    fig.update_yaxes(range=[0, 100], showgrid=True, gridcolor='rgba(200,200,200,0.4)')
    return fig
