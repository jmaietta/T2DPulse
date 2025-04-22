"""
Add consumer sentiment data to the T2D Pulse dashboard
"""

import os
import pandas as pd
import requests
from datetime import datetime, timedelta
import plotly.graph_objs as go

# Import from app.py
from app import fetch_fred_data, save_data_to_csv, load_data_from_csv, FRED_API_KEY
from chart_styling import custom_template, color_scheme

# Data directory
DATA_DIR = 'data'
os.makedirs(DATA_DIR, exist_ok=True)

def fetch_consumer_sentiment_data():
    """Fetch Consumer Confidence Composite Index data from FRED API
    
    Returns a DataFrame with date and value columns formatted like other FRED data.
    Uses FRED series USACSCICP02STSAM: Consumer Opinion Surveys: Composite 
    Consumer Confidence for United States
    """
    try:
        # Use Consumer Opinion Surveys: Composite Consumer Confidence
        series_id = "USACSCICP02STSAM"
        
        df = fetch_fred_data(series_id)
        
        if not df.empty:
            print(f"Successfully retrieved {len(df)} observations for Consumer Confidence Index")
            
            # Calculate year-over-year change
            df = df.sort_values('date')
            df['yoy_change'] = df['value'].pct_change(periods=12) * 100
            
            return df
        else:
            print("Error retrieving Consumer Confidence data from FRED")
            return pd.DataFrame()
    except Exception as e:
        print(f"Exception while fetching Consumer Confidence data: {str(e)}")
        return pd.DataFrame()

def create_consumer_sentiment_graph(consumer_sentiment_data):
    """Generate a graph of Consumer Sentiment data"""
    if consumer_sentiment_data.empty:
        return go.Figure().update_layout(
            title="No Consumer Sentiment data available",
            height=400
        )
    
    # Filter for last 5 years
    cutoff_date = datetime.now() - timedelta(days=5*365)
    filtered_data = consumer_sentiment_data[consumer_sentiment_data['date'] >= cutoff_date].copy()
    
    # Create figure
    fig = go.Figure()
    
    # Add Consumer Sentiment line
    fig.add_trace(
        go.Scatter(
            x=filtered_data['date'],
            y=filtered_data['value'],
            mode='lines',
            name='Consumer Sentiment Index',
            line=dict(color=color_scheme['primary'], width=3)
        )
    )
    
    # Add YoY change if available
    if 'yoy_change' in filtered_data.columns:
        # Create second y-axis for YoY change
        fig.add_trace(
            go.Scatter(
                x=filtered_data['date'],
                y=filtered_data['yoy_change'],
                mode='lines',
                name='Year-over-Year Change',
                line=dict(color=color_scheme['secondary'], width=2, dash='dot'),
                yaxis='y2'
            )
        )
        
        # Add zero line for YoY change
        fig.add_shape(
            type="line",
            x0=filtered_data['date'].min(),
            x1=filtered_data['date'].max(),
            y0=0,
            y1=0,
            line=dict(
                color=color_scheme["neutral"],
                width=1.5,
                dash="dot",
            ),
            yref="y2"
        )
    
    # Add current value annotation
    if len(filtered_data) > 0:
        current_value = filtered_data.sort_values('date', ascending=False).iloc[0]['value']
        current_yoy = filtered_data.sort_values('date', ascending=False).iloc[0]['yoy_change'] if 'yoy_change' in filtered_data.columns else None
        
        if current_yoy is not None:
            arrow_color = color_scheme["positive"] if current_yoy > 0 else color_scheme["negative"]
            arrow_symbol = "▲" if current_yoy > 0 else "▼"
            
            annotation_text = f"Current: {current_value:.1f} ({arrow_symbol} {abs(current_yoy):.1f}% YoY)"
        else:
            annotation_text = f"Current: {current_value:.1f}"
            arrow_color = "gray"
        
        fig.add_annotation(
            x=0.02,
            y=0.95,
            xref="paper",
            yref="paper",
            text=annotation_text,
            showarrow=False,
            font=dict(size=14, color=arrow_color),
            align="left",
            bgcolor="rgba(255, 255, 255, 0.9)",
            bordercolor=arrow_color,
            borderwidth=1,
            borderpad=4,
            opacity=0.9
        )
    
    # Update layout
    fig.update_layout(
        template=custom_template,
        height=400,
        title="Consumer Confidence Index",
        margin=dict(l=40, r=40, t=60, b=40),
        hovermode="x unified",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        yaxis=dict(
            title="Index Value"
        ),
        yaxis2=dict(
            title="Year-over-Year Change (%)",
            overlaying="y",
            side="right",
            showgrid=False,
            zeroline=False,
            ticksuffix="%"
        )
    )
    
    return fig

def main():
    """Main function to fetch and save consumer sentiment data and generate graph"""
    consumer_sentiment_data = load_data_from_csv('consumer_sentiment_data.csv')
    print(f"Successfully loaded {len(consumer_sentiment_data)} rows from consumer_sentiment_data.csv" 
          if not consumer_sentiment_data.empty else "No cached consumer sentiment data found")
    
    # If no existing data or data is old, fetch new data
    if consumer_sentiment_data.empty or (datetime.now() - pd.to_datetime(consumer_sentiment_data['date'].max())).days > 30:
        consumer_sentiment_data = fetch_consumer_sentiment_data()
        
        if not consumer_sentiment_data.empty:
            # Save data
            save_data_to_csv(consumer_sentiment_data, 'consumer_sentiment_data.csv')
            print(f"Consumer confidence data updated with {len(consumer_sentiment_data)} observations")
        else:
            print("Failed to fetch Consumer confidence data, using cached data if available")
    
    # Create and save the graph
    if not consumer_sentiment_data.empty:
        fig = create_consumer_sentiment_graph(consumer_sentiment_data)
        # To display the figure in a browser (if running standalone)
        # fig.show()
        
        print("Successfully created Consumer Sentiment graph")
        return fig
    else:
        print("Cannot create Consumer Sentiment graph: No data available")
        return None

if __name__ == "__main__":
    main()