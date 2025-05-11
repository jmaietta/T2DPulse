"""
Update sector sentiment scores for the AI Infrastructure sector.

This script calculates and updates sentiment scores for the AI Infrastructure sector
based on recent market cap trends.
"""
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import matplotlib.pyplot as plt

def get_db_connection():
    """Connect to the database."""
    return sqlite3.connect("market_cap_data.db")

def calculate_sentiment(market_caps_df):
    """
    Calculate sentiment score based on:
    1. Recent market cap growth rate
    2. Relative performance to other sectors
    3. Current market cap compared to historical average
    """
    # 1. Recent market cap growth rate (last 5 days)
    if len(market_caps_df) >= 5:
        recent_data = market_caps_df.sort_values('date', ascending=False).head(5)
        oldest = recent_data.iloc[-1]['market_cap']
        newest = recent_data.iloc[0]['market_cap']
        
        if oldest > 0:
            growth_rate = (newest - oldest) / oldest
        else:
            growth_rate = 0
    else:
        growth_rate = 0
    
    # 2. Current market cap compared to 30-day average
    if len(market_caps_df) > 0:
        current = market_caps_df.iloc[-1]['market_cap']
        avg = market_caps_df['market_cap'].mean()
        
        if avg > 0:
            relative_to_avg = current / avg - 1
        else:
            relative_to_avg = 0
    else:
        relative_to_avg = 0
    
    # Calculate sentiment score (0-100 scale)
    # Growth rate contributes 60%, relative position contributes 40%
    growth_component = min(max(50 + growth_rate * 200, 0), 100) * 0.6
    average_component = min(max(50 + relative_to_avg * 150, 0), 100) * 0.4
    
    sentiment_score = growth_component + average_component
    
    return round(sentiment_score, 1)

def get_sector_market_caps(conn, sector_id, days=30):
    """Get market cap data for a sector for the last N days."""
    cursor = conn.cursor()
    
    # Calculate the date N days ago
    today = datetime.now()
    start_date = (today - timedelta(days=days)).strftime('%Y-%m-%d')
    
    # Get market cap data
    cursor.execute(
        """
        SELECT date, market_cap 
        FROM sector_market_caps 
        WHERE sector_id = ? AND date >= ? 
        ORDER BY date
        """,
        (sector_id, start_date)
    )
    
    rows = cursor.fetchall()
    return pd.DataFrame(rows, columns=['date', 'market_cap'])

def update_sector_sentiment(conn, sector_id, sentiment_score, date):
    """Update the sentiment score for a sector on a specific date."""
    cursor = conn.cursor()
    
    # Check if there's already a row for this sector and date
    cursor.execute(
        """
        SELECT id FROM sector_market_caps 
        WHERE sector_id = ? AND date = ?
        """,
        (sector_id, date)
    )
    
    result = cursor.fetchone()
    
    if result:
        # Update existing row
        cursor.execute(
            """
            UPDATE sector_market_caps 
            SET sentiment_score = ? 
            WHERE sector_id = ? AND date = ?
            """,
            (sentiment_score, sector_id, date)
        )
        print(f"Updated sentiment score for sector_id={sector_id} on {date} to {sentiment_score}")
    else:
        print(f"No market cap data found for sector_id={sector_id} on {date}")
    
    conn.commit()

def plot_sentiment_history(market_caps_df, sentiment_score):
    """Plot the market cap history and sentiment score."""
    plt.figure(figsize=(12, 6))
    
    # Plot market cap history
    plt.subplot(1, 2, 1)
    plt.plot(market_caps_df['date'], market_caps_df['market_cap']/1e12, marker='o')
    plt.title('AI Infrastructure Market Cap History')
    plt.xlabel('Date')
    plt.ylabel('Market Cap (Trillion USD)')
    plt.grid(True)
    plt.xticks(rotation=45)
    
    # Plot sentiment gauge
    plt.subplot(1, 2, 2)
    sentiment_categories = [
        (0, 30, 'red', 'Bearish'),
        (30, 60, 'yellow', 'Neutral'),
        (60, 100, 'green', 'Bullish')
    ]
    
    # Create a simple gauge chart
    for start, end, color, label in sentiment_categories:
        plt.barh(0, end-start, left=start, height=0.5, color=color, alpha=0.3)
        plt.text(start + (end-start)/2, 0, label, ha='center', va='center')
    
    # Add pointer for current sentiment
    plt.scatter(sentiment_score, 0, color='blue', s=300, zorder=5, marker='v')
    plt.text(sentiment_score, 0.1, f"{sentiment_score}", ha='center')
    
    plt.xlim(0, 100)
    plt.ylim(-0.5, 0.5)
    plt.title('AI Infrastructure Sentiment Score')
    plt.axis('off')
    
    plt.tight_layout()
    plt.savefig('ai_infrastructure_sentiment.png')
    print("Saved sentiment chart to ai_infrastructure_sentiment.png")

def main():
    """Main function to update AI Infrastructure sentiment score."""
    conn = get_db_connection()
    
    # Get the sector ID for AI Infrastructure
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM sectors WHERE name = 'AI Infrastructure'")
    result = cursor.fetchone()
    
    if not result:
        print("AI Infrastructure sector not found in the database.")
        conn.close()
        return
    
    sector_id = result[0]
    print(f"Found AI Infrastructure sector with ID: {sector_id}")
    
    # Get market cap data
    market_caps_df = get_sector_market_caps(conn, sector_id)
    
    if market_caps_df.empty:
        print("No market cap data found for AI Infrastructure sector in the last 30 days.")
        conn.close()
        return
    
    # Calculate sentiment score
    sentiment_score = calculate_sentiment(market_caps_df)
    print(f"Calculated sentiment score: {sentiment_score}")
    
    # Update sentiment score for the latest date
    latest_date = market_caps_df.iloc[-1]['date']
    update_sector_sentiment(conn, sector_id, sentiment_score, latest_date)
    
    # Plot sentiment history
    plot_sentiment_history(market_caps_df, sentiment_score)
    
    conn.close()
    print("Sentiment update complete.")

if __name__ == "__main__":
    main()