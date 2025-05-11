"""
Simple test script to create a basic sector chart.
This is a diagnostic tool to help troubleshoot why charts aren't showing.
"""
import os
import pandas as pd
import plotly.graph_objects as go

def create_simple_sector_chart(sector_name="AdTech"):
    """Create a very simple sparkline chart for testing"""
    
    # Load the data
    df = pd.read_csv('data/authentic_sector_history.csv')
    
    # Convert date and sort
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')
    
    # Extract just this sector's data
    sector_data = pd.DataFrame({
        'date': df['date'],
        'score': df[sector_name]
    })
    
    # Create a super simple chart
    fig = go.Figure()
    
    # Add data line
    fig.add_trace(go.Scatter(
        x=sector_data['date'],
        y=sector_data['score'],
        mode='lines',
        line=dict(width=2, color='blue'),
    ))
    
    # Basic layout
    fig.update_layout(
        title=f"{sector_name} 30-Day Trend",
        height=400,
        width=600,
        xaxis_title="Date",
        yaxis_title="Score (0-100)",
        yaxis_range=[0, 100]
    )
    
    # Save to a file to verify it works
    fig.write_html("test_chart.html")
    
    # Also return the figure
    return fig

def main():
    """Create test charts for verification"""
    # First check if the data file exists
    if not os.path.exists('data/authentic_sector_history.csv'):
        print("ERROR: authentic_sector_history.csv file not found!")
        return
        
    # Create a test chart
    sector = "AdTech"
    print(f"Creating test chart for {sector}")
    fig = create_simple_sector_chart(sector)
    
    # Success message
    print(f"Test chart for {sector} created successfully.")
    print("Check test_chart.html file to verify it worked.")

if __name__ == "__main__":
    main()