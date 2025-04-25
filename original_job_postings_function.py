def update_job_postings_graph(n):
    """Generate the Software Job Postings chart figure"""
    if job_postings_data.empty or 'yoy_growth' not in job_postings_data.columns:
        return go.Figure().update_layout(
            title="No data available",
            height=400
        )
    
    # Filter for last 3 years
    cutoff_date = datetime.now() - timedelta(days=3*365)
    filtered_data = job_postings_data[job_postings_data['date'] >= cutoff_date].copy()
    
    # Create figure
    fig = go.Figure()
    
    # Add Job Postings YoY line
    fig.add_trace(go.Scatter(
        x=filtered_data['date'],
        y=filtered_data['yoy_growth'],
        mode='lines',
        name='YoY % Change',
        line=dict(color='#4C78A8', width=3),  # Blue color for tech jobs
    ))
    
    # Add reference lines for key thresholds
    x_range = [filtered_data['date'].min(), filtered_data['date'].max()]
    
    # Add +20% Growth threshold line
    fig.add_trace(go.Scatter(
        x=x_range,
        y=[20, 20],
        mode='lines',
        line=dict(color='green', width=1, dash='dash'),
        name='Hiring Boom (20%)'
    ))
    
    # Add +5% Growth threshold line
    fig.add_trace(go.Scatter(
        x=x_range,
        y=[5, 5],
        mode='lines',
        line=dict(color='lightgreen', width=1, dash='dash'),
        name='Healthy Recovery (5%)'
    ))
    
    # Add 0% Growth threshold line
    fig.add_trace(go.Scatter(
        x=x_range,
        y=[0, 0],
        mode='lines',
        line=dict(color='gray', width=1),
        name='Neutral'
    ))
    
    # Add -5% Growth threshold line
    fig.add_trace(go.Scatter(
        x=x_range,
        y=[-5, -5],
        mode='lines',
        line=dict(color='orange', width=1, dash='dash'),
        name='Slowdown (-5%)'
    ))
    
    # Add -20% Growth threshold line
    fig.add_trace(go.Scatter(
        x=x_range,
        y=[-20, -20],
        mode='lines',
        line=dict(color='red', width=1, dash='dash'),
        name='Hiring Recession (-20%)'
    ))
    
    # Add current value annotation
    current_value = filtered_data['yoy_growth'].iloc[-1]
    previous_value = filtered_data['yoy_growth'].iloc[-2]
    change = current_value - previous_value
    
    # Using absolute value change (not percentage)
    arrow_color = 'green' if change > 0 else 'red'
    arrow_symbol = "▲" if change > 0 else "▼"
    
    current_value_annotation = f"Current: {current_value:.2f}% {arrow_symbol} {abs(change):.2f}%"
    
    fig.add_annotation(
        x=0.02,
        y=0.95,
        xref="paper",
        yref="paper",
        text=current_value_annotation,
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
        title=None,
        margin=dict(l=40, r=40, t=40, b=40),
        xaxis_title="",
        yaxis_title="Year-over-Year % Change",
        yaxis=dict(
            ticksuffix="%",
            zeroline=True,
            zerolinecolor='rgba(0,0,0,0.2)',
        ),
        hovermode="x unified",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    return fig
