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
        hovertemplate='%{y:.1f}%<extra></extra>'  # Show only the YOY percentage value
    ))
    
    # Add reference lines for key thresholds
    x_range = [filtered_data['date'].min(), filtered_data['date'].max()]
    
    # Add +20% Growth threshold line
    fig.add_trace(go.Scatter(
        x=x_range,
        y=[20, 20],
        mode='lines',
        line=dict(color='green', width=1, dash='dash'),
        name='Hiring Boom (20%)',
        hoverinfo='skip'  # Don't show hover for threshold lines
    ))
    
    # Add +5% Growth threshold line
    fig.add_trace(go.Scatter(
        x=x_range,
        y=[5, 5],
        mode='lines',
        line=dict(color='lightgreen', width=1, dash='dash'),
        name='Healthy Recovery (5%)',
        hoverinfo='skip'  # Don't show hover for threshold lines
    ))
    
    # Add 0% Growth threshold line
    fig.add_trace(go.Scatter(
        x=x_range,
        y=[0, 0],
        mode='lines',
        line=dict(color='gray', width=1),
        name='Neutral',
        hoverinfo='skip'  # Don't show hover for threshold lines
    ))
    
    # Add -5% Growth threshold line
    fig.add_trace(go.Scatter(
        x=x_range,
        y=[-5, -5],
        mode='lines',
        line=dict(color='lightcoral', width=1, dash='dash'),
        name='Moderate Decline (-5%)',
        hoverinfo='skip'  # Don't show hover for threshold lines
    ))
    
    # Add -20% Growth threshold line
    fig.add_trace(go.Scatter(
        x=x_range,
        y=[-20, -20],
        mode='lines',
        line=dict(color='red', width=1, dash='dash'),
        name='Hiring Freeze (-20%)',
        hoverinfo='skip'  # Don't show hover for threshold lines
    ))
    
    # Add current value annotation only showing YOY % change
    if not filtered_data.empty:
        current_value = filtered_data['yoy_growth'].iloc[-1]
        
        # If we have at least two data points, calculate the change
        if len(filtered_data) >= 2:
            previous_value = filtered_data['yoy_growth'].iloc[-2]
            change = current_value - previous_value
            
            # Determine arrow direction and color
            arrow_color = color_scheme["positive"] if change > 0 else color_scheme["negative"]
            arrow_symbol = "▲" if change > 0 else "▼"
            
            # Only show the YOY percentage, not the change indicator
            current_value_annotation = f"YoY Change: {current_value:.1f}%"
        else:
            current_value_annotation = f"YoY Change: {current_value:.1f}%"
            arrow_color = color_scheme["neutral"]
        
        fig.add_annotation(
            x=0.02,
            y=0.95,
            xref="paper",
            yref="paper",
            text=current_value_annotation,
            showarrow=False,
            font=dict(size=14, color=arrow_color),
            align="left",
            bgcolor="rgba(255, 255, 255, 1.0)",  # Full opacity white background
            bordercolor=arrow_color,
            borderwidth=1,
            borderpad=4,
            opacity=1.0  # Full opacity
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
        # Make hover label more legible with solid white background
        hoverlabel=dict(
            bgcolor="white",
            font_size=14,
            font_family="Arial",
            bordercolor="#cccccc",
            namelength=-1  # Show full variable name
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    return fig