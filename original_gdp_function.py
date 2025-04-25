def update_gdp_graph(n):
    """Generate the GDP chart figure"""
    if gdp_data.empty or 'yoy_growth' not in gdp_data.columns:
        return go.Figure().update_layout(
            title="No data available",
            height=400
        )
    
    # Filter for last 5 years
    cutoff_date = datetime.now() - timedelta(days=5*365)
    filtered_data = gdp_data[gdp_data['date'] >= cutoff_date].copy()
    
    # Create figure
    fig = go.Figure()
    
    # Add GDP Growth line with consistent color scheme
    fig.add_trace(go.Scatter(
        x=filtered_data['date'],
        y=filtered_data['yoy_growth'],
        mode='lines+markers',
        name='Real GDP Growth (YoY %)',
        line=dict(color=color_scheme["growth"], width=3),
        marker=dict(size=8)
    ))
    
    # Add zero reference line
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
    )
    
    # Add current value annotation
    if len(filtered_data) >= 2:
        current_value = filtered_data['yoy_growth'].iloc[-1]
        previous_value = filtered_data['yoy_growth'].iloc[-2]
        change = current_value - previous_value
        
        # Using absolute value change (not percentage) to match key indicators
        arrow_color = color_scheme["positive"] if change > 0 else color_scheme["negative"]
        arrow_symbol = "▲" if change > 0 else "▼"
        
        current_value_annotation = f"Current: {current_value:.1f}% {arrow_symbol} {abs(change):.1f}%"
        
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
    
    # Update layout with custom template
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
