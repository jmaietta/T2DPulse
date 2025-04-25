def update_interest_rate_graph(n):
    """Generate the Federal Funds Rate chart figure"""
    if interest_rate_data.empty:
        return go.Figure().update_layout(
            title="No data available",
            height=400
        )
    
    # Filter for last 5 years
    cutoff_date = datetime.now() - timedelta(days=5*365)
    filtered_data = interest_rate_data[interest_rate_data['date'] >= cutoff_date].copy()
    
    # Create figure
    fig = go.Figure()
    
    # Add Federal Funds Rate line with consistent color scheme
    fig.add_trace(go.Scatter(
        x=filtered_data['date'],
        y=filtered_data['value'],
        mode='lines',
        name='Federal Funds Rate',
        line=dict(color=color_scheme["rates"], width=3),
    ))
    
    # Add current value annotation
    current_value = filtered_data['value'].iloc[-1]
    previous_value = filtered_data['value'].iloc[-2]
    change = current_value - previous_value
    
    # Using absolute value change (not percentage) to match key indicators
    arrow_color = color_scheme["positive"] if change < 0 else color_scheme["negative"]
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
    
    # Update layout with custom template
    fig.update_layout(
        template=custom_template,
        height=400,
        title=None,
        margin=dict(l=40, r=40, t=40, b=40),
        xaxis_title="",
        yaxis_title="Rate (%)",
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

# Update Interest Rate Container with chart and insights panel
@app.callback(
    Output("interest-rate-container", "children"),
    [Input("interval-component", "n_intervals")]
)
def update_interest_rate_container(n):
    """Update the Federal Funds Rate container to include both the graph and insights panel"""
    # Get the chart figure
    figure = update_interest_rate_graph(n)
    
    # Check for valid data and required columns
    if interest_rate_data.empty or 'value' not in interest_rate_data.columns or 'date' not in interest_rate_data.columns:
        # Return just the graph without insights panel
        print("Interest Rate data missing required columns or empty")
