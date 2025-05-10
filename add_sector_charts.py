#!/usr/bin/env python3
"""
Add sector charts to app.py in a very simple way (direct embed).
"""

# Import app.py
with open('app.py', 'r') as f:
    app_content = f.read()

# Define pattern to find
target_pattern = 'html.Div(["Trend:", '

# Check if pattern exists
if target_pattern not in app_content:
    print("Error: Target pattern not found in app.py")
    exit(1)

# Add HTML for chart display
updated_content = app_content.replace(
    '                    # Tickers with label',
    """                    # Sector chart
                    html.Iframe(
                        srcDoc=open(f"data/sector_chart_{sector.replace(' ', '_').replace('/', '_')}.html", 'r').read() if os.path.exists(f"data/sector_chart_{sector.replace(' ', '_').replace('/', '_')}.html") else "",
                        style={
                            'width': '100%',
                            'height': '40px',
                            'border': 'none',
                            'padding': '0',
                            'margin': '0 0 10px 0',
                        }
                    ),
                    
                    # Tickers with label"""
)

# Write updated content back to app.py
with open('app.py', 'w') as f:
    f.write(updated_content)

print("Successfully added sector charts to app.py")