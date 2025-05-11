#!/usr/bin/env python3
"""
Fix the app.py sector sparkline issue by introducing the pre-generated
sparkline data into the app's sector card visualization.
"""

import os
import re
import shutil
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def backup_app_file():
    """Create a backup of the app.py file"""
    app_file = 'app.py'
    backup_file = f'app_backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.py'
    
    if os.path.exists(app_file):
        shutil.copy(app_file, backup_file)
        logger.info(f"Created backup of app.py as {backup_file}")
        return True
    else:
        logger.error(f"App file not found: {app_file}")
        return False

def update_app_imports():
    """Update app.py imports to include the sector_sparkline_data module"""
    app_file = 'app.py'
    
    if not os.path.exists(app_file):
        logger.error(f"App file not found: {app_file}")
        return False
    
    with open(app_file, 'r') as f:
        app_content = f.read()
    
    # Check if the import is already present
    if 'import sector_sparkline_data' in app_content:
        logger.info("App.py already imports sector_sparkline_data")
        return True
    
    # Add the import after the other imports
    import_pattern = r'import.*\n'
    last_import = list(re.finditer(import_pattern, app_content))[-1]
    last_import_pos = last_import.end()
    
    # Insert the new import after the last existing import
    new_content = (
        app_content[:last_import_pos] + 
        "import sector_sparkline_data  # Pre-generated sector sparkline data\n" + 
        app_content[last_import_pos:]
    )
    
    # Write the updated content back to the file
    with open(app_file, 'w') as f:
        f.write(new_content)
    
    logger.info("Added sector_sparkline_data import to app.py")
    return True

def fix_create_sector_sparkline():
    """Fix the create_sector_sparkline function in app.py"""
    app_file = 'app.py'
    
    if not os.path.exists(app_file):
        logger.error(f"App file not found: {app_file}")
        return False
    
    with open(app_file, 'r') as f:
        app_content = f.read()
    
    # Find the create_sector_sparkline function
    sparkline_pattern = r'def create_sector_sparkline\(sector_name, current_score=50\):.*?\)'
    match = re.search(sparkline_pattern, app_content, re.DOTALL)
    
    if not match:
        logger.error("Could not find create_sector_sparkline function in app.py")
        return False
    
    # Get the entire function
    function_start = match.start()
    # Find the end of the function (look for the next def or the end of the file)
    next_def_match = re.search(r'\ndef ', app_content[function_start+10:])
    
    if next_def_match:
        function_end = function_start + 10 + next_def_match.start()
    else:
        # If there's no next function, assume it's the end of the file
        function_end = len(app_content)
    
    old_function = app_content[function_start:function_end]
    
    # Create the updated function
    new_function = """def create_sector_sparkline(sector_name, current_score=50):
    """
    Create a simple sparkline chart for a sector showing 30-day trend
    
    Args:
        sector_name (str): The name of the sector
        current_score (float): The current score for coloring the endpoint
        
    Returns:
        Figure: A plotly figure object for the sparkline
    """
    # Use pre-generated sparkline data if available
    if hasattr(sector_sparkline_data, 'SECTOR_SPARKLINE_DATA') and sector_name in sector_sparkline_data.SECTOR_SPARKLINE_DATA:
        # Get data from the pre-generated module
        data = sector_sparkline_data.SECTOR_SPARKLINE_DATA[sector_name]
        values = data['values']
        dates = data['dates']
        
        # Create the sparkline figure
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=list(range(len(values))),
            y=values,
            mode='lines',
            line=dict(width=2, color='rgba(0, 120, 200, 0.8)'),
            hoverinfo='text',
            hovertext=[f"{date}: {value:.2f}B" for date, value in zip(dates, values)],
        ))
        
        # Add endpoint (current value) as a marker
        fig.add_trace(go.Scatter(
            x=[len(values)-1],
            y=[values[-1]],
            mode='markers',
            marker=dict(
                size=6,
                color=get_score_color(current_score),
                line=dict(width=1, color='white')
            ),
            hoverinfo='text',
            hovertext=f"Latest: {values[-1]:.2f}B",
        ))
        
        # Update layout
        fig.update_layout(
            margin=dict(l=0, r=0, t=0, b=0),
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            showlegend=False,
            xaxis=dict(
                showticklabels=False,
                showgrid=False,
                zeroline=False,
                fixedrange=True,
            ),
            yaxis=dict(
                showticklabels=False,
                showgrid=False,
                zeroline=False,
                fixedrange=True,
            ),
            height=50,
        )
        
        return fig
    else:
        # Fallback for missing data (shouldn't happen with pre-generated data)
        logger.warning(f"No sparkline data available for sector: {sector_name}")
        
        # Return an empty figure to avoid errors
        fig = go.Figure()
        fig.update_layout(
            margin=dict(l=0, r=0, t=0, b=0),
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            showlegend=False,
            xaxis=dict(
                showticklabels=False,
                showgrid=False,
                zeroline=False,
                fixedrange=True,
            ),
            yaxis=dict(
                showticklabels=False,
                showgrid=False,
                zeroline=False,
                fixedrange=True,
            ),
            height=50,
            annotations=[dict(
                text="No data",
                xref="paper",
                yref="paper",
                x=0.5,
                y=0.5,
                showarrow=False,
                font=dict(size=10, color="gray")
            )]
        )
        
        return fig"""
    
    # Replace the old function with the new one
    new_content = app_content[:function_start] + new_function + app_content[function_end:]
    
    # Write the updated content back to the file
    with open(app_file, 'w') as f:
        f.write(new_content)
    
    logger.info("Fixed create_sector_sparkline function in app.py")
    return True

def main():
    """Run all app.py updates"""
    logger.info("Starting app.py fixes for sector sparklines...")
    
    # Backup the original file
    if not backup_app_file():
        logger.error("Failed to create backup, aborting")
        return False
    
    # Update imports
    if not update_app_imports():
        logger.error("Failed to update imports, aborting")
        return False
    
    # Fix the sparkline function
    if not fix_create_sector_sparkline():
        logger.error("Failed to fix sparkline function, aborting")
        return False
    
    logger.info("All app.py fixes completed successfully")
    return True

if __name__ == "__main__":
    main()