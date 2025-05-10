"""
Patch the broken sector sentiment function in app.py
"""
import os
import sys

def patch_app_file():
    """Replace the problematic update_sector_sentiment_container function in app.py"""
    # Path to the app.py file
    app_path = 'app.py'
    
    # Find the location of the function to replace
    start_pattern = "@app.callback(\n    Output(\"sector-sentiment-container\", \"children\"),"
    end_pattern = "    return html.Div([\n        scale_legend,"
    
    # Read the original file
    with open(app_path, 'r') as f:
        content = f.read()
    
    # Find the start of the function
    start_pos = content.find(start_pattern)
    if start_pos == -1:
        print("Error: Could not find the start of the function to replace")
        sys.exit(1)
    
    # Find where to insert the fixed function
    fixed_function = """@app.callback(
    Output("sector-sentiment-container", "children"),
    [Input("interval-component", "n_intervals")]
)
def update_sector_sentiment_container(n):
    """Update the Sector Sentiment container with cards for each technology sector"""
    try:
        # Use the simplified sector sentiment implementation
        import sector_fix
        return sector_fix.create_fixed_sector_sentiment_container()
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        print(f"Error updating sector sentiment container: {e}")
        print(error_details)
        return html.Div([
            html.H3("Error loading sector data"),
            html.P(str(e)),
        ], style={"color": "red", "padding": "20px"})
"""
    
    # Find where the original function definition starts
    # Look for a context pattern before the actual callback
    context_before = content[:start_pos].rfind("\n\n")
    if context_before == -1:
        context_before = 0
        
    # Find where the original function ends - look for a new callback
    # or the next function definition after the start
    next_callback = content.find("@app.callback", start_pos + len(start_pattern))
    if next_callback == -1:
        # If no next callback, search for the next function def
        next_callback = content.find("\ndef ", start_pos + len(start_pattern))
    
    if next_callback == -1:
        print("Error: Could not find the end of the function to replace")
        sys.exit(1)
    
    # Create the patched content
    patched_content = (
        content[:context_before] + 
        "\n\n" + fixed_function + 
        content[next_callback:]
    )
    
    # Backup the original file
    backup_path = app_path + '.bak'
    os.rename(app_path, backup_path)
    
    # Write the patched file
    with open(app_path, 'w') as f:
        f.write(patched_content)
    
    print(f"Successfully patched {app_path}")
    print(f"Backup saved to {backup_path}")

if __name__ == "__main__":
    patch_app_file()