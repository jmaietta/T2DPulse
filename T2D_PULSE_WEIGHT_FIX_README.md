# T2D Pulse Dashboard Weight Fix

## Problem Description

The T2D Pulse dashboard has an issue with updating sector weights. When users change a weight and click Apply, the following problems occur:

1. **Race Condition**: The dashboard uses a chain of callbacks where one callback updates `stored-weights` and another callback reads from `stored-weights` to update the input fields and T2D Pulse score. This creates a race condition.

2. **Duplicate Callback Outputs**: Multiple callbacks target the same outputs, resulting in Dash errors like:
   ```
   Duplicate callback outputs: In the callback for output(s): stored-weights.children
   Output 0 (stored-weights.children) is already in use.
   ```

3. **UI Not Updating**: Despite weights being correctly processed in the backend, the UI does not consistently reflect these changes.

## Root Cause Analysis

The issue stems from how Dash handles callback chains and multiple callbacks targeting the same outputs:

1. Two separate callbacks (`update_weight_from_input` and `update_weight_inputs`) update and read from the same `stored-weights` component
2. Multiple callbacks attempt to modify the same outputs (`weight-input` values, `t2d-pulse-value`, etc.)
3. Client-side JavaScript intended to provide immediate visual feedback conflicts with server callback execution

## Solution

We've provided multiple solutions to fix this issue:

### Option 1: Apply the Automated Fix Script (Recommended)

The `fix_t2d_pulse_weights.py` script automatically:
1. Creates a backup of your app.py
2. Identifies the problematic callbacks
3. Replaces them with a single combined callback
4. Restarts the server

To use this option:
```bash
# Make the script executable
chmod +x fix_t2d_pulse_weights.py

# Run the fix script
./fix_t2d_pulse_weights.py

# Restart the dashboard server
python wsgi.py
```

### Option 2: Use the Standalone Fixed Dashboard

We've created a completely standalone version of the dashboard with only the weight functionality to verify the fix:

```bash
# Run the standalone dashboard
python standalone_wsgi.py
```

Then visit: http://localhost:5020

### Option 3: Manual Implementation

If the automated fix doesn't work for your specific setup, you can manually implement the fix:

1. Open `app.py`
2. Find and remove BOTH of these callbacks:
   - `update_weight_from_input` 
   - `update_weight_inputs`
3. Replace them with the single combined callback in `app_fixed_weights.py`

## How the Fix Works

The fix addresses all issues by:

1. **Eliminating the Callback Chain**: Instead of one callback updating stored weights and another reading them, a single callback handles both operations.

2. **Removing Duplicate Outputs**: By consolidating into one callback, we eliminate duplicate output targets.

3. **Direct Updates**: The fix directly updates all UI elements with specific values rather than relying on stored intermediate state.

4. **Preserving Weight Logic**: The fix maintains the original weight normalization logic to ensure weights sum to 100%.

## Verification Steps

After applying the fix:

1. Open the dashboard
2. Find any sector weight input field
3. Change the value to 20% and click Apply
4. Verify:
   - The sector weight updates to 20%
   - Other weights are automatically adjusted to maintain a 100% total
   - The T2D Pulse score updates
   - A notification message appears confirming the update

## Fallback Options

If you encounter issues after applying the fix:

1. Restore from backup:
   ```bash
   cp app.py.bak app.py
   ```

2. Run the original dashboard server:
   ```bash
   python wsgi.py
   ```

## Technical Implementation Details

The fix replaces problematic callbacks with a single, combined callback:

```python
@app.callback(
    [Output({"type": "weight-input", "index": sector}, "value") for sector in SECTORS] +
    [Output("t2d-pulse-value", "children"),
     Output("stored-weights", "children"),
     Output("weight-update-notification", "children"),
     Output("weight-update-notification", "style")],
    [Input({"type": "apply-weight", "index": ALL}, "n_clicks")],
    [State({"type": "weight-input", "index": ALL}, "value"),
     State({"type": "weight-input", "index": ALL}, "id"),
     State("stored-weights", "children")],
    prevent_initial_call=True
)
def direct_weight_update(n_clicks_list, input_values, input_ids, weights_json):
    # Implementation details in fixed_app.py
```

The key improvements are:
1. One callback with all outputs explicitly listed
2. Direct update of input values without intermediate state
3. Proportional adjustment of other weights to maintain 100%
4. Immediate update of all UI elements in a single operation