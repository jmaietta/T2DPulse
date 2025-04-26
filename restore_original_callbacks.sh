#!/bin/bash

# This script will restore the original app.py from a snapshot backup

# Create a dated backup of the current problematic version
cp app.py app.py.$(date +%s).broken

# Copy the original callback structure from app-callback-fix.py
# and adapt it to the current app.py file

echo "Extracting original callback structure..."

# Extract the update_weight_from_input function from app-callback-fix.py
ORIGINAL_CALLBACK=$(grep -A 120 "def update_weight_from_input" app-callback-fix.py)

# Now we'll patch the app.py file
echo "Patching app.py..."

# Find the line numbers of the direct_weight_update function
START_LINE=$(grep -n "def direct_weight_update" app.py | cut -d':' -f1)
END_LINE=$(grep -n "def reset_weights" app.py | cut -d':' -f1)

# If we found the problematic function, remove it
if [ ! -z "$START_LINE" ] && [ ! -z "$END_LINE" ]; then
    # Start from 5 lines before the function to capture the decorator
    START_LINE=$((START_LINE - 5))
    
    # Create a temporary file without the problematic function
    sed -i "${START_LINE},${END_LINE}d" app.py
    
    echo "Removed problematic function, patching with original callback..."
    
    # Find where to insert the original callback
    INSERT_LINE=$(grep -n "def update_weight_inputs_for_other_changes" app.py | cut -d':' -f1)
    
    if [ ! -z "$INSERT_LINE" ]; then
        # Replace the simplified callback with the original one
        sed -i "${INSERT_LINE}s/def update_weight_inputs_for_other_changes/def update_weight_from_input/" app.py
    fi
else
    echo "Could not find the problematic function"
fi

echo "Restoring outputs with allow_duplicate..."
# Remove allow_duplicate=True from outputs
sed -i 's/Output([^,]*), allow_duplicate=True/Output(\1)/g' app.py

echo "Restoration complete."