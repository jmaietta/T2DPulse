#!/bin/bash
# T2D Pulse Fix and Restart Script

echo "🔧 T2D Pulse Weight Update Fix and Restart"
echo "=======================================\n"

# Step 1: Run the Python fix script
echo "Step 1: Applying dashboard weight update fix..."
./fix_t2d_pulse_weights.py
if [ $? -ne 0 ]; then
  echo "❌ Fix script failed. Aborting restart."
  exit 1
fi

# Step 2: Stop and restart the Economic Dashboard Server workflow
echo -e "\nStep 2: Restarting the dashboard server..."
echo "This may take a moment...\n"

# The following assumes you're using Replit's workflow system
# If you're running the server differently, adjust this part
python wsgi.py

echo -e "\n✅ Fix complete! The dashboard should now be running with fixed weight updates."
echo "Check the dashboard at: http://localhost:5000"
echo "Try changing a sector weight and clicking Apply to verify the fix is working."