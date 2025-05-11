#!/bin/bash

# T2D Pulse Comprehensive Fix Script
# This script runs all the fixes for the T2D Pulse dashboard:
# 1. Database migration
# 2. Sparkline data generation
# 3. App.py fixes

echo "Starting T2D Pulse comprehensive fixes at $(date)"

# Create a backup directory
BACKUP_DIR="backups_$(date +%Y%m%d_%H%M%S)"
mkdir -p $BACKUP_DIR
echo "Created backup directory: $BACKUP_DIR"

# Backup important files
echo "Creating backups of important files..."
cp app.py "$BACKUP_DIR/app.py.bak"
cp -r data "$BACKUP_DIR/data_backup"
echo "Backups created successfully"

# 1. Run database migration
echo "Step 1: Running database migration..."
python migrate_to_database.py
if [ $? -ne 0 ]; then
    echo "Database migration failed, see logs for details"
    exit 1
fi
echo "Database migration completed successfully"

# 2. Fix sparkline data 
echo "Step 2: Creating sector sparkline data..."
python fix_sector_display.py
if [ $? -ne 0 ]; then
    echo "Sparkline data creation failed, see logs for details"
    exit 1
fi
echo "Sector sparkline data created successfully"

# 3. Fix app.py
echo "Step 3: Updating app.py to use pre-generated sparkline data..."
python fix_app_sparklines.py
if [ $? -ne 0 ]; then
    echo "App.py fix failed, see logs for details"
    exit 1
fi
echo "App.py updated successfully"

# Restart all workflows to apply changes
echo "Restarting all workflows to apply changes..."
echo "- To restart the Economic Dashboard Server, use 'restart_workflow' tool"
echo "- To restart the Background Data Collection, use 'restart_workflow' tool"
echo "- To restart the Sector Market Cap Updater, use 'restart_workflow' tool"

echo "All fixes completed successfully at $(date)"
echo "Please restart workflows to see the changes"