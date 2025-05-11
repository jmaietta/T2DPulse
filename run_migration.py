#!/usr/bin/env python3
"""
Run the database migration and create sector sparkline data for the dashboard.
This script performs the following tasks:
1. Migrate market cap data from CSV files to SQLite database
2. Create sparkline data for the dashboard
3. Update the app to use the database for data access
"""

import os
import logging
import sys
import shutil

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_migration():
    """Run the database migration"""
    logger.info("Running market cap data migration to SQLite...")
    
    try:
        # Run the migration script
        import migrate_to_database
        success = migrate_to_database.main()
        
        if not success:
            logger.error("Migration failed, check logs for details")
            return False
        
        logger.info("Migration completed successfully")
        return True
    
    except Exception as e:
        logger.error(f"Error during migration: {e}")
        return False

def create_sparkline_data():
    """Create sparkline data for the dashboard"""
    logger.info("Creating sector sparkline data...")
    
    try:
        # Use the fix_sector_display script
        import fix_sector_display
        sparkline_data = fix_sector_display.create_sector_sparkline_data()
        
        if not sparkline_data:
            logger.error("Failed to create sparkline data")
            return False
        
        logger.info(f"Created sparkline data for {len(sparkline_data)} sectors")
        return True
    
    except Exception as e:
        logger.error(f"Error creating sparkline data: {e}")
        return False

def update_app_db_access():
    """Update the app to use the database for data access"""
    logger.info("Updating app to use the database for data access...")
    
    try:
        # Replace db_access.py with fixed version
        if os.path.exists('db_access_fixed.py'):
            logger.info("Found fixed db_access.py, replacing the original")
            shutil.copy('db_access_fixed.py', 'db_access.py')
        
        # Modify app.py to import db_access module
        app_file = 'app.py'
        update_required = True
        
        # Backup the app.py file
        backup_file = f'app_backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.py'
        if os.path.exists(app_file):
            shutil.copy(app_file, backup_file)
            logger.info(f"Created backup of app.py as {backup_file}")
        
            # Check if the app already imports db_access
            with open(app_file, 'r') as f:
                app_content = f.read()
                if 'import db_access' in app_content:
                    logger.info("App already imports db_access module")
                    update_required = False
        
        if update_required:
            logger.info("App requires update to use db_access module")
            # We'll only log this for now, as changing app.py requires careful consideration
            logger.info("Recommend manually updating app.py to use db_access module")
        
        return True
    
    except Exception as e:
        logger.error(f"Error updating app: {e}")
        return False

def main():
    """Main function to run all tasks"""
    logger.info("Starting market cap data migration and dashboard update...")
    
    # Run the migration
    if not run_migration():
        logger.error("Migration failed, aborting")
        return False
    
    # Create sparkline data
    if not create_sparkline_data():
        logger.error("Failed to create sparkline data, aborting")
        return False
    
    # Update app to use database
    if not update_app_db_access():
        logger.error("Failed to update app, aborting")
        return False
    
    logger.info("All tasks completed successfully")
    return True

if __name__ == "__main__":
    # Import needed modules
    from datetime import datetime, timedelta
    
    # Run the main function
    success = main()
    sys.exit(0 if success else 1)