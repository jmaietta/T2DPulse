#!/usr/bin/env python3
"""
Prepare T2D Pulse Export Package
--------------------------------
This script creates a clean export package with all essential files for migrating
T2D Pulse to another platform. The script removes temporary files, logs, and 
platform-specific configurations.
"""

import os
import shutil
import json
import sys
import re
from datetime import datetime

# Define export directory
EXPORT_DIR = "t2d_pulse_export"
TODAY = datetime.now().strftime('%Y%m%d')
EXPORT_FILENAME = f"t2d_pulse_export_{TODAY}.zip"

# Essential directories to include
DIRS_TO_INCLUDE = [
    "data",
    "assets",
    "analysis_reports"
]

# Essential file patterns to include
ESSENTIAL_FILES = [
    # Main application files
    "app.py",
    "config.py",
    "wsgi.py",
    "background_data_collector.py",
    
    # Data collection and processing
    "authentic_*.py",
    "batch_ticker_collector.py",
    "calculate_*.py",
    "fix_sector_*.py",
    
    # Configuration and documentation
    "requirements.txt",
    "README.md",
    "t2d_pulse_migration_guide.md",
    
    # Other utility scripts
    "data_helpers.py", 
    "data_reader.py",
    "data_cache.py",
    "ema_calculator.py",
    "sentiment_engine.py",
    "check_ticker_coverage.py"
]

# Files to explicitly exclude
FILES_TO_EXCLUDE = [
    # Temporary files and backups
    "*_backup_*.py",
    "*.bak",
    "*.log",
    "*.tmp",
    
    # Replit-specific
    ".replit",
    "replit.nix",
    
    # Test scripts
    "test_*.py",
    
    # Duplicate copies
    "app_before_*.py",
    "*_improved.py",
    "*_fixed.py",
    "*_backup.py"
]

def create_requirements_file():
    """Create a requirements.txt file with all necessary dependencies"""
    requirements = [
        "dash",
        "dash-bootstrap-components",
        "docx",
        "fastparquet",
        "filelock",
        "finnhub-python",
        "flask",
        "matplotlib",
        "nasdaq-data-link",
        "numpy",
        "openai",
        "openpyxl",
        "pandas",
        "plotly",
        "pyarrow",
        "pypdf2",
        "python-docx",
        "pytz",
        "requests",
        "sendgrid",
        "tabulate",
        "tenacity",
        "torch",
        "tqdm",
        "trafilatura",
        "twilio",
        "yfinance",
        "psycopg2-binary",
    ]
    
    with open(os.path.join(EXPORT_DIR, "requirements.txt"), "w") as f:
        for req in requirements:
            f.write(f"{req}\n")
    
    print(f"Created requirements.txt with {len(requirements)} dependencies")

def create_env_template():
    """Create a template .env file"""
    env_content = """# T2D Pulse Environment Variables

# API Keys
FINNHUB_API_KEY=your_key_here
ALPHAVANTAGE_API_KEY=your_key_here
FRED_API_KEY=your_key_here
BEA_API_KEY=your_key_here
BLS_API_KEY=your_key_here
NASDAQ_DATA_LINK_API_KEY=your_key_here
POLYGON_API_KEY=your_key_here
SENDGRID_API_KEY=your_key_here

# Database Configuration
DATABASE_URL=postgres://username:password@host:port/database
PGHOST=host
PGPORT=5432
PGUSER=username
PGPASSWORD=password
PGDATABASE=database

# Application Configuration
FLASK_ENV=production
"""
    
    with open(os.path.join(EXPORT_DIR, ".env.template"), "w") as f:
        f.write(env_content)
    
    print("Created .env.template file")

def create_start_script():
    """Create a start script for the application"""
    start_script = """#!/bin/bash
# Start T2D Pulse application

# Load environment variables
source .env

# Start the background data collector
python background_data_collector.py --check 15 --update 30 &

# Start the main application
python wsgi.py
"""
    
    with open(os.path.join(EXPORT_DIR, "start.sh"), "w") as f:
        f.write(start_script)
    
    # Make it executable
    os.chmod(os.path.join(EXPORT_DIR, "start.sh"), 0o755)
    
    print("Created start.sh script")

def should_include_file(filename):
    """Check if a file should be included in the export"""
    # Check exclusion patterns first
    for pattern in FILES_TO_EXCLUDE:
        if re.match(pattern.replace("*", ".*"), filename):
            return False
    
    # Then check inclusion patterns
    for pattern in ESSENTIAL_FILES:
        if re.match(pattern.replace("*", ".*"), filename):
            return True
    
    # By default, don't include files not explicitly listed
    return False

def prepare_export():
    """Prepare the export package"""
    # Create export directory
    if os.path.exists(EXPORT_DIR):
        print(f"Removing existing {EXPORT_DIR} directory")
        shutil.rmtree(EXPORT_DIR)
    
    os.makedirs(EXPORT_DIR)
    print(f"Created export directory: {EXPORT_DIR}")
    
    # Copy essential directories
    for dirname in DIRS_TO_INCLUDE:
        if os.path.exists(dirname):
            dest_dir = os.path.join(EXPORT_DIR, dirname)
            os.makedirs(dest_dir, exist_ok=True)
            
            for item in os.listdir(dirname):
                src_path = os.path.join(dirname, item)
                dst_path = os.path.join(dest_dir, item)
                
                if os.path.isfile(src_path):
                    shutil.copy2(src_path, dst_path)
                elif os.path.isdir(src_path):
                    shutil.copytree(src_path, dst_path)
            
            print(f"Copied directory: {dirname}")
    
    # Copy essential files
    files_copied = 0
    for filename in os.listdir("."):
        if os.path.isfile(filename) and should_include_file(filename):
            shutil.copy2(filename, os.path.join(EXPORT_DIR, filename))
            files_copied += 1
    
    print(f"Copied {files_copied} essential files")
    
    # Create necessary configuration files
    create_requirements_file()
    create_env_template()
    create_start_script()
    
    # Copy the migration guide
    if os.path.exists("t2d_pulse_migration_guide.md"):
        shutil.copy2("t2d_pulse_migration_guide.md", 
                    os.path.join(EXPORT_DIR, "MIGRATION_GUIDE.md"))
        print("Copied migration guide")
    
    # Create a manifest file
    create_manifest()
    
    # Create a zip archive
    print(f"Creating zip archive: {EXPORT_FILENAME}")
    shutil.make_archive(
        EXPORT_FILENAME.replace(".zip", ""), 
        'zip', 
        ".", 
        EXPORT_DIR
    )
    
    print(f"\nExport package created successfully: {EXPORT_FILENAME}")
    print(f"Export directory: {EXPORT_DIR}")

def create_manifest():
    """Create a manifest file listing all included files"""
    manifest = {
        "export_date": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "files": [],
        "directories": []
    }
    
    for root, dirs, files in os.walk(EXPORT_DIR):
        rel_path = os.path.relpath(root, EXPORT_DIR)
        if rel_path != ".":
            manifest["directories"].append(rel_path)
        
        for file in files:
            file_path = os.path.join(rel_path, file)
            if file_path.startswith("./"):
                file_path = file_path[2:]
            manifest["files"].append(file_path)
    
    with open(os.path.join(EXPORT_DIR, "manifest.json"), "w") as f:
        json.dump(manifest, f, indent=2)
    
    print(f"Created manifest with {len(manifest['files'])} files and {len(manifest['directories'])} directories")

if __name__ == "__main__":
    print("Preparing T2D Pulse export package...")
    prepare_export()