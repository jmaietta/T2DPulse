#!/bin/bash
# Start T2D Pulse application

# Load environment variables
source .env

# Start the background data collector
python background_data_collector.py --check 15 --update 30 &

# Start the main application
python wsgi.py
