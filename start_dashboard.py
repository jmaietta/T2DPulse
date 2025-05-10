"""
Simple wrapper to ensure the T2D Pulse dashboard starts properly
"""
import subprocess
import time
import os
import signal
import threading
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def start_app():
    """Start the dashboard app using a subprocess"""
    logger.info("Starting T2D Pulse dashboard")
    return subprocess.Popen(["python", "app.py"], 
                           stdout=subprocess.PIPE, 
                           stderr=subprocess.PIPE,
                           universal_newlines=True)

def monitor_output(process):
    """Monitor output from the app process"""
    for line in process.stdout:
        print(line, end='')
        # Check if the app is running by looking for specific output
        if "Running on http://127.0.0.1:3000" in line:
            logger.info("Dashboard is running on port 3000")
            print("\n✅ Dashboard is ready! Access it at: https://[your-repl-url]")
    
    # Read any errors
    for line in process.stderr:
        print(f"ERROR: {line}", end='')

def main():
    """Main function to start and monitor the dashboard"""
    try:
        process = start_app()
        
        # Start a thread to monitor output
        monitor_thread = threading.Thread(target=monitor_output, args=(process,), daemon=True)
        monitor_thread.start()
        
        # Wait for the process to complete
        process.wait()
        
        # If we get here, something went wrong
        logger.error("Dashboard process exited unexpectedly")
        return 1
        
    except KeyboardInterrupt:
        logger.info("Shutting down dashboard")
        return 0
    except Exception as e:
        logger.error(f"Error running dashboard: {str(e)}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)