#!/usr/bin/env python3
# run_mockups.py - Run all mockups and display URLs

import os
import sys
import webbrowser
import time
import subprocess

# Using ports 5000, 5001, 5002, 5003 for our mockups
MOCKUPS = [
    {"name": "Option A: Side-by-Side Layout", "script": "option_a_mockup.py", "port": 5000},
    {"name": "Option B: Enlarged Circle with Mini-Chart", "script": "option_b_mockup.py", "port": 5001},
    {"name": "Option C: Card Layout with Sectors", "script": "option_c_mockup.py", "port": 5002},
    {"name": "Option D: Dashboard Integration", "script": "option_d_mockup.py", "port": 5003},
]

# Update port numbers in all mockup files
def update_port_in_file(filename, new_port):
    with open(filename, 'r') as file:
        content = file.read()
    
    # Replace port in run statement
    if 'port=' in content:
        # Find the port number
        port_start = content.find('port=') + 5
        port_end = content.find(')', port_start)
        old_port = content[port_start:port_end]
        
        # Replace with new port
        content = content.replace(f'port={old_port}', f'port={new_port}')
        
        with open(filename, 'w') as file:
            file.write(content)
        
        print(f"Updated {filename} to use port {new_port}")

# Update all mockup files with correct ports
for mockup in MOCKUPS:
    update_port_in_file(mockup["script"], mockup["port"])

# Start all mockups
processes = []
for mockup in MOCKUPS:
    print(f"Starting {mockup['name']} on port {mockup['port']}...")
    process = subprocess.Popen(
        [sys.executable, mockup["script"]],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    processes.append((process, mockup))
    time.sleep(1)  # Brief delay to prevent conflicts

# Display URLs
print("\n===== MOCKUP URLS =====\n")
for _, mockup in processes:
    url = f"http://localhost:{mockup['port']}/"
    print(f"{mockup['name']}: {url}")

print("\nAll mockups are running. Press Ctrl+C to stop all processes.")

try:
    # Keep the script running
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    # Clean up processes on exit
    for process, _ in processes:
        process.terminate()
    print("\nAll mockups have been stopped.")
