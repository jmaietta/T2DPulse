#!/usr/bin/env python3
"""
WSGI entry point for T2D Pulse application
------------------------------------------
This is the production entry point for the T2D Pulse application.
"""

import os
from app import app as application

if __name__ == "__main__":
    # Get port from environment or use 5000 as default
    port = int(os.environ.get("PORT", 5000))
    
    # Run the application
    application.run(host="0.0.0.0", port=port, debug=False)