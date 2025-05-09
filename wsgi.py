#!/usr/bin/env python3
# wsgi.py
# -----------------------------------------------------------
# WSGI entry point for T2D Pulse economic dashboard

from app import server as application

if __name__ == '__main__':
    application.run(host='0.0.0.0', port=5000)