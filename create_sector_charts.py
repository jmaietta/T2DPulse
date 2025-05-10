#!/usr/bin/env python3
# create_sector_charts.py
"""
Create simple charts for sector cards.
"""

import os
import sys
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_sector_charts():
    """Create simple SVG charts for sectors"""
    try:
        # Create data directory if it doesn't exist
        os.makedirs('data', exist_ok=True)
        
        # Save a simple HTML template for each sector
        sectors = [
            "SMB SaaS", "Enterprise SaaS", "Cloud Infrastructure", 
            "AdTech", "Fintech", "Consumer Internet", "eCommerce",
            "Cybersecurity", "Dev Tools / Analytics", "Semiconductors",
            "AI Infrastructure", "Vertical SaaS", "IT Services / Legacy Tech",
            "Hardware / Devices"
        ]
        
        for sector in sectors:
            # Create file name with underscores
            file_name = f"data/sector_chart_{sector.replace(' ', '_').replace('/', '_')}.html"
            
            # Simple SVG sparkline (a basic placeholder)
            html_content = f"""
            <svg width="100%" height="40" xmlns="http://www.w3.org/2000/svg">
                <path d="M0,20 L15,18 L30,22 L45,15 L60,25 L75,10 L90,15 L105,20 L120,18 L135,22 L150,20" 
                    stroke="#3498db" stroke-width="2" fill="none" />
            </svg>
            """
            
            # Write the file
            with open(file_name, 'w') as f:
                f.write(html_content)
            
            logger.info(f"Created chart HTML file for {sector}")
        
        return True
    except Exception as e:
        logger.error(f"Error creating chart HTML files: {e}")
        return False

if __name__ == "__main__":
    success = create_sector_charts()
    print(f"Created sector charts successfully: {success}")
    sys.exit(0 if success else 1)