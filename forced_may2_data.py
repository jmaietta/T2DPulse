import json
import os

# This file contains hardcoded data from May 2nd to be used when the date is a weekend
# This ensures sector cards and T2D Pulse show the correct May 2nd data

# Direct data from the exportable file for May 2nd
# These are the authentic May 2nd values directly from the file
MAY_2ND_SECTOR_SCORES = {
    'SMB SaaS': 59.0,
    'Enterprise SaaS': 59.5,
    'Cloud Infrastructure': 60.0,
    'AdTech': 62.0,
    'Fintech': 60.0,
    'Consumer Internet': 60.0,
    'eCommerce': 62.0,
    'Cybersecurity': 58.5,
    'Dev Tools / Analytics': 57.5,
    'Semiconductors': 65.5,
    'AI Infrastructure': 60.0,
    'Vertical SaaS': 56.5,
    'IT Services / Legacy Tech': 66.0,
    'Hardware / Devices': 65.0
}

# T2D Pulse score calculated using May 2nd data with equal weights
# Authentic score using the calculation from authentic_historical_data.py
MAY_2ND_T2D_PULSE_SCORE = 55.7

# Sector drivers mapping - key macro factors influencing each sector
SECTOR_DRIVERS = {
    'SMB SaaS': ['Interest Rates', 'Small Business Confidence', 'Job Market'],
    'Enterprise SaaS': ['Corporate IT Spending', 'Digital Transformation', 'Interest Rates'],
    'Cloud Infrastructure': ['Data Center Expansion', 'Enterprise Cloud Migration', 'AI Adoption'],
    'AdTech': ['Digital Ad Spending', 'Privacy Regulations', 'Retail Sales'],
    'Fintech': ['Consumer Spending', 'Interest Rates', 'Payment Volumes'],
    'Consumer Internet': ['User Engagement', 'Ad Spending', 'Subscription Growth'],
    'eCommerce': ['Retail Sales', 'Consumer Confidence', 'Shipping Costs'],
    'Cybersecurity': ['Security Breaches', 'Compliance Requirements', 'Corporate IT Budgets'],
    'Dev Tools / Analytics': ['Software Development Trends', 'Cloud Adoption', 'Engineering Hiring'],
    'Semiconductors': ['Global Chip Demand', 'AI Demand', 'Supply Chain Health'],
    'AI Infrastructure': ['Cloud Compute Growth', 'AI Adoption', 'Enterprise Digital Transformation'],
    'Vertical SaaS': ['Industry-Specific Adoption', 'Regulatory Changes', 'Operating Efficiency'],
    'IT Services / Legacy Tech': ['Enterprise IT Spending', 'Digital Transformation', 'Outsourcing Trends'],
    'Hardware / Devices': ['Consumer Electronics Demand', 'Component Costs', 'Supply Chain Health']
}

# Sector tickers mapping for representative stocks (limited to 5 per sector)
SECTOR_TICKERS = {
    'AdTech': ['GOOGL', 'META', 'MGNI', 'PUBM', 'TTD'],
    'Cloud Infrastructure': ['AMZN', 'CRM', 'MSFT', 'NET', 'SNOW'],
    'Fintech': ['AFRM', 'COIN', 'PYPL', 'SQ', 'FISV'],
    'eCommerce': ['AMZN', 'ETSY', 'SHOP', 'WMT', 'BABA'],
    'Consumer Internet': ['GOOGL', 'META', 'NFLX', 'SNAP', 'PINS'],
    'IT Services / Legacy Tech': ['ACN', 'IBM', 'INFY', 'PLTR', 'CTSH'],
    'Hardware / Devices': ['AAPL', 'DELL', 'HPQ', 'LOGI', 'SMCI'],
    'Cybersecurity': ['CRWD', 'OKTA', 'PANW', 'ZS', 'FTNT'],
    'Dev Tools / Analytics': ['DDOG', 'ESTC', 'GTLB', 'MDB', 'TEAM'],
    'AI Infrastructure': ['AMZN', 'GOOGL', 'MSFT', 'NVDA', 'META'],
    'Semiconductors': ['AMD', 'NVDA', 'INTC', 'TSM', 'AVGO'],
    'Vertical SaaS': ['CCCS', 'CSGP', 'GWRE', 'PCOR', 'SSNC'],
    'Enterprise SaaS': ['CRM', 'MSFT', 'NOW', 'WDAY', 'ADSK'],
    'SMB SaaS': ['ADBE', 'BILL', 'GOOGL', 'HUBS', 'INTU']
}

def get_may2nd_sector_data():
    """
    Return a list of sector data dictionaries with May 2nd values
    in the format expected by the sector cards generation code
    """
    authentic_scores = []
    
    for sector, score_value in MAY_2ND_SECTOR_SCORES.items():
        # Convert to -1 to +1 scale for consistency with sector cards code
        raw_score = (score_value / 50.0) - 1.0
        
        # Create sector data structure in the expected format
        sector_data = {
            "sector": sector,
            "score": raw_score,
            "normalized_score": score_value,  # Already in 0-100 scale
            "stance": "Bullish" if score_value >= 60 else "Bearish" if score_value <= 30 else "Neutral",
            "takeaway": "Outperforming peers" if score_value >= 60 else 
                      "Bearish macro setup" if score_value <= 30 else 
                      "Neutral – monitor trends",
            "drivers": SECTOR_DRIVERS.get(sector, []),  # Key drivers for this sector
            "tickers": SECTOR_TICKERS.get(sector, [])  # Representative tickers for this sector
        }
        authentic_scores.append(sector_data)
    
    return authentic_scores

def get_may2nd_t2d_pulse_score():
    """
    Return the T2D Pulse score calculated using May 2nd data
    """
    return MAY_2ND_T2D_PULSE_SCORE

def get_may2nd_sector_dict():
    """
    Return a dictionary of May 2nd sector scores
    for use in T2D Pulse score calculation
    """
    return MAY_2ND_SECTOR_SCORES