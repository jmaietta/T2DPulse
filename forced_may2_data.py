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
# Average of the May 2nd scores: (59 + 59.5 + 60 + 62 + 60 + 60 + 62 + 58.5 + 57.5 + 65.5 + 60 + 56.5 + 66 + 65) / 14 = 60.8
MAY_2ND_T2D_PULSE_SCORE = 60.8

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
            "drivers": [],  # Empty drivers for historical data
            "tickers": []   # Empty tickers for historical data
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