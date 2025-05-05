# Configuration for Finnhub API and sector definitions
import os

# Get Finnhub API key from environment variable
FINNHUB_API_KEY = os.environ.get("FINNHUB_API_KEY", "")

SECTORS = {
    "AdTech": ["APP", "APPS", "CRTO", "DV", "GOOGL", "IAD", "META", "MGNI", "PUBM", "TTD"],
    "Cloud Infrastructure": ["AMZN", "CRM", "CSCO", "GOOGL", "MSFT", "NET", "ORCL", "SNOW"],
    "Fintech": ["ADYEY", "AFRM", "BILL", "COIN", "FIS", "FISV", "GPN", "PYPL", "SQ", "SSNC"],
    "eCommerce": ["AMZN", "BABA", "BKNG", "CHWY", "EBAY", "ETSY", "PDD", "SE", "SHOP", "WMT"],
    "Consumer Internet": ["ABNB", "BKNG", "GOOGL", "META", "NFLX", "PINS", "SNAP", "SPOT", "TRIP", "YELP"],
    "IT Services / Legacy Tech": ["ACN", "CTSH", "DXC", "HPQ", "IBM", "INFY", "PLTR", "WIT"],
    "Hardware / Devices": ["AAPL", "DELL", "HPQ", "LOGI", "PSTG", "SMCI", "SSYS", "STX", "WDC"],
    "Cybersecurity": ["CHKP", "CRWD", "CYBR", "FTNT", "NET", "OKTA", "PANW", "S", "ZS"],
    "Dev Tools / Analytics": ["DDOG", "ESTC", "GTLB", "MDB", "TEAM"],
    "AI Infrastructure": ["AMZN", "GOOGL", "IBM", "META", "MSFT", "NVDA", "ORCL"],
    "Semiconductors": ["AMAT", "AMD", "ARM", "AVGO", "INTC", "NVDA", "QCOM", "TSM"],
    "Vertical SaaS": ["CCCS", "CPRT", "CSGP", "GWRE", "ICE", "PCOR", "SSNC", "TTAN"],
    "Enterprise SaaS": ["ADSK", "AMZN", "CRM", "IBM", "MSFT", "NOW", "ORCL", "SAP", "WDAY"],
    "SMB SaaS": ["ADBE", "BILL", "GOOGL", "HUBS", "INTU", "META"]
}

# Map our sector names to the ones in app.py
SECTOR_NAME_MAP = {
    "Cloud Infrastructure": "Cloud Infrastructure",
    "IT Services / Legacy Tech": "IT Services / Legacy Tech",
    "Hardware / Devices": "Hardware / Devices",
    "Dev Tools / Analytics": "Dev Tools / Analytics",
    "AdTech": "AdTech",
    "Fintech": "Fintech",
    "eCommerce": "eCommerce",
    "Consumer Internet": "Consumer Internet",
    "Cybersecurity": "Cybersecurity",
    "AI Infrastructure": "AI Infrastructure",
    "Semiconductors": "Semiconductors",
    "Vertical SaaS": "Vertical SaaS",
    "Enterprise SaaS": "Enterprise SaaS",
    "SMB SaaS": "SMB SaaS"
}

# EMA configuration
EMA_SPAN = 20  # Days for EMA calculation
EMA_WEIGHT = 0.2  # Weight of EMA factor in sector sentiment (20%)
EMA_NORMALIZATION_FACTOR = 5.0  # +/- 5% change is normalized to +/- 1.0 signal