import os

# Load API keys from environment variables with fallbacks
FRED_API_KEY = os.getenv("FRED_API_KEY", "")
BEA_API_KEY = os.getenv("BEA_API_KEY", "")
BLS_API_KEY = os.getenv("BLS_API_KEY", "")

# Print warning if keys are missing
if not FRED_API_KEY:
    print("WARNING: FRED_API_KEY environment variable not set")
if not BEA_API_KEY:
    print("WARNING: BEA_API_KEY environment variable not set")
if not BLS_API_KEY:
    print("WARNING: BLS_API_KEY environment variable not set")
