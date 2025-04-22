import fileinput
import sys
import re

weight_map = {
    "unemployment-weight": "6.36",
    "job-postings-weight": "6.36",
    "cpi-weight": "6.36",
    "pcepi-weight": "6.36",
    "nasdaq-weight": "15.45",
    "data-ppi-weight": "6.36",
    "software-ppi-weight": "6.36",
    "interest-rate-weight": "6.36",
    "treasury-yield-weight": "9.09",
    "vix-weight": "9.09",
    "consumer-sentiment-weight": "9.09"
}

pattern = re.compile(r'id="([^"]+)",[^v]*value=([0-9.]+),')

for line in fileinput.input(files=["app.py"], inplace=True):
    match = pattern.search(line)
    if match and match.group(1) in weight_map:
        weight_id = match.group(1)
        line = line.replace(f'value={match.group(2)}', f'value={weight_map[weight_id]}')
        # Also change step to 0.01 for precision
        line = line.replace('step=0.1', 'step=0.01')
    sys.stdout.write(line)
