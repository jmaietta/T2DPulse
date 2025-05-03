import csv
from datetime import datetime
import os

def append_sector_values(results, filepath="data/sector_values.csv"):
    today = datetime.now().strftime("%Y-%m-%d")
    header_needed = not os.path.exists(filepath)
    with open(filepath, "a", newline="") as f:
        writer = csv.writer(f)
        if header_needed:
            writer.writerow(["Date"] + list(results.keys()))
        row = [today] + [results.get(sector, 0) for sector in results]
        writer.writerow(row)