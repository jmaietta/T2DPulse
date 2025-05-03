from config import SECTORS
from eva_calculator import compute_sector_value
from storage import append_sector_values

def main():
    results = {}
    for sector, tickers in SECTORS.items():
        value = compute_sector_value(tickers)
        results[sector] = value
    append_sector_values(results)

if __name__ == "__main__":
    main()