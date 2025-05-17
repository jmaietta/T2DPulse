#!/usr/bin/env python3
"""
Build daily T2D Pulse history from sector-level market‑cap history.

Prerequisites
-------------
*   data/sector_history.parquet – written by recalculate_sector_history.py
Outputs
-------
*   data/t2d_pulse_history.parquet – two columns: date, pulse
"""

from pathlib import Path
import pandas as pd

DATA_DIR = Path(__file__).parent / "data"
SECTOR_FILE = DATA_DIR / "sector_history.parquet"
PULSE_FILE  = DATA_DIR / "t2d_pulse_history.parquet"

def main() -> None:
    if not SECTOR_FILE.exists():
        raise SystemExit(f"❌ {SECTOR_FILE} not found – run recalculate_sector_history.py first")

    # 1) Load sector caps
    df = pd.read_parquet(SECTOR_FILE).sort_index()

    # 2) Compute the Pulse – equal‑weighted mean for now
    pulse = df.mean(axis=1).rename("pulse").to_frame()
    pulse.index.name = "date"

    # 3) Write Parquet
    PULSE_FILE.parent.mkdir(exist_ok=True, parents=True)
    pulse.reset_index().to_parquet(PULSE_FILE, index=False)

    print(f"✅  Wrote {PULSE_FILE} with {len(pulse):,} rows")

if __name__ == "__main__":
    main()
