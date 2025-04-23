# sentiment_engine.py
# -----------------------------------------------------------
# T2D Pulse – Sector‑level macro‑sentiment scoring
# -----------------------------------------------------------

from __future__ import annotations
from typing import Dict, List, TypedDict

# ---------- 1) Sector universe ----------
SECTORS = [
    "SMB SaaS", "Enterprise SaaS", "Cloud Infrastructure", "AdTech", "Fintech",
    "Consumer Internet", "eCommerce", "Cybersecurity", "Dev Tools / Analytics",
    "Semiconductors", "AI Infrastructure", "Vertical SaaS",
    "IT Services / Legacy Tech", "Hardware / Devices"
]

# ---------- 2) Impact grid (1‒3) ----------
IMPACT: Dict[str, Dict[str, int]] = {
    "10Y_Treasury_Yield_%":      dict.fromkeys(SECTORS, 2) | {
        "SMB SaaS": 3, "Enterprise SaaS": 3, "Cloud Infrastructure": 3,
        "Dev Tools / Analytics": 3, "AI Infrastructure": 3,
        "Vertical SaaS": 3, "IT Services / Legacy Tech": 1},
    "VIX":                       dict.fromkeys(SECTORS, 2),
    "NASDAQ_20d_gap_%":          dict.fromkeys(SECTORS, 3) | {
        "IT Services / Legacy Tech": 2, "Hardware / Devices": 2},
    "Fed_Funds_Rate_%":          dict.fromkeys(SECTORS, 2) | {
        "SMB SaaS": 3, "Enterprise SaaS": 3, "Cloud Infrastructure": 3,
        "Dev Tools / Analytics": 3, "AI Infrastructure": 3,
        "Vertical SaaS": 3, "Fintech": 3, "IT Services / Legacy Tech": 1},
    "CPI_YoY_%":                 dict.fromkeys(SECTORS, 2) | {
        "AdTech": 3, "Consumer Internet": 3, "eCommerce": 3,
        "Semiconductors": 3, "Hardware / Devices": 3},
    "PCEPI_YoY_%":               dict.fromkeys(SECTORS, 2) | {
        "AdTech": 3, "Consumer Internet": 3, "eCommerce": 3,
        "Semiconductors": 3, "Hardware / Devices": 3},
    "Real_GDP_Growth_%_SAAR":    dict.fromkeys(SECTORS, 2) | {
        "AdTech": 3, "Consumer Internet": 3, "eCommerce": 3,
        "Semiconductors": 3, "Hardware / Devices": 3},
    "Real_PCE_YoY_%":            dict.fromkeys(SECTORS, 2) | {
        "AdTech": 3, "Consumer Internet": 3, "eCommerce": 3,
        "Semiconductors": 3, "Hardware / Devices": 3},
    "Unemployment_%":            dict.fromkeys(SECTORS, 2),
    "Software_Dev_Job_Postings_YoY_%": dict.fromkeys(SECTORS, 1) | {
        "SMB SaaS": 3, "Enterprise SaaS": 2, "Cloud Infrastructure": 3,
        "Cybersecurity": 3, "Dev Tools / Analytics": 3,
        "AI Infrastructure": 3, "Vertical SaaS": 3},
    "PPI_Data_Processing_YoY_%": dict.fromkeys(SECTORS, 1) | {
        "Cloud Infrastructure": 3, "AI Infrastructure": 3},
    "PPI_Software_Publishers_YoY_%": dict.fromkeys(SECTORS, 1) | {
        "Cloud Infrastructure": 3, "AI Infrastructure": 3},
    "Consumer_Sentiment": {
        "Consumer Internet": 3, "eCommerce": 3, "AdTech": 3,
        "Fintech": 2, "Hardware / Devices": 2, "Semiconductors": 2,
        "SMB SaaS": 2, "Enterprise SaaS": 1, "Vertical SaaS": 1,
        "Cloud Infrastructure": 1, "AI Infrastructure": 1,
        "Cybersecurity": 1, "Dev Tools / Analytics": 1,
        "IT Services / Legacy Tech": 1
    }
}

# ---------- 3) Importance weights (1‒4 shared across sectors) ----------
IMPORTANCE = {
    "NASDAQ_20d_gap_%": 3,  # Changed from 4 to 3 as requested
    "10Y_Treasury_Yield_%": 3,
    "VIX": 3,
    "Consumer_Sentiment": 3,  # Adding Consumer Sentiment with equal weight to VIX and Treasury
    # all others default to 1
}

# ---------- 4) Macro favourability bands ----------
BANDS = {
    "10Y_Treasury_Yield_%":      ("lower", 3.25, 4.00),
    "VIX":                       ("lower", 18,   25),
    "NASDAQ_20d_gap_%":          ("higher", 4.0, -4.0),
    "Fed_Funds_Rate_%":          ("lower", 4.5,  5.25),
    "CPI_YoY_%":                 ("lower", 3.0,  4.0),
    "PCEPI_YoY_%":               ("lower", 3.0,  4.0),
    "Real_GDP_Growth_%_SAAR":    ("higher", 2.5, 1.0),
    "Real_PCE_YoY_%":            ("higher", 2.5, 1.0),
    "Unemployment_%":            ("lower", 4.5,  5.5),
    "Software_Dev_Job_Postings_YoY_%": ("higher", 5.0, 0.0),
    "PPI_Data_Processing_YoY_%": ("higher", 5.0, 0.0),
    "PPI_Software_Publishers_YoY_%": ("higher", 5.0, 0.0),
    "Consumer_Sentiment":        ("higher", 100.0, 90.0),
}

# ---------- 5) Types ----------
# Using Dict[str, float] instead of TypedDict for flexibility
MacroDict = Dict[str, float]

class SectorScore(TypedDict):
    sector: str
    score: float

# ---------- 6) Core functions ----------
def raw_signal(name: str, value) -> int:
    """Return +1, 0, -1 based on favourability band."""
    # Ensure value is float
    try:
        fvalue = float(value)
    except (ValueError, TypeError):
        print(f"Warning: Could not convert value '{value}' for '{name}' to float, using 0")
        return 0  # Neutral if can't parse the value
        
    dirn, fav_hi, unfav_lo = BANDS[name]
    if dirn == "lower":
        return 1 if fvalue <= fav_hi else -1 if fvalue >= unfav_lo else 0
    else:
        return 1 if fvalue >= fav_hi else -1 if fvalue <= unfav_lo else 0

def score_sectors(macros: MacroDict) -> List[SectorScore]:
    """Compute average score for each sector."""
    sector_sum = {s: 0.0 for s in SECTORS}
    sector_weight = {s: 0.0 for s in SECTORS}
    
    # For debugging, track contribution of each indicator to AdTech
    adtech_contributions = {}

    for ind, val in macros.items():
        raw = raw_signal(ind, val)
        imp = IMPORTANCE.get(ind, 1)
        
        # Debug print for AdTech
        if ind == "NASDAQ_20d_gap_%":
            print(f"NASDAQ importance weight: {imp}")
        
        for sec in SECTORS:
            weight = IMPACT[ind][sec] * imp
            contribution = raw * weight
            sector_sum[sec] += contribution
            sector_weight[sec] += abs(weight)
            
            # Track contributions for AdTech
            if sec == "AdTech":
                adtech_contributions[ind] = {
                    "raw_signal": raw,
                    "impact": IMPACT[ind][sec],
                    "importance": imp,
                    "weight": weight,
                    "contribution": contribution
                }
    
    # Print AdTech contributions
    if "AdTech" in SECTORS and macros:
        print("\nAdTech indicator contributions:")
        total_weight = sector_weight["AdTech"]
        for ind, data in adtech_contributions.items():
            weight_percent = (abs(data["weight"]) / total_weight) * 100
            print(f"  {ind:<25}: signal={data['raw_signal']:+d}, " +
                  f"impact={data['impact']}, imp={data['importance']}, " +
                  f"weight={data['weight']:.1f} ({weight_percent:.1f}%), " +
                  f"contribution={data['contribution']:+.2f}")
        print(f"  Total AdTech score: {sector_sum['AdTech'] / sector_weight['AdTech']:.2f}")
    
    return [
        {"sector": sec, "score": round(sector_sum[sec] / sector_weight[sec], 2)}
        for sec in SECTORS
    ]

# ---------- 7) Example run ----------
if __name__ == "__main__":
    latest_macros: MacroDict = {
        "10Y_Treasury_Yield_%": 4.422,
        "VIX": 32.6,
        "NASDAQ_20d_gap_%": -4.6,
        "Fed_Funds_Rate_%": 4.33,
        "CPI_YoY_%": 2.4,
        "PCEPI_YoY_%": 2.5,
        "Real_GDP_Growth_%_SAAR": 2.5,
        "Real_PCE_YoY_%": 5.3,
        "Unemployment_%": 4.2,
        "Software_Dev_Job_Postings_YoY_%": -8.8,
        "PPI_Data_Processing_YoY_%": 9.0,
        "PPI_Software_Publishers_YoY_%": 8.1,
        "Consumer_Sentiment": 97.1,
    }

    for row in score_sectors(latest_macros):
        print(f"{row['sector']:<25} {row['score']:>5.2f}")