-- 1) Add pct_change column
ALTER TABLE sector_market_cap_history
  ADD COLUMN IF NOT EXISTS pct_change DOUBLE PRECISION;

-- 2) Compute daily % change for each sector
UPDATE sector_market_cap_history AS t
SET pct_change = 100.0 * (t.sector_cap - lag(t.sector_cap) OVER w) / lag(t.sector_cap) OVER w
FROM (
  SELECT date, sector,
         lag(sector_cap) OVER w AS prev_cap
  FROM sector_market_cap_history
  WINDOW w AS (PARTITION BY sector ORDER BY date)
) prev
WHERE t.sector = prev.sector
  AND t.date   = prev.date;

-- 3) Add momentum flag column: +1 for 3-day up, -1 for 3-day down
ALTER TABLE sector_market_cap_history
  ADD COLUMN IF NOT EXISTS momentum SMALLINT;

-- 4) Compute 3-day streak momentum
WITH streaks AS (
  SELECT
    date,
    sector,
    CASE
      WHEN pct_change > 0
        AND lag(pct_change, 1) OVER w > 0
        AND lag(pct_change, 2) OVER w > 0 THEN  1
      WHEN pct_change < 0
        AND lag(pct_change, 1) OVER w < 0
        AND lag(pct_change, 2) OVER w < 0 THEN -1
      ELSE 0
    END AS m
  FROM sector_market_cap_history
  WINDOW w AS (PARTITION BY sector ORDER BY date)
)
UPDATE sector_market_cap_history AS t
SET momentum = s.m
FROM streaks s
WHERE t.sector = s.sector
  AND t.date   = s.date;
