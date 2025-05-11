-- SQLite database schema for the T2D Pulse market cap data

-- Drop existing tables if they exist
DROP TABLE IF EXISTS ticker_sectors;
DROP TABLE IF EXISTS ticker_market_caps;
DROP TABLE IF EXISTS sector_market_caps;
DROP TABLE IF EXISTS tickers;
DROP TABLE IF EXISTS sectors;

-- Create sectors table
CREATE TABLE sectors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create tickers table
CREATE TABLE tickers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT UNIQUE NOT NULL,
    company_name TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create ticker-sector relationship table (many-to-many)
CREATE TABLE ticker_sectors (
    ticker_id INTEGER NOT NULL,
    sector_id INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ticker_id, sector_id),
    FOREIGN KEY (ticker_id) REFERENCES tickers(id) ON DELETE CASCADE,
    FOREIGN KEY (sector_id) REFERENCES sectors(id) ON DELETE CASCADE
);

-- Create ticker market caps table
CREATE TABLE ticker_market_caps (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker_id INTEGER NOT NULL,
    date TEXT NOT NULL,
    price REAL,
    market_cap REAL NOT NULL,
    shares_outstanding REAL,
    data_source TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (ticker_id) REFERENCES tickers(id) ON DELETE CASCADE,
    UNIQUE (ticker_id, date)
);

-- Create sector market caps table
CREATE TABLE sector_market_caps (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sector_id INTEGER NOT NULL,
    date TEXT NOT NULL,
    market_cap REAL NOT NULL,
    sentiment_score REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (sector_id) REFERENCES sectors(id) ON DELETE CASCADE,
    UNIQUE (sector_id, date)
);

-- Create indexes for performance
CREATE INDEX idx_ticker_market_caps_date ON ticker_market_caps(date);
CREATE INDEX idx_sector_market_caps_date ON sector_market_caps(date);

-- Create view for sector market caps with sector names
CREATE VIEW sector_market_caps_view AS
SELECT 
    s.name AS sector_name,
    smc.date,
    smc.market_cap,
    smc.sentiment_score
FROM 
    sector_market_caps smc
JOIN
    sectors s ON smc.sector_id = s.id
ORDER BY 
    smc.date DESC, s.name;

-- Create view for ticker market caps with ticker symbols
CREATE VIEW ticker_market_caps_view AS
SELECT 
    t.symbol AS ticker,
    tmc.date,
    tmc.price,
    tmc.market_cap,
    tmc.shares_outstanding,
    tmc.data_source
FROM 
    ticker_market_caps tmc
JOIN
    tickers t ON tmc.ticker_id = t.id
ORDER BY 
    tmc.date DESC, t.symbol;

-- Create view for tickers by sector
CREATE VIEW tickers_by_sector AS
SELECT 
    s.name AS sector_name,
    t.symbol AS ticker,
    t.company_name
FROM 
    tickers t
JOIN
    ticker_sectors ts ON t.id = ts.ticker_id
JOIN
    sectors s ON ts.sector_id = s.id
ORDER BY 
    s.name, t.symbol;