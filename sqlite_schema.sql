-- Create table for sectors
CREATE TABLE IF NOT EXISTS sectors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create table for tickers
CREATE TABLE IF NOT EXISTS tickers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT UNIQUE NOT NULL,
    name TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create junction table for ticker-sector relationships (many-to-many)
CREATE TABLE IF NOT EXISTS ticker_sectors (
    ticker_id INTEGER,
    sector_id INTEGER,
    PRIMARY KEY (ticker_id, sector_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (ticker_id) REFERENCES tickers(id) ON DELETE CASCADE,
    FOREIGN KEY (sector_id) REFERENCES sectors(id) ON DELETE CASCADE
);

-- Create table for ticker market cap data
CREATE TABLE IF NOT EXISTS ticker_market_caps (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker_id INTEGER,
    date DATE NOT NULL,
    price REAL,
    market_cap REAL,
    shares_outstanding REAL,
    data_source TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (ticker_id, date),
    FOREIGN KEY (ticker_id) REFERENCES tickers(id) ON DELETE CASCADE
);

-- Create table for sector market cap history
CREATE TABLE IF NOT EXISTS sector_market_caps (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sector_id INTEGER,
    date DATE NOT NULL,
    market_cap REAL,
    sentiment_score REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (sector_id, date),
    FOREIGN KEY (sector_id) REFERENCES sectors(id) ON DELETE CASCADE
);