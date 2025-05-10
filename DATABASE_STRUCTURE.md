# T2D Pulse Database Structure

This document outlines the database structure and schema for the T2D Pulse application, which helps with migrating the application to a new platform.

## Database Overview

T2D Pulse uses a PostgreSQL database to store:
- Historical sector sentiment data
- Market cap history
- User-defined weights and configurations
- Cached API responses

## Connection Configuration

The database connection is configured using environment variables:

```
DATABASE_URL=postgres://username:password@host:port/database
PGHOST=host
PGPORT=5432
PGUSER=username
PGPASSWORD=password
PGDATABASE=database
```

## Table Definitions

### Historical Sector Data

```sql
CREATE TABLE sector_sentiment_history (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    sector VARCHAR(100) NOT NULL,
    score DECIMAL(5,2) NOT NULL,
    market_cap_billions DECIMAL(10,2),
    ticker_count INTEGER,
    UNIQUE (date, sector)
);

CREATE INDEX idx_sector_sentiment_date ON sector_sentiment_history(date);
CREATE INDEX idx_sector_sentiment_sector ON sector_sentiment_history(sector);
```

### Ticker Market Cap Data

```sql
CREATE TABLE ticker_market_cap (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(20) NOT NULL,
    date DATE NOT NULL,
    market_cap_billions DECIMAL(20,4) NOT NULL,
    price DECIMAL(20,4) NOT NULL,
    volume BIGINT,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE (ticker, date)
);

CREATE INDEX idx_ticker_market_cap_ticker ON ticker_market_cap(ticker);
CREATE INDEX idx_ticker_market_cap_date ON ticker_market_cap(date);
```

### Sector Weights

```sql
CREATE TABLE sector_weights (
    id SERIAL PRIMARY KEY,
    sector VARCHAR(100) NOT NULL,
    weight DECIMAL(5,2) NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE (sector)
);
```

### Economic Indicators

```sql
CREATE TABLE economic_indicators (
    id SERIAL PRIMARY KEY,
    indicator_name VARCHAR(100) NOT NULL,
    date DATE NOT NULL,
    value DECIMAL(15,4) NOT NULL,
    source VARCHAR(50),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE (indicator_name, date)
);

CREATE INDEX idx_economic_indicators_name ON economic_indicators(indicator_name);
CREATE INDEX idx_economic_indicators_date ON economic_indicators(date);
```

### API Cache

```sql
CREATE TABLE api_cache (
    id SERIAL PRIMARY KEY,
    api_name VARCHAR(50) NOT NULL,
    endpoint VARCHAR(200) NOT NULL,
    params JSONB NOT NULL,
    response JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    expires_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (api_name, endpoint, params)
);

CREATE INDEX idx_api_cache_api_name ON api_cache(api_name);
CREATE INDEX idx_api_cache_expires_at ON api_cache(expires_at);
```

## Database Access in the Code

The database is accessed in the code using the following approaches:

### Direct SQL Execution

```python
import psycopg2
import os
from urllib.parse import urlparse

def get_db_connection():
    """Get a connection to the PostgreSQL database"""
    db_url = os.environ.get("DATABASE_URL")
    
    if db_url:
        # Parse the URL
        result = urlparse(db_url)
        username = result.username
        password = result.password
        database = result.path[1:]
        hostname = result.hostname
        port = result.port
        
        # Create connection
        conn = psycopg2.connect(
            database=database,
            user=username,
            password=password,
            host=hostname,
            port=port
        )
        return conn
    else:
        # Use individual connection parameters
        conn = psycopg2.connect(
            database=os.environ.get("PGDATABASE"),
            user=os.environ.get("PGUSER"),
            password=os.environ.get("PGPASSWORD"),
            host=os.environ.get("PGHOST"),
            port=os.environ.get("PGPORT")
        )
        return conn

def execute_query(query, params=None):
    """Execute a query and return the results"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(query, params or ())
            if query.strip().upper().startswith("SELECT"):
                return cur.fetchall()
            conn.commit()
    finally:
        conn.close()
```

### Repository Pattern in Files

Several files implement a repository pattern for database access:

- `sector_repository.py`: Handles sector data operations
- `market_cap_repository.py`: Manages market cap data
- `cache_repository.py`: Handles API cache operations

## Data Initialization

On first run, the application initializes the database by:

1. Creating tables if they don't exist
2. Loading default sector weights if not present
3. Initializing the cache tables
4. Setting up indexes for performance

This initialization happens in `app.py` within the application startup code.

## Database Migrations

The application doesn't use a formal migration system. When making schema changes:

1. Create a file called `migrations.py` in the export package
2. Implement the necessary SQL statements to update the schema
3. Run this file once on the new platform

## Best Practices for Migration

When migrating the database to a new platform:

1. **Export the existing data**:
   ```sql
   COPY sector_sentiment_history TO '/path/to/sector_sentiment_history.csv' WITH CSV HEADER;
   COPY ticker_market_cap TO '/path/to/ticker_market_cap.csv' WITH CSV HEADER;
   COPY sector_weights TO '/path/to/sector_weights.csv' WITH CSV HEADER;
   COPY economic_indicators TO '/path/to/economic_indicators.csv' WITH CSV HEADER;
   ```

2. **Create the schema on the new database**:
   ```bash
   psql -U username -d database -f schema.sql
   ```

3. **Import the data**:
   ```sql
   COPY sector_sentiment_history FROM '/path/to/sector_sentiment_history.csv' WITH CSV HEADER;
   COPY ticker_market_cap FROM '/path/to/ticker_market_cap.csv' WITH CSV HEADER;
   COPY sector_weights FROM '/path/to/sector_weights.csv' WITH CSV HEADER;
   COPY economic_indicators FROM '/path/to/economic_indicators.csv' WITH CSV HEADER;
   ```

4. **Update connection settings** in the environment variables

## Database Backup Strategy

Implement a regular backup strategy on the new platform:

```bash
pg_dump -U username -d database -Fc > t2d_pulse_backup_$(date +%Y%m%d).dump
```

For automated backups, create a cron job to run daily:

```
0 0 * * * pg_dump -U username -d database -Fc > /path/to/backups/t2d_pulse_backup_$(date +%Y%m%d).dump
```

This document should help with understanding and migrating the database structure for T2D Pulse to a new platform.