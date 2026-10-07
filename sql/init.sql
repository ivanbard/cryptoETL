CREATE DATABASE metabase_app;
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS raw.pool_snapshots (
    source text NOT NULL CHECK (source = 'geckoterminal'),
    network text NOT NULL,
    pool_address text NOT NULL,
    observed_at timestamptz NOT NULL,
    run_id uuid NOT NULL,
    request_url text NOT NULL,
    payload jsonb NOT NULL,
    PRIMARY KEY (source, network, pool_address, observed_at)
);

-- Local demo credentials. Change before hosting outside this machine.
CREATE ROLE analyst LOGIN PASSWORD 'local_readonly';
GRANT CONNECT ON DATABASE crypto_etl TO analyst;
GRANT USAGE ON SCHEMA analytics TO analyst;
ALTER DEFAULT PRIVILEGES FOR ROLE crypto IN SCHEMA analytics
    GRANT SELECT ON TABLES TO analyst;
