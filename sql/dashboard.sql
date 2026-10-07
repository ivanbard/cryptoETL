-- Metabase: liquidity by DEX (bar chart); share is a fraction, format as percent.
SELECT dex_id, liquidity_usd, cohort_liquidity_share FROM analytics.dex_liquidity;

-- Metabase: token price differences (table), grouped by contract identity.
SELECT token_id, count(*) AS pools, min(price_usd) AS min_price_usd,
       max(price_usd) AS max_price_usd,
       (max(price_usd) - min(price_usd)) * 10000 AS price_range_bps
FROM analytics.token_prices
GROUP BY token_id;

-- Metabase: liquidity history (line chart), one series per pool.
SELECT observed_at, dex_id || ': ' || pool_name AS pool, liquidity_usd
FROM analytics.stg_pool_snapshots
ORDER BY observed_at;

-- Metabase: collection health (table). Missing metrics remain NULL.
SELECT pool_name, observed_at, collection_age_minutes, liquidity_usd,
       rolling_volume_24h_usd, volume_to_liquidity
FROM analytics.latest_pools
ORDER BY collection_age_minutes DESC;
