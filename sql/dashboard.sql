-- Metabase: Liquidity by DEX (USD), bar chart. Tracked cohort only.
SELECT dex_id AS dex, liquidity_usd
FROM analytics.dex_liquidity
ORDER BY liquidity_usd DESC;

-- Metabase: Stablecoin price differences, table. Text preserves six decimals
-- in the default BI table format; grouping still uses contract identity.
SELECT CASE token_id
           WHEN 'eth_0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48' THEN 'USDC'
           WHEN 'eth_0xdac17f958d2ee523a2206206994597c13d831ec7' THEN 'USDT'
       END AS token,
       COUNT(*) AS pools,
       ROUND(MIN(price_usd), 6)::text AS "Min USD",
       ROUND(MAX(price_usd), 6)::text AS "Max USD",
       ROUND((MAX(price_usd) - MIN(price_usd)) * 10000, 2) AS "Range (bps)"
FROM analytics.token_prices
GROUP BY token_id
ORDER BY token;

-- Metabase: Liquidity observations (USD), line chart. One tracked pool per DEX.
-- Lines connect samples; values between collection times are unknown.
SELECT observed_at, dex_id AS dex, liquidity_usd
FROM analytics.stg_pool_snapshots
ORDER BY observed_at;

-- Metabase: Collection health and activity, table. Collection age does not
-- measure upstream freshness. Rolling volume is not additive across snapshots.
SELECT dex_id AS dex, ROUND(collection_age_minutes, 1) AS "Age (min)",
       CASE WHEN collection_age_minutes > 120 THEN 'Stale collection'
            WHEN liquidity_usd IS NULL OR rolling_volume_24h_usd IS NULL THEN 'Missing metrics'
            ELSE 'Collected'
       END AS status,
       ROUND(liquidity_usd, 2) AS "Liquidity USD",
       ROUND(rolling_volume_24h_usd, 2) AS "Rolling 24h USD",
       ROUND(volume_to_liquidity, 2) AS "Volume / liquidity"
FROM analytics.latest_pools
ORDER BY collection_age_minutes DESC;
