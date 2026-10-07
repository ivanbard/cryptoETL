select * from {{ ref('stg_pool_snapshots') }}
where base_price_usd < 0 or quote_price_usd < 0
    or liquidity_usd < 0 or rolling_volume_24h_usd < 0
