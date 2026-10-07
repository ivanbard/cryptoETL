with prices as (
    select network, pool_address, pool_name, dex_id, observed_at, liquidity_usd,
        base_token_id as token_id, base_price_usd as price_usd
    from {{ ref('latest_pools') }}
    where collection_age_minutes between 0 and 120
    union all
    select network, pool_address, pool_name, dex_id, observed_at, liquidity_usd,
        quote_token_id as token_id, quote_price_usd as price_usd
    from {{ ref('latest_pools') }}
    where collection_age_minutes between 0 and 120
)
select *, (price_usd - 1) * 10000 as peg_deviation_bps
from prices
where token_id in (
    'eth_0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
    'eth_0xdac17f958d2ee523a2206206994597c13d831ec7'
)
