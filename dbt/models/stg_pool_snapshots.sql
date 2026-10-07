with extracted as (
    select
        source, network, pool_address, observed_at, run_id,
        payload -> 'data' -> 'attributes' as attrs,
        payload -> 'data' -> 'relationships' as rel
    from {{ source('raw', 'pool_snapshots') }}
)
select
    md5(source || ':' || network || ':' || pool_address || ':' || observed_at::text) as observation_id,
    source, network, pool_address, observed_at, run_id,
    attrs ->> 'name' as pool_name,
    rel -> 'dex' -> 'data' ->> 'id' as dex_id,
    rel -> 'base_token' -> 'data' ->> 'id' as base_token_id,
    rel -> 'quote_token' -> 'data' ->> 'id' as quote_token_id,
    (attrs ->> 'base_token_price_usd')::numeric as base_price_usd,
    (attrs ->> 'quote_token_price_usd')::numeric as quote_price_usd,
    (attrs ->> 'reserve_in_usd')::numeric as liquidity_usd,
    (attrs -> 'volume_usd' ->> 'h24')::numeric as rolling_volume_24h_usd
from extracted
