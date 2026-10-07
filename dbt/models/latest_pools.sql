with ranked as (
    select *, row_number() over (
        partition by source, network, pool_address order by observed_at desc
    ) as observation_rank
    from {{ ref('stg_pool_snapshots') }}
)
select *, extract(epoch from (current_timestamp - observed_at)) / 60 as collection_age_minutes,
    rolling_volume_24h_usd / nullif(liquidity_usd, 0) as volume_to_liquidity
from ranked
where observation_rank = 1
