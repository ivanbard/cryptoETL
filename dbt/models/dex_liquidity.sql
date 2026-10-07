with totals as (
    select dex_id, count(*) as tracked_pools, sum(liquidity_usd) as liquidity_usd,
        sum(rolling_volume_24h_usd) as rolling_volume_24h_usd
    from {{ ref('latest_pools') }}
    where collection_age_minutes between 0 and 120
    group by dex_id
)
select *, liquidity_usd / nullif(sum(liquidity_usd) over (), 0) as cohort_liquidity_share
from totals
