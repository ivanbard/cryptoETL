select pool_address as failure
from {{ ref('latest_pools') }}
where collection_age_minutes not between 0 and 120
union all
select 'No observations' as failure
where not exists (
    select 1 from {{ ref('latest_pools') }}
)
