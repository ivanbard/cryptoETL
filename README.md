# Stablecoin Pool Observatory

A data engineering learning project that records USDC/USDT pool observations
across Uniswap v3, Curve, and Fluid on Ethereum. The question: **how do liquidity
and observed stablecoin prices differ across these pools over time?**

```text
GeckoTerminal API -> immutable JSON archives -> PostgreSQL raw layer
                                             -> dbt SQL views + data checks
                                             -> Metabase
```

## Run locally

Prerequisite: Docker Desktop running with Linux containers. No API key required
for the GeckoTerminal public endpoints used here. API availability can change.
The first build downloads Python dependencies, PostgreSQL, and Metabase.

```powershell
docker compose up -d postgres metabase
docker compose build pipeline
docker compose run --rm pipeline collect
docker compose run --rm --entrypoint dbt pipeline build --project-dir dbt --profiles-dir .
```

Metabase: http://localhost:3000. Complete its local account setup, then add a
PostgreSQL data connection using:

| Field | Value |
| --- | --- |
| Host | `postgres` |
| Port | `5432` |
| Database | `crypto_etl` |
| Username | `analyst` |
| Password | `local_readonly` |
| Schemas | `analytics` |
| SSL | Off for this local Docker connection |

The analyst role can read analytics views but cannot modify data or read raw
payloads. `metabase_app` is a separate database for Metabase's own settings.
Host-side PostgreSQL tools connect to `localhost:55432` as `crypto`, password
`local_dev_only`. These are local demo credentials. Services bind to loopback;
set proper credentials and permissions before any external deployment.

Create a dashboard called **Stablecoin Pool Observatory** with the four queries
in [sql/dashboard.sql](sql/dashboard.sql): DEX liquidity share, token price
range, liquidity history, and collection health. Save each as a native SQL
question and select the indicated chart. Collection history becomes useful
after several runs; a single run is only a snapshot.

## Collect and recover

Run `collect` again to add a new observation for each pool. Each batch gets its
own UUID directory under `data/raw/`; each file includes the provider response,
request URL, pool identity, run ID, and UTC collection timestamp. Failed
validation leaves the response archived and exits nonzero. Earlier responses
from a partially failed batch remain available; no rows from that collection
are committed until all configured pools validate. A database failure also
leaves the archive intact.

Replay all valid saved observations without calling the API:

```powershell
docker compose run --rm pipeline replay data/raw
docker compose run --rm --entrypoint dbt pipeline build --project-dir dbt --profiles-dir .
```

You can pass an individual run directory or JSON file instead. A malformed
archive stops replay before any rows are committed: investigate it, then replay
the valid files explicitly. Replaying the same observation twice inserts no
duplicate rows. Intentionally corrected observations require a separate
correction workflow; replay does not overwrite an existing observation.

To exercise rebuilding without deleting history, truncate only the derived raw
database table, then replay and rebuild dbt. Do this only on your local demo
database, once you have verified the archives are present.

To collect without Docker or PostgreSQL, using Python's standard library:

```powershell
python pipeline.py collect --archive-only
python -m unittest test_pipeline -v
```

Stop services with `docker compose stop`. Database state lives in a Docker
volume; raw archives live in this repository's ignored `data/` directory.
Deleting the Docker volume destroys database state, including Metabase setup.

## Data contracts and interpretation

- Grain: one provider, network, pool, and collection timestamp. The database
  primary key enforces this grain. Token IDs include network and contract
  address, never ticker alone. Money-like values use PostgreSQL `numeric`.
- `observed_at` means the time the HTTP response was collected. The endpoint
  does not provide a guaranteed market-data update timestamp. A fresh collection
  does not prove the underlying prices are fresh.
- `liquidity_usd` is the provider's pool reserve valuation, not executable
  market depth. USD prices are provider estimates, not trade execution quotes.
- `rolling_volume_24h_usd` covers a moving 24-hour window. Never sum successive
  hourly observations to calculate daily volume; their windows overlap.
- The watchlist is a small fixed cohort, not all Ethereum liquidity. Shares
  describe only fresh tracked pools. Observations are collected sequentially,
  not at exactly the same instant.
- Missing numeric fields remain NULL. Zero is a real value. Models exclude
  collections older than two hours from current comparison views; the health
  view still shows old observations.
- `token_prices` emits both base and quote token prices and restricts peg
  calculations to Ethereum USDC/USDT contract IDs. Add explicit token metadata
  before extending peg analysis to other assets.
- dbt checks observation uniqueness, required identities, nonnegative metrics,
  and collection recency for every pool present. It does not yet reconcile the
  database against the configured watchlist or detect upstream stale estimates.
- Live requests have bounded retries for network failures, rate limits, and
  server errors. Requests are serial and spaced to stay below 30/minute.

## Learning milestones

1. **Ingestion and SQL:** run a collection, inspect one raw response, query
   `raw.pool_snapshots`, and trace each field into `stg_pool_snapshots`.
2. **Recovery:** replay one batch twice and verify the row count stays the same.
   Rebuild from the archives and explain the primary key and transaction scope.
3. **BI:** build the four Metabase charts and explain missing data, rolling
   windows, token identity, and the cohort's coverage limits.
4. **Operations:** add an hourly scheduled run with failure reporting and a
   configured-pool coverage check. Learn Airflow when implementing dependency
   management and recovery. Until then, run collection manually.
5. **Second source:** add another provider for the same contract IDs; preserve
   source-specific observations and compare definitions before joining metrics.
6. **Cloud:** move the raw archive to S3 and deploy the proven pipeline with a
   budget. Add historical backfills only from a source that actually supplies
   history; replaying local snapshots cannot recover missed market observations.

For the resume, report measured runtime, observation count, successful recovery,
and the checks you implemented. Avoid claiming complete market coverage,
production reliability, or a scheduler until you have demonstrated them.

References: [GeckoTerminal API](https://api.geckoterminal.com/docs/index.html),
[dbt PostgreSQL setup](https://docs.getdbt.com/docs/local/connect-data-platform/postgres-setup),
[Metabase PostgreSQL connection](https://www.metabase.com/docs/latest/administration-guide/databases/postgresql).
