# Stablecoin Pool Observatory

A data engineering learning project that records USDC/USDT pool observations
across Uniswap v3, Curve, and Fluid on Ethereum. The question: **how do liquidity
and observed stablecoin prices differ across these pools over time?**

```text
GeckoTerminal API -> immutable JSON archives -> PostgreSQL raw layer
                                             -> dbt SQL views + data checks
                                             -> Metabase
```

Airflow can schedule the collection, load, and dbt checks hourly; see below.

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

## Run with Airflow

The optional Airflow 3.3.2 profile runs a local standalone instance with
LocalExecutor. It has its own PostgreSQL metadata database and persistent
Airflow home; the existing PostgreSQL database still stores pool observations.
An initialization container gives Airflow's non-root user access to the archive
directory and existing dbt output files, including those created by manual runs.
dbt and the collector run in a separate Python environment inside the Airflow
image so their dependencies do not replace Airflow's runtime dependencies.

```powershell
docker compose --profile airflow up -d --build airflow
docker compose exec airflow cat /opt/airflow/simple_auth_manager_passwords.json.generated
```

The first startup initializes Airflow and may take a minute. Open
http://localhost:8080 and sign in as `admin` with the generated password from
the command above. The DAG `pool_observatory` starts **paused**. Unpause it to
enable hourly collection, then use **Trigger DAG** to test immediately and
inspect the graph and task logs. Unpausing can also start the latest eligible
scheduled run; the concurrency limit keeps the runs sequential.
Metabase can be started separately with `docker compose up -d metabase`.

```text
collect_archive -> load_batch -> build_analytics
```

- `collect_archive` validates the watchlist, fetches and archives observations,
  and passes only the batch directory path through Airflow's XCom.
- `load_batch` replays that exact batch into PostgreSQL in one transaction.
- `build_analytics` runs `dbt build`, including the existing data tests.
- Each task has two retries, two minutes apart, and a ten-minute timeout.
  Only one DAG run is active at a time. Exhausted failures appear in the UI and
  task logs; a callback also logs the failed DAG, run, and task identities.
  External email or chat notifications are not configured.

An Airflow run ID maps to a stable archive UUID. Retrying or clearing collection
within the same run reuses validated saved responses and fetches only missing
pools. Invalid saved responses stop the task for investigation; they are never
silently overwritten. Do not change the watchlist while recovering a batch.
Partial collections can contain different observation times across retries.
Clearing a load task replays the same batch, so existing observations are not
duplicated. **Triggering a new DAG run creates a new collection.**

To recover a failed load, fix the database issue, then clear `load_batch` and
its downstream task in the existing run using the UI. Keep `collect_archive`
successful and keep its archived files. If collection failed, investigate the
archive before clearing it and its downstream tasks. A delayed recovery may
fail the two-hour dbt freshness check even though replay itself succeeds.

Catchup is disabled: this endpoint supplies current observations, so executing
missed schedules cannot recover historical prices. The hourly schedule requires
Docker and this computer to stay running. Avoid manual collection or replay
while a DAG run is active; Airflow's concurrency limit only governs this DAG.

Verify the pipeline and DAG without calling the API:

```powershell
docker compose exec airflow python -m unittest test_pipeline test_airflow -v
```

Stop orchestration with `docker compose --profile airflow stop airflow airflow-db`.
Keep the named volumes and `data/raw/` to retain run history and recovery data.
This uses local demo database credentials and Airflow's development auth manager;
the UI binds to loopback. It is a learning setup, not a production deployment.
The shared local archive works because all tasks run on one Docker host; move it
to shared object storage before introducing workers on other machines.

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
4. **Operations:** run the Airflow DAG hourly, inspect failures, and demonstrate
   recovery from a saved batch. Add a database check against the configured
   watchlist; current dbt recency checks cover only pools present in the data.
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
