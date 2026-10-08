"""Collect pool observations, archive responses, and replay them into PostgreSQL."""

import argparse
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import json
import logging
from pathlib import Path
import re
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from uuid import UUID, uuid4


LOG = logging.getLogger(__name__)
ROOT = Path(__file__).resolve().parent
API = "https://api.geckoterminal.com/api/v2"
ADDRESS = re.compile(r"0x[0-9a-fA-F]{40}")


def fetch(url):
    for attempt in range(4):
        try:
            request = Request(url, headers={"Accept": "application/json", "User-Agent": "crypto-etl/0.1"})
            with urlopen(request, timeout=30) as response:
                return json.load(response)
        except HTTPError as exc:
            if exc.code != 429 and exc.code < 500:
                raise
            retry_after = exc.headers.get("Retry-After", "")
            delay = min(float(retry_after), 60) if retry_after.isdigit() else 2 ** (attempt + 1)
        except (URLError, TimeoutError):
            delay = 2 ** (attempt + 1)
        if attempt == 3:
            raise RuntimeError(f"Request failed after four attempts: {url}")
        LOG.warning("Retrying %s in %ss", url, delay)
        time.sleep(delay)


def validate(record):
    if record["source"] != "geckoterminal" or record["network"] != "eth":
        raise ValueError("This first version accepts GeckoTerminal Ethereum records only")
    address = record["pool_address"]
    if not ADDRESS.fullmatch(address):
        raise ValueError("Invalid pool contract address")
    observed_at = datetime.fromisoformat(record["observed_at"])
    if observed_at.tzinfo is None:
        raise ValueError("Observation timestamp must include a timezone")
    UUID(record["run_id"])
    if record["request_url"] != f"{API}/networks/eth/pools/{address}":
        raise ValueError("Archive request URL does not match pool identity")
    pool = record["payload"]["data"]
    if not isinstance(pool, dict) or pool["type"] != "pool":
        raise ValueError("Expected one pool object; empty responses are not successful collections")
    attrs = pool["attributes"]
    if attrs["address"].lower() != address.lower() or pool["id"].lower() != f"eth_{address}".lower():
        raise ValueError("API returned a different pool")
    if not attrs["name"] or not pool["relationships"]["dex"]["data"]["id"]:
        raise ValueError("Pool name and DEX are required")
    for side in ("base_token", "quote_token"):
        token_id = pool["relationships"][side]["data"]["id"]
        if not token_id.startswith("eth_") or not ADDRESS.fullmatch(token_id[4:]):
            raise ValueError("Token identity must include network and contract address")
    metrics = [attrs.get(key) for key in (
        "base_token_price_usd", "quote_token_price_usd", "reserve_in_usd"
    )] + [attrs.get("volume_usd", {}).get("h24")]
    for value in metrics:
        if value is not None:
            number = Decimal(str(value))
            if not number.is_finite() or number < 0:
                raise ValueError("Prices, liquidity, and volume must be finite and nonnegative")
    return record


def collect(watchlist, archive, run_id=None):
    config = json.loads(watchlist.read_text(encoding="utf-8"))
    if config["network"] != "eth" or not config["pools"]:
        raise ValueError("Configure a nonempty Ethereum watchlist")
    addresses = [pool["address"].lower() for pool in config["pools"]]
    if len(set(addresses)) != len(addresses) or any(not ADDRESS.fullmatch(a) for a in addresses):
        raise ValueError("Watchlist requires unique Ethereum pool addresses")
    run_id = str(UUID(run_id)) if run_id is not None else str(uuid4())
    directory = archive / run_id
    directory.mkdir(parents=True, exist_ok=True)
    if {path.stem for path in directory.glob("*.json")} - set(addresses):
        raise ValueError("Archived batch contains pools outside the current watchlist")
    records = []
    for address in addresses:
        path = directory / f"{address}.json"
        if path.exists():
            record = validate(json.loads(path.read_text(encoding="utf-8")))
            if record["run_id"] != run_id or record["pool_address"].lower() != address:
                raise ValueError("Archived observation does not match its batch or filename")
            records.append(record)
            LOG.info("Reusing archived observation %s", path)
            continue
        url = f"{API}/networks/eth/pools/{address}"
        payload = fetch(url)
        record = dict(source="geckoterminal", network="eth", pool_address=address,
                      observed_at=datetime.now(timezone.utc).isoformat(), run_id=run_id,
                      request_url=url, payload=payload)
        # Preserve even malformed responses so failures can be investigated.
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps(record, indent=2, allow_nan=False), encoding="utf-8")
        temporary.replace(path)
        records.append(validate(record))
        LOG.info("Archived %s at %s", address, path)
        # ponytail: serial requests at <30/min; batch endpoint if watchlist grows.
        time.sleep(2.2)
    return records


def load(records):
    import psycopg
    from psycopg.types.json import Jsonb

    records = [validate(record) for record in records]
    if not records:
        raise ValueError("No archived observations to load")
    inserted = 0
    with psycopg.connect() as connection:
        for record in records:
            result = connection.execute(
                """INSERT INTO raw.pool_snapshots
                   (source, network, pool_address, observed_at, run_id, request_url, payload)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)
                   ON CONFLICT (source, network, pool_address, observed_at) DO NOTHING""",
                (record["source"], record["network"], record["pool_address"].lower(),
                 record["observed_at"], record["run_id"], record["request_url"], Jsonb(record["payload"])),
            )
            inserted += result.rowcount
    LOG.info("Committed %s new observations (%s replayed duplicates)", inserted, len(records) - inserted)
    return inserted


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    collect_parser = sub.add_parser("collect", help="Fetch, archive, then load the watchlist")
    collect_parser.add_argument("--watchlist", type=Path, default=ROOT / "watchlist.json")
    collect_parser.add_argument("--archive", type=Path, default=ROOT / "data" / "raw")
    collect_parser.add_argument("--archive-only", action="store_true")
    collect_parser.add_argument("--run-id", help="UUID identifying a batch to resume on retry")
    replay_parser = sub.add_parser("replay", help="Load saved observations without API calls")
    replay_parser.add_argument("path", type=Path)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        if args.command == "collect":
            records = collect(args.watchlist, args.archive, args.run_id)
            if not args.archive_only:
                load(records)
        else:
            paths = [args.path] if args.path.is_file() else sorted(args.path.rglob("*.json"))
            load([json.loads(path.read_text(encoding="utf-8")) for path in paths])
    except (KeyError, TypeError, ValueError, InvalidOperation, RuntimeError, OSError) as exc:
        LOG.error("Pipeline failed: %s", exc)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
