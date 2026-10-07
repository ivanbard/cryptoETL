"""Small offline regression check; no database or API access needed."""

from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch
from uuid import uuid4

import pipeline


class PipelineCheck(unittest.TestCase):
    def test_archive_validation_and_retries(self):
        address = "0x3416cf6c708da44db2624d63ea0aaef7113527c6"
        payload = {"data": {"id": f"eth_{address}", "type": "pool", "attributes": {
            "address": address, "name": "USDC / USDT", "reserve_in_usd": "100000.01",
            "base_token_price_usd": "1.0001", "quote_token_price_usd": None,
            "volume_usd": {"h24": "200.5"},
        }, "relationships": {
            "dex": {"data": {"id": "uniswap_v3"}},
            "base_token": {"data": {"id": "eth_0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"}},
            "quote_token": {"data": {"id": "eth_0xdac17f958d2ee523a2206206994597c13d831ec7"}},
        }}}
        record = dict(source="geckoterminal", network="eth", pool_address=address,
                      observed_at=datetime.now(timezone.utc).isoformat(), run_id=str(uuid4()),
                      request_url=f"{pipeline.API}/networks/eth/pools/{address}", payload=payload)
        self.assertIs(pipeline.validate(record), record)
        for key, value in (("reserve_in_usd", "-1"), ("base_token_price_usd", "NaN"),
                           ("address", "0x" + "0" * 40)):
            invalid = deepcopy(record)
            invalid["payload"]["data"]["attributes"][key] = value
            with self.assertRaises(ValueError):
                pipeline.validate(invalid)
        invalid = deepcopy(record)
        invalid["payload"]["data"] = None
        with self.assertRaises(ValueError):
            pipeline.validate(invalid)
        invalid = deepcopy(record)
        invalid["observed_at"] = "2026-01-01T00:00:00"
        with self.assertRaises(ValueError):
            pipeline.validate(invalid)

        with TemporaryDirectory() as directory:
            watchlist = Path(directory) / "watchlist.json"
            watchlist.write_text(pipeline.json.dumps({"network": "eth", "pools": [{"address": address}]}))
            archive = Path(directory) / "raw"
            with patch.object(pipeline, "fetch", return_value=payload), patch.object(pipeline.time, "sleep"):
                collected = pipeline.collect(watchlist, archive)
            files = list(archive.rglob("*.json"))
            self.assertEqual(len(files), 1)
            saved = pipeline.json.loads(files[0].read_text())
            self.assertEqual(saved, collected[0])
            self.assertIs(pipeline.validate(saved), saved)
            with patch.object(pipeline, "fetch", return_value={"data": None}), patch.object(pipeline.time, "sleep"):
                with self.assertRaises(ValueError):
                    pipeline.collect(watchlist, archive)
            self.assertEqual(len(list(archive.rglob("*.json"))), 2)

        with patch.object(pipeline, "urlopen", side_effect=TimeoutError), patch.object(pipeline.time, "sleep") as sleep:
            with self.assertRaises(RuntimeError):
                pipeline.fetch("https://example.com")
            self.assertEqual(sleep.call_count, 3)


if __name__ == "__main__":
    unittest.main()
