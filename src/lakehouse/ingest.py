"""Download one TLC month and land it, byte-for-byte unmodified, in the lake's raw zone.

Idempotent by construction: the destination key is a pure function of the month, so a re-run
overwrites exactly one object and can never create a second copy. Nothing reaches the lake
until the download is complete and readable, so a failed re-run leaves the previous good file
in place. The manifest is written last; its presence means the file beside it is complete.

Raw is deliberately not schema-checked here. Enforcing the canonical schema is Spark's job.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import tempfile
from datetime import UTC, datetime
from typing import Any

import pyarrow.parquet as pq
import requests

from lakehouse.config import Month, lake_root, raw_file_uri, raw_manifest_uri, source_url
from lakehouse.storage import put_file, write_json

log = logging.getLogger(__name__)
CHUNK = 1 << 20


class SourceNotPublishedError(RuntimeError):
    """TLC has not published this month yet (its CDN answers 403/404 for missing objects)."""


class IngestValidationError(RuntimeError):
    """The download was truncated or is not a readable, non-empty Parquet file."""


def download(url: str, dest_path: str, session: requests.Session) -> dict[str, Any]:
    with session.get(url, stream=True, timeout=(10, 300)) as resp:
        if resp.status_code in (403, 404):
            raise SourceNotPublishedError(f"{url} returned HTTP {resp.status_code}")
        resp.raise_for_status()
        expected = resp.headers.get("Content-Length")
        digest, size = hashlib.sha256(), 0
        with open(dest_path, "wb") as out:
            for chunk in resp.iter_content(chunk_size=CHUNK):
                out.write(chunk)
                digest.update(chunk)
                size += len(chunk)
    if expected is not None and size != int(expected):
        raise IngestValidationError(f"{url}: got {size} bytes, Content-Length said {expected}")
    return {
        "source_url": url,
        "etag": resp.headers.get("ETag"),
        "last_modified": resp.headers.get("Last-Modified"),
        "bytes": size,
        "sha256": digest.hexdigest(),
    }


def validate_parquet(path: str) -> int:
    try:
        rows = pq.ParquetFile(path).metadata.num_rows
    except Exception as exc:  # pyarrow raises several types for corrupt/non-Parquet input
        raise IngestValidationError(f"{path} is not a readable Parquet file: {exc}") from exc
    if rows == 0:
        raise IngestValidationError(f"{path} has zero rows")
    return rows


def ingest_month(
    month: Month, root: str, *, session: requests.Session | None = None
) -> dict[str, Any]:
    session = session or requests.Session()
    with tempfile.TemporaryDirectory() as tmp:
        local = os.path.join(tmp, f"yellow_tripdata_{month}.parquet")
        manifest = download(source_url(month), local, session)
        manifest.update(
            month=str(month),
            num_rows=validate_parquet(local),
            raw_uri=raw_file_uri(root, month),
            ingested_at=datetime.now(UTC).isoformat(),
        )
        put_file(local, manifest["raw_uri"])
    write_json(manifest, raw_manifest_uri(root, month))
    log.info("ingested %s: %s rows -> %s", month, manifest["num_rows"], manifest["raw_uri"])
    return manifest


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--month", required=True, type=Month.parse, help="YYYY-MM")
    parser.add_argument("--lake-root", default=lake_root())
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    print(json.dumps(ingest_month(args.month, args.lake_root), indent=2))


if __name__ == "__main__":
    main()
