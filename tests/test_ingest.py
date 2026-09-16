import io
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import requests

from lakehouse.config import Month, raw_file_uri, raw_manifest_uri, source_url
from lakehouse.ingest import IngestValidationError, SourceNotPublishedError, ingest_month

MONTH = Month(2024, 6)


def parquet_bytes(rows: int) -> bytes:
    buf = io.BytesIO()
    pq.write_table(pa.table({"VendorID": list(range(rows))}), buf)
    return buf.getvalue()


class FakeResponse:
    def __init__(self, status=200, body=b"", headers=None):
        self.status_code, self.body = status, body
        self.headers = (
            {"Content-Length": str(len(body)), "ETag": '"abc"'} if headers is None else headers
        )

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(str(self.status_code))

    def iter_content(self, chunk_size):
        for i in range(0, len(self.body), chunk_size):
            yield self.body[i : i + chunk_size]


class FakeSession:
    def __init__(self, *responses):
        self.responses, self.urls = list(responses), []

    def get(self, url, **kwargs):
        self.urls.append(url)
        return self.responses.pop(0)


def read_rows(uri):
    return pq.read_metadata(uri).num_rows


def test_lands_raw_file_and_manifest_at_deterministic_key(tmp_path):
    root = str(tmp_path)
    manifest = ingest_month(MONTH, root, session=FakeSession(FakeResponse(body=parquet_bytes(3))))
    assert read_rows(raw_file_uri(root, MONTH)) == 3
    on_disk = json.loads(open(raw_manifest_uri(root, MONTH)).read())
    assert on_disk["num_rows"] == manifest["num_rows"] == 3
    assert on_disk["source_url"] == source_url(MONTH)
    assert len(on_disk["sha256"]) == 64


def test_rerun_overwrites_instead_of_duplicating(tmp_path):
    root = str(tmp_path)
    session = FakeSession(FakeResponse(body=parquet_bytes(3)), FakeResponse(body=parquet_bytes(5)))
    ingest_month(MONTH, root, session=session)
    ingest_month(MONTH, root, session=session)
    month_dir = tmp_path / "raw/yellow/year=2024/month=06"
    assert sorted(p.name for p in month_dir.iterdir()) == [
        "_manifest.json",
        "yellow_tripdata_2024-06.parquet",
    ]
    assert read_rows(raw_file_uri(root, MONTH)) == 5


@pytest.mark.parametrize("status", [403, 404])
def test_unpublished_month_raises_specific_error(tmp_path, status):
    with pytest.raises(SourceNotPublishedError):
        ingest_month(MONTH, str(tmp_path), session=FakeSession(FakeResponse(status=status)))
    assert not (tmp_path / "raw").exists()


def test_truncated_download_is_rejected_and_nothing_lands(tmp_path):
    body = parquet_bytes(3)
    resp = FakeResponse(body=body[:-10], headers={"Content-Length": str(len(body))})
    with pytest.raises(IngestValidationError, match="Content-Length"):
        ingest_month(MONTH, str(tmp_path), session=FakeSession(resp))
    assert not (tmp_path / "raw").exists()


def test_failed_rerun_keeps_previous_good_file(tmp_path):
    root = str(tmp_path)
    session = FakeSession(FakeResponse(body=parquet_bytes(3)), FakeResponse(body=b"not parquet"))
    ingest_month(MONTH, root, session=session)
    before = open(raw_manifest_uri(root, MONTH)).read()
    with pytest.raises(IngestValidationError):
        ingest_month(MONTH, root, session=session)
    assert read_rows(raw_file_uri(root, MONTH)) == 3
    assert open(raw_manifest_uri(root, MONTH)).read() == before
