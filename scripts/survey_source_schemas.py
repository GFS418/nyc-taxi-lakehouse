"""Survey column names, physical types, and row counts of every monthly TLC yellow file.

Reads only each file's Parquet footer through HTTP range requests, so it downloads a few
hundred kilobytes instead of gigabytes. This is the evidence behind the canonical schema.
Usage: uv run python scripts/survey_source_schemas.py
"""

import collections
import concurrent.futures as cf

import fsspec
import pyarrow.parquet as pq

BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{}.parquet"
MONTHS = [f"{y}-{m:02d}" for y in range(2019, 2025) for m in range(1, 13)]
fs = fsspec.filesystem("https")


def footer(month: str):
    with fs.open(BASE.format(month), "rb", block_size=2**20) as f:
        md = pq.ParquetFile(f).metadata
        return month, md.num_rows, {n.name: str(n.type) for n in md.schema.to_arrow_schema()}


def main() -> None:
    with cf.ThreadPoolExecutor(12) as ex:
        results = sorted(ex.map(footer, MONTHS))
    rows_by_year: collections.Counter[str] = collections.Counter()
    types = collections.defaultdict(lambda: collections.defaultdict(list))
    for month, rows, cols in results:
        rows_by_year[month[:4]] += rows
        for col, typ in cols.items():
            types[col][typ].append(month)
    print("rows by year:", dict(rows_by_year), "total:", sum(rows_by_year.values()))
    for col, by_type in types.items():
        present = sum(len(v) for v in by_type.values())
        desc = "; ".join(f"{t} x{len(ms)} ({ms[0]}..{ms[-1]})" for t, ms in by_type.items())
        print(f"{col:24s} present {present:2d}/{len(MONTHS)}  {desc}")


if __name__ == "__main__":
    main()
