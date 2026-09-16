"""Build tiny raw TLC files with the exact physical types each era of real files uses."""

from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq

MONEY = [
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
]


def trip(**overrides):
    row = dict(
        VendorID=1,
        tpep_pickup_datetime=datetime(2019, 1, 15, 8, 0),
        tpep_dropoff_datetime=datetime(2019, 1, 15, 8, 20),
        passenger_count=1,
        trip_distance=3.2,
        RatecodeID=1,
        store_and_fwd_flag="N",
        PULocationID=161,
        DOLocationID=236,
        payment_type=1,
        fare_amount=15.0,
        extra=0.5,
        mta_tax=0.5,
        tip_amount=3.0,
        tolls_amount=0.0,
        improvement_surcharge=0.3,
        total_amount=19.3,
        congestion_surcharge=2.5,
    )
    row.update(overrides)
    return row


def reversal_of(row):
    negated = dict(row)
    for col in MONEY:
        negated[col] = -row[col] if row[col] is not None else None
    negated["payment_type"] = 4
    return negated


def table_2019_era(rows):
    """2019-01..2023-01: int64 IDs, double counts, null-typed lowercase airport_fee."""
    col = lambda name: [r[name] for r in rows]  # noqa: E731
    fields = {
        "VendorID": pa.array(col("VendorID"), pa.int64()),
        "tpep_pickup_datetime": pa.array(col("tpep_pickup_datetime"), pa.timestamp("us")),
        "tpep_dropoff_datetime": pa.array(col("tpep_dropoff_datetime"), pa.timestamp("us")),
        "passenger_count": pa.array(
            [None if v is None else float(v) for v in col("passenger_count")], pa.float64()
        ),
        "trip_distance": pa.array(col("trip_distance"), pa.float64()),
        "RatecodeID": pa.array(
            [None if v is None else float(v) for v in col("RatecodeID")], pa.float64()
        ),
        "store_and_fwd_flag": pa.array(col("store_and_fwd_flag"), pa.string()),
        "PULocationID": pa.array(col("PULocationID"), pa.int64()),
        "DOLocationID": pa.array(col("DOLocationID"), pa.int64()),
        "payment_type": pa.array(col("payment_type"), pa.int64()),
        **{m: pa.array(col(m), pa.float64()) for m in MONEY},
        "airport_fee": pa.nulls(len(rows)),
    }
    return pa.table(fields)


def table_2023_era(rows):
    """2023-02 onward: int32 IDs, int64 counts, large_string flag, capitalized Airport_fee."""
    col = lambda name: [r[name] for r in rows]  # noqa: E731
    fields = {
        "VendorID": pa.array(col("VendorID"), pa.int32()),
        "tpep_pickup_datetime": pa.array(col("tpep_pickup_datetime"), pa.timestamp("us")),
        "tpep_dropoff_datetime": pa.array(col("tpep_dropoff_datetime"), pa.timestamp("us")),
        "passenger_count": pa.array(col("passenger_count"), pa.int64()),
        "trip_distance": pa.array(col("trip_distance"), pa.float64()),
        "RatecodeID": pa.array(col("RatecodeID"), pa.int64()),
        "store_and_fwd_flag": pa.array(col("store_and_fwd_flag"), pa.large_string()),
        "PULocationID": pa.array(col("PULocationID"), pa.int32()),
        "DOLocationID": pa.array(col("DOLocationID"), pa.int32()),
        "payment_type": pa.array(col("payment_type"), pa.int64()),
        **{m: pa.array(col(m), pa.float64()) for m in MONEY},
        "Airport_fee": pa.array([r.get("Airport_fee", 1.75) for r in rows], pa.float64()),
    }
    return pa.table(fields)


def write(table, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path)
    return str(path)
