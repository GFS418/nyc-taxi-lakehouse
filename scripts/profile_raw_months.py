"""Profile raw TLC yellow months to ground the data-quality rules in real counts.

Usage: uv run python scripts/profile_raw_months.py data/samples/yellow_tripdata_2019-01.parquet ...
Each file is read on its own (types drift between files, so no schema merging).
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

MONEY = [
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "airport_fee",
]

spark = (
    SparkSession.builder.master("local[*]")
    .appName("profile")
    .config("spark.driver.memory", "8g")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

for path in sys.argv[1:]:
    ym = path.rsplit("_", 1)[1][:7]
    df = spark.read.parquet(path)
    df = df.toDF(*[c.lower() for c in df.columns])
    print(f"\n===== {ym}  pickup type: {df.schema['tpep_pickup_datetime'].dataType.simpleString()}")
    start = F.to_timestamp(F.lit(f"{ym}-01 00:00:00"))
    end = F.add_months(F.lit(f"{ym}-01"), 1).cast("timestamp")
    pu, do = F.col("tpep_pickup_datetime"), F.col("tpep_dropoff_datetime")
    dur_min = (
        F.unix_micros(F.col("tpep_dropoff_datetime").cast("timestamp"))
        - F.unix_micros(F.col("tpep_pickup_datetime").cast("timestamp"))
    ) / 6e7
    d = df.withColumn("dur_min", dur_min)
    c = lambda cond: F.sum(F.when(cond, 1).otherwise(0))  # noqa: E731
    agg = (
        d.agg(
            F.count("*").alias("rows"),
            c(F.col("passenger_count").isNull()).alias("pax_null"),
            c(
                F.col("passenger_count").isNull()
                & F.col("ratecodeid").isNull()
                & F.col("store_and_fwd_flag").isNull()
            ).alias("pax_rate_flag_all_null"),
            c(F.col("passenger_count") == 0).alias("pax_zero"),
            c(F.col("passenger_count") > 6).alias("pax_gt6"),
            c(F.col("passenger_count") != F.floor("passenger_count")).alias("pax_fractional"),
            c(F.col("airport_fee").isNull()).alias("airport_fee_null"),
            c(F.col("congestion_surcharge").isNull()).alias("congestion_null"),
            c(pu.isNull() | do.isNull()).alias("ts_null"),
            c(pu < start).alias("pickup_before_month"),
            c(pu >= end).alias("pickup_after_month"),
            F.min(pu).alias("min_pickup"),
            F.max(pu).alias("max_pickup"),
            c(F.col("dur_min") < 0).alias("dur_negative"),
            c(F.col("dur_min") == 0).alias("dur_zero"),
            c(F.col("dur_min") > 180).alias("dur_gt_3h"),
            c(F.col("dur_min") > 360).alias("dur_gt_6h"),
            c(F.col("dur_min") > 1440).alias("dur_gt_24h"),
            F.max("dur_min").alias("dur_max_min"),
            F.percentile_approx("dur_min", [0.999, 0.9999]).alias("dur_p999_p9999"),
            c(F.col("trip_distance") < 0).alias("dist_negative"),
            c(F.col("trip_distance") == 0).alias("dist_zero"),
            c(F.col("trip_distance") > 100).alias("dist_gt_100"),
            c(F.col("trip_distance") > 500).alias("dist_gt_500"),
            F.max("trip_distance").alias("dist_max"),
            F.percentile_approx("trip_distance", [0.999, 0.9999]).alias("dist_p999_p9999"),
            c(F.col("fare_amount") < 0).alias("fare_negative"),
            c(F.col("total_amount") < 0).alias("total_negative"),
            c(F.greatest(*[F.col(m) < 0 for m in MONEY])).alias("any_money_negative"),
            c(F.col("total_amount") > 1000).alias("total_gt_1000"),
            c(F.col("total_amount") > 10000).alias("total_gt_10000"),
            F.max("total_amount").alias("total_max"),
            F.max(F.greatest(*[F.abs(F.col(m)) for m in MONEY])).alias("max_abs_any_money"),
            c(
                F.greatest(*[F.abs(F.col(m) * 100 - F.round(F.col(m) * 100)) > 1e-6 for m in MONEY])
            ).alias("money_more_than_2dp"),
            c(
                ~F.col("pulocationid").between(1, 265)
                | ~F.col("dolocationid").between(1, 265)
                | F.col("pulocationid").isNull()
                | F.col("dolocationid").isNull()
            ).alias("loc_invalid"),
        )
        .collect()[0]
        .asDict()
    )
    for k, v in agg.items():
        print(f"  {k:26s} {v}")
    for col in ["vendorid", "ratecodeid", "payment_type", "store_and_fwd_flag"]:
        vals = sorted(
            ((r[col], r["count"]) for r in d.groupBy(col).count().collect()),
            key=lambda t: (t[0] is None, str(t[0])),
        )
        print(f"  values {col:18s} {vals}")
    biz = [c_ for c_ in d.columns if c_ != "dur_min"]
    rows, full_distinct = agg["rows"], d.select(biz).distinct().count()
    key = [
        "vendorid",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "pulocationid",
        "dolocationid",
        "fare_amount",
    ]
    key_distinct = d.select(key).distinct().count()
    print(f"  dup_full_row_excess        {rows - full_distinct}")
    print(f"  dup_key_subset_excess      {rows - key_distinct}  (vendor, ts, zones, fare)")
spark.stop()
