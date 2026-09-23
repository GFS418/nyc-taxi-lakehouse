"""Ground the implied-speed rule and the soft flags in real counts.

Only rows that already pass the existing hard duration and distance rules are considered, so
the counts show what a new rule would add on top of them.
Usage: uv run python scripts/profile_speed_and_flags.py data/samples/*.parquet
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder.master("local[*]")
    .config("spark.driver.memory", "8g")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.ui.enabled", "false")
    .config("spark.ui.showConsoleProgress", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

for path in sys.argv[1:]:
    d = spark.read.parquet(path)
    d = d.toDF(*[c.lower() for c in d.columns])
    secs = F.unix_seconds(F.col("tpep_dropoff_datetime").cast("timestamp")) - F.unix_seconds(
        F.col("tpep_pickup_datetime").cast("timestamp")
    )
    d = d.withColumn("secs", secs).withColumn("dist", F.col("trip_distance"))
    ok = d.filter("secs >= 0 AND secs <= 86400 AND dist >= 0 AND dist <= 500")
    ok = ok.withColumn("mph", F.when(F.col("secs") > 0, F.col("dist") / (F.col("secs") / 3600)))
    c = lambda cond: F.sum(F.when(cond, 1).otherwise(0))  # noqa: E731
    r = ok.agg(
        F.count("*").alias("rows"),
        c((F.col("secs") == 0) & (F.col("dist") > 0)).alias("zero_dur_pos_dist"),
        c((F.col("secs") == 0) & (F.col("dist") >= 1)).alias("zero_dur_dist_ge_1mi"),
        c((F.col("secs") == 0) & (F.col("dist") == 0)).alias("zero_dur_zero_dist"),
        c(F.col("dist") == 0).alias("zero_distance"),
        c(F.col("secs") > 3 * 3600).alias("over_3h"),
        c(F.col("mph") > 60).alias("mph_gt_60"),
        c(F.col("mph") > 80).alias("mph_gt_80"),
        c(F.col("mph") > 100).alias("mph_gt_100"),
        c(F.col("mph") > 200).alias("mph_gt_200"),
        c((F.col("mph") > 100) & (F.col("dist") >= 1)).alias("mph_gt_100_dist_ge_1mi"),
        F.percentile_approx("mph", [0.5, 0.99, 0.999]).alias("mph_p50_p99_p999"),
    ).collect()[0]
    fast = ok.filter("mph > 100")
    q = fast.agg(
        F.percentile_approx("dist", 0.5).alias("median_miles"),
        F.percentile_approx("secs", 0.5).alias("median_secs"),
        F.percentile_approx("fare_amount", 0.5).alias("median_fare"),
    ).collect()[0]
    print(f"\n===== {path.rsplit('_', 1)[1][:7]}")
    for k, v in r.asDict().items():
        print(f"  {k:24s} {v}")
    print(
        f"  among mph>100: median {q.median_miles} mi in {q.median_secs}s,"
        f" median fare {q.median_fare}"
    )
spark.stop()
