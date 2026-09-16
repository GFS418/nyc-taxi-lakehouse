"""Profile negative-amount rows: are they reversals of a real trip, and what defines the pair?

A "negative row" has fare_amount < 0 or total_amount < 0. A "twin" is a non-negative row with
the same vendor, pickup/dropoff timestamps and pickup/dropoff zones whose amounts are negated.
Usage: uv run python scripts/profile_reversals.py data/samples/yellow_tripdata_2024-06.parquet ...
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
KEY = ["vendorid", "tpep_pickup_datetime", "tpep_dropoff_datetime", "pulocationid", "dolocationid"]
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


def twins(neg, pos, cols):
    cond = [F.col(f"n.{k}") == F.col(f"p.{k}") for k in KEY]
    cond += [F.col(f"p.{c}").eqNullSafe(-F.col(f"n.{c}")) for c in cols]
    return neg.alias("n").join(pos.alias("p"), cond, "inner").select("n.rid", "p.rid")


for path in sys.argv[1:]:
    d = spark.read.parquet(path)
    d = d.toDF(*[c.lower() for c in d.columns]).withColumn("rid", F.monotonically_increasing_id())
    d = d.withColumn("airport_fee", F.col("airport_fee").cast("double")).cache()
    is_neg = (F.col("fare_amount") < 0) | (F.col("total_amount") < 0)
    neg, pos = d.filter(is_neg), d.filter(~is_neg)
    print(f"\n===== {path.rsplit('_', 1)[1][:7]}  rows={d.count()}  negative rows={neg.count()}")
    for label, cols in [
        ("all money negated", MONEY),
        ("fare+total negated", ["fare_amount", "total_amount"]),
        ("fare negated", ["fare_amount"]),
        ("total negated", ["total_amount"]),
    ]:
        t = twins(neg, pos, cols)
        n_matched = t.select(F.col("n.rid")).distinct().count()
        multi_n = t.groupBy(F.col("n.rid")).count().filter("count > 1").count()
        multi_p = t.groupBy(F.col("p.rid")).count().filter("count > 1").count()
        print(
            f"  twin by {label:20s} matched neg={n_matched:7d}  neg w/ >1 twin={multi_n:5d}  "
            f"pos claimed by >1 neg={multi_p:5d}"
        )
    odd = d.filter((F.col("fare_amount") < 0) & (F.col("total_amount") >= 0))
    print(
        "  fare<0 & total>=0: total==0:",
        odd.filter("total_amount = 0").count(),
        " total>0:",
        odd.filter("total_amount > 0").count(),
        " payment_type:",
        sorted(tuple(r) for r in odd.groupBy("payment_type").count().collect()),
    )
    odd.select(
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "improvement_surcharge",
        "congestion_surcharge",
        "airport_fee",
        "total_amount",
        "payment_type",
    ).show(5)
    unmatched = neg.alias("n").join(
        twins(neg, pos, ["fare_amount"]).select(F.col("n.rid").alias("rid")), "rid", "left_anti"
    )
    print(
        "  unmatched negatives (fare twin):",
        unmatched.count(),
        " payment_type:",
        sorted(tuple(r) for r in unmatched.groupBy("payment_type").count().collect()),
    )
    d.unpersist()
spark.stop()
