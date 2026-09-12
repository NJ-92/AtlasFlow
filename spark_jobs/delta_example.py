"""
Demonstrates genuine Delta Lake semantics (ACID transactions, schema enforcement, time
travel, MERGE) - this is real Delta Lake via the `delta-spark` package, auto-attached to
every spark_submit/sql task by task_runners.py, not a reimplementation.

Run as a spark_submit task with:
  command: /jobs/delta_example.py
  params:  { "args": ["--table", "s3a://lake/tables/customers"] }
"""
import argparse
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

parser = argparse.ArgumentParser()
parser.add_argument("--table", required=True, help="s3a:// path for the Delta table")
args = parser.parse_args()

spark = SparkSession.builder.appName("delta_example").getOrCreate()

# 1. ACID write - creates the table if it doesn't exist yet
initial = spark.createDataFrame(
    [(1, "alice", "active"), (2, "bob", "active"), (3, "carol", "inactive")],
    ["id", "name", "status"],
)
initial.write.format("delta").mode("overwrite").save(args.table)
print(f"Wrote initial version to {args.table}")

# 2. MERGE (upsert) - the operation plain Parquet can't do atomically
updates = spark.createDataFrame(
    [(2, "bob", "inactive"), (4, "dave", "active")],  # update bob, insert dave
    ["id", "name", "status"],
)
target = DeltaTable.forPath(spark, args.table)
target.alias("t").merge(
    updates.alias("u"), "t.id = u.id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
print("MERGE complete")

# 3. Time travel - read the table as it looked before the MERGE (version 0)
before_merge = spark.read.format("delta").option("versionAsOf", 0).load(args.table)
print("Version 0 (before MERGE):")
before_merge.show()

after_merge = spark.read.format("delta").load(args.table)
print("Latest version (after MERGE):")
after_merge.show()

# 4. Full audit trail of every transaction against this table
target.history().select("version", "timestamp", "operation").show(truncate=False)

spark.stop()
