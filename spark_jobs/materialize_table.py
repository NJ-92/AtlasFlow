"""
Runs one declared pipeline table's SQL and writes the result as a Delta table - the
"CREATE OR REFRESH ... AS SELECT ..." semantics behind AtlasFlow's declarative Pipelines.
Each run fully recomputes the table (no incremental/streaming materialization - that
would need Spark Structured Streaming with checkpointing, a materially bigger feature).

Usage: spark-submit materialize_table.py --sql-file query.sql --output /data/tables/orders_clean
"""
import argparse
import json

from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument("--sql-file", required=True)
parser.add_argument("--output", required=True)
args = parser.parse_args()

with open(args.sql_file) as f:
    sql_text = f.read()

spark = SparkSession.builder.appName("materialize_pipeline_table").getOrCreate()

df = spark.sql(sql_text)
df.write.format("delta").mode("overwrite").save(args.output)

row_count = df.count()
columns = [{"name": f.name, "type": f.dataType.simpleString()} for f in df.schema.fields]
print(f"Materialized {row_count} rows to {args.output}")
print("ATLASFLOW_OUTPUT_JSON:" + json.dumps({"row_count": row_count, "columns": columns}))

spark.stop()
