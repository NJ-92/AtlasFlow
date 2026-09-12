"""
Onboards a raw uploaded file into a real Delta table: infers schema, writes it as Delta
at the given path, and prints the resulting schema + row count as structured output so
the backend can register it in the table catalog.

Usage: spark-submit onboard_table.py --input /data/uploads/x.csv --output s3a://lake/tables/x --format csv
"""
import argparse
import json
from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument("--input", required=True)
parser.add_argument("--output", required=True)
parser.add_argument("--format", default="csv", choices=["csv", "json", "parquet"])
args = parser.parse_args()

spark = SparkSession.builder.appName("onboard_table").getOrCreate()

reader = spark.read.option("header", "true").option("inferSchema", "true")
if args.format == "csv":
    df = reader.csv(args.input)
elif args.format == "json":
    df = spark.read.option("inferSchema", "true").json(args.input)
else:
    df = spark.read.parquet(args.input)

df.write.format("delta").mode("overwrite").save(args.output)

row_count = df.count()
columns = [{"name": f.name, "type": f.dataType.simpleString()} for f in df.schema.fields]

print(f"Onboarded {row_count} rows, {len(columns)} columns to {args.output}")
print("ATLASFLOW_OUTPUT_JSON:" + json.dumps({"row_count": row_count, "columns": columns}))

spark.stop()
