"""
Executes a SQL statement via Spark SQL (Delta Lake-enabled) and prints the result as both
a human-readable table (for logs) and a machine-readable ATLASFLOW_OUTPUT_JSON line (for
task-to-task data passing and the ad-hoc SQL query API).

Usage: spark-submit run_sql.py --sql-file query.sql --limit 200
"""
import argparse
import json

from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument("--sql-file", required=True)
parser.add_argument("--limit", type=int, default=200)
args = parser.parse_args()

with open(args.sql_file) as f:
    sql_text = f.read()

spark = SparkSession.builder.appName("atlasflow_sql").getOrCreate()

result = spark.sql(sql_text)
limited = result.limit(args.limit)
rows = [row.asDict(recursive=True) for row in limited.collect()]
columns = result.columns

# human-readable, for the run's log output
result.show(args.limit, truncate=False)

# machine-readable, for the UI / task-value passing / ad-hoc query API
payload = {"columns": columns, "rows": rows, "row_count": len(rows)}
print("ATLASFLOW_OUTPUT_JSON:" + json.dumps(payload, default=str))

spark.stop()
