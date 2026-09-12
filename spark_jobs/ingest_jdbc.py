"""
Ingests data from a JDBC source (Postgres or MySQL) via Spark's real JDBC connector -
distributed reads, not a single-connection pull. Used by both the ingestion wizard's
"preview" step (--preview, small LIMIT, no write) and its full ingest step (writes to
a Delta table).

Usage:
  spark-submit ingest_jdbc.py --driver postgres --host db.example.com --port 5432 \
    --database mydb --user reader --password '***' --query "SELECT * FROM orders" \
    --output /data/tables/orders_ingested [--preview]
"""
import argparse
import json

from pyspark.sql import SparkSession

DRIVER_CLASSES = {
    "postgres": "org.postgresql.Driver",
    "mysql": "com.mysql.cj.jdbc.Driver",
}
JDBC_URL_TEMPLATES = {
    "postgres": "jdbc:postgresql://{host}:{port}/{database}",
    "mysql": "jdbc:mysql://{host}:{port}/{database}",
}

parser = argparse.ArgumentParser()
parser.add_argument("--driver", required=True, choices=["postgres", "mysql"])
parser.add_argument("--host", required=True)
parser.add_argument("--port", required=True)
parser.add_argument("--database", required=True)
parser.add_argument("--user", required=True)
parser.add_argument("--password", required=True)
parser.add_argument("--query", required=True, help="A table name, or a SELECT query wrapped in parens with an alias, e.g. '(SELECT * FROM orders WHERE created_at > NOW() - INTERVAL 1 DAY) t'")
parser.add_argument("--output", required=False, help="Delta output path - omit with --preview")
parser.add_argument("--preview", action="store_true", help="Read a small sample and print it, don't write anywhere")
args = parser.parse_args()

spark = SparkSession.builder.appName("jdbc_ingest").getOrCreate()

jdbc_url = JDBC_URL_TEMPLATES[args.driver].format(host=args.host, port=args.port, database=args.database)

reader = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", args.query)
    .option("user", args.user)
    .option("password", args.password)
    .option("driver", DRIVER_CLASSES[args.driver])
)

if args.preview:
    df = reader.load().limit(20)
    rows = [row.asDict(recursive=True) for row in df.collect()]
    columns = df.columns
    print("ATLASFLOW_OUTPUT_JSON:" + json.dumps({"columns": columns, "rows": rows, "row_count": len(rows)}, default=str))
else:
    df = reader.load()
    df.write.format("delta").mode("overwrite").save(args.output)
    row_count = df.count()
    columns = [{"name": f.name, "type": f.dataType.simpleString()} for f in df.schema.fields]
    print(f"Ingested {row_count} rows to {args.output}")
    print("ATLASFLOW_OUTPUT_JSON:" + json.dumps({"row_count": row_count, "columns": columns}))

spark.stop()
