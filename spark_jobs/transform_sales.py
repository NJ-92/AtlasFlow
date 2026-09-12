"""
Example Spark job, submitted to the cluster by a `spark_submit` task.
Mounted into the backend container at /jobs/transform_sales.py (see docker-compose.yml).

Run manually against the cluster with:
  spark-submit --master spark://spark-master:7077 transform_sales.py --input /data/raw --output /data/curated
"""
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

parser = argparse.ArgumentParser()
parser.add_argument("--input", required=True)
parser.add_argument("--output", required=True)
args = parser.parse_args()

spark = SparkSession.builder.appName("transform_sales").getOrCreate()

# In a real pipeline this would read Parquet/Delta files from the lakehouse (MinIO/S3).
# Demonstrated here with an inline sample dataset so the job runs standalone.
df = spark.createDataFrame(
    [("east", 120.0), ("east", 80.5), ("west", 340.0), ("west", 10.0), ("north", 75.25)],
    ["region", "amount"],
)

result = df.groupBy("region").agg(
    F.sum("amount").alias("total_sales"),
    F.count("*").alias("num_orders"),
)

result.show()
result.write.mode("overwrite").parquet(args.output)

spark.stop()
