"""
Point-in-time correct join: for each row in a labels/events table, joins in the most
recent feature values as of that row's timestamp - the core operation a feature store
needs to avoid leaking future information into a training set.

Usage: spark-submit point_in_time_join.py \
  --labels s3a://lake/tables/labels --labels-entity-key customer_id --labels-timestamp-key event_time \
  --features s3a://lake/tables/customer_features --features-entity-key customer_id --features-timestamp-key updated_at \
  --output s3a://lake/tables/training_set
"""
import argparse
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

parser = argparse.ArgumentParser()
parser.add_argument("--labels", required=True)
parser.add_argument("--labels-entity-key", required=True)
parser.add_argument("--labels-timestamp-key", required=True)
parser.add_argument("--features", required=True)
parser.add_argument("--features-entity-key", required=True)
parser.add_argument("--features-timestamp-key", required=True)
parser.add_argument("--output", required=True)
args = parser.parse_args()

spark = SparkSession.builder.appName("point_in_time_join").getOrCreate()

labels = spark.read.format("delta").load(args.labels)
features = spark.read.format("delta").load(args.features)

joined = labels.join(
    features,
    (labels[args.labels_entity_key] == features[args.features_entity_key]) &
    (features[args.features_timestamp_key] <= labels[args.labels_timestamp_key]),
    "left",
)

# keep only the most recent feature row as-of each label's timestamp (no future leakage)
window = Window.partitionBy(
    *[labels[c] for c in labels.columns]
).orderBy(F.col(args.features_timestamp_key).desc())

result = (
    joined.withColumn("_rn", F.row_number().over(window))
    .filter(F.col("_rn") == 1)
    .drop("_rn", args.features_entity_key, args.features_timestamp_key)
)

result.write.format("delta").mode("overwrite").save(args.output)
print(f"Wrote point-in-time joined training set to {args.output} ({result.count()} rows)")

spark.stop()
