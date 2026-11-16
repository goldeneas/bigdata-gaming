from pyspark.sql import SparkSession, Window, functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

spark = SparkSession.builder.appName("taxi-demand").getOrCreate()

# ----------------------------------------------------------------------
# Settings
# ----------------------------------------------------------------------
PATH = "/Users/vera_k/Desktop/SDU/1. Sem/Big Data/Taxi/yellow_tripdata_2026-07.parquet"
ZONES = None            # e.g. [132, 138, 161, 237] to model only specific pickup zones
SPLIT_DATE = "2026-07-25"   # train before this date, test on/after it

# ----------------------------------------------------------------------
# 1. Load and clean (TLC files contain trips with dates outside the month)
# ----------------------------------------------------------------------
raw = spark.read.parquet(PATH)

trips = (raw
    .filter(F.col("PULocationID").isNotNull())
    .filter((F.col("tpep_pickup_datetime") >= "2026-07-01") &
            (F.col("tpep_pickup_datetime") <  "2026-08-01")))

if ZONES:
    trips = trips.filter(F.col("PULocationID").isin(ZONES))

# ----------------------------------------------------------------------
# 2. Demand = number of pickups per zone per hour
# ----------------------------------------------------------------------
counts = (trips
    .withColumn("hour_ts", F.date_trunc("hour", "tpep_pickup_datetime"))
    .groupBy("PULocationID", "hour_ts")
    .agg(F.count("*").alias("demand")))

# Fill in hours with zero pickups so lag features line up correctly
zones = counts.select("PULocationID").distinct()
hours = (spark.range(1).select(
            F.explode(F.sequence(F.lit("2026-07-01 00:00:00").cast("timestamp"),
                                 F.lit("2026-07-31 23:00:00").cast("timestamp"),
                                 F.expr("interval 1 hour"))).alias("hour_ts")))

grid = (zones.crossJoin(hours)
        .join(counts, ["PULocationID", "hour_ts"], "left")
        .fillna(0, subset=["demand"]))

# ----------------------------------------------------------------------
# 3. Features: hour, day of week, and recent demand in the same zone
# ----------------------------------------------------------------------
w = Window.partitionBy("PULocationID").orderBy("hour_ts")

data = (grid
    .withColumn("hour", F.hour("hour_ts").cast("double"))
    .withColumn("dow", (F.dayofweek("hour_ts") - 1).cast("double"))   # 0=Sun ... 6=Sat
    .withColumn("zone", F.col("PULocationID").cast("string"))
    .withColumn("lag_1h",  F.lag("demand", 1).over(w))
    .withColumn("lag_24h", F.lag("demand", 24).over(w))
    .dropna())

# Time-based split (never shuffle time series data)
train = data.filter(F.col("hour_ts") <  SPLIT_DATE)
test  = data.filter(F.col("hour_ts") >= SPLIT_DATE)

# ----------------------------------------------------------------------
# 4. Pipeline: zone index -> assemble -> random forest
# ----------------------------------------------------------------------
# No one-hot encoding and no VectorIndexer needed: StringIndexer marks zone_idx as a
# categorical feature, and VectorAssembler keeps that information, so the forest
# treats each zone as a category. Trees can split directly on hour, day of week
# and the lag features.
zone_idx = StringIndexer(inputCol="zone", outputCol="zone_idx", handleInvalid="keep")
feature_names = ["zone_idx", "hour", "dow", "lag_1h", "lag_24h"]
assembler = VectorAssembler(inputCols=feature_names, outputCol="features")

rf = RandomForestRegressor(featuresCol="features", labelCol="demand",
                           numTrees=60, maxDepth=12,
                           maxBins=300,      # must be >= number of zones (260) + 1
                           seed=42)

pipeline = Pipeline(stages=[zone_idx, assembler, rf])
model = pipeline.fit(train)

# ----------------------------------------------------------------------
# 5. Evaluate (same metrics as the linear regression, for comparison)
# ----------------------------------------------------------------------
def report(name, df):
    p = model.transform(df)
    for metric in ("rmse", "mae", "r2"):
        val = RegressionEvaluator(labelCol="demand", predictionCol="prediction",
                                  metricName=metric).evaluate(p)
        print("%s %s: %f" % (name, metric, val))

report("Train", train)
report("Test ", test)

rfModel = model.stages[-1]
print("Feature importance (how much each input is used by the forest):")
for n, imp in zip(["zone", "hour", "day of week", "lag_1h", "lag_24h"],
                  rfModel.featureImportances.toArray()):
    print("  %-12s %.3f" % (n, imp))

# ----------------------------------------------------------------------
# 6. Hourly predictions for ALL zones together
# ----------------------------------------------------------------------
BASE = "/Users/vera_k/Desktop/SDU/1. Sem/Big Data/Taxi/"

all_preds = (model.transform(data)
             .withColumn("split", F.when(F.col("hour_ts") >= SPLIT_DATE, "test")
                                   .otherwise("train")))

long_df = all_preds.select("hour_ts", "PULocationID",
                           F.round("prediction", 1).alias("predicted"),
                           F.col("demand").alias("actual"), "split")

zone_ids = sorted(r[0] for r in data.select("PULocationID").distinct().collect())
zone_cols = [str(z) for z in zone_ids]

wide_df = (long_df.groupBy("hour_ts")
           .pivot("PULocationID", zone_ids)
           .agg(F.first("predicted"))
           .orderBy("hour_ts"))
wide_df = wide_df.withColumn("all_zones_total", F.round(sum(F.col(c) for c in zone_cols), 1))

print("Predictions cover %d hours x %d zones" % (wide_df.count(), len(zone_ids)))

(wide_df.filter(F.col("hour_ts").between("2026-07-25 00:00:00", "2026-07-25 23:00:00"))
        .select("hour_ts", "all_zones_total", *zone_cols[:8])
        .show(24, truncate=False))

print("Top 15 zones by predicted demand, 2026-07-25 18:00")
(long_df.filter(F.col("hour_ts") == "2026-07-25 18:00:00")
        .orderBy(F.desc("predicted")).show(15))

# Saved under different names so the linear regression files are not overwritten
long_df.orderBy("hour_ts", "PULocationID").toPandas().to_csv(BASE + "predicted_demand_rf.csv", index=False)
wide_df.toPandas().to_csv(BASE + "predicted_demand_by_hour_rf.csv", index=False)
print("Saved predicted_demand_rf.csv (long) and predicted_demand_by_hour_rf.csv (wide)")
