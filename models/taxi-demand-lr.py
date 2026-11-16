from pyspark.sql import SparkSession, Window, functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.regression import LinearRegression
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
# 4. Pipeline: encode categoricals -> assemble -> linear regression
# ----------------------------------------------------------------------
zone_idx = StringIndexer(inputCol="zone", outputCol="zone_idx", handleInvalid="keep")
encoder  = OneHotEncoder(inputCols=["zone_idx", "hour", "dow"],
                         outputCols=["zone_vec", "hour_vec", "dow_vec"],
                         handleInvalid="keep")
assembler = VectorAssembler(
    inputCols=["zone_vec", "hour_vec", "dow_vec", "lag_1h", "lag_24h"],
    outputCol="features")

lr = LinearRegression(featuresCol="features", labelCol="demand",
                      maxIter=50, regParam=0.01, elasticNetParam=0.5)

pipeline = Pipeline(stages=[zone_idx, encoder, assembler, lr])
model = pipeline.fit(train)

# ----------------------------------------------------------------------
# 5. Evaluate
# ----------------------------------------------------------------------
lrModel = model.stages[-1]
s = lrModel.summary
print("Intercept: %f" % lrModel.intercept)
print("numIterations: %d" % s.totalIterations)
print("objectiveHistory: %s" % str(s.objectiveHistory))
print("Train RMSE: %f" % s.rootMeanSquaredError)
print("Train r2:   %f" % s.r2)

preds = (model.transform(test)
         .withColumn("prediction", F.greatest(F.col("prediction"), F.lit(0.0))))

for metric in ("rmse", "mae", "r2"):
    val = RegressionEvaluator(labelCol="demand", predictionCol="prediction",
                              metricName=metric).evaluate(preds)
    print("Test %s: %f" % (metric, val))

# ----------------------------------------------------------------------
# 6. Hourly predictions for ALL zones together
#    One row per hour, one column per zone (column name = PULocationID)
# ----------------------------------------------------------------------
BASE = "/Users/vera_k/Desktop/SDU/1. Sem/Big Data/Taxi/"

all_preds = (model.transform(data)
             .withColumn("prediction", F.greatest(F.col("prediction"), F.lit(0.0)))
             .withColumn("split", F.when(F.col("hour_ts") >= SPLIT_DATE, "test")
                                   .otherwise("train")))

# Long format: one row per zone per hour
long_df = all_preds.select("hour_ts", "PULocationID",
                           F.round("prediction", 1).alias("predicted"),
                           F.col("demand").alias("actual"), "split")

# Wide format: one row per hour, all zones side by side
zone_ids = sorted(r[0] for r in data.select("PULocationID").distinct().collect())
zone_cols = [str(z) for z in zone_ids]

wide_df = (long_df.groupBy("hour_ts")
           .pivot("PULocationID", zone_ids)
           .agg(F.first("predicted"))
           .orderBy("hour_ts"))
wide_df = wide_df.withColumn("all_zones_total", F.round(sum(F.col(c) for c in zone_cols), 1))

print("Predictions cover %d hours x %d zones" % (wide_df.count(), len(zone_ids)))

# Preview: first 8 zones, one test day, hour by hour (the full table has every zone)
(wide_df.filter(F.col("hour_ts").between("2026-07-25 00:00:00", "2026-07-25 23:00:00"))
        .select("hour_ts", "all_zones_total", *zone_cols[:8])
        .show(24, truncate=False))

# Preview: all zones ranked for one hour
print("Top 15 zones by predicted demand, 2026-07-25 18:00")
(long_df.filter(F.col("hour_ts") == "2026-07-25 18:00:00")
        .orderBy(F.desc("predicted")).show(15))

# Save both versions
long_df.orderBy("hour_ts", "PULocationID").toPandas().to_csv(BASE + "predicted_demand.csv", index=False)
wide_df.toPandas().to_csv(BASE + "predicted_demand_by_hour.csv", index=False)
print("Saved predicted_demand.csv (long) and predicted_demand_by_hour.csv (wide)")
