import os
from pyspark.sql import SparkSession, Window, functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
import glob
os.makedirs("D:/spark_temp", exist_ok=True)
spark = (SparkSession.builder
                .appName("taxi-demand-rf-multi")
                .config("spark.driver.memory", "6g")
                .config("spark.local.dir", "D:/spark_temp")
                .config("spark.sql.shuffle.partitions", "64")  # Cuts task overhead on 190k rows
                .getOrCreate())

spark.sparkContext.setLogLevel("ERROR")

# ----------------------------------------------------------------------
# Settings
# ----------------------------------------------------------------------
#PATH = "D:/Downloads/Pdfs/yellow_tripdata_2026-03.parquet"
PATHS = sorted(glob.glob(r"D:\Downloads\Pdfs\yellow_tripdata_2025-*.parquet"))
print(f"Found {len(PATHS)} parquet files:", [os.path.basename(p) for p in PATHS])
#SPLIT_DATE = "2026-07-25"
START_DATE = "2025-01-01"
END_DATE   = "2026-01-01"    
SPLIT_DATE = "2025-11-01"


# ----------------------------------------------------------------------
# 1. Load and clean
# ----------------------------------------------------------------------
raw = spark.read.parquet(*PATHS)

trips = (raw
    .filter(F.col("PULocationID").isNotNull() & F.col("DOLocationID").isNotNull())
    .filter((F.col("tpep_pickup_datetime") >= START_DATE) &
            (F.col("tpep_pickup_datetime") <  END_DATE)))

# ----------------------------------------------------------------------
# 2. Aggregate Pickups (Demand) AND Dropoffs (Inflow) per zone per hour
# ----------------------------------------------------------------------
pu_counts = (trips
    .withColumn("hour_ts", F.date_trunc("hour", "tpep_pickup_datetime"))
    .groupBy("PULocationID", "hour_ts")
    .agg(F.count("*").alias("demand")))

do_counts = (trips
    .filter((F.col("tpep_dropoff_datetime") >= START_DATE) &
            (F.col("tpep_dropoff_datetime") <  END_DATE))
    .withColumn("hour_ts", F.date_trunc("hour", "tpep_dropoff_datetime"))
    .groupBy(F.col("DOLocationID").alias("PULocationID"), "hour_ts")
    .agg(F.count("*").alias("dropoffs")))

zones = pu_counts.select("PULocationID").distinct()
bounds = pu_counts.agg(F.min("hour_ts").alias("min_ts"), F.max("hour_ts").alias("max_ts")).first()
hours = (spark.range(1).select(
           F.explode(F.sequence(F.lit(bounds["min_ts"]),
                                 F.lit(bounds["max_ts"]),
                                 F.expr("interval 1 hour"))).alias("hour_ts")))

grid = (zones.crossJoin(hours)
        .join(pu_counts, ["PULocationID", "hour_ts"], "left")
        .join(do_counts, ["PULocationID", "hour_ts"], "left")
        .fillna(0, subset=["demand", "dropoffs"]))

# ----------------------------------------------------------------------
# 3. Richer Time-Series & Spatial Inflow Features
# ----------------------------------------------------------------------
w = Window.partitionBy("PULocationID").orderBy("hour_ts")
#w_3h = Window.partitionBy("PULocationID").orderBy("hour_ts").rowsBetween(-3, -1)

data = (grid
    .withColumn("hour", F.hour("hour_ts").cast("double"))
    .withColumn("dow", (F.dayofweek("hour_ts") - 1).cast("double"))
    .withColumn("month", F.month("hour_ts").cast("double"))
    .withColumn("zone", F.col("PULocationID").cast("string"))
    # Pickup lags & momentum
    .withColumn("lag_1h",  F.lag("demand", 1).over(w))
    .withColumn("lag_2h",  F.lag("demand", 2).over(w))
    .withColumn("lag_3h",  F.lag("demand", 3).over(w))
    .withColumn("lag_24h", F.lag("demand", 24).over(w))
    .withColumn("lag_168h", F.lag("demand", 168).over(w))
    .withColumn("diff_1h", F.col("lag_1h") - F.col("lag_2h"))
    .withColumn("roll_mean_3h", (F.col("lag_1h") + F.col("lag_2h") + F.col("lag_3h")) / 3.0)
    # Dropoff inflow lags (people arriving into the zone)
    .withColumn("do_lag_1h", F.lag("dropoffs", 1).over(w))
    .dropna())

train = data.filter(F.col("hour_ts") <  SPLIT_DATE).cache()
test  = data.filter(F.col("hour_ts") >= SPLIT_DATE).cache()

# ----------------------------------------------------------------------
# 4. Pipeline: zone index -> assemble -> Random Forest
# ----------------------------------------------------------------------
zone_idx = StringIndexer(inputCol="zone", outputCol="zone_idx", handleInvalid="keep")
feature_names = [
    "zone_idx", "hour", "dow", 
    "month",
    "lag_1h", "lag_2h", "lag_3h", "lag_24h",
     "lag_168h",
     "diff_1h", "roll_mean_3h", "do_lag_1h"
]
assembler = VectorAssembler(inputCols=feature_names, outputCol="features")

rf = RandomForestRegressor(featuresCol="features", labelCol="demand",
                           numTrees=60, maxDepth=12,
                           maxBins=300, seed=42)

pipeline = Pipeline(stages=[zone_idx, assembler, rf])
model = pipeline.fit(train)

# ----------------------------------------------------------------------
# 5. Evaluate & Print Feature Importances
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
print("\nFeature importance (RF v3):")
for n, imp in sorted(zip(feature_names, rfModel.featureImportances.toArray()), key=lambda x: -x[1]):
    print("  %-15s %.3f" % (n, imp))

# ----------------------------------------------------------------------
# 6. Save predictions for comparison
# ----------------------------------------------------------------------
BASE = "models/rf_2025"
os.makedirs(BASE, exist_ok=True)

export_data = data.filter(F.col("hour_ts") >= F.date_sub(F.lit(SPLIT_DATE), 2))
all_preds = (model.transform(export_data)
             .withColumn("split", F.when(F.col("hour_ts") >= SPLIT_DATE, "test")
                                   .otherwise("train")))

long_df = all_preds.select("hour_ts", "PULocationID",
                           F.round("prediction", 1).alias("predicted"),
                           F.col("demand").alias("actual"), "split")

long_df.orderBy("hour_ts", "PULocationID").toPandas().to_csv(os.path.join(BASE, "predicted_demand_rf_2025.csv"), index=False)
print("\nSaved predicted_demand_rf_2025.csv in", BASE)