import os
import numpy as np
import pandas as pd

# 1. Load your saved predictions CSV
model_name = "rf_march"
CSV_PATH = f"models/{model_name}/predicted_demand_{model_name}.csv"
df = pd.read_csv(CSV_PATH)
df["hour_ts"] = pd.to_datetime(df["hour_ts"])

# Ensure chronological sorting per zone before shifting for naive baselines
df = df.sort_values(["PULocationID", "hour_ts"]).reset_index(drop=True)

# Reconstruct Naive Baselines from the 'actual' column
df["naive_1h"] = df.groupby("PULocationID")["actual"].shift(1)
df["naive_24h"] = df.groupby("PULocationID")["actual"].shift(24)

# Filter strictly to the test set (if 'split' column exists, use it; otherwise filter by date)
if "split" in df.columns:
    test_df = df[df["split"] == "test"].copy()
else:
    test_df = df.copy()

# Drop rows where naive_24h is NaN (only happens if CSV didn't include the day before test started)
eval_df = test_df.dropna(subset=["naive_1h", "naive_24h"]).copy()

# Helper function to compute RMSE, MAE, and R2
def calc_metrics(y_true, y_pred):
    err = y_true - y_pred
    rmse = np.sqrt(np.mean(err ** 2))
    mae = np.mean(np.abs(err))
    ss_res = np.sum(err ** 2)
    ss_tot = np.sum((y_true - np.mean(y_true)) ** 2)
    r2 = 1 - (ss_res / ss_tot)
    return {"RMSE": round(rmse, 3), "MAE": round(mae, 3), "R2": round(r2, 4)}

# ----------------------------------------------------------------------
# A. Model vs. Naive Baselines Comparison
# ----------------------------------------------------------------------
comparison = pd.DataFrame([
    {"Model": model_name, **calc_metrics(eval_df["actual"], eval_df["predicted"])},
    {"Model": "Naive 1h Lag (y_t-1)",      **calc_metrics(eval_df["actual"], eval_df["naive_1h"])},
    {"Model": "Naive 24h Lag (y_t-24)",    **calc_metrics(eval_df["actual"], eval_df["naive_24h"])},
])

print("\n=== 1. MODEL VS. NAIVE BASELINES (TEST SET) ===")
print(comparison.to_string(index=False))


# Calculate errors on eval_df (where both model and naive baselines exist)
eval_df["model_err"] = eval_df["predicted"] - eval_df["actual"]
eval_df["model_abs_err"] = eval_df["model_err"].abs()
eval_df["model_sq_err"] = eval_df["model_err"] ** 2

eval_df["naive1h_abs_err"] = (eval_df["naive_1h"] - eval_df["actual"]).abs()
eval_df["naive1h_sq_err"] = (eval_df["naive_1h"] - eval_df["actual"]) ** 2

# ----------------------------------------------------------------------
# B. Error Analysis by Taxi Zone (Model vs. Naive 1h)
# ----------------------------------------------------------------------
zone_summary = (
    eval_df.groupby("PULocationID")
    .agg(
        avg_actual=("actual", "mean"),
        Model_MAE=("model_abs_err", "mean"),
        Naive1h_MAE=("naive1h_abs_err", "mean"),
        Model_RMSE=("model_sq_err", lambda x: np.sqrt(np.mean(x))),
        Naive1h_RMSE=("naive1h_sq_err", lambda x: np.sqrt(np.mean(x))),
        mean_bias=("model_err", "mean")
    )
    .round(2)
)
zone_summary["MAE_Improvement_%"] = (
    ((zone_summary["Naive1h_MAE"] - zone_summary["Model_MAE"]) / zone_summary["Naive1h_MAE"]) * 100
).round(1)

zone_summary = zone_summary.sort_values("Model_MAE", ascending=False)

print("\n=== 2. TOP 15 WEAKEST ZONES (MODEL VS. NAIVE 1H) ===")
print(zone_summary.head(15).to_string())

print("\n=== AIRPORT ZONES CHECK (132 = JFK, 138 = LGA) ===")
print(zone_summary.loc[zone_summary.index.isin([132, 138])].to_string())

# ----------------------------------------------------------------------
# C. Error Analysis by Hour of Day (Model vs. Naive 1h)
# ----------------------------------------------------------------------
eval_df["hour"] = eval_df["hour_ts"].dt.hour

hourly_summary = (
    eval_df.groupby("hour")
    .agg(
        avg_actual=("actual", "mean"),
        Model_MAE=("model_abs_err", "mean"),
        Naive1h_MAE=("naive1h_abs_err", "mean"),
        Model_RMSE=("model_sq_err", lambda x: np.sqrt(np.mean(x))),
        Naive1h_RMSE=("naive1h_sq_err", lambda x: np.sqrt(np.mean(x))),
        mean_bias=("model_err", "mean")
    )
    .round(2)
)
hourly_summary["MAE_Improvement_%"] = (
    ((hourly_summary["Naive1h_MAE"] - hourly_summary["Model_MAE"]) / hourly_summary["Naive1h_MAE"]) * 100
).round(1)

print("\n=== 3. ERROR BY HOUR OF DAY (MODEL VS. NAIVE 1H) ===")
print(hourly_summary.to_string())

# ----------------------------------------------------------------------
# D. Biggest Single Hourly Misses (Outliers / Surges)
# ----------------------------------------------------------------------
# ----------------------------------------------------------------------
worst_misses = (
    eval_df.sort_values("model_abs_err", ascending=False)
    [["hour_ts", "PULocationID", "actual", "predicted", "model_err"]]
    .head(15)
)

print("\n=== 4. TOP 15 BIGGEST SINGLE-HOUR PREDICTION MISSES ===")
print(worst_misses.to_string(index=False))

out_dir = f"models/{model_name}"
os.makedirs(out_dir, exist_ok=True)
comparison.to_csv(f"{out_dir}/baseline_comparison_{model_name}.csv", index=False)
zone_summary.to_csv(f"{out_dir}/error_by_zone_{model_name}.csv")
hourly_summary.to_csv(f"{out_dir}/error_by_hour_{model_name}.csv")
print(f"\nSaved diagnostic CSVs to {out_dir}/")