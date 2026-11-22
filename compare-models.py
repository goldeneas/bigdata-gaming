import os
import numpy as np
import pandas as pd

# ----------------------------------------------------------------------
# 1. Load all model prediction files
# ----------------------------------------------------------------------
MODELS = {
    "Linear Regression": "models/lr/predicted_demand.csv",
    "Random Forest":     "models/rf/predicted_demand_rf.csv",
    "GBTRegressor":      "models/gbt/predicted_demand_gbt.csv",
    "Random Forest v3": "models/rf_v3/predicted_demand_rf_v3.csv",
}

base_df = None

for name, path in MODELS.items():
    if not os.path.exists(path):
        print(f"Warning: {path} not found, skipping {name}.")
        continue

    temp = pd.read_csv(path)
    temp["hour_ts"] = pd.to_datetime(temp["hour_ts"])
    temp = temp.rename(columns={"predicted": name})

    if base_df is None:
        cols = ["PULocationID", "hour_ts", "actual", name]
        if "split" in temp.columns:
            cols.append("split")
        base_df = temp[cols].copy()
    else:
        base_df = base_df.merge(
            temp[["PULocationID", "hour_ts", name]],
            on=["PULocationID", "hour_ts"],
            how="inner"
        )

# Sort chronologically per zone and add Naive baselines
base_df = base_df.sort_values(["PULocationID", "hour_ts"]).reset_index(drop=True)
base_df["Naive 1h Lag"] = base_df.groupby("PULocationID")["actual"].shift(1)
base_df["Naive 24h Lag"] = base_df.groupby("PULocationID")["actual"].shift(24)

# Filter strictly to test set
if "split" in base_df.columns:
    test_df = base_df[base_df["split"] == "test"].copy()
else:
    test_df = base_df.copy()

eval_df = test_df.dropna(subset=["Naive 1h Lag", "Naive 24h Lag"]).copy()

# Metric helper
def calc_metrics(y_true, y_pred):
    err = y_true - y_pred
    rmse = np.sqrt(np.mean(err ** 2))
    mae = np.mean(np.abs(err))
    r2 = 1 - (np.sum(err ** 2) / np.sum((y_true - np.mean(y_true)) ** 2))
    return {"RMSE": round(rmse, 3), "MAE": round(mae, 3), "R2": round(r2, 4)}

# ----------------------------------------------------------------------
# 2. Overall Citywide Leaderboard (Sorted by RMSE)
# ----------------------------------------------------------------------
all_candidates = list(MODELS.keys()) + ["Naive 1h Lag", "Naive 24h Lag"]
all_candidates = [c for c in all_candidates if c in eval_df.columns]

overall_rows = [
    {"Model": m, **calc_metrics(eval_df["actual"], eval_df[m])}
    for m in all_candidates
]
overall_df = pd.DataFrame(overall_rows).sort_values("RMSE")

print("\n=== 1. OVERALL TEST SET LEADERBOARD (ALL ZONES) ===")
print(overall_df.to_string(index=False))

# ----------------------------------------------------------------------
# 3. Airport Zones Comparison (132 = JFK, 138 = LaGuardia)
# ----------------------------------------------------------------------
airport_df = eval_df[eval_df["PULocationID"].isin([132, 138])]
airport_rows = [
    {"Model": m, **calc_metrics(airport_df["actual"], airport_df[m])}
    for m in all_candidates
]
airport_leaderboard = pd.DataFrame(airport_rows).sort_values("RMSE")

print("\n=== 2. AIRPORT ZONES LEADERBOARD (JFK 132 + LGA 138 ONLY) ===")
print(airport_leaderboard.to_string(index=False))

# ----------------------------------------------------------------------
# 4. Active Zones Only (avg_actual >= 20 pickups/hr — excludes dead zones)
# ----------------------------------------------------------------------
zone_means = eval_df.groupby("PULocationID")["actual"].mean()
active_zones = zone_means[zone_means >= 20].index
active_df = eval_df[eval_df["PULocationID"].isin(active_zones)]

active_rows = [
    {"Model": m, **calc_metrics(active_df["actual"], active_df[m])}
    for m in all_candidates
]
active_leaderboard = pd.DataFrame(active_rows).sort_values("RMSE")

print(f"\n=== 3. ACTIVE ZONES LEADERBOARD ({len(active_zones)} BUSY ZONES, AVG >= 20/HR) ===")
print(active_leaderboard.to_string(index=False))

# ----------------------------------------------------------------------
# 5. Zone-by-Zone Winner Count (Which model wins the most zones by MAE?)
# ----------------------------------------------------------------------
model_names = [m for m in MODELS.keys() if m in eval_df.columns]
zone_mae = {}
for m in model_names:
    zone_mae[m] = eval_df.groupby("PULocationID").apply(
        lambda g: np.mean(np.abs(g["actual"] - g[m])), include_groups=False
    )
zone_mae_df = pd.DataFrame(zone_mae)
zone_mae_df["Best_Model"] = zone_mae_df.idxmin(axis=1)

print("\n=== 4. NUMBER OF ZONES WON BY EACH MODEL (LOWEST MAE) ===")
print(zone_mae_df["Best_Model"].value_counts().to_string())


os.makedirs("models/comparisons", exist_ok=True)
overall_df.to_csv("models/comparisons/full_model_comparison.csv", index=False)
airport_leaderboard.to_csv("models/comparisons/airport_model_comparison.csv", index=False)
print("\nSaved full_model_comparison.csv and airport_model_comparison.csv to models/comparisons")
