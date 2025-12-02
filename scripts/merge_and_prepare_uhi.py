# merge_and_prepare_uhi.py
import pandas as pd
import numpy as np
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data" / "raw" / "aemet"
OUT_DIR = BASE_DIR / "data" / "processed"
OUT_DIR.mkdir(parents=True, exist_ok=True)

csv_files = [
    "aemet_0066X_1980_2025_resume.csv",
    "aemet_0076_1980_2025_resume.csv",
    "aemet_0149X_1980_2025_resume.csv",
    "aemet_0158O_1980_2025_resume.csv",
    "aemet_0171X_1980_2025_resume.csv",
    "aemet_0200E_1980_2025_resume.csv",
    "aemet_0201X_1980_2025_resume.csv",
    "aemet_0229I_1980_2025_resume.csv",
]

def load_and_normalize(path):
    df = pd.read_csv(path, parse_dates=["fecha"], index_col="fecha", dayfirst=False)
    # normalize decimals and common bad values
    for c in df.columns:
        df[c] = df[c].astype(str).str.replace(",", ".").replace({"Ip": np.nan, "Varias": np.nan, "nan": np.nan, "": np.nan})
        # try to coerce numerics where reasonable
        try:
            df[c] = pd.to_numeric(df[c], errors="ignore")
        except Exception:
            pass
    return df

# 1) Load each and rename columns with suffix _<indicativo>
dfs = {}
for fname in csv_files:
    path = DATA_DIR / fname
    ind = fname.split("_")[1]
    df = load_and_normalize(path)
    # rename numeric columns: add suffix
    df_ren = df.rename(columns={col: f"{col}_{ind}" for col in df.columns})
    dfs[ind] = df_ren

# 2) Outer merge all
merged = None
for ind, df in dfs.items():
    if merged is None:
        merged = df.copy()
    else:
        merged = merged.join(df, how="outer")

merged.sort_index(inplace=True)
merged.to_csv(OUT_DIR / "merged_all_stations.csv", index=True)
print("Guardado merged_all_stations.csv, shape:", merged.shape)

# 3) Function: get common window between two indicatives
def common_window(ind_u, ind_r):
    col_tmin_u = f"tmin_{ind_u}"
    col_tmin_r = f"tmin_{ind_r}"
    if col_tmin_u not in merged.columns or col_tmin_r not in merged.columns:
        raise ValueError("Columnas tmin no encontradas en merged.")
    dfpair = merged[[col_tmin_u, col_tmin_r]].dropna(how='any')
    start = dfpair.index.min()
    end = dfpair.index.max()
    n_days = len(dfpair)
    return start, end, n_days

# 4) Create composite rural (median of available rural tmin columns per day)
rural_inds = ["0149X", "0171X", "0229I", "0158O"]  # ajusta según elección
tmin_r_cols = [f"tmin_{i}" for i in rural_inds if f"tmin_{i}" in merged.columns]
merged["tmin_rural_median"] = merged[tmin_r_cols].median(axis=1, skipna=True)
merged["tmax_rural_median"] = merged[[c.replace("tmin","tmax") for c in tmin_r_cols if c.replace("tmin","tmax") in merged.columns]].median(axis=1, skipna=True)

# 5) Example: prepare UHI input for pair (urbana=0076, rural_median)
urb = "0076"
merged["UHI_tmin_0076_vs_ruralMedian"] = merged[f"tmin_{urb}"] - merged["tmin_rural_median"]

# Save cleaned merged with rural median
merged.to_csv(OUT_DIR / "merged_all_stations_with_ruralMedian.csv", index=True)
print("Guardado merged_all_stations_with_ruralMedian.csv")

# 6) Build pair-specific CSV with filtering example (velmedia < 3 m/s on urban)
urb_col_vel = f"velmedia_{urb}"
pair_df = merged[[f"tmin_{urb}", "tmin_rural_median", urb_col_vel]].copy()
# keep only rows where both tmin present
pair_df = pair_df.dropna(subset=[f"tmin_{urb}", "tmin_rural_median"])
# Example filter: weak wind nights
pair_df["weak_wind"] = pair_df[urb_col_vel] < 3.0
pair_df.to_csv(OUT_DIR / f"uhi_input_{urb}_ruralMedian.csv", index=True)
print("Guardado uhi_input_{urb}_ruralMedian.csv")
