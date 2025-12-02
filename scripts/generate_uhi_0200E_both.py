# generate_uhi_0200E_both.py
import pandas as pd
import numpy as np
from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parents[1]
PROC_DIR = BASE_DIR / "data" / "processed"
PROC_DIR.mkdir(parents=True, exist_ok=True)

# Input merged file (con ruralMedian creado previamente)
merged_path = PROC_DIR / "merged_all_stations_with_ruralMedian.csv"
if not merged_path.exists():
    # try alternative location (in case merged was saved in data/processed earlier)
    alt = BASE_DIR / "data" / "processed" / "merged_all_stations_with_ruralMedian.csv"
    if alt.exists():
        merged_path = alt
    else:
        print("ERROR: no se encuentra merged_all_stations_with_ruralMedian.csv en data/processed.")
        sys.exit(1)

# Load merged; handle index as 'fecha' or as index
try:
    df = pd.read_csv(merged_path, parse_dates=["fecha"], index_col="fecha")
except Exception:
    # fallback: read and try to find date-like column
    df = pd.read_csv(merged_path)
    if "fecha" in df.columns:
        df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
        df = df.set_index("fecha")
    else:
        raise RuntimeError("El archivo merged no contiene columna 'fecha' y no se pudo inferir índice.")

# Urban station to process
urban = "0200E"

# Column names we expect
col_tmin_urb = f"tmin_{urban}"
col_vel_urb = f"velmedia_{urban}"
col_tmin_ruralMedian = "tmin_rural_median"
col_tmin_0229 = "tmin_0229I"

# Basic checks
missing_cols = [c for c in [col_tmin_urb, col_vel_urb, col_tmin_ruralMedian, col_tmin_0229] if c not in df.columns]
if missing_cols:
    print("Atención: faltan columnas en merged:", missing_cols)
    # If only 0229I missing, still allow ruralMedian route; warn and continue
    # decide whether to abort on critical missing columns
    # abort only if urban tmin missing or urban vel missing or rural median missing
    critical = [col_tmin_urb, col_vel_urb, col_tmin_ruralMedian]
    missing_critical = [c for c in critical if c not in df.columns]
    if missing_critical:
        print("Columnas críticas faltantes:", missing_critical, "=> abortando.")
        raise SystemExit(1)
    else:
        print("Continuamos; la comparación con 0229I no estará disponible si falta esa columna.")

# Helper to create UHI pair CSV
def make_pair_csv(urb_col_tmin, rural_col_tmin, urb_vel_col, out_name, date_slice=None):
    d = df[[urb_col_tmin, rural_col_tmin, urb_vel_col]].copy()
    # rename to standard names
    d = d.rename(columns={
        urb_col_tmin: "tmin_urban",
        rural_col_tmin: "tmin_rural",
        urb_vel_col: "velmedia_urban"
    })
    # apply optional date slice
    if date_slice is not None:
        start, end = date_slice
        d = d.loc[(d.index >= pd.to_datetime(start)) & (d.index <= pd.to_datetime(end))]
    # drop rows where either tmin is missing
    before = len(d)
    d = d.dropna(subset=["tmin_urban", "tmin_rural"])
    after_drop = len(d)
    # create weak_wind flag
    d["weak_wind"] = d["velmedia_urban"] < 3.0
    # compute UHI
    d["UHI_tmin"] = d["tmin_urban"] - d["tmin_rural"]
    # Save
    out_path = PROC_DIR / out_name
    d.to_csv(out_path, index=True)
    # Print summary
    print(f"Guardado: {out_path.name}  | filas antes_drop={before}  after_drop={after_drop}  rango: {d.index.min()} - {d.index.max()}")
    # Provide some diagnostics
    print("  UHI_tmin stats (mean, median, std):", d["UHI_tmin"].mean(), d["UHI_tmin"].median(), d["UHI_tmin"].std())
    print("  weak_wind fraction:", d["weak_wind"].mean())
    return out_path

# 1) Create 0200E vs 0229I for 1980-01-01 to 2016-12-31 (if data available)
if col_tmin_0229 in df.columns:
    start_long = "1980-01-01"
    end_long = "2016-12-31"
    try:
        path1 = make_pair_csv(col_tmin_urb, col_tmin_0229, col_vel_urb,
                              f"uhi_input_{urban}_vs_0229I_1980_2016.csv",
                              date_slice=(start_long, end_long))
    except Exception as e:
        print("No se pudo generar 0200E vs 0229I:", e)
else:
    print("No existe columna", col_tmin_0229, "- se omite la generación 0200E vs 0229I.")

# 2) Create 0200E vs ruralMedian for recent period 2005-01-01 to 2025-12-31 (or full available range)
start_recent = "2005-01-01"
end_recent = "2025-12-31"
path2 = make_pair_csv(col_tmin_urb, col_tmin_ruralMedian, col_vel_urb,
                      f"uhi_input_{urban}_ruralMedian_2005_2025.csv",
                      date_slice=(start_recent, end_recent))

print("\nProceso finalizado.")
