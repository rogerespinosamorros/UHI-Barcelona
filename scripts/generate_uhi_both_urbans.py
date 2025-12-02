# generate_uhi_both_urbans.py
"""
Genera CSVs listos para analizar UHI para 0200E y 0076.
Crea comparaciones vs 0229I (1980-2016) y vs ruralMedian (2005-2025).
"""

import pandas as pd
import numpy as np
from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parents[1]
PROC_DIR = BASE_DIR / "data" / "processed"
PROC_DIR.mkdir(parents=True, exist_ok=True)

# === CARGAR MERGED ===
merged_path = PROC_DIR / "merged_all_stations_with_ruralMedian.csv"

if not merged_path.exists():
    print("ERROR: No se encuentra merged_all_stations_with_ruralMedian.csv en data/processed/")
    sys.exit(1)

df = pd.read_csv(
    merged_path,
    parse_dates=["fecha"],
    index_col="fecha",
    low_memory=False
)

# VARIABLES
urbans = ["0200E", "0076"]
rural_median_col = "tmin_rural_median"
rural_0229_col = "tmin_0229I"

# === FUNCIÓN CORREGIDA ===
def make_pair_csv(urb, rural_col, out_name, date_slice):
    """
    Crea CSV de comparación urbana-rural para UHI.
    - urb: código urbana (ej: "0200E")
    - rural_col: nombre columna rural (ej: "tmin_0229I" o "tmin_rural_median")
    - out_name: nombre del archivo de salida
    - date_slice: (fecha_inicial, fecha_final)
    """

    col_tmin_urb = f"tmin_{urb}"
    col_vel_urb  = f"velmedia_{urb}"

    # verificar columnas
    needed = [col_tmin_urb, col_vel_urb, rural_col]
    missing = [c for c in needed if c not in df.columns]

    if missing:
        print(f"[SKIP] {urb} vs {rural_col}: faltan columnas {missing}")
        return None

    # extraer columnas relevantes
    d = df[[col_tmin_urb, rural_col, col_vel_urb]].copy()
    d = d.rename(columns={
        col_tmin_urb: "tmin_urban",
        rural_col: "tmin_rural",
        col_vel_urb: "velmedia_urban"
    })

    # recorte temporal
    start, end = date_slice
    d = d.loc[(d.index >= pd.to_datetime(start)) & (d.index <= pd.to_datetime(end))]

    before = len(d)
    d = d.dropna(subset=["tmin_urban", "tmin_rural"])
    after = len(d)

    # viento débil
    d["weak_wind"] = d["velmedia_urban"] < 3.0

    # UHI
    d["UHI_tmin"] = d["tmin_urban"] - d["tmin_rural"]

    # guardar
    out_path = PROC_DIR / out_name
    d.to_csv(out_path, index=True)

    print(f"✔ Guardado {out_name} | filas {after}/{before}")
    print(f"  Rango: {d.index.min()} → {d.index.max()}")
    print(f"  UHI mean={d['UHI_tmin'].mean():.3f}, median={d['UHI_tmin'].median():.3f}")
    print(f"  weak_wind={d['weak_wind'].mean():.3f}")
    print("")

    return out_path


# === 1) URBANAS vs 0229I (1980–2016) ===
start_long = "1980-01-01"
end_long   = "2016-12-31"

for urb in urbans:
    make_pair_csv(
        urb=urb,
        rural_col=rural_0229_col,
        out_name=f"uhi_input_{urb}_vs_0229I_1980_2016.csv",
        date_slice=(start_long, end_long)
    )

# === 2) URBANAS vs ruralMedian (2005–2025) ===
start_recent = "2005-01-01"
end_recent   = "2025-12-31"

for urb in urbans:
    make_pair_csv(
        urb=urb,
        rural_col=rural_median_col,
        out_name=f"uhi_input_{urb}_ruralMedian_2005_2025.csv",
        date_slice=(start_recent, end_recent)
    )

print("Proceso completado. Archivos guardados en:", PROC_DIR)
