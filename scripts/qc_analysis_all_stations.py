import pandas as pd
import numpy as np
from pathlib import Path

# ====================
# CONFIGURACIÓN RUTAS
# ====================
BASE_DIR = Path(__file__).resolve().parents[1]   # raíz del proyecto
DATA_DIR = BASE_DIR / "data" / "raw" / "aemet"

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

num_vars = ["tmin", "tmax", "tmed", "velmedia", "racha", "sol", "hrMedia", "presMin", "presMax", "prec"]

def classify_station(indicativo):
    urban_codes = ["0066X", "0076", "0200E", "0201X"]
    rural_codes = ["0158OX", "0171X", "0149X", "0229I"]

    if indicativo in urban_codes:
        return "urbana"
    elif indicativo in rural_codes:
        return "rural"
    else:
        return "desconocida"

def load_station(filename):
    filepath = DATA_DIR / filename
    df = pd.read_csv(filepath, parse_dates=["fecha"], index_col="fecha")
    return df


results = []

for file in csv_files:
    indicativo = file.split("_")[1]

    df = load_station(file)

    start = df.index.min()
    end = df.index.max()
    total_days = len(df)

    missing = {}

    for v in num_vars:
        if v in df.columns:
            missing[v] = df[v].isna().mean() * 100
        else:
            missing[v] = np.nan

    if "tmin" in df.columns and "tmax" in df.columns:
        temp_inversion = (df["tmin"] > df["tmax"]).sum()
        temp_inversion_pct = temp_inversion / len(df) * 100
    else:
        temp_inversion = np.nan
        temp_inversion_pct = np.nan

    station_class = classify_station(indicativo)

    results.append({
        "indicativo": indicativo,
        "archivo": file,
        "clase_estacion": station_class,
        "fecha_inicio": start,
        "fecha_fin": end,
        "dias_totales": total_days,
        "tmin>tmax_count": temp_inversion,
        "tmin>tmax_%": temp_inversion_pct,
        **{f"missing_{k}": v for k, v in missing.items()}
    })

df_res = pd.DataFrame(results).sort_values("indicativo")

df_res.to_csv("QC_summary_all_stations.csv", index=False)

print("\n======= RESUMEN QC–QA COMPLETADO =======\n")
print(df_res.to_string(index=False))
print("\nArchivo generado: QC_summary_all_stations.csv")
