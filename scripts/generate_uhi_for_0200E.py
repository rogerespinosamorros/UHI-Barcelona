import pandas as pd
import numpy as np
from pathlib import Path

# Rutas
BASE_DIR = Path(__file__).resolve().parents[1]
PROC_DIR = BASE_DIR / "data" / "processed"

merged_path = PROC_DIR / "merged_all_stations_with_ruralMedian.csv"
df = pd.read_csv(merged_path, parse_dates=["fecha"], index_col="fecha")

urban = "0200E"

# Comprobar que existe
if f"tmin_{urban}" not in df.columns:
    raise ValueError(f"No existe la columna tmin_{urban} en merged_all_stations_with_ruralMedian.csv")

# Seleccionar columnas relevantes
cols = [
    f"tmin_{urban}",
    f"velmedia_{urban}",
    "tmin_rural_median"
]

df_uhi = df[cols].copy()

# Crear etiqueta de viento débil (< 3 m/s)
df_uhi["weak_wind"] = df_uhi[f"velmedia_{urban}"] < 3.0

# Eliminar días con temperaturas faltantes
df_uhi = df_uhi.dropna(subset=[f"tmin_{urban}", "tmin_rural_median"])

# Guardar
out_path = PROC_DIR / f"uhi_input_{urban}_ruralMedian.csv"
df_uhi.to_csv(out_path, index=True)

print("Archivo generado:", out_path)
print("Filas finales:", len(df_uhi))
