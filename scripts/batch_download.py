# batch_download.py
from datetime import datetime
from download_aemet_resume import download_full_station_resume

stations = [
    ("0201D","aemet_0076_1980_2025_resume.csv"),
    ("0075","aemet_0075_1980_2025_resume.csv"),
    ("0200E","aemet_0200E_1980_2025_resume.csv"),
    ("0158O","aemet_0158O_1980_2025_resume.csv"),
    ("0201X", "aemet_0201X_1980_2025_resume.csv"),
    ("0171X", "aemet_0171X_1980_2025_resume.csv"),
    ("0149X", "aemet_0149X_1980_2025_resume.csv"),
    ("0229I", "aemet_0229I_1980_2025_resume.csv"),
    ("0066X", "aemet_0066X_1980_2025_resume.csv"),
]

start = datetime(1980,1,1)
end   = datetime(2025,12,31)

for est, out in stations:
    print("===== INICIANDO ESTACION:", est, "->", out, "=====")
    try:
        df = download_full_station_resume(est, start, end, out, months_chunk=3)
        print("DONE:", est, "rows:", len(df))
    except Exception as e:
        print("ERROR en", est, e)
        # continuar con la siguiente
