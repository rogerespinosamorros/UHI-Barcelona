# download_aemet_resume.py
"""
Descarga robusta con reanudación y throttling para AEMET OpenData.
- Usa header 'api_key'
- Chunk por defecto: 3 meses (reduce carga)
- Respeta Retry-After y aplica backoff largo en 429
- Guarda cada chunk en disk/chunks para reanudar
"""
import requests, json, time, os
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
import numpy as np

load_dotenv()
API_KEY = os.getenv("AEMET_API_KEY")
if not API_KEY:
    raise SystemExit("ERROR: AEMET_API_KEY no encontrada en .env")

HEADERS_META = {"api_key": API_KEY, "User-Agent": "TFG-UHI-resume/1.0"}
BASE_META = "https://opendata.aemet.es/opendata/api/valores/climatologicos/diarios/datos/fechaini/{ini}/fechafin/{fin}/estacion/{est}"

OUTDIR = Path(".")
CHUNKDIR = OUTDIR / "chunks"
CHUNKDIR.mkdir(exist_ok=True)

LOGFILE = OUTDIR / "download_resume.log"

def log(msg):
    t = datetime.utcnow().isoformat()
    print(msg)
    with open(LOGFILE, "a", encoding="utf-8") as f:
        f.write(f"{t} {msg}\n")

def daterange_chunks(start, end, months_chunk=3):
    cur = start
    while cur < end:
        nxt = cur + relativedelta(months=months_chunk) - relativedelta(days=1)
        if nxt > end:
            nxt = end
        yield cur, nxt
        cur = nxt + relativedelta(days=1)

def save_chunk_file(est, ini, fin, arr):
    fname = CHUNKDIR / f"{est}_{ini.strftime('%Y%m%d')}_{fin.strftime('%Y%m%d')}.json"
    with open(fname, "w", encoding="utf-8") as f:
        json.dump(arr, f, ensure_ascii=False)
    return fname

def read_chunk_file(est, ini, fin):
    fname = CHUNKDIR / f"{est}_{ini.strftime('%Y%m%d')}_{fin.strftime('%Y%m%d')}.json"
    if fname.exists():
        with open(fname, "r", encoding="utf-8") as f:
            return json.load(f)
    return None

def fetch_metadata_and_data_with_rate_handling(est, ini_dt, fin_dt, max_attempts=10):
    ini = ini_dt.strftime("%Y-%m-%dT00:00:00UTC")
    fin = fin_dt.strftime("%Y-%m-%dT00:00:00UTC")
    meta_url = BASE_META.format(ini=ini, fin=fin, est=est)
    last_err = None
    for attempt in range(1, max_attempts+1):
        try:
            r = requests.get(meta_url, headers=HEADERS_META, timeout=30)
            # Si devuelve 429, mirar Retry-After
            if r.status_code == 429:
                ra = r.headers.get("Retry-After")
                wait = int(ra) if ra and ra.isdigit() else min(60 * attempt, 3600)
                log(f"[429] meta {ini}->{fin} attempt={attempt} -> esperar {wait}s (Retry-After={ra})")
                time.sleep(wait)
                continue
            r.raise_for_status()
            try:
                meta = r.json()
            except ValueError:
                last_err = f"Metadata non-JSON status={r.status_code} len={len(r.text)}"
                log(f"[WARN] {last_err}")
                # espera larga y reintenta
                time.sleep(min(60 * attempt, 600))
                continue
            datos_url = meta.get("datos")
            log(f"[META_OK] {ini}->{fin} estado={meta.get('estado')} datos_url={datos_url}")
            if not datos_url:
                return None
            # descargar datos_url (adulto)
            for a2 in range(1, max_attempts+1):
                try:
                    rr = requests.get(datos_url, headers={"User-Agent": HEADERS_META["User-Agent"]}, timeout=90)
                    if rr.status_code == 429:
                        ra = rr.headers.get("Retry-After")
                        wait = int(ra) if ra and ra.isdigit() else min(60 * a2, 3600)
                        log(f"[429] datos_url {ini}->{fin} inner attempt={a2} -> esperar {wait}s")
                        time.sleep(wait)
                        continue
                    rr.raise_for_status()
                    text = rr.text
                    if not text or text.strip() == "":
                        log("[WARN] datos_url body vacío, reintentando")
                        time.sleep(min(10 * a2, 300))
                        continue
                    try:
                        arr = json.loads(text)
                    except ValueError:
                        # intentar leer con pandas si es JSON-like
                        try:
                            df_tmp = pd.read_json(text)
                            arr = df_tmp.to_dict(orient="records")
                        except Exception as e:
                            log(f"[ERROR] No JSON en datos_url: {e}")
                            time.sleep(min(30 * a2, 600))
                            continue
                    return arr
                except requests.RequestException as e2:
                    log(f"[WARN] error datos_url attempt {a2}: {e2}")
                    time.sleep(min(5 * a2, 300))
            # si no se pudo descargar datos_url, reintenta metadata
        except requests.RequestException as e:
            last_err = f"Request metadata err attempt {attempt}: {e}"
            log(f"[WARN] {last_err}")
            time.sleep(min(10 * attempt, 600))
    raise RuntimeError(f"Fallo persistente {ini} - {fin}. Last err: {last_err}")

def download_full_station_resume(est, start_date, end_date, out_csv, months_chunk=3):
    dfs = []
    for ini, fin in daterange_chunks(start_date, end_date, months_chunk=months_chunk):
        # si chunk ya descargado, lo usamos
        existing = read_chunk_file(est, ini, fin)
        if existing is not None:
            log(f"Chunk ya en disco: {ini.date()}->{fin.date()}")
            if len(existing) > 0:
                dfs.append(pd.DataFrame(existing))
            continue
        log(f"Descargando chunk {ini.date()} -> {fin.date()}")
        arr = fetch_metadata_and_data_with_rate_handling(est, ini, fin)
        if arr is None or len(arr) == 0:
            log(f"Chunk vacío (sin datos) {ini.date()}->{fin.date()} - guardando archivo vacío")
            save_chunk_file(est, ini, fin, [])
            continue
        # guardar chunk y agregar
        save_chunk_file(est, ini, fin, arr)
        dfs.append(pd.DataFrame(arr))
        # pausa cortita para no saturar
        time.sleep(0.3)
    if not dfs:
        raise RuntimeError("No se descargó ningún chunk con datos en el rango.")
    df_all = pd.concat(dfs, ignore_index=True).drop_duplicates(subset=["fecha"], keep="first")
    df_all["fecha"] = pd.to_datetime(df_all["fecha"], format="%Y-%m-%d", errors="coerce")
    df_all = df_all.set_index("fecha").sort_index()
    # normalizar
    cols = []
    for c in ["tmed","tmin","tmax","prec","sol","velmedia","racha","presMax","presMin","hrMedia"]:
        if c in df_all.columns:
            cols.append(c)
            df_all[c] = df_all[c].astype(str).str.replace(",", ".").replace({"Ip": None, "Varias": None, "": None})
            df_all[c] = pd.to_numeric(df_all[c], errors="coerce")
    # reindex completo
    idx = pd.date_range(df_all.index.min(), df_all.index.max(), freq="D")
    df_all = df_all.reindex(idx)
    # guardar
    df_all.to_csv(out_csv, index_label="fecha", encoding="utf-8")
    log(f"Guardado CSV final: {out_csv}")
    return df_all

if __name__ == "__main__":
    est = "0076"
    start = datetime(1980,1,1)
    end = datetime(2025,12,31)
    out = f"aemet_{est}_1980_2025_resume.csv"
    df = download_full_station_resume(est, start, end, out, months_chunk=3)
    log("Proceso completado.")
