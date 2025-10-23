"""
Download daily temperature data from the AEMET OpenData API
Example usage:

python scripts/aemet_download.py --station 0200E --start 1980-01-01 --end 2025-01-01
"""

# scripts/aemet_download.py
import os, io, time, json, argparse, random
import requests
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

AEMET_BASE = "https://opendata.aemet.es/opendata/api/valores/climatologicos/diarios/datos"
STATE_DIR = "data/raw/aemet"
os.makedirs(STATE_DIR, exist_ok=True)
HEADERS = {"User-Agent": "tfg-uhi/1.0"}  


def make_session():
    s = requests.Session()
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=1.2,  # backoff exponencial
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://", HTTPAdapter(max_retries=retry))
    return s

def get_api_key():
    load_dotenv()
    key = os.getenv("AEMET_API_KEY")
    if not key:
        raise RuntimeError("Falta AEMET_API_KEY en .env")
    return key

def state_path(station, start, end):
    return os.path.join(STATE_DIR, f"{station}_{start}_{end}.state.json")

def load_state(station, start, end):
    p = state_path(station, start, end)
    if os.path.exists(p):
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"station": station, "start": start, "end": end, "months": 3, "last_done": start}

def save_state(st):
    with open(state_path(st["station"], st["start"], st["end"]), "w", encoding="utf-8") as f:
        json.dump(st, f, ensure_ascii=False, indent=2)

def build_url(station, start, end):
    return f"{AEMET_BASE}/fechaini/{start}T00:00:00UTC/fechafin/{end}T00:00:00UTC/estacion/{station}"

def month_chunks(cur, end, months=3):
    while cur < end:
        nxt = min(cur + relativedelta(months=months), end)
        yield cur, nxt
        cur = nxt

def fetch_index(session, url, api_key, timeout=60):
    # 1) with header
    r  = session.get(url, headers={"api_key": api_key, **HEADERS}, timeout=timeout)
    try:
        j = r.json()
    except Exception:
        raise RuntimeError(f"Índice no JSON ({r.status_code}): {r.text[:300]}")
    if "datos" in j:
        return j
    # 2) try as query param
    r2 = session.get(url, params={"api_key": api_key}, headers=HEADERS, timeout=timeout)
    try:
        j2 = r2.json()
    except Exception:
        raise RuntimeError(f"Índice no JSON ({r2.status_code}): {r2.text[:300]}")
    return j2

def fetch_csv_from_datos(session, datos_url, timeout=180):
    r = session.get(datos_url, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    return pd.read_csv(io.StringIO(r.text), sep=";", decimal=",")

def download_station_range(station, start_date, end_date, months=3, sleep_base=1.5):
    api_key = get_api_key()
    session = make_session()
    start = datetime.fromisoformat(start_date)
    end = datetime.fromisoformat(end_date)

    state = load_state(station, start_date, end_date)
    # Start from last_done
    cur = datetime.fromisoformat(state["last_done"])
    if cur < start:  # healing
        cur = start
    print(f"📌 Reanudando desde {cur.date()}")

    parts = []
    idx_count = 0
    for i, (s, e) in enumerate(month_chunks(cur, end, months), start=1):
        s_str, e_str = s.strftime("%Y-%m-%d"), e.strftime("%Y-%m-%d")
        print(f"[{i}] {station} {s_str}→{e_str} (intento 1)")

        # Trying again indexs (data/metadata)
        idx_json = None
        skip_chunk = False  # <-- bandera para saltar el tramo
        for attempt in range(1, 4):
            try:
                idx_json = fetch_index(session, build_url(station, s_str, e_str), api_key)
                if "datos" in idx_json:
                    break
                else:
                    est  = idx_json.get("estado")
                    desc = idx_json.get("descripcion")

                    # 429/5xx -> reintenta con backoff
                    if est in (429, 500, 502, 503, 504):
                        wait = sleep_base * attempt + random.uniform(0, 0.8)
                        print(f"⚠️ idx estado={est} {desc}. Reintento {attempt}/3 en {wait:.1f}s")
                        time.sleep(wait)
                        continue

                    # 404 -> no hay datos en este tramo: saltar
                    if est == 404:
                        print(f"ℹ️ Sin datos {s_str}→{e_str} (404). Se salta el tramo.")
                        state["last_done"] = e_str
                        save_state(state)
                        skip_chunk = True
                        break

                    # Otros errores -> cortar
                    raise RuntimeError(f"AEMET sin 'datos' {s_str}→{e_str}. estado={est}. desc={desc}")

            except requests.exceptions.RequestException as ex:
                wait = sleep_base * attempt + random.uniform(0, 0.8)
                print(f"⚠️ idx conexión: {ex}. Reintento {attempt}/3 en {wait:.1f}s")
                time.sleep(wait)

        # si tras reintentos no hay datos o decidimos saltar el tramo
        if skip_chunk:
            time.sleep(sleep_base)
            continue  # pasa al siguiente (NO intentes descargar CSV)

        if not idx_json or "datos" not in idx_json:
            raise RuntimeError("No se pudo obtener 'datos' tras reintentos.")


        datos_url = idx_json["datos"]
        # Trying again loading CSV
        df_part = None
        for attempt in range(1, 4):
            try:
                df_part = fetch_csv_from_datos(session, datos_url)
                break
            except requests.exceptions.RequestException as ex:
                wait = sleep_base * attempt + random.uniform(0, 1.0)
                print(f"⚠️ csv conexión: {ex}. Reintento {attempt}/3 en {wait:.1f}s")
                time.sleep(wait)
        if df_part is None:
            raise RuntimeError("No se pudo descargar CSV tras reintentos.")

        if not df_part.empty:
            parts.append(df_part)

        # Save progress and respect rate limit
        state["last_done"] = e_str
        save_state(state)
        time.sleep(sleep_base + random.uniform(0.3, 1.0))
        idx_count += 1

    if not parts:
        return pd.DataFrame()

    df = pd.concat(parts, ignore_index=True).drop_duplicates()
    if "FECHA" in df.columns:
        df["FECHA"] = pd.to_datetime(df["FECHA"], errors="coerce")
        df = df.sort_values("FECHA")
    return df

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--station", required=True)
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--months", type=int, default=3)
    args = ap.parse_args()

    os.makedirs("data/raw/aemet", exist_ok=True)
    df = download_station_range(args.station, args.start, args.end, args.months)
    out_csv = os.path.join(STATE_DIR, f"{args.station}_{args.start}_{args.end}.csv")
    df.to_csv(out_csv, index=False)
    print(f"✅ Guardado {len(df)} filas en {out_csv}")

if __name__ == "__main__":
    main()
