# scripts/aemet_clean_csv.py
import argparse, io, json, re
from pathlib import Path
import pandas as pd

def to_float(s):
    if pd.isna(s):
        return pd.NA
    s = str(s).strip()
    if s.lower() == "ip":  # precip inapreciable
        return 0.0
    s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return pd.NA

# --- 1) Lecturas "normales" ---
def read_as_csv_std(path: Path):
    try:
        df = pd.read_csv(path)
        return df
    except Exception:
        return None

def read_as_csv_aemet(path: Path):
    try:
        txt = path.read_text(encoding="utf-8", errors="ignore")
        df = pd.read_csv(io.StringIO(txt), sep=";", decimal=",")
        return df
    except Exception:
        return None

def read_as_json_array(path: Path):
    try:
        txt = path.read_text(encoding="utf-8", errors="ignore").strip()
        # JSON correcto
        if txt.startswith("[") and txt.endswith("]"):
            return pd.DataFrame(json.loads(txt))
    except Exception:
        pass
    return None

# --- 2) Parser para “dump” raro con comillas duplicadas ---
def read_weird_dump(path: Path):
    txt = path.read_text(encoding="utf-8", errors="ignore").strip()
    if not txt:
        return None

    # Limpia comillas duplicadas: "" -> "
    t = txt.replace('""', '"')

    # Quita posibles comas sueltas antes de llaves de cierre
    t = re.sub(r',\s*}', '}', t)
    t = re.sub(r',\s*]', ']', t)

    # Si es array con objetos, intenta separar manualmente
    # Normalizamos a algo tipo [{...},{...}]
    # 1) Quita encabezados/colas extrañas
    t = t.lstrip(' ,\n\r\t')
    t = t.rstrip(' ,\n\r\t')

    # Si no empieza por '[' pero parece lista de objetos, intenta envolver
    if not t.startswith("["):
        if t.startswith("{") and t.endswith("}"):
            t = f"[{t}]"

    # Intenta trocear objetos a mano
    # Quitamos la cabecera '[' y la cola ']'
    if t.startswith("[") and t.endswith("]"):
        inner = t[1:-1]
    else:
        inner = t

    # Dividimos por "},{" aproximado (tolerante a espacios)
    parts = re.split(r'\}\s*,\s*\{', inner)
    recs = []
    for i, p in enumerate(parts):
        # reconstruye llaves
        p_obj = p
        if not p_obj.strip().startswith("{"):
            p_obj = "{" + p_obj
        if not p_obj.strip().endswith("}"):
            p_obj = p_obj + "}"

        # Extrae pares "clave":"valor" (solo comillas dobles)
        pairs = re.findall(r'"([^"]+)"\s*:\s*"([^"]*)"', p_obj)
        if not pairs:
            # Si falla, intenta limpiar aún más espacios/quotes
            p_clean = re.sub(r'\s+"', '"', p_obj)
            pairs = re.findall(r'"([^"]+)"\s*:\s*"([^"]*)"', p_clean)
        if pairs:
            recs.append(dict(pairs))

    if recs:
        return pd.DataFrame(recs)
    return None

def read_aemet_any(path: Path) -> pd.DataFrame:
    # Orden de intentos: CSV std -> JSON -> CSV AEMET -> weird dump
    for reader in (read_as_csv_std, read_as_json_array, read_as_csv_aemet, read_weird_dump):
        df = reader(path)
        if df is not None and len(df) > 0:
            return df
    raise RuntimeError("No se pudo interpretar el archivo con ningún parser.")

def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    # normaliza nombres
    df.columns = [c.strip().lower() for c in df.columns]

    # intenta detectar una columna de fecha por nombre o por patrón de valores
    date_col = None
    # 1) por nombre típico
    for cand in ("fecha","fechao","date","fecha_observacion"):
        if cand in df.columns:
            date_col = cand
            break
    # 2) por patrón YYYY-MM-DD
    if date_col is None:
        for c in df.columns:
            serie = df[c].astype(str).str.strip()
            # detecta si >80% de valores parecen YYYY-MM-DD
            mask = serie.str.match(r"\d{4}-\d{2}-\d{2}")
            if mask.mean(skipna=True) > 0.8:
                date_col = c
                break

    if date_col is None:
        # Último recurso: intenta extraer una fecha de cadenas tipo '... "fecha":"1980-01-01" ...'
        # (cuando toda la fila es un texto)
        if df.shape[1] == 1:
            s = df.iloc[:,0].astype(str)
            m = s.str.extract(r'"fecha"\s*:\s*"(\d{4}-\d{2}-\d{2})"')
            if m[0].notna().any():   # ✅ ahora es booleano
                df = pd.DataFrame({"fecha": m[0]})
                date_col = "fecha"


    if date_col is None:
        raise ValueError("No se encuentra columna de fecha (ni por nombre ni por patrón).")

    df["fecha"] = pd.to_datetime(df[date_col], errors="coerce")

    # convierte numéricos
    for col in ("tmed","tmin","tmax","prec","racha","sol","altitud"):
        if col in df.columns:
            df[col] = df[col].map(to_float)

    keep = [c for c in ("fecha","indicativo","nombre","provincia","altitud","tmed","tmin","tmax","prec","racha","sol") if c in df.columns]
    df = df[keep].dropna(subset=["fecha"]).sort_values("fecha").reset_index(drop=True)
    return df

def main():
    ap = argparse.ArgumentParser(description="Limpia CSV/JSON AEMET y guarda en Parquet.")
    ap.add_argument("--in", dest="inp", required=True, help="Ruta al archivo bruto AEMET")
    ap.add_argument("--out", dest="outp", required=False, help="Salida Parquet")
    args = ap.parse_args()

    inp = Path(args.inp)
    df_raw = read_aemet_any(inp)
    df = clean_df(df_raw)

    # Fallback: si solo tenemos 'fecha', reintenta parseo profundo del bruto
    if list(df.columns) == ["fecha"]:
        raw_txt = Path(args.inp).read_text(encoding="utf-8", errors="ignore")

        # Normaliza comillas dobles duplicadas y limpia "comas colgantes"
        t = raw_txt.replace('""', '"')
        t = re.sub(r',\s*}', '}', t)
        t = re.sub(r',\s*]', ']', t)

        # Divide en pseudo-registros aproximando por aparición de "fecha"
        parts = re.split(r'(?="fecha"\s*:\s*")', t)

        records = []
        wanted = {"fecha","indicativo","nombre","provincia","altitud","tmed","tmin","tmax","prec","racha","sol","dir","horaracha"}

        pair_re = re.compile(r'"([^"]+)"\s*:\s*"([^"]*)"')

        for chunk in parts:
            pairs = dict(pair_re.findall(chunk))
            if "fecha" in pairs:
                # Quédate solo con las claves de interés
                rec = {k: pairs.get(k) for k in wanted if k in pairs}
                records.append(rec)

        if records:
            df = pd.DataFrame(records)

            # --- normaliza tipos ---
            df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")

            def to_float(s):
                if pd.isna(s): return pd.NA
                s = str(s).strip()
                if s.lower() == "ip": return 0.0
                s = s.replace(",", ".")
                try: return float(s)
                except: return pd.NA

            for col in ("tmed","tmin","tmax","prec","racha","sol","altitud"):
                if col in df.columns:
                    df[col] = df[col].map(to_float)

            keep = [c for c in ("fecha","indicativo","nombre","provincia","altitud","tmed","tmin","tmax","prec","racha","sol") if c in df.columns]
            df = df[keep].dropna(subset=["fecha"]).sort_values("fecha").reset_index(drop=True)


    # salida por defecto
    outp = Path(args.outp) if args.outp else None
    if outp is None:
        station = None
    if "indicativo" in df.columns and df["indicativo"].notna().any():
        station = str(df["indicativo"].dropna().iloc[0])
    if not station:
        station = inp.stem.split("_")[0]
    outp = Path(f"data/processed/aemet/{station}_daily.parquet")

    outp.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(outp, index=False)
    print(f"✅ Guardado limpio: {outp}  ({len(df)} filas)")
    print("🔎 Columnas:", list(df.columns))

if __name__ == "__main__":
    main()
