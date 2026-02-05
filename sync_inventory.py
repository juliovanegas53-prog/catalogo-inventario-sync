import os
import requests
import mysql.connector
from datetime import datetime, timezone

TABLE = "inventario"
BATCH_SIZE = 500

def env(name: str, default: str | None = None) -> str:
    v = os.environ.get(name, default)
    if v is None or str(v).strip() == "":
        raise RuntimeError(f"Falta variable de entorno: {name}")
    return str(v).strip()

def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def supabase_headers() -> dict:
    key = env("SUPABASE_SERVICE_ROLE_KEY")
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }

def normalize_text(v):
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None

def mysql_connect():
    host = env("DB_HOST")
    port = int(env("DB_PORT"))
    db = env("DB_NAME")
    user = env("DB_USER")
    pwd = env("DB_PASSWORD")

    print("DB connect:", host, port, db, user)

    # ssl_disabled=True equivale a SSL Mode=None del config .NET
    return mysql.connector.connect(
        host=host,
        port=port,
        database=db,
        user=user,
        password=pwd,
        ssl_disabled=True,
        connection_timeout=30,
    )

def fetch_view_rows():
    import datetime

    meses = {
        1: "ENERO", 2: "FEBRERO", 3: "MARZO", 4: "ABRIL",
        5: "MAYO", 6: "JUNIO", 7: "JULIO", 8: "AGOSTO",
        9: "SEPTIEMBRE", 10: "OCTUBRE", 11: "NOVIEMBRE", 12: "DICIEMBRE",
    }

    hoy = datetime.datetime.now()
    mes_actual = f"{meses[hoy.month]} {hoy.year}"

    limit = int(os.environ.get("DB_QUERY_LIMIT", "0") or "0")

    cnx = mysql_connect()
    cur = cnx.cursor(dictionary=True)

    q = """
    SELECT *
    FROM viewInventarioDisneylandia
    WHERE UPPER(Mes) = %s
      AND Cantidad_fisica > 0
    """
    params = [mes_actual]

    if limit and limit > 0:
        q += " LIMIT %s"
        params.append(limit)

    print("Mes filtro:", mes_actual)
    cur.execute(q, params)

    rows = cur.fetchall()
    cur.close()
    cnx.close()

    print("Rows fetched:", len(rows))
    return rows

def map_row(row: dict) -> dict:
    # Ajusta aquí si en la vista los nombres cambian (por ejemplo Codigo_bodega vs CodigoBodega)
    return {
        "mes": normalize_text(row.get("Mes")),
        "bodega_codigo": normalize_text(row.get("Codigo_bodega")),
        "bodega_nombre": normalize_text(row.get("Nombre_bodega")),
        "tipo_producto_codigo": normalize_text(row.get("Codigo_tipo_producto")),
        "tipo_producto_nombre": normalize_text(row.get("Nombre_tipo_producto")),
        "referencia": normalize_text(row.get("Referencia")),
        "nombre": normalize_text(row.get("Nombre_largo_producto")),
        "talla": normalize_text(row.get("Talla")),
        "color_raw": normalize_text(row.get("Color")),
        "cantidad_fisica": row.get("Cantidad_fisica"),
        "costo_promedio_unitario_local": row.get("Costo_promedio_unitario_local"),
        "costo_promedio_unitario_niif": row.get("Costo_promedio_unitario_niif"),
        "costo_promedio_unitario_total": row.get("Costo_promedio_unitario_total"),
        "updated_at": now_utc_iso(),
    }

def upsert_supabase(mapped_rows: list[dict]) -> None:
    supa_url = env("SUPABASE_URL").rstrip("/")
    url = f"{supa_url}/rest/v1/{TABLE}?on_conflict=mes,bodega_codigo,referencia,talla,color_raw"
    headers = supabase_headers()

    total = len(mapped_rows)
    print(f"Upsert Supabase: {total} rows (batch {BATCH_SIZE})")

    for i in range(0, total, BATCH_SIZE):
        batch = mapped_rows[i:i + BATCH_SIZE]
        resp = requests.post(url, headers=headers, json=batch, timeout=60)

        if resp.status_code not in (200, 201, 204):
            print("Supabase status:", resp.status_code)
            print("Supabase body (first 500):", (resp.text or "")[:500])
            raise RuntimeError("Falló el upsert a Supabase")

    print("Upsert OK")

def main():
    print("Sync start")
    rows = fetch_view_rows()

    mapped = []
    for r in rows:
        if r.get("Referencia") is None:
            continue
        mapped.append(map_row(r))

    if not mapped:
        print("No hay filas para sincronizar.")
        return

    upsert_supabase(mapped)
    print(f"Sync OK → {len(mapped)} filas")

if __name__ == "__main__":
    main()
