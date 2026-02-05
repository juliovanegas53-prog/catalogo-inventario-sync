import os
import requests
import mysql.connector
from datetime import datetime, timezone
from decimal import Decimal

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except Exception:
    ZoneInfo = None

BATCH_SIZE = 500


# ======================
# Helpers
# ======================
def env(name: str, default: str | None = None) -> str:
    v = os.environ.get(name, default)
    if v is None or str(v).strip() == "":
        raise RuntimeError(f"Falta variable de entorno: {name}")
    return str(v).strip()


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_text(v):
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


def json_safe(v):
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, datetime):
        return v.isoformat()
    return v


def month_label_es_colombia() -> str:
    meses = {
        1: "ENERO", 2: "FEBRERO", 3: "MARZO", 4: "ABRIL",
        5: "MAYO", 6: "JUNIO", 7: "JULIO", 8: "AGOSTO",
        9: "SEPTIEMBRE", 10: "OCTUBRE", 11: "NOVIEMBRE", 12: "DICIEMBRE",
    }
    if ZoneInfo:
        hoy = datetime.now(ZoneInfo("America/Bogota"))
    else:
        # fallback simple si zoneinfo no está
        hoy = datetime.utcnow()
    return f"{meses[hoy.month]} {hoy.year}"


def supabase_headers() -> dict:
    key = env("SUPABASE_SERVICE_ROLE_KEY")
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }


# ======================
# MySQL
# ======================
def mysql_connect():
    host = env("DB_HOST")
    port = int(env("DB_PORT"))
    db = env("DB_NAME")
    user = env("DB_USER")
    pwd = env("DB_PASSWORD")

    print("DB connect:", host, port, db, user)

    return mysql.connector.connect(
        host=host,
        port=port,
        database=db,
        user=user,
        password=pwd,
        ssl_disabled=True,          # SSL Mode=None
        connection_timeout=30,
    )


# ======================
# Fetch Inventario
# ======================
def fetch_inventario_rows():
    mes_actual = month_label_es_colombia()
    bodegas = (os.environ.get("BODEGAS_PERMITIDAS", "") or "").strip()
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

    if bodegas:
        b_list = [b.strip() for b in bodegas.split(",") if b.strip()]
        placeholders = ",".join(["%s"] * len(b_list))
        q += f" AND Codigo_bodega IN ({placeholders})"
        params.extend(b_list)

    if limit and limit > 0:
        q += " LIMIT %s"
        params.append(limit)

    print("Mes filtro inventario:", mes_actual)
    print("Bodegas filtro:", bodegas if bodegas else "todas")

    cur.execute(q, params)
    rows = cur.fetchall()

    cur.close()
    cnx.close()

    print("Inventario rows fetched:", len(rows))
    return rows


def map_inventario(row: dict) -> dict:
    talla = normalize_text(row.get("Talla")) or ""
    color_raw = normalize_text(row.get("Color")) or ""

    return {
        "mes": normalize_text(row.get("Mes")),
        "bodega_codigo": normalize_text(row.get("Codigo_bodega")),
        "bodega_nombre": normalize_text(row.get("Nombre_bodega")),
        "tipo_producto_codigo": normalize_text(row.get("Codigo_tipo_producto")),
        "tipo_producto_nombre": normalize_text(row.get("Nombre_tipo_producto")),
        "referencia": normalize_text(row.get("Referencia")),
        "nombre": normalize_text(row.get("Nombre_largo_producto")),
        "talla": talla,
        "color_raw": color_raw,
        "cantidad_fisica": json_safe(row.get("Cantidad_fisica")),
        "costo_promedio_unitario_local": json_safe(row.get("Costo_promedio_unitario_local")),
        "costo_promedio_unitario_niif": json_safe(row.get("Costo_promedio_unitario_niif")),
        "costo_promedio_unitario_total": json_safe(row.get("Costo_promedio_unitario_total")),
        "updated_at": now_utc_iso(),
    }


# ======================
# Fetch Productos + Precios Lista
# ======================
def fetch_productos_precios_rows():
    limit = int(os.environ.get("DB_QUERY_LIMIT_PRODUCTOS", "0") or "0")

    cnx = mysql_connect()
    cur = cnx.cursor(dictionary=True)

    q = """
    SELECT
      codigoAlternoProducto,
      nombreLargoProducto,
      codigoBarrasProducto,
      nombreTemporada,
      CodigoAlternoListaPrecio,
      PrecioListaPrecioDetalle
    FROM viewProductoListaPrecioDisneylandia
    WHERE TRIM(CodigoAlternoListaPrecio) IN ('01','1','001')
    """

    if limit and limit > 0:
        q += " LIMIT %s"
        cur.execute(q, (limit,))
    else:
        cur.execute(q)

    rows = cur.fetchall()

    cur.close()
    cnx.close()

    print("Productos/Precios rows fetched:", len(rows))
    return rows


def map_producto(row: dict) -> dict:
    return {
        "referencia": normalize_text(row.get("codigoAlternoProducto")),
        "nombre": normalize_text(row.get("nombreLargoProducto")),
        "codigo_barras": normalize_text(row.get("codigoBarrasProducto")),
        "temporada": normalize_text(row.get("nombreTemporada")),
        "updated_at": now_utc_iso(),
    }


def map_precio(row: dict) -> dict:
    # tolerante por si cambia el case
    precio = row.get("PrecioListaPrecioDetalle")
    if precio is None:
        precio = row.get("precioListaPrecioDetalle")

    lista = row.get("CodigoAlternoListaPrecio")
    if lista is None:
        lista = row.get("codigoAlternoListaPrecio")

    return {
        "referencia": normalize_text(row.get("codigoAlternoProducto")),
        "lista_codigo": (normalize_text(lista) or "").strip(),
        "precio": json_safe(precio),
        "updated_at": now_utc_iso(),
    }


# ======================
# Upsert Supabase
# ======================
def upsert_supabase(table: str, on_conflict: str, rows: list[dict]) -> None:
    if not rows:
        print(f"Upsert {table}: 0 rows (skip)")
        return

    supa_url = env("SUPABASE_URL").rstrip("/")
    url = f"{supa_url}/rest/v1/{table}?on_conflict={on_conflict}"
    headers = supabase_headers()

    total = len(rows)
    print(f"Upsert {table}: {total} rows (batch {BATCH_SIZE})")

    for i in range(0, total, BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        resp = requests.post(url, headers=headers, json=batch, timeout=90)

        if resp.status_code not in (200, 201, 204):
            print("Supabase status:", resp.status_code)
            print("Supabase body (first 800):", (resp.text or "")[:800])
            print("Table:", table)
            print("URL:", url)
            print("Sample row:", batch[0] if batch else None)
            raise RuntimeError(f"Falló el upsert a Supabase ({table})")

    print(f"Upsert OK ({table})")


# ======================
# Main
# ======================
def main():
    print("Sync start")

    # 1) Inventario
    inv_rows = fetch_inventario_rows()
    mapped_inv = []
    for r in inv_rows:
        if not r.get("Referencia"):
            continue
        mapped_inv.append(map_inventario(r))

    upsert_supabase(
    table="precios_lista",
    on_conflict="referencia,lista_codigo",
    rows=list(precios_by_key.values())
)

    # 2) Productos + precios lista
    prod_rows = fetch_productos_precios_rows()

    productos_by_ref = {}
   precios_by_key = {}
...
p = map_precio(r)
if p["referencia"] and p["lista_codigo"] and p["precio"] is not None:
    key = (p["referencia"], p["lista_codigo"])
    # Si hay duplicados, nos quedamos con el último (o el mayor, si prefieres)
    precios_by_key[key] = p

    for r in prod_rows:
        ref = normalize_text(r.get("codigoAlternoProducto"))
        if not ref:
            continue

        productos_by_ref[ref] = map_producto(r)

        p = map_precio(r)
        if p["referencia"] and p["lista_codigo"] and p["precio"] is not None:
            precios.append(p)

    upsert_supabase(
        table="productos",
        on_conflict="referencia",
        rows=list(productos_by_ref.values())
    )

    upsert_supabase(
        table="precios_lista",
        on_conflict="referencia,lista_codigo",
        rows=precios
    )

    print("Sync OK")


if __name__ == "__main__":
    main()
