import os
import requests
from datetime import datetime, timezone

ERP_URL = os.environ["ERP_URL"]
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_ROLE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
TABLE = "inventario"


def supabase_headers():
    return {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }

def fetch_erp_rows():
    username = os.environ["ERP_USERNAME"]
    password = os.environ["ERP_PASSWORD"]

    r = requests.get(
        ERP_URL,
        headers={"Accept": "application/json"},
        auth=(username, password),
        timeout=60,
        allow_redirects=False,  # clave para ver si te redirige a login
    )

    print("ERP status:", r.status_code)
    print("ERP content-type:", r.headers.get("content-type"))

    # Si hay redirect, casi seguro te manda a login
    if 300 <= r.status_code < 400:
        print("ERP redirect location:", r.headers.get("location"))
        raise RuntimeError("ERP respondió con redirect (probable login). Revisa autenticación/endpoint.")

    # Si no es 200, imprime body corto
    if r.status_code != 200:
        print("ERP response (first 500 chars):", (r.text or "")[:500])
        r.raise_for_status()

    ctype = (r.headers.get("content-type") or "").lower()
    if "application/json" not in ctype:
        print("ERP response (first 500 chars):", (r.text or "")[:500])
        raise RuntimeError("ERP no devolvió JSON. Probable HTML/login o error del endpoint.")

    data = r.json()
    return data.get("rows", [])

def map_row(row):
    return {
        "mes": row.get("Mes"),
        "bodega_codigo": row.get("Codigo_bodega"),
        "bodega_nombre": row.get("Nombre_bodega"),
        "tipo_producto_codigo": row.get("Codigo_tipo_producto"),
        "tipo_producto_nombre": row.get("Nombre_tipo_producto"),
        "referencia": row.get("Referencia"),
        "nombre": row.get("Nombre_largo_producto"),
        "talla": row.get("Talla"),
        "color_raw": row.get("Color"),
        "cantidad_fisica": row.get("Cantidad_fisica"),
        "costo_promedio_unitario_local": row.get("Costo_promedio_unitario_local"),
        "costo_promedio_unitario_niif": row.get("Costo_promedio_unitario_niif"),
        "costo_promedio_unitario_total": row.get("Costo_promedio_unitario_total"),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }

def upsert_supabase(rows):
    url = f"{SUPABASE_URL}/rest/v1/{TABLE}?on_conflict=mes,bodega_codigo,referencia,talla,color_raw"
    headers = supabase_headers()

    batch_size = 500
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i+batch_size]
        r = requests.post(url, headers=headers, json=batch)
        if r.status_code not in (200, 201, 204):
            raise Exception(r.text)

def main():
    rows = fetch_erp_rows()
    mapped = [map_row(r) for r in rows if r.get("Referencia")]
    upsert_supabase(mapped)
    print(f"OK → {len(mapped)} filas sincronizadas")

if __name__ == "__main__":
    main()
