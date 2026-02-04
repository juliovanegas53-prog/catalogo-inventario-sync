import os
import requests
from datetime import datetime, timezone

ERP_URL = os.environ["ERP_URL"]
ERP_TOKEN = os.environ.get("ERP_TOKEN")
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_ROLE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
TABLE = "inventario"

def erp_headers():
    h = {"Accept": "application/json"}
    if ERP_TOKEN:
        h["Authorization"] = f"Bearer {ERP_TOKEN}"
    return h

def supabase_headers():
    return {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }

def fetch_erp_rows():
    r = requests.get(ERP_URL, headers=erp_headers(), timeout=60)
    r.raise_for_status()
    return r.json().get("rows", [])

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
