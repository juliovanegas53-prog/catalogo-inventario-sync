import os
import requests
from datetime import datetime, timezone


# =========================
# Config
# =========================
ERP_URL = os.environ.get("ERP_URL", "").strip()
ERP_LOGIN_URL = os.environ.get("ERP_LOGIN_URL", "").strip()
SUPABASE_URL = os.environ.get("SUPABASE_URL", "").strip()
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()

TABLE = "inventario"
BATCH_SIZE = 500


print("🟢 Archivo cargado")


# =========================
# Helpers
# =========================
def require_env(name: str) -> str:
    val = os.environ.get(name, "").strip()
    if not val:
        raise RuntimeError(f"Falta variable de entorno: {name}")
    return val


def is_http_url(url: str) -> bool:
    return url.startswith("http://") or url.startswith("https://")


def supabase_headers() -> dict:
    key = require_env("SUPABASE_SERVICE_ROLE_KEY")
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }


def normalize_text(v) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# =========================
# ERP Auth
# =========================
def erp_login(session: requests.Session) -> dict:
    """
    Intenta autenticar en el ERP.
    Soporta:
      - Login que devuelve JSON con token (access_token/token/jwt)
      - Login que setea cookie de sesión (Set-Cookie)
    """
    login_url = require_env("ERP_LOGIN_URL")
    username = require_env("ERP_USERNAME")
    password = require_env("ERP_PASSWORD")

    if not is_http_url(login_url):
        raise RuntimeError(f"ERP_LOGIN_URL inválida: {login_url!r} (debe iniciar con http/https)")

    print("🔐 Entrando a erp_login()")
    print("➡️ Enviando POST a ERP_LOGIN_URL")

    # Probamos varios payloads típicos
    payloads = [
        {"username": username, "password": password},
        {"user": username, "pass": password},
        {"Usuario": username, "Clave": password},
        {"usuario": username, "contrasena": password},
        {"email": username, "password": password},
    ]

    last_resp = None

    for payload in payloads:
        try:
            r = session.post(
                login_url,
                json=payload,
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                    "User-Agent": "Mozilla/5.0",
                },
                timeout=30,
                allow_redirects=False,
            )
            last_resp = r
        except Exception as e:
            print("❌ Excepción en login:", repr(e))
            continue

        print("LOGIN status:", r.status_code)
        print("LOGIN content-type:", r.headers.get("content-type"))

        # Si setea cookie, ya nos sirve para el siguiente GET
        if r.headers.get("set-cookie"):
            print("✅ Login por cookie (Set-Cookie detectado)")
            return {"mode": "cookie", "token": None}

        # Si devuelve JSON, buscamos token
        ctype = (r.headers.get("content-type") or "").lower()
        if r.status_code == 200 and "application/json" in ctype:
            try:
                data = r.json()
            except Exception:
                data = {}

            token = data.get("access_token") or data.get("token") or data.get("jwt")
            if token:
                print("✅ Login por token")
                return {"mode": "token", "token": token}

        # Si redirige, es un login web (cookie/sesión), pero sin cookie no nos sirve aún
        if 300 <= r.status_code < 400:
            print("ℹ️ Login respondió con redirect, location:", r.headers.get("location"))

    # Si llegamos aquí, no autenticó
    if last_resp is not None:
        print("❌ LOGIN FALLÓ - diagnóstico")
        print("status:", last_resp.status_code)
        print("content-type:", last_resp.headers.get("content-type"))
        print("body (first 400):", (last_resp.text or "")[:400])

    raise RuntimeError("No se pudo autenticar en el ERP con los payloads probados (puede requerir CSRF).")


def fetch_erp_rows() -> list[dict]:
    erp_url = require_env("ERP_URL")

    if not is_http_url(erp_url):
        raise RuntimeError(f"ERP_URL inválida: {erp_url!r} (debe iniciar con http/https)")

    print("🚀 fetch_erp_rows() iniciado")

    session = requests.Session()
    auth = erp_login(session)

    headers = {"Accept": "application/json", "User-Agent": "Mozilla/5.0"}
    if auth["mode"] == "token" and auth.get("token"):
        headers["Authorization"] = f"Bearer {auth['token']}"

    print("➡️ Llamando ERP inventario...")

    r = session.get(
        erp_url,
        headers=headers,
        timeout=60,
        allow_redirects=False,
    )

    print("ERP status:", r.status_code)
    print("ERP content-type:", r.headers.get("content-type"))

    # Si redirige, seguimos sin sesión válida o el endpoint es web, no API
    if 300 <= r.status_code < 400:
        print("ERP redirect location:", r.headers.get("location"))
        raise RuntimeError("ERP redirige a login: falta cookie/token válido o el endpoint no es API.")

    if r.status_code != 200:
        print("ERP body (first 500):", (r.text or "")[:500])
        r.raise_for_status()

    ctype = (r.headers.get("content-type") or "").lower()
    if "application/json" not in ctype:
        print("ERP body (first 500):", (r.text or "")[:500])
        raise RuntimeError("El endpoint de inventario no devolvió JSON.")

    data = r.json()
    rows = data.get("rows", [])
    if not isinstance(rows, list):
        raise RuntimeError("La respuesta JSON no contiene 'rows' como lista.")

    print(f"✅ ERP rows recibidas: {len(rows)}")
    return rows


# =========================
# Mapping & Supabase upsert
# =========================
def map_row(row: dict) -> dict:
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
    supa_url = require_env("SUPABASE_URL")

    if not is_http_url(supa_url):
        raise RuntimeError(f"SUPABASE_URL inválida: {supa_url!r}")

    headers = supabase_headers()

    # Debe coincidir con tu índice unique (ajústalo si cambiaste columnas)
    on_conflict = "mes,bodega_codigo,referencia,talla,color_raw"

    url = f"{supa_url}/rest/v1/{TABLE}?on_conflict={on_conflict}"

    total = len(mapped_rows)
    print(f"⬆️ Upsert a Supabase: {total} filas (batch {BATCH_SIZE})")

    for i in range(0, total, BATCH_SIZE):
        batch = mapped_rows[i:i + BATCH_SIZE]
        resp = requests.post(url, headers=headers, json=batch, timeout=60)

        if resp.status_code not in (200, 201, 204):
            print("❌ Supabase error:", resp.status_code)
            print("body (first 500):", (resp.text or "")[:500])
            raise RuntimeError("Falló el upsert a Supabase.")

    print("✅ Upsert completado")


# =========================
# Main
# =========================
def main():
    print("🧪 main() arrancó")

    rows = fetch_erp_rows()

    mapped = [map_row(r) for r in rows if r.get("Referencia")]
    if not mapped:
        print("⚠️ No hay filas con Referencia. Nada que sincronizar.")
        return

    upsert_supabase(mapped)

    print(f"🎉 OK → {len(mapped)} filas sincronizadas")


if __name__ == "__main__":
    main()
