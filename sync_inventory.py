import os
import requests
from datetime import datetime, timezone

ERP_URL = os.environ["ERP_URL"]
ERP_LOGIN_URL = os.environ["ERP_LOGIN_URL"]
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

def erp_login(session: requests.Session):
    """
    Soporta dos estilos:
    1) Login que devuelve JSON con token (access_token / token / jwt)
    2) Login que devuelve cookie de sesión (Set-Cookie) y ya queda autenticado en el session
    """
    username = os.environ["ERP_USERNAME"]
    password = os.environ["ERP_PASSWORD"]

    # Intento 1: JSON login típico
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
                ERP_LOGIN_URL,
                json=payload,
                headers={"Accept": "application/json"},
                timeout=60,
                allow_redirects=False
            )
            last_resp = r
        except Exception:
            continue

        # Si redirige a /saya/ o similar, probablemente es login tipo web -> cookie
        if 300 <= r.status_code < 400:
            # seguimos con sesión/cookies si las hay
            return {"mode": "cookie", "token": None}

        # Si devuelve JSON con token
        ctype = (r.headers.get("content-type") or "").lower()
        if r.status_code == 200 and "application/json" in ctype:
            data = r.json()
            token = data.get("access_token") or data.get("token") or data.get("jwt")
            if token:
                return {"mode": "token", "token": token}

            # Si no hay token, pero el login fue OK y setea cookie
            if r.headers.get("set-cookie"):
                return {"mode": "cookie", "token": None}

        # Si devuelve 204 a veces significa “OK sin body”
        if r.status_code == 204 and r.headers.get("set-cookie"):
            return {"mode": "cookie", "token": None}

    # Si nada funcionó, imprime diagnóstico útil
    if last_resp is not None:
        print("LOGIN status:", last_resp.status_code)
        print("LOGIN content-type:", last_resp.headers.get("content-type"))
        print("LOGIN body (first 400):", (last_resp.text or "")[:400])

    raise RuntimeError("No se pudo autenticar en el ERP. El login endpoint/payload no coincide o requiere CSRF.")

def fetch_erp_rows():
    session = requests.Session()

    auth = erp_login(session)
    headers = {"Accept": "application/json"}

    if auth["mode"] == "token" and auth["token"]:
        headers["Authorization"] = f"Bearer {auth['token']}"

    r = session.get(
        ERP_URL,
        headers=headers,
        timeout=60,
        allow_redirects=False
    )

    print("ERP status:", r.status_code)
    print("ERP content-type:", r.headers.get("content-type"))

    if 300 <= r.status_code < 400:
        print("ERP redirect location:", r.headers.get("location"))
        raise RuntimeError("Aún redirige a login: faltan cookies/token o el endpoint no es el de API.")

    if r.status_code != 200:
        print("ERP body (first 500):", (r.text or "")[:500])
        r.raise_for_status()

    ctype = (r.headers.get("content-type") or "").lower()
    if "application/json" not in ctype:
        print("ERP body (first 500):", (r.text or "")[:500])
        raise RuntimeError("El endpoint de inventario no está devolviendo JSON.")

    data = r.json()
    return data.get("rows", [])
