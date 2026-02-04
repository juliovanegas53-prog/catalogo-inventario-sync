print("🟢 Archivo cargado")

import os
import requests
from datetime import datetime, timezone

def erp_login(session: requests.Session):
    print("🔐 Entrando a erp_login()")

    username = os.environ["ERP_USERNAME"]
    password = os.environ["ERP_PASSWORD"]

    payload = {
        "username": username,
        "password": password
    }

    print("➡️ Enviando POST a ERP_LOGIN_URL")

    try:
        r = session.post(
            ERP_LOGIN_URL,
            json=payload,
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "User-Agent": "Mozilla/5.0"
            },
            timeout=10,              # 👈 clave
            allow_redirects=False
        )
    except Exception as e:
        print("❌ Excepción en login:", repr(e))
        raise

    print("LOGIN status:", r.status_code)
    print("LOGIN content-type:", r.headers.get("content-type"))
    print("LOGIN headers:", dict(r.headers))
    print("LOGIN body (first 300):", (r.text or "")[:300])

    # Token JSON
    if r.status_code == 200 and "application/json" in (r.headers.get("content-type") or "").lower():
        data = r.json()
        token = data.get("access_token") or data.get("token") or data.get("jwt")
        if token:
            print("✅ Login por token")
            return {"mode": "token", "token": token}

    # Cookie de sesión
    if r.headers.get("set-cookie"):
        print("✅ Login por cookie")
        return {"mode": "cookie", "token": None}

    raise RuntimeError("Login no exitoso")


def fetch_erp_rows():
    print("🚀 fetch_erp_rows() iniciado")
    session = requests.Session()
    auth = erp_login(session)

    headers = {"Accept": "application/json"}
    if auth["mode"] == "token":
        headers["Authorization"] = f"Bearer {auth['token']}"

    print("➡️ Llamando ERP inventario...")

    r = session.get(
        ERP_URL,
        headers=headers,
        timeout=30,
        allow_redirects=False
    )

    print("ERP status:", r.status_code)
    print("ERP content-type:", r.headers.get("content-type"))

    if 300 <= r.status_code < 400:
        print("redirect:", r.headers.get("location"))
        raise RuntimeError("Sigue redirigiendo a login")

    if r.status_code != 200:
        print("body:", (r.text or "")[:300])
        r.raise_for_status()

    content_type = (r.headers.get("content-type") or "").lower()
    if "application/json" not in content_type:
        print("body:", (r.text or "")[:300])
        raise RuntimeError("Inventario no devolvió JSON")

    print("✅ Inventario recibido")
    data = r.json()
    return data.get("rows", [])
