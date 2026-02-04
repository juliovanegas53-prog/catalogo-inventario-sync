import os
import requests

print("🟢 Script iniciado")

ERP_BASE_URL = os.environ["ERP_BASE_URL"].rstrip("/")
ERP_VIEW_NAME = os.environ["ERP_VIEW_NAME"]

ERP_USERNAME = os.environ["ERP_USERNAME"]
ERP_PASSWORD = os.environ["ERP_PASSWORD"]

def main():
    print("🧪 main() arrancó")

    # Construimos la URL final de la vista
    erp_url = f"{ERP_BASE_URL}/api/{ERP_VIEW_NAME}"
    print("➡️ ERP_URL final:", erp_url)

    # Sesión para mantener cookies
    session = requests.Session()

    # --- LOGIN (simple, solo para probar sesión) ---
    print("🔐 Intentando login (form)")
    login_resp = session.post(
        ERP_BASE_URL + "/login",
        data={
            "username": ERP_USERNAME,
            "password": ERP_PASSWORD
        },
        headers={
            "User-Agent": "Mozilla/5.0",
            "Content-Type": "application/x-www-form-urlencoded"
        },
        timeout=20,
        allow_redirects=True
    )

    print("LOGIN status:", login_resp.status_code)
    print("Cookies tras login:", session.cookies.get_dict())

    # --- LLAMADA A LA VISTA ---
    print("📦 Llamando vista de inventario...")
    r = session.get(
        erp_url,
        headers={
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0"
        },
        timeout=30,
        allow_redirects=False
    )

    print("ERP status:", r.status_code)
    print("ERP content-type:", r.headers.get("cont
