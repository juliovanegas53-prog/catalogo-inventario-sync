def erp_login(session: requests.Session):
    print("🔐 Iniciando login ERP")

    username = os.environ["ERP_USERNAME"]
    password = os.environ["ERP_PASSWORD"]

    payloads = [
        {"username": username, "password": password},
        {"user": username, "pass": password},
        {"Usuario": username, "Clave": password},
        {"usuario": username, "contrasena": password},
        {"email": username, "password": password},
    ]

    last_resp = None

    for payload in payloads:
        r = session.post(
            ERP_LOGIN_URL,
            json=payload,
            headers={"Accept": "application/json"},
            timeout=30,
            allow_redirects=False
        )

        last_resp = r

        if 300 <= r.status_code < 400:
            print("ℹ️ Login por cookie (redirect detectado)")
            return {"mode": "cookie", "token": None}

        content_type = (r.headers.get("content-type") or "").lower()

        if r.status_code == 200 and "application/json" in content_type:
            data = r.json()
            token = data.get("access_token") or data.get("token") or data.get("jwt")

            if token:
                print("✅ Login por token")
                return {"mode": "token", "token": token}

            if r.headers.get("set-cookie"):
                print("✅ Login por cookie (JSON)")
                return {"mode": "cookie", "token": None}

        if r.status_code == 204 and r.headers.get("set-cookie"):
            print("✅ Login por cookie (204)")
            return {"mode": "cookie", "token": None}

    if last_resp is not None:
        print("❌ LOGIN FALLÓ")
        print("status:", last_resp.status_code)
        print("content-type:", last_resp.headers.get("content-type"))
        print("body:", (last_resp.text or "")[:300])

    raise RuntimeError("No se pudo autenticar en el ERP")


def fetch_erp_rows():
    print("🚀 Script iniciado")

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
