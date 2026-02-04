import os
import requests

# ======================
# Configuración
# ======================
ERP_BASE_URL = os.environ["ERP_BASE_URL"].rstrip("/")
ERP_VIEW_NAME = os.environ["ERP_VIEW_NAME"]
ERP_USERNAME = os.environ["ERP_USERNAME"]
ERP_PASSWORD = os.environ["ERP_PASSWORD"]

print("Script loaded")

def main():
    print("Main started")

    erp_url = ERP_BASE_URL + "/api/" + ERP_VIEW_NAME
    print("ERP URL:", erp_url)

    session = requests.Session()

    # -------- LOGIN (FORM) --------
    login_url = ERP_BASE_URL + "/login"
    print("Login URL:", login_url)

    login_response = session.post(
        login_url,
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

    print("Login status:", login_response.status_code)
    print("Cookies after login:", session.cookies.get_dict())

    # -------- LLAMADA A LA VISTA --------
response = session.get(
    erp_url,
    headers={
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0"
    },
    timeout=30,
    allow_redirects=True
)

print("Final URL after redirects:", response.url)
print("Redirect history:", [h.status_code for h in response.history])

    if response.status_code != 200:
        print("ERP response (first 300 chars):")
        print(response.text[:300])
        raise RuntimeError("ERP did not return 200")

    print("ERP response OK")
    print("First 300 chars of response:")
    print(response.text[:300])

if __name__ == "__main__":
    main()
