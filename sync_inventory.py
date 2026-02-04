import os
import requests

print("Script loaded")

ERP_BASE_URL = os.environ["ERP_BASE_URL"].rstrip("/")
ERP_VIEW_NAME = os.environ["ERP_VIEW_NAME"]

def main():
    print("Main started")

    erp_url = ERP_BASE_URL + "/api/" + ERP_VIEW_NAME
    print("ERP URL:", erp_url)

    response = requests.get(
        erp_url,
        headers={
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0"
        },
        timeout=30,
        allow_redirects=True
    )

    print("Final URL:", response.url)
    print("Status:", response.status_code)
    print("Content-Type:", response.headers.get("content-type"))

    print("First 200 chars of response:")
    print(response.text[:200])

if __name__ == "__main__":
    main()
