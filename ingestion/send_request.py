# send_requests.py
import requests
import time

URL = "http://localhost:8000/api/loan-application"
FRAUD_RATE = 0.01      # tỉ lệ fraud bạn muốn
SLEEP = 0.05           # 0.05s ~ 20 req/s

i = 0
print(f"Start sending requests to {URL} (fraud_rate={FRAUD_RATE})")
print("Nhấn Ctrl+C để dừng.\n")

try:
    while True:
        try:
            resp = requests.post(
                URL,
                params={"fraud_rate": FRAUD_RATE},
                json={}
            )
            i += 1
            if i % 100 == 0:
                print(f"{i} requests sent, status = {resp.status_code}")
        except Exception as e:
            print(f"Error at {i}: {e}")

        time.sleep(SLEEP)
except KeyboardInterrupt:
    print(f"\nStopped by user. Total sent = {i}")
