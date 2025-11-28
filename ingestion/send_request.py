# send_requests.py
import requests
import time

URL = "http://localhost:8000/api/loan-application"
FRAUD_RATE = 0.01      # tỉ lệ fraud bạn muốn
TOTAL = 1000           # số request muốn gửi
SLEEP = 0.05           # delay giữa 2 request (giây) -> 0.05 = 20 req/s

for i in range(TOTAL):
    try:
        resp = requests.post(
            URL,
            params={"fraud_rate": FRAUD_RATE},
            json={}
        )
        if i % 100 == 0:
            print(f"{i} requests sent, status = {resp.status_code}")
    except Exception as e:
        print(f"Error at {i}: {e}")
    time.sleep(SLEEP)

print("DONE")
