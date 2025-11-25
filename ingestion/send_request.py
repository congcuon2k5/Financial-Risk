# send_requests.py (chạy từ host)
import requests
import time

URL = "http://localhost:8000/api/loan-application"

for i in range(10000):
    r = requests.post(URL, json={})
    if i % 100 == 0:
        print(i, r.status_code)
    time.sleep(0.01)  # 100 req/s
