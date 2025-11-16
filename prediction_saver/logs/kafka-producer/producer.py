import os
import time
import random
import json
from kafka import KafkaProducer

# --- 1. LẤY BIẾN MÔI TRƯỜNG TỪ DOCKER-COMPOSE ---
BOOTSTRAP_SERVERS = os.environ.get("BOOTSTRAP_SERVERS", "kafka:9092")
# Lấy tên topic mà docker-compose cung cấp
TARGET_TOPIC = os.environ.get("DAILY_TOPIC", "daily_prices") 
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL_SECONDS", 10)) # Tần suất gửi (giây)

# --- 2. KHỞI TẠO KAFKA PRODUCER ---
try:
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS.split(','), # Tách nếu có nhiều broker
        # Chuyển đổi giá trị thành bytes (JSON -> bytes)
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        # Cấu hình thử lại (retry) để đảm bảo kết nối ổn định
        retries=5,
        request_timeout_ms=30000
    )
    print(f"Đã kết nối thành công đến Kafka tại {BOOTSTRAP_SERVERS}")
    print(f"Sẽ gửi sự kiện vào topic: {TARGET_TOPIC}")

except Exception as e:
    print(f"LỖI: Không thể kết nối đến Kafka. Đang chờ 10s rồi thử lại...")
    print(e)
    time.sleep(10)
    # (Trong Docker, container sẽ tự khởi động lại nếu bị crash)
    raise e

# --- 3. KHỞI TẠO FAKER (Logic nghiệp vụ của bạn) ---
ACTIVE_CUSTOMER_IDS = [random.randint(100000, 500000) for _ in range(1000)]

def generate_payment_event():
    """Tạo một sự kiện 'Khách hàng vừa trả góp'."""
    customer_id = random.choice(ACTIVE_CUSTOMER_IDS)
    
    payment_amount = round(random.uniform(500.0, 5000.0), 2)
    days_late = random.choices([0, random.randint(1, 10)], weights=[0.8, 0.2], k=1)[0]
    
    event_data = {
        "event_type": "INSTALLMENT_PAID",
        "timestamp": time.time(),
        "sk_id_curr": customer_id,
        "payload": {
            "amt_payment": payment_amount,
            "days_late": days_late
        }
    }
    return event_data

# --- 4. VÒNG LẶP CHÍNH (Gửi sự kiện) ---
print("--- BẮT ĐẦU GỬI SỰ KIỆN TÍN DỤNG ---")
try:
    while True:
        # Chúng ta chỉ tạo sự kiện trả góp
        event_to_send = generate_payment_event()

        # Gửi vào topic 'daily_prices'
        producer.send(TARGET_TOPIC, value=event_to_send)
        
        print(f"\n[TIME: {time.ctime()}] Gửi sự kiện:")
        print(json.dumps(event_to_send, indent=2))
        
        producer.flush()

        # Chờ theo thời gian trong docker-compose
        print(f"--- (Chờ {POLL_INTERVAL} giây) ---")
        time.sleep(POLL_INTERVAL)

except KeyboardInterrupt:
    print("\n--- Đã dừng chương trình producer ---")
except Exception as e:
    print(f"LỖI TRONG VÒNG LẶP CHÍNH: {e}")
finally:
    producer.close()