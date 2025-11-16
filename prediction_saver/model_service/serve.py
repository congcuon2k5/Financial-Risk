import os
import json
import redis
import threading
import joblib # Thư viện để load model .joblib
import pandas as pd
import requests # Thư viện để gọi API (historical_service)
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from uvicorn import run
from kafka import KafkaConsumer

# --- 1. CONFIG & KẾT NỐI ---
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
INPUT_TOPIC = os.environ.get("INPUT_TOPIC", "credit_events") 
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
MODEL_PATH = os.environ.get("MODEL_PATH", "/models/credit_model.joblib")
FEATURES_PATH = os.environ.get("FEATURES_PATH", "/models/model_features.json")
HISTORICAL_API_URL = "http://historical-service:8001" # API batch của bạn

# Khởi tạo API
app = FastAPI(title="Real-time Credit Scoring Service")

# Kết nối Redis
try:
    redis_client = redis.Redis(host=REDIS_HOST, port=6379, db=0, decode_responses=True)
    print(f"Đã kết nối Redis tại {REDIS_HOST}")
except Exception as e:
    print(f"LỖI: Không thể kết nối Redis: {e}")

# Biến toàn cục để giữ model và danh sách features
model = None
model_features = None

# --- 2. LOGIC TÍNH TOÁN ĐẶC TRƯNG REAL-TIME (BACKGROUND THREAD) ---
# (Phần này giống như code tôi đã gửi bạn ở lượt trước)

def calculate_features():
    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset='earliest',
        group_id='credit_feature_calculator'
    )
    print(f"Bắt đầu lắng nghe topic '{INPUT_TOPIC}' để tính đặc trưng...")

    for message in consumer:
        try:
            event = json.loads(message.value.decode('utf-8'))
            sk_id = str(event['sk_id_curr'])
            
            if event['event_type'] == 'INSTALLMENT_PAID':
                feature_key = f"credit_features:{sk_id}"
                try:
                    current_features_json = redis_client.get(feature_key)
                    current_features = json.loads(current_features_json) if current_features_json else {'total_payments': 0, 'total_paid_late': 0}
                except Exception:
                    current_features = {'total_payments': 0, 'total_paid_late': 0}

                # Tính toán đặc trưng mới
                current_features['total_payments'] += 1
                if event['payload']['days_late'] > 0:
                    current_features['total_paid_late'] += 1
                
                # Lưu (Ghi đè) đặc trưng mới vào Redis
                redis_client.set(feature_key, json.dumps(current_features))
                print(f"[CẬP NHẬT RT-FEATURE] ID: {sk_id} | Features: {current_features}")

        except Exception as e:
            print(f"LỖI xử lý message: {e}")

# --- 3. API DỰ ĐOÁN (PREDICTION) ---

# Định nghĩa dữ liệu đầu vào cho đơn vay mới
class NewLoanApplication(BaseModel):
    SK_ID_CURR: int
    AMT_INCOME_TOTAL: float
    AMT_CREDIT: float
    DAYS_BIRTH: int
    DAYS_EMPLOYED: int
    CODE_GENDER: str
    # ... Thêm TẤT CẢ các cột "tĩnh" (static) từ đơn vay vào đây
    # Ví dụ:
    # NAME_CONTRACT_TYPE: str
    # FLAG_OWN_CAR: str
    # ...

@app.on_event("startup")
async def startup_event():
    global model, model_features
    
    # 1. Khởi động background thread để tính feature
    print("Bắt đầu background thread (Feature Calculator)...")
    thread = threading.Thread(target=calculate_features, daemon=True)
    thread.start()
    
    # 2. Load model và danh sách features khi khởi động
    try:
        model = joblib.load(MODEL_PATH)
        with open(FEATURES_PATH) as f:
            model_features = json.load(f) # Danh sách các cột mà model cần
        print(f"--- ĐÃ LOAD THÀNH CÔNG MODEL từ {MODEL_PATH} ---")
    except Exception as e:
        print(f"LỖI NGHIÊM TRỌNG: Không thể load model hoặc file features: {e}")
        model = None

@app.get("/")
def read_root():
    return {"message": "Credit Scoring API (Calculator & Predictor) đang chạy."}

@app.post("/predict")
def predict_risk(application: NewLoanApplication):
    """
    API chính: Nhận đơn vay mới, gộp features và dự đoán rủi ro.
    """
    if model is None:
        raise HTTPException(status_code=503, detail="Model chưa được load. Kiểm tra logs.")

    sk_id = str(application.SK_ID_CURR)
    print(f"Nhận yêu cầu dự đoán cho SK_ID_CURR: {sk_id}")

    # 1. Lấy đặc trưng Tĩnh (Static Features)
    # (Từ chính đơn vay vừa gửi lên)
    static_features = application.dict()

    # 2. Lấy đặc trưng Real-time (RT Features)
    # (Từ Redis, do background thread tính)
    try:
        rt_features = json.loads(redis_client.get(f"credit_features:{sk_id}"))
    except:
        rt_features = {'total_payments': 0, 'total_paid_late': 0} # Mặc định nếu chưa có
    
    # 3. Lấy đặc trưng Batch (Batch Features)
    # (Từ HDFS/Spark, thông qua API 'historical_service')
    try:
        response = requests.get(f"{HISTORICAL_API_URL}/api/bureau_history/{sk_id}")
        if response.status_code == 200:
             # (Đây là ví dụ, bạn cần code agg/join các feature này)
             # Tạm thời chỉ đếm số lượng
            batch_features = {"bureau_total_loans_count": len(response.json())}
        else:
            batch_features = {"bureau_total_loans_count": 0}
    except Exception as e:
        print(f"Lỗi khi gọi historical-service: {e}")
        batch_features = {"bureau_total_loans_count": 0} # Mặc định

    # 4. Gộp tất cả Features
    # (Đây là bước quan trọng, cần đảm bảo bạn gộp đúng)
    all_features = {**static_features, **rt_features, **batch_features}
    
    # 5. Chuẩn bị DataFrame cho Model
    # Model cần DataFrame có thứ tự cột chính xác
    # 'model_features' là danh sách cột đã lưu từ file JSON
    try:
        # Tạo 1 hàng DataFrame, fill 0 cho các cột bị thiếu
        input_df = pd.DataFrame([all_features])
        input_df = input_df.reindex(columns=model_features, fill_value=0) 
        
        # 6. Dự đoán
        # [0][1] -> lấy xác suất của class 1 (vỡ nợ)
        probability_of_default = model.predict_proba(input_df)[0][1] 
        
        return {
            "sk_id_curr": sk_id,
            "risk_score": float(probability_of_default),
            "message": "Dự đoán thành công"
        }
        
    except Exception as e:
        print(f"LỖI khi dự đoán: {e}")
        raise HTTPException(status_code=500, detail=f"Lỗi khi xử lý model: {e}")

if __name__ == "__main__":
    run(app, host="0.0.0.0", port=8000)