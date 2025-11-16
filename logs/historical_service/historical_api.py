import os
import json
from fastapi import FastAPI, HTTPException
from uvicorn import run
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# --- 1. CONFIGURATION & SPARK SESSION ---

# Lấy thông tin từ biến môi trường của docker-compose
SPARK_MASTER = "spark://spark-master:7077"
HDFS_NAMENODE = os.environ.get("SPARK_HADOOP_FS_DEFAULTFS", "hdfs://namenode:9000")

# Đường dẫn (giả định) tới thư mục data thô trên HDFS
# (Đây là nơi 'spark-jobs' của bạn nên đọc/ghi)
DATA_PATH = f"{HDFS_NAMENODE}/data/raw/home-credit-default-risk"

# Khởi tạo API
app = FastAPI(
    title="Historical Credit Data API",
    description="API để truy vấn dữ liệu batch (lịch sử) từ HDFS/Spark."
)

# Khởi tạo Spark Session (chỉ 1 lần khi API khởi động)
try:
    spark = SparkSession \
        .builder \
        .appName("HistoricalAPIServiceClient") \
        .master(SPARK_MASTER) \
        .getOrCreate()
    
    print(f"--- Đã kết nối thành công đến Spark Master tại {SPARK_MASTER} ---")
    
    # --- 2. TẢI VÀ CACHE DỮ LIỆU TẠI KHỞI ĐỘNG ---
    # Tải các file thô. 
    # .cache() sẽ giữ DataFrame này trong bộ nhớ của Spark workers
    # Điều này giúp API truy vấn cực kỳ nhanh mà không cần đọc lại file mỗi lần.
    print(f"Đang tải và cache dữ liệu thô từ: {DATA_PATH}")
    
    # (Bạn phải đảm bảo dữ liệu đã có trên HDFS ở đường dẫn này)
    # bureau_df = spark.read.csv(f"{DATA_PATH}/bureau.csv", header=True, inferSchema=True).cache()
    # installments_df = spark.read.csv(f"{DATA_PATH}/installments_payments.csv", header=True, inferSchema=True).cache()
    # app_train_df = spark.read.csv(f"{DATA_PATH}/application_train.csv", header=True, inferSchema=True).cache()
    
    # print(f"Đã tải và cache {bureau_df.count()} hàng từ bureau.csv")
    # print(f"Đã tải và cache {installments_df.count()} hàng từ installments_payments.csv")
    
    # (Tạm thời comment lại 3 dòng trên nếu bạn chưa upload data lên HDFS)
    # (Để API vẫn chạy được)
    print("--- Khởi động API thành công ---")

except Exception as e:
    print(f"LỖI NGHIÊM TRỌNG: Không thể kết nối Spark hoặc tải data: {e}")
    spark = None # Đặt là None để API báo lỗi

# --- 3. API ENDPOINTS ---

@app.get("/")
def read_root():
    if spark is None:
        return {"error": "Spark Session không khởi động được. Kiểm tra logs."}
    return {"message": "Historical Credit API (Spark/HDFS) đang chạy."}

@app.get("/api/bureau_history/{sk_id_curr}")
def get_customer_bureau_history(sk_id_curr: int):
    """
    Lấy toàn bộ lịch sử vay (bureau) của khách hàng.
    """
    if spark is None:
        raise HTTPException(status_code=500, detail="Spark Session không khởi động được.")
        
    print(f"Nhận yêu cầu (bureau) cho SK_ID_CURR: {sk_id_curr}")
    
    # (Bỏ comment 2 dòng dưới khi bạn đã tải data)
    # results = bureau_df.filter(F.col("SK_ID_CURR") == sk_id_curr).toJSON().collect()
    
    # if not results:
    #     raise HTTPException(status_code=404, detail="Không tìm thấy lịch sử bureau cho SK_ID_CURR này.")
    
    # # Chuyển list các chuỗi JSON thành list các dict
    # return [json.loads(row) for row in results]
    
    return {"message": "API Bureau (Demo)", "requested_id": sk_id_curr}

@app.get("/api/installments_history/{sk_id_curr}")
def get_customer_installments_history(sk_id_curr: int):
    """
    Lấy toàn bộ lịch sử trả góp (installments) của khách hàng.
    """
    if spark is None:
        raise HTTPException(status_code=500, detail="Spark Session không khởi động được.")
        
    print(f"Nhận yêu cầu (installments) cho SK_ID_CURR: {sk_id_curr}")

    # (Bỏ comment 2 dòng dưới khi bạn đã tải data)
    # results = installments_df.filter(F.col("SK_ID_CURR") == sk_id_curr).toJSON().collect()
    
    # if not results:
    #     raise HTTPException(status_code=404, detail="Không tìm thấy lịch sử trả góp cho SK_ID_CURR này.")
        
    # return [json.loads(row) for row in results]
    
    return {"message": "API Installments (Demo)", "requested_id": sk_id_curr}

# --- 4. CHẠY SERVER ---
if __name__ == "__main__":
    # Port 8001 là cổng được expose trong docker-compose
    run(app, host="0.0.0.0", port=8001)