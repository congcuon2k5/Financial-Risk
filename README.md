# 🏦 Credit Risk Scoring Pipeline

![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=flat&logo=apachespark&logoColor=white) ![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?style=flat&logo=apachekafka&logoColor=white) ![HBase](https://img.shields.io/badge/Apache%20HBase-204178?style=flat&logo=apachehbase&logoColor=white) ![PostgreSQL](https://img.shields.io/badge/PostgreSQL-336791?style=flat&logo=postgresql&logoColor=white) ![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat&logo=docker&logoColor=white)

Hệ thống đánh giá rủi ro tín dụng (Credit Scoring) theo kiến trúc Lambda: xử lý lô (Batch) để huấn luyện mô hình và xử lý luồng (Streaming) để dự đoán thời gian thực.

---

## Mục lục
- [Yêu cầu hệ thống](#yêu-cầu-hệ-thống)
- [Quickstart](#quickstart)
- [Batch (Training)](#batch-training)
- [Realtime (Inference)](#realtime-inference)
- [Serving (Spring Boot)](#serving-spring-boot)
- [Troubleshooting & Tips](#troubleshooting--tips)
- [Liên hệ](#liên-hệ)

---

## Yêu cầu hệ thống

- Docker & Docker Compose
- Java 

## Quickstart

1. Build images và khởi chạy toàn bộ dịch vụ:

```powershell
docker-compose build
docker-compose up -d
docker ps
```

2. Kiểm tra các giao diện quản trị:

| Service | URL / Access | Chức năng |
| :--- | :--- | :--- |
| **Spark Master** | [http://localhost:8080](http://localhost:8080) | Quản lý Cluster & Job |
| **HDFS NameNode** | [http://localhost:9870](http://localhost:9870) | Quản lý File System |
| **Kafka UI** | [http://localhost:8083](http://localhost:8083) | Giao diện quản lí topic |
| **Spring Boot** | [http://localhost:8085](http://localhost:8085) | API Serving |
| **PostgreSQL** | [http://localhost:8082](http://localhost:8082)  | Database lưu kết quả batch |

## Batch (Training)

Quy trình: HDFS ➜ Spark ML (XGBoost) ➜ PostgreSQL

1) Upload dữ liệu lên HDFS:

```bash
# copy từ host vào container namenode
docker cp "Path_to_file/train.csv" namenode:/tmp/train.csv
docker cp "Path_to_file/test.csv" namenode:/tmp/test.csv

docker exec -it namenode bash
hdfs dfs -mkdir -p /data
hdfs dfs -put -f /tmp/train.csv /data/train.csv
hdfs dfs -put -f /tmp/test.csv  /data/test.csv
hdfs dfs -ls /data # Kiểm tra xem đã có data chưa
```

2) Chạy `train.py` trên Spark Master:

```bash
docker exec -it spark-master bash
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.pyspark.python=python3 \
  --conf spark.pyspark.driver.python=python3 \
  --conf spark.executor.memory=2g \
  --conf spark.driver.memory=2g \
  --conf spark.sql.shuffle.partitions=32 \
  --conf spark.task.cpus=1 \
  /opt/app/train.py
```

3) Kiểm tra kết quả trong PostgreSQL:

```bash
docker exec -it postgres bash
psql -U finuser -d finrisk
SELECT * FROM public.spark_train_scores LIMIT 20;
```

## Realtime (Inference)

### Ingestion (API gửi dữ liệu vào Kafka)

```bash
docker exec -it kafka bash -lc "kafka-topics --create --topic credit_applications --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1 || true"
docker-compose build ingestion-api
docker-compose up -d ingestion-api
```

Gửi yêu cầu mẫu (PowerShell):

```powershell
Invoke-RestMethod `
  -Uri "http://localhost:8000/api/loan-application?fraud_rate=0.01" `
  -Method Post `
  -ContentType "application/json" `
  -Body "{}"
```

Hoặc dùng script Python:

```bash
pip install requests
python send_request.py
```

### Streaming layer (Spark Structured Streaming)

1) Tạo topic output:

```bash
docker exec -it kafka bash -lc "kafka-topics --create --topic credit_scores --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1 || true"
```

2) (Nếu cần) Xóa checkpoint khi chạy lại streaming job:

```bash
docker exec -it namenode bash
hdfs dfs -rm -r /checkpoints/credit_scoring_v3 || true
```

3) Chạy `streaming_score.py` trên Spark Master (đảm bảo jar Kafka trong `/opt/spark/jars`):

```bash
docker exec -it spark-master bash
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.pyspark.python=python3 \
  --conf spark.pyspark.driver.python=python3 \
  --conf spark.executor.memory=3g \
  --conf spark.driver.memory=3g \
  --conf spark.executor.cores=1 \
  --conf spark.task.cpus=1 \
  --conf spark.sql.shuffle.partitions=6 \
  --conf spark.driver.extraClassPath=/opt/spark/jars/* \
  --conf spark.executor.extraClassPath=/opt/spark/jars/* \
  /opt/app/streaming_score.py
```

Kiểm tra Kafka UI: topic `credit_applications` (input) và `credit_scores` (output).

### HBase (lưu realtime scores)

```bash
docker exec -it hbase bash
hbase shell
create 'realtime_scores', 'score', 'meta'
scan 'realtime_scores', {LIMIT => 5}
```

## Serving (Spring Boot)

```bash
docker compose build spring-boot-api
docker compose up -d spring-boot-api
docker ps
```

Sử dụng UI cho user trên : `http://localhost:8085`


## Troubleshooting & Tips

- Nếu container không khởi động: xem logs `docker-compose logs <service>`.
- Spark không thấy Kafka jars: kiểm tra `/opt/spark/jars` trong container `spark-master`.
- Streaming không ra `credit_scores`: kiểm tra checkpoint, offsets và logs của Spark.
