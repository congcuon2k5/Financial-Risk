
# How to run

B1: Spark đọc dữ liệu train & test từ HDFS

B2: Spark ML/XGBoost train mô hình

B3: Mô hình dự đoán ra “pd_1” = xác suất vỡ nợ

B4: Kết quả này được Spark ghi vào PostgreSQL:
- public.spark_train_scores
- public.spark_test_scores
 
## Chạy bằng docker
- docker-compose build 
- docker-compose up -d
- docker ps: kiểm tra xem tất cả container đều chạy chưa

## Kiểm tra các local host
| Service       | UI                                             |
| ------------- | ---------------------------------------------- |
| Spark Master  | [http://localhost:8080](http://localhost:8080) |
| HDFS NameNode | [http://localhost:9870](http://localhost:9870) |
| PostgreSQL    | chạy bằng psql                                 |

## Tải data lên HDFS
- dùng file data chưa fill: test.csv, train.csv(kh dùng application_test_filled.csv và application_train_filled.csv)
- docker cp "Filled Data/train.csv" namenode:/tmp/train.csv
- docker cp "Filled Data/test.csv" namenode:/tmp/test.csv
- docker exec -it namenode bash
- hdfs dfs -mkdir -p /data
- hdfs dfs -put -f /tmp/train.csv /data/train.csv
- hdfs dfs -put -f /tmp/test.csv  /data/test.csv
- hdfs dfs -ls /data
- Kiểm tra trong file train.py , TRAIN_PATH và TEST_PATH phải đúng y chang trong đấy


## Chạy train.py trong container spark-Master
- docker exec -it spark-master bash
- spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.pyspark.python=python3 \
  --conf spark.pyspark.driver.python=python3 \
  --conf spark.executor.memory=2g \
  --conf spark.driver.memory=2g \
  --conf spark.sql.shuffle.partitions=32 \
    --conf spark.task.cpus=1 \
  /opt/app/train.py
- chạy xong nó sẽ lưu 3 model trên localhost 9870

## Kiểm tra kết quả trong PostgreSQL
- docker exec -it postgres bash
- psql -U finuser -d finrisk
- SELECT * FROM public.spark_train_scores LIMIT 20;


# Realtime

## ingestion
- docker exec -it kafka bash -lc "kafka-topics --create --topic credit_applications --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1 || true"
- docker-compose build ingestion-api
- docker-compose up -d ingestion-api
- Invoke-RestMethod `
   -Uri "http://localhost:8000/api/loan-application?fraud_rate=0.01" `
   -Method Post `
   -ContentType "application/json" `
   -Body "{}"
- pip install requests
- python send_requests.py

## Xây streaming_layer
- docker exec -it kafka bash -lc \
  "kafka-topics --create --topic credit_scores --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1 || true"
- docker exec -it namenode bash(dành cho docker-compose up -d lần 2)
- hdfs dfs -rm -r /checkpoints/credit_scoring_v3(dành cho docker-compose up -d lần 2)
- docker exec -it spark-master bash 
- ls /opt/spark/jars | grep kafka
  ls /opt/spark/jars | grep spark-sql-kafka
  ls /opt/spark/jars | grep token-provider
- /opt/spark/bin/spark-submit \
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
- Mở cmd trên máy tính và chạy: Invoke-WebRequest `
  -Uri "http://localhost:8000/api/loan-application?fraud_rate=0.01" `
  -Method Post `
  -ContentType "application/json" `
  -Body "{}"
- Trình duyệt: vào http://localhost:8083

  Check: Topic credit_applications:
  Thấy message JSON vào (SK_ID_CURR, AMT_CREDIT, …).


  Topic credit_scores: Đây là output của Spark streaming,
  Message dạng JSON có SK_ID_CURR, pd_1, ts.
  
  Nếu credit_applications có message mà credit_scores chưa có, đợi vài giây xem


  ## Hbase
- docker exec -it hbase bash
- hbase shell
- create 'realtime_scores', 'score', 'meta'
- docker exec -it spark-master bash -lc "
  cd /opt/app && \
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
- Invoke-WebRequest `
  -Uri "http://localhost:8000/api/loan-application?fraud_rate=0.01" `
  -Method Post `
  -ContentType "application/json" `
  -Body "{}"
- docker exec -it hbase bash
  hbase shell
  scan 'realtime_scores', {LIMIT => 5}

# Spring-boot-api
- docker compose build spring-boot-api
- docker compose up -d spring-boot-api
- docker ps  (thấy spring-boot-api Up, port 8085)
- Mở http://localhost:8085/api/health -> {"status":"OK"}
- http://localhost:8085/api/score/<SK_ID_CURR>