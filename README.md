
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


