# /opt/app/streaming_score.py
import os
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, LongType, StringType, DoubleType
)
from pyspark.ml.pipeline import PipelineModel
from pyspark.ml.functions import vector_to_array
import random

# ==== CONFIG ====
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
SRC_TOPIC       = os.getenv("SRC_TOPIC", "credit_applications")
SINK_TOPIC      = os.getenv("SINK_TOPIC", "credit_scores")

CHECKPOINT_DIR  = os.getenv(
    "CHECKPOINT_DIR",
    "hdfs://namenode:9000/checkpoints/credit_scoring_v3"
)

JDBC_URL  = os.getenv("JDBC_URL", "jdbc:postgresql://postgres:5432/finrisk")
JDBC_USER = os.getenv("JDBC_USER", "finuser")
JDBC_PASS = os.getenv("JDBC_PASS", "finpass")
JDBC_DRV  = "org.postgresql.Driver"
PG_TABLE  = os.getenv("PG_TABLE", "public.realtime_scores")

MODEL_PATH = os.getenv(
    "MODEL_PATH",
    "hdfs://namenode:9000/opt/model/xgboost_spark_model"
)

ID_COL = "SK_ID_CURR"

# ==== SPARK SESSION ====
spark = (
    SparkSession.builder
    .appName("streaming-realtime-scoring")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print(f"[stream] Using model: {MODEL_PATH}")
print(f"[stream] Kafka src={SRC_TOPIC}, sink={SINK_TOPIC}")
print(f"[stream] Checkpoint dir={CHECKPOINT_DIR}")
print(f"[stream] JDBC URL={JDBC_URL}, table={PG_TABLE}")

# ==== LOAD PIPELINE MODEL FROM HDFS ====
model = PipelineModel.load(MODEL_PATH)

# ==== SCHEMA JSON (raw input) ====
event_schema = StructType([
    StructField("SK_ID_CURR", LongType(), True),
    StructField("NAME_CONTRACT_TYPE", StringType(), True),
    StructField("CODE_GENDER", StringType(), True),
    StructField("FLAG_OWN_CAR", StringType(), True),
    StructField("FLAG_OWN_REALTY", StringType(), True),
    StructField("AMT_INCOME_TOTAL", DoubleType(), True),
    StructField("AMT_CREDIT", DoubleType(), True),
    StructField("AMT_ANNUITY", DoubleType(), True),
    StructField("AMT_GOODS_PRICE", DoubleType(), True),
    StructField("DAYS_BIRTH", DoubleType(), True),
    StructField("DAYS_EMPLOYED", DoubleType(), True),
    StructField("NAME_EDUCATION_TYPE", StringType(), True),
    StructField("NAME_FAMILY_STATUS", StringType(), True),
    StructField("NAME_HOUSING_TYPE", StringType(), True),
    StructField("ORGANIZATION_TYPE", StringType(), True),
    StructField("EXT_SOURCE_1", DoubleType(), True),
    StructField("EXT_SOURCE_2", DoubleType(), True),
    StructField("EXT_SOURCE_3", DoubleType(), True),
    # nếu pipeline train cần thêm cột nào thì bổ sung ở đây
])

# ==== READ STREAM FROM KAFKA ====
raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", SRC_TOPIC)
    .option("startingOffsets", "latest")
    .option("maxOffsetsPerTrigger", "1000")
    .load()
)

parsed = (
    raw.selectExpr("CAST(key AS STRING) AS key", "CAST(value AS STRING) AS json")
    .select(F.from_json("json", event_schema).alias("data"))
    .select("data.*")
)
cols_with_default = {
    "CNT_CHILDREN": F.floor(F.rand(seed=42) * 4).cast("int"), 
    "REGION_POPULATION_RELATIVE": (F.rand(seed=42) * 0.05) + 0.001,
    "DAYS_REGISTRATION": -F.floor(F.rand(seed=42) * 4001).cast("int"),
    "DAYS_ID_PUBLISH": -F.floor(F.rand(seed=42) * 2000).cast("int"),
    "FLAG_MOBIL": F.floor(F.rand(seed=42) * 2).cast("int"),
    "FLAG_EMP_PHONE": F.floor(F.rand(seed=42) * 2).cast("int"),
    "FLAG_WORK_PHONE": F.floor(F.rand(seed=42) * 2).cast("int"),
    "FLAG_CONT_MOBILE": F.floor(F.rand(seed=42) * 2).cast("int"),
    "FLAG_PHONE": F.floor(F.rand(seed=42) * 2).cast("int"),
    "FLAG_EMAIL": F.floor(F.rand(seed=42) * 2).cast("int"),
    "CNT_FAM_MEMBERS": F.floor(F.rand(seed=42) * 5 + 1).cast("int"),
    "REGION_RATING_CLIENT": F.floor(F.rand(seed=42) * 3 + 1).cast("int"),
    "REGION_RATING_CLIENT_W_CITY": F.floor(F.rand(seed=42) * 4 + 1).cast("int"),
    "HOUR_APPR_PROCESS_START": F.floor(F.rand(seed=42) * 20).cast("int"),
    "REG_REGION_NOT_LIVE_REGION": F.floor(F.rand(seed=42) * 2).cast("int"),
    "REG_REGION_NOT_WORK_REGION": F.floor(F.rand(seed=42) * 2).cast("int"),
    "LIVE_REGION_NOT_WORK_REGION": F.floor(F.rand(seed=42) * 2).cast("int"),
    "REG_CITY_NOT_LIVE_CITY": F.floor(F.rand(seed=42) * 2).cast("int"),
    "REG_CITY_NOT_WORK_CITY": F.floor(F.rand(seed=42) * 2).cast("int"),
    "LIVE_CITY_NOT_WORK_CITY": F.floor(F.rand(seed=42) * 2).cast("int"),
    "OBS_30_CNT_SOCIAL_CIRCLE": F.floor(F.rand(seed=42) * 10).cast("int"),
    "DEF_30_CNT_SOCIAL_CIRCLE": F.floor(F.rand(seed=42) * 5).cast("int"),
    "OBS_60_CNT_SOCIAL_CIRCLE": F.floor(F.rand(seed=42) * 10).cast("int"),
    "DEF_60_CNT_SOCIAL_CIRCLE": F.floor(F.rand(seed=42) * 5).cast("int"),
    "DAYS_LAST_PHONE_CHANGE": -F.floor(F.rand(seed=42) * 1001).cast("int"),
    "AMT_REQ_CREDIT_BUREAU_HOUR": F.floor(F.rand(seed=42) * 2).cast("int"),
    "AMT_REQ_CREDIT_BUREAU_DAY": F.floor(F.rand(seed=42) * 5).cast("int"),
    "AMT_REQ_CREDIT_BUREAU_WEEK": F.floor(F.rand(seed=42) * 3).cast("int"),
    "AMT_REQ_CREDIT_BUREAU_MON": F.floor(F.rand(seed=42) * 10).cast("int"),
    "AMT_REQ_CREDIT_BUREAU_QRT": F.floor(F.rand(seed=42) * 7).cast("int"),
    "AMT_REQ_CREDIT_BUREAU_YEAR": F.floor(F.rand(seed=42) * 20).cast("int"),
    "bureau_total_loans_count": F.floor(F.rand(seed=42) * 20).cast("int"),
    "bureau_DAYS_CREDIT_mean":  -F.floor(F.rand(seed=42) * 2000).cast("int"),
    "bureau_DAYS_CREDIT_min": -F.floor(F.rand(seed=42) * 3001).cast("int"),
    "bureau_DAYS_CREDIT_max": -F.floor(F.rand(seed=42) * 100).cast("int"),
    "bureau_AMT_CREDIT_SUM_OVERDUE_sum": F.lit(0).cast("double"),
    "bureau_AMT_CREDIT_SUM_DEBT_sum":F.floor(F.rand(seed=42) * 2000000).cast("double"),
    "bureau_CREDIT_ACTIVE_Active_sum": F.floor(F.rand(seed=42) * 10).cast("int"),
    "bureau_CREDIT_ACTIVE_Closed_sum": F.floor(F.rand(seed=42) * 10).cast("int"),
    "bureau_CREDIT_TYPE_Consumer credit_sum": F.floor(F.rand(seed=42) * 15).cast("int"),
    "bureau_CREDIT_TYPE_Credit card_sum": F.floor(F.rand(seed=42) * 10).cast("int"),
    "bureau_AMT_CREDIT_SUM_sum": F.floor(F.rand(seed=42) * 5000000).cast("double"),
    "bureau_CREDIT_TYPE_Consumer_credit_sum": F.floor(F.rand(seed=42) * 15).cast("int"),
    "bureau_CREDIT_TYPE_Credit_card_sum": F.floor(F.rand(seed=42) * 10).cast("int"),
    "bureau_debt_over_credit_ratio":F.floor(F.rand(seed=42) * 2).cast("double"),
    "bureau_active_loans_ratio":F.floor(F.rand(seed=42) * 2).cast("double"),
    "bureau_bal_SK_ID_BUREAU_count": F.floor(F.rand(seed=42) * 25).cast("int"),
    "bureau_bal_bb_MONTHS_BALANCE_min_mean": -F.floor(F.rand() * 100).cast("double"),
    "NAME_TYPE_SUITE_idx": F.floor(F.rand(seed=42) * 5).cast("int"),
    "NAME_INCOME_TYPE_idx": F.floor(F.rand(seed=42) * 6).cast("int"),
    "OCCUPATION_TYPE_idx": F.floor(F.rand(seed=42) * 11).cast("int"),
    "WEEKDAY_APPR_PROCESS_START_idx": F.floor(F.rand(seed=42) * 7).cast("int"),
    # nếu sau này log báo thiếu thêm cột nào,

    # bạn bổ sung thêm vào dict này, ví dụ:
    # "DAYS_ID_PUBLISH": F.lit(0.0).cast("double"),
}

for col_name, expr in cols_with_default.items():
    if col_name not in parsed.columns:
        parsed = parsed.withColumn(col_name, expr)

extra_feature_cols = [
    # === bureau balance / bb ===
    "bureau_bal_bb_MONTHS_BALANCE_min_min",
    "bureau_bal_bb_MONTHS_BALANCE_min_max",
    "bureau_bal_bb_MONTHS_BALANCE_max_mean",
    "bureau_bal_bb_MONTHS_BALANCE_max_min",
    "bureau_bal_bb_MONTHS_BALANCE_max_max",
    "bureau_bal_bb_MONTHS_BALANCE_count_mean",
    "bureau_bal_bb_MONTHS_BALANCE_count_min",
    "bureau_bal_bb_MONTHS_BALANCE_count_max",
    "bureau_bal_bb_STATUS_0_sum_mean",
    "bureau_bal_bb_STATUS_0_sum_min",
    "bureau_bal_bb_STATUS_0_sum_max",
    "bureau_bal_bb_STATUS_1_sum_mean",
    "bureau_bal_bb_STATUS_1_sum_min",
    "bureau_bal_bb_STATUS_1_sum_max",
    "bureau_bal_bb_STATUS_2_sum_mean",
    "bureau_bal_bb_STATUS_2_sum_min",
    "bureau_bal_bb_STATUS_2_sum_max",
    "bureau_bal_bb_STATUS_3_sum_mean",
    "bureau_bal_bb_STATUS_3_sum_min",
    "bureau_bal_bb_STATUS_3_sum_max",
    "bureau_bal_bb_STATUS_4_sum_mean",
    "bureau_bal_bb_STATUS_4_sum_min",
    "bureau_bal_bb_STATUS_4_sum_max",
    "bureau_bal_bb_STATUS_5_sum_mean",
    "bureau_bal_bb_STATUS_5_sum_min",
    "bureau_bal_bb_STATUS_5_sum_max",
    "bureau_bal_bb_STATUS_C_sum_mean",
    "bureau_bal_bb_STATUS_C_sum_min",
    "bureau_bal_bb_STATUS_C_sum_max",
    "bureau_bal_bb_STATUS_X_sum_mean",
    "bureau_bal_bb_STATUS_X_sum_min",
    "bureau_bal_bb_STATUS_X_sum_max",

    # === prev_app ===
    "prev_app_SK_ID_PREV_count",
    "prev_app_AMT_ANNUITY_mean",
    "prev_app_AMT_ANNUITY_max",
    "prev_app_AMT_ANNUITY_min",
    "prev_app_AMT_APPLICATION_mean",
    "prev_app_AMT_APPLICATION_max",
    "prev_app_AMT_APPLICATION_min",
    "prev_app_AMT_CREDIT_mean",
    "prev_app_AMT_CREDIT_max",
    "prev_app_AMT_CREDIT_min",
    "prev_app_DAYS_DECISION_mean",
    "prev_app_DAYS_DECISION_max",
    "prev_app_DAYS_DECISION_min",
    "prev_app_CNT_PAYMENT_mean",
    "prev_app_CNT_PAYMENT_sum",
    "prev_app_prev_app_credit_perc_mean",
    "prev_app_prev_app_credit_perc_max",
    "prev_app_prev_app_credit_perc_min",
    "prev_app_NAME_CONTRACT_STATUS_Approved_sum",
    "prev_app_NAME_CONTRACT_STATUS_Refused_sum",
    "prev_app_NAME_CONTRACT_STATUS_Canceled_sum",

    # === install ===
    "install_SK_ID_PREV_nunique",
    "install_payment_diff_mean",
    "install_payment_diff_max",
    "install_payment_diff_sum",
    "install_days_late_mean",
    "install_days_late_max",
    "install_days_late_sum",
    "install_flag_paid_late_mean",
    "install_flag_paid_late_sum",
    "install_flag_paid_under_mean",
    "install_flag_paid_under_sum",
    "install_AMT_PAYMENT_mean",
    "install_AMT_PAYMENT_sum",
    "install_AMT_PAYMENT_min",
    "install_AMT_PAYMENT_max",
    "install_AMT_INSTALMENT_mean",
    "install_AMT_INSTALMENT_sum",
    "install_AMT_INSTALMENT_min",
    "install_AMT_INSTALMENT_max",

    # === pos_cash ===
    "pos_cash_SK_ID_PREV_nunique",
    "pos_cash_MONTHS_BALANCE_min",
    "pos_cash_MONTHS_BALANCE_max",
    "pos_cash_MONTHS_BALANCE_count",
    "pos_cash_SK_DPD_mean",
    "pos_cash_SK_DPD_max",
    "pos_cash_SK_DPD_sum",
    "pos_cash_SK_DPD_DEF_mean",
    "pos_cash_SK_DPD_DEF_max",
    "pos_cash_SK_DPD_DEF_sum",
    "pos_cash_NAME_CONTRACT_STATUS_Active_sum",
    "pos_cash_NAME_CONTRACT_STATUS_Completed_sum",
    "pos_cash_NAME_CONTRACT_STATUS_Signed_sum",

    # === cc_bal ===
    "cc_bal_SK_ID_PREV_nunique",
    "cc_bal_MONTHS_BALANCE_min",
    "cc_bal_MONTHS_BALANCE_max",
    "cc_bal_MONTHS_BALANCE_count",
    "cc_bal_AMT_BALANCE_mean",
    "cc_bal_AMT_BALANCE_max",
    "cc_bal_AMT_BALANCE_min",
    "cc_bal_AMT_CREDIT_LIMIT_ACTUAL_mean",
    "cc_bal_AMT_CREDIT_LIMIT_ACTUAL_max",
    "cc_bal_AMT_DRAWINGS_CURRENT_mean",
    "cc_bal_AMT_DRAWINGS_CURRENT_max",
    "cc_bal_AMT_DRAWINGS_CURRENT_sum",
    "cc_bal_AMT_PAYMENT_TOTAL_CURRENT_mean",
    "cc_bal_AMT_PAYMENT_TOTAL_CURRENT_max",
    "cc_bal_AMT_PAYMENT_TOTAL_CURRENT_sum",
    "cc_bal_SK_DPD_mean",
    "cc_bal_SK_DPD_max",
    "cc_bal_SK_DPD_sum",
]
extra_feature_cols = [c for c in extra_feature_cols if c not in cols_with_default]

for col_name in extra_feature_cols:
    if col_name in parsed.columns:
        continue

    if "MONTHS_BALANCE" in col_name:
        expr = (-F.floor(F.rand(seed=42) * 60)).cast("double")  # 0..-59

    elif "DAYS_" in col_name:
        expr = (-F.floor(F.rand(seed=42) * 4000)).cast("double")  # 0..-3999

    elif "ratio" in col_name or "perc" in col_name:
        expr = F.rand(seed=42).cast("double")  # 0..1

    elif "count" in col_name or "nunique" in col_name:
        expr = F.floor(F.rand(seed=42) * 50).cast("double")

    elif "sum" in col_name:
        expr = (F.rand(seed=42) * 10000).cast("double")

    elif "mean" in col_name or "max" in col_name or "min" in col_name:
        expr = (F.rand(seed=42) * 1000).cast("double")

    else:
        expr = (F.rand(seed=42) * 10).cast("double")

    parsed = parsed.withColumn(col_name, expr)

# ==== SCORING ====
scored_full = model.transform(parsed)

scored = (
    scored_full
    .withColumn("pd_1", vector_to_array(F.col("probability"))[1])
    .select(
        F.col(ID_COL).alias("sk_id_curr"),
        "pd_1",
    )
    .withColumn("ts", F.current_timestamp())
)

TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "15 seconds")
# ==== SINK 1: POSTGRES (foreachBatch) ====
def write_to_postgres(df, epoch_id: int):
    if df.rdd.isEmpty():
        return
    (
        df.write.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", PG_TABLE)
        .option("user", JDBC_USER)
        .option("password", JDBC_PASS)
        .option("driver", JDBC_DRV)
        .mode("append")
        .save()
    )
    print(f"[stream] Batch {epoch_id} -> Postgres rows={df.count()}")


q_pg = (
    scored.writeStream
    .outputMode("append")
    .foreachBatch(write_to_postgres)
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/pg")
    .trigger(processingTime=TRIGGER_INTERVAL)
    .start()
)

# ==== SINK 2: KAFKA credit_scores ====
to_kafka = scored.select(
    F.col("sk_id_curr").cast("string").alias("key"),
    F.to_json(F.struct("sk_id_curr", "pd_1", "ts")).alias("value"),
)

q_kafka = (
    to_kafka.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("topic", SINK_TOPIC)
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/kafka")
    .outputMode("append")
    .trigger(processingTime=TRIGGER_INTERVAL)
    .start()
)

print("[stream] Streaming started, awaiting termination...")
spark.streams.awaitAnyTermination()

#thieu spark-token-provider-kafka-0-10_2.12-3.4.1.jar và dependency khác như commons-pool2 (được dùng bởi Kafka connector).