#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, re
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, NumericType, StringType

from pyspark.ml.functions import vector_to_array

from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    StringIndexer,
    OneHotEncoder,
    VectorAssembler,
    Imputer,
    VectorIndexer,
)

from pyspark.ml.classification import (
    LogisticRegression,
    RandomForestClassifier,
)

from pyspark.ml.evaluation import BinaryClassificationEvaluator

# XGBoost4J-PySpark API (snake_case params)
from xgboost.spark import SparkXGBClassifier


# ============================== PATHS & CONST ===============================
HDFS_DEFAULT = "hdfs://namenode:9000"
TRAIN_PATH = f"{HDFS_DEFAULT}/data/train.csv"
TEST_PATH  = f"{HDFS_DEFAULT}/data/test.csv"

ID_COL    = "SK_ID_CURR"
LABEL_COL = "TARGET"

MODEL_DIR = "/opt/model"
os.makedirs(MODEL_DIR, exist_ok=True)

# ============================== JDBC POSTGRES ===============================
JDBC_URL = "jdbc:postgresql://postgres:5432/finrisk"
JDBC_PROPS = {
    "driver": "org.postgresql.Driver",
    "user": "finuser",
    "password": "finpass",
}

# JARs (nếu tồn tại thì add tự động)
XGB_JAR_1 = "/opt/spark/jars/xgboost4j_2.12-1.7.6.jar"
XGB_JAR_2 = "/opt/spark/jars/xgboost4j-spark_2.12-1.7.6.jar"
PG_JDBC   = "/opt/spark/jars/postgresql-42.7.1.jar"
extra_jars = [p for p in (XGB_JAR_1, XGB_JAR_2, PG_JDBC) if os.path.exists(p)]
spark_jars = ",".join(extra_jars) if extra_jars else None

# ============================== BUILD SPARK SESSION =========================
builder = (
    SparkSession.builder
    .appName("train_logistic_rf_xgboost_sparkml")
    .config("spark.hadoop.fs.defaultFS", HDFS_DEFAULT)
    .config("spark.sql.shuffle.partitions", "64")        # giảm shuffle default (200)
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
)
if spark_jars:
    builder = builder.config("spark.jars", spark_jars)

spark = builder.getOrCreate()

print("\n>>> Spark version:", spark.version)
print(">>> Extra JARs present:", spark_jars if spark_jars else "(none)")

# ============================== READ DATA ===================================
print("\n>>> Reading CSV from HDFS ...")
train = spark.read.option("header", True).option("inferSchema", True).csv(TRAIN_PATH)
test  = spark.read.option("header", True).option("inferSchema", True).csv(TEST_PATH)

print(f">>> Train rows: {train.count()} | Test rows: {test.count()}")

# Bỏ hàng không có label (nếu có)
train = train.filter(F.col(LABEL_COL).isNotNull())
# Label → double
train = train.withColumn(LABEL_COL, F.col(LABEL_COL).cast(DoubleType()))

# ============================== SANITIZE COL NAMES ==========================
# (tránh khoảng trắng/ký tự lạ gây lỗi trong pipeline)
def sanitize_cols(df, keep=set([ID_COL, LABEL_COL])):
    cols = df.columns
    mapping = {}
    used = set(cols)
    for c in cols:
        if c in keep:
            mapping[c] = c
            continue
        s = re.sub(r'[^0-9A-Za-z_]', '_', c)
        s = re.sub(r'_+', '_', s).strip('_')
        if not s:
            s = "col"
        orig_s = s
        i = 1
        while s in mapping.values() or s in keep:
            s = f"{orig_s}_{i}"
            i += 1
        mapping[c] = s
    df2 = df
    for old, new in mapping.items():
        if old != new:
            df2 = df2.withColumnRenamed(old, new)
    return df2, mapping

train, mapping = sanitize_cols(train)
# áp dụng mapping giống hệt cho test
for old, new in mapping.items():
    if old != new and old in test.columns:
        test = test.withColumnRenamed(old, new)

# ============================== DETECT SCHEMA ===============================
all_cols = [c for c in train.columns if c not in [ID_COL, LABEL_COL]]

num_cols, cat_cols = [], []
for f in train.schema.fields:
    c = f.name
    if c not in all_cols:
        continue
    if isinstance(f.dataType, NumericType):
        num_cols.append(c)
    elif isinstance(f.dataType, StringType):
        cat_cols.append(c)

print("\n>>> Columns detected:")
print("  Categorical:", cat_cols)
print("  Numerical  :", num_cols)

# Bỏ các cột all-null (nếu có)
if num_cols or cat_cols:
    nn_counts = train.select(*[F.count(F.col(c)).alias(c) for c in (num_cols + cat_cols)]).collect()[0].asDict()
    num_cols = [c for c in num_cols if nn_counts.get(c, 0) > 0]
    cat_cols = [c for c in cat_cols if nn_counts.get(c, 0) > 0]

print("\n>>> After dropping all-null columns:")
print("  Categorical:", cat_cols)
print("  Numerical  :", num_cols)

# ============================== CAST & CLEAN NUMERIC ========================
print("\n>>> Casting numeric to Double & cleaning NaN/Inf ...")
for c in num_cols:
    train = train.withColumn(c, F.col(c).cast(DoubleType()))
    test  = test.withColumn(c,  F.col(c).cast(DoubleType()))

    # NaN / +/-Inf -> null (để Imputer xử lý)
    train = train.withColumn(
        c,
        F.when(F.isnan(F.col(c)) | F.col(c).isNull() |
               (F.col(c) == float("inf")) | (F.col(c) == float("-inf")), None)
         .otherwise(F.col(c))
    )
    test = test.withColumn(
        c,
        F.when(F.isnan(F.col(c)) | F.col(c).isNull() |
               (F.col(c) == float("inf")) | (F.col(c) == float("-inf")), None)
         .otherwise(F.col(c))
    )

# ============================== PREPROCESS: SHARED PARTS =====================
# Impute numeric (tạo *_imp để không overwrite cột gốc)
imp_num_cols = [f"{c}_imp" for c in num_cols] if num_cols else []

imputer = None
if num_cols:
    imputer = Imputer(inputCols=num_cols, outputCols=imp_num_cols, strategy="median")

# ============================== FEATURIZER 1: Logistic (OHE) ================
stages_ohe = []
if cat_cols:
    indexers_ohe = [StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep") for c in cat_cols]
    encoders_ohe = [OneHotEncoder(inputCol=f"{c}_idx", outputCol=f"{c}_ohe", handleInvalid="keep", dropLast=True)
                    for c in cat_cols]
    stages_ohe += indexers_ohe + encoders_ohe

if imputer:
    stages_ohe.append(imputer)

assembler_ohe = VectorAssembler(
    inputCols=([f"{c}_ohe" for c in cat_cols] if cat_cols else []) + (imp_num_cols if imp_num_cols else []),
    outputCol="features",
    handleInvalid="keep",
)
stages_ohe.append(assembler_ohe)

logistic = LogisticRegression(featuresCol="features", labelCol=LABEL_COL, maxIter=100)

pipeline_logistic = Pipeline(stages=stages_ohe + [logistic])

# ============================== FEATURIZER 2: Trees (NO OHE) ================
# StringIndexer để mã hoá label rời rạc thành số nguyên; VectorIndexer sẽ đánh dấu categorical theo maxCategories
stages_tree = []
if cat_cols:
    indexers_idx = [StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep") for c in cat_cols]
    stages_tree += indexers_idx

if imputer:
    stages_tree.append(imputer)

assembler_idx = VectorAssembler(
    inputCols=([f"{c}_idx" for c in cat_cols] if cat_cols else []) + (imp_num_cols if imp_num_cols else []),
    outputCol="features_idx",
    handleInvalid="keep",
)
stages_tree.append(assembler_idx)

vindexer = VectorIndexer(
    inputCol="features_idx",
    outputCol="features",
    handleInvalid="keep",
    maxCategories=32)
stages_tree.append(vindexer)

# RandomForest: cấu hình "nhẹ RAM"
rf = RandomForestClassifier(
    featuresCol="features", labelCol=LABEL_COL,
    numTrees=100, maxDepth=8, maxBins=64,
    featureSubsetStrategy="sqrt",
    subsamplingRate=0.8,
    minInstancesPerNode=5,
    cacheNodeIds=False,
    seed=1,
)

pipeline_rf = Pipeline(stages=stages_tree + [rf])

# XGBoost Spark: dùng featurizer cây (không OHE)
xgb = SparkXGBClassifier(
    features_col="features_idx",
    label_col=LABEL_COL,
    prediction_col="prediction",
    probability_col="probability",

    num_round=300,
    max_depth=4,
    eta=0.05,
    subsample=0.9,
    colsample_bytree=0.8,
    
    eval_metric="auc",
    num_workers=2,
    missing=0.0,
    seed=1,
)
stages_tree_xgb = [s for s in stages_tree if not isinstance(s, VectorIndexer)]
pipeline_xgb = Pipeline(stages=stages_tree_xgb + [xgb])

# ============================== TRAIN/VALID SPLIT ============================
print("\n>>> Splitting train/validation 80/20 ...")
train_tr, train_val = train.randomSplit([0.8, 0.2], seed=1)

evaluator = BinaryClassificationEvaluator(labelCol=LABEL_COL, rawPredictionCol="probability", metricName="areaUnderROC")

models = {
    "logistic": pipeline_logistic,
    "random_forest": pipeline_rf,
    "xgboost": pipeline_xgb,
}

results = {}
trained = {}

# ============================== TRAIN LOOP ==================================
for name, pipe in models.items():
    print(f"\n>>> Training model: {name}")
    model = pipe.fit(train_tr)
    pred_val = model.transform(train_val)
    auc = evaluator.evaluate(pred_val)
    results[name] = auc
    trained[name] = model
    print(f"AUC({name}) = {auc:.4f}")

    save_path = os.path.join(MODEL_DIR, f"{name}_spark_model")
    model.write().overwrite().save(save_path)
    print(f"Saved model → {save_path}")

print("\n===== SUMMARY AUC =====")
for k, v in results.items():
    print(f"{k}: {v:.4f}")

best_name = max(results, key=results.get)
best_model = trained[best_name]
print(f"\n>>> BEST MODEL = {best_name.upper()} (AUC={results[best_name]:.4f})")

# ============================== SCORING FULL DATA ============================
print("\n>>> Scoring full train & test with the BEST model ...")
def add_pd1(df):
    dtype = dict(df.dtypes).get("probability", "")
    if dtype == "vector":
        return df.withColumn("pd_1", vector_to_array(F.col("probability"))[1])
    else:
        # đã là scalar/double
        return df.withColumn("pd_1", F.col("probability"))

train_scored = add_pd1(best_model.transform(train)).select(ID_COL, LABEL_COL, "pd_1")
test_scored  = add_pd1(best_model.transform(test)).select(ID_COL, "pd_1")
best_model.transform(train).select("probability").printSchema()
# ============================ SAVE TO POSTGRES =============================
print("\n>>> Writing scores to Postgres via JDBC ...")
(train_scored.write
 .format("jdbc")
 .option("url", JDBC_URL)
 .option("dbtable", "public.spark_train_scores")
 .option("user", JDBC_PROPS["user"])
 .option("password", JDBC_PROPS["password"])
 .option("driver", JDBC_PROPS["driver"])
 .mode("overwrite")
 .save())

(test_scored.write
 .format("jdbc")
 .option("url", JDBC_URL)
 .option("dbtable", "public.spark_test_scores")
 .option("user", JDBC_PROPS["user"])
 .option("password", JDBC_PROPS["password"])
 .option("driver", JDBC_PROPS["driver"])
 .mode("overwrite")
 .save())
spark.stop()
