import os, joblib
from pyspark.sql import SparkSession
import pandas as pd

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.metrics import roc_auc_score

from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from xgboost import XGBClassifier


# ============================== PATH HDFS ===============================
train_path = "hdfs://namenode:9000/data/train.csv"
test_path  = "hdfs://namenode:9000/data/test.csv"

id_col = "SK_ID_CURR"
label_col = "TARGET"

model_dir = "/opt/model"
os.makedirs(model_dir, exist_ok=True)

# ============================== POSTGRES ===============================
JDBC_URL = "jdbc:postgresql://postgres:5432/finrisk"
JDBC_PROPS = {
    "driver": "org.postgresql.Driver",
    "user": "finuser",
    "password": "finpass"
}

# ============================ SPARK SESSION =============================
spark = (
    SparkSession.builder
    .appName("train_models_no_gridsearch")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
    .getOrCreate()
)

print("\n>>> Reading data from HDFS...", flush=True)
train_sp = spark.read.csv(train_path, header=True, inferSchema=True)
test_sp  = spark.read.csv(test_path, header=True, inferSchema=True)

train = train_sp.toPandas()
test  = test_sp.toPandas()
print(">>> Loaded train:", train.shape, "test:", test.shape, flush=True)


# ============================ PREPROCESS ================================
y = train[label_col]
X_train = train.drop(columns=[label_col])
X_test  = test.copy()

train_id = X_train[id_col].values
test_id  = X_test[id_col].values

X_train = X_train.drop(columns=[id_col])
X_test  = X_test.drop(columns=[id_col])

cat_list = X_train.select_dtypes(include=["object"]).columns.tolist()
num_list = [c for c in X_train.columns if c not in cat_list]

numeric_tf = Pipeline([
    ("imputer", SimpleImputer(strategy="median")),
    ("scaler", StandardScaler())
])

categorical_tf = Pipeline([
    ("imputer", SimpleImputer(strategy="most_frequent")),
    ("ohe", OneHotEncoder(handle_unknown="ignore"))
])

preprocess = ColumnTransformer(
    transformers=[
        ("num", numeric_tf, num_list),
        ("cat", categorical_tf, cat_list),
    ],
    remainder="drop"
)


# ============================ TRAIN/VALIDATION ==========================
print("\n>>> Splitting train/validation...", flush=True)
X_tr, X_val, y_tr, y_val = train_test_split(
    X_train, y, test_size=0.2, stratify=y, random_state=1
)


# ============================ DEFINE MODELS =============================
models = {
    "logistic": LogisticRegression(
        random_state=1,
        max_iter=3000,
        class_weight="balanced",
        n_jobs=1
    ),

    "random_forest": RandomForestClassifier(
        random_state=1,
        class_weight="balanced",
        n_jobs=1,
        n_estimators=200,
        max_depth=12
    ),

    "xgboost": XGBClassifier(
        random_state=1,
        objective="binary:logistic",
        eval_metric="logloss",
        nthread=1,
        n_estimators=300,
        max_depth=4,
        learning_rate=0.05
    )
}

results = {}
pipelines = {}


# ============================ TRAIN 3 MODELS ============================
for name, clf in models.items():
    print(f"\n>>> Training model: {name}", flush=True)

    pipe = Pipeline([
        ("prep", preprocess),
        ("clf", clf)
    ])

    pipe.fit(X_tr, y_tr)
    y_pred = pipe.predict_proba(X_val)[:, 1]
    auc = roc_auc_score(y_val, y_pred)

    results[name] = auc
    pipelines[name] = pipe

    print(f"AUC({name}) = {auc:.4f}", flush=True)

    joblib.dump(pipe, f"{model_dir}/{name}_model.pkl")
    print(f"Saved model: {model_dir}/{name}_model.pkl", flush=True)


# ============================ BEST MODEL ================================
print("\n===== SUMMARY AUC =====")
for k, v in results.items():
    print(f"{k}: {v:.4f}")

best_name = max(results, key=results.get)
best_pipe = pipelines[best_name]

print(f"\n>>> BEST MODEL = {best_name.upper()} (AUC={results[best_name]:.4f})", flush=True)


# ============================ SCORING FULL DATASET ======================
print("\n>>> Predicting full dataset...", flush=True)
train_proba = best_pipe.predict_proba(X_train)[:, 1]
test_proba  = best_pipe.predict_proba(X_test)[:, 1]

train_out = pd.DataFrame({
    id_col: train_id,
    label_col: y.values,
    "pd_1": train_proba
})

test_out = pd.DataFrame({
    id_col: test_id,
    "pd_1": test_proba
})


# ============================ SAVE POSTGRES =============================
print("\n>>> Saving results to Postgres...", flush=True)

sdf_train_out = spark.createDataFrame(train_out)
sdf_test_out  = spark.createDataFrame(test_out)

sdf_train_out.write.mode("overwrite").jdbc(
    JDBC_URL, "public.sklearn_train_scores", JDBC_PROPS
)

sdf_test_out.write.mode("overwrite").jdbc(
    JDBC_URL, "public.sklearn_test_scores", JDBC_PROPS
)

print("\n>>> TRAINING DONE!", flush=True)
spark.stop()
