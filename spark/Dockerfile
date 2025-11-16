FROM apache/spark:3.4.1

USER root

# Cài Python3 + pip
RUN apt-get update && \
    apt-get install -y python3 python3-pip libpq-dev gcc build-essential curl && \
    apt-get install -y --no-install-recommends
    
RUN python3 -m pip install --upgrade pip setuptools wheel && \
    python3 -m pip install --no-cache-dir \
      xgboost==1.7.6 \
      pyspark==3.4.1 \
      psycopg2==2.9.9 \
      numpy==1.24.3 \
      pandas==1.5.3 \
      scipy==1.10.1 \
      pyarrow==12.0.1 \
      scikit-learn==1.2.2

RUN mkdir -p /opt/spark/jars && \
    curl -L -o /opt/spark/jars/xgboost4j_2.12-1.7.6.jar \
        https://repo1.maven.org/maven2/ml/dmlc/xgboost4j_2.12/1.7.6/xgboost4j_2.12-1.7.6.jar && \
    curl -L -o /opt/spark/jars/xgboost4j-spark_2.12-1.7.6.jar \
        https://repo1.maven.org/maven2/ml/dmlc/xgboost4j-spark_2.12/1.7.6/xgboost4j-spark_2.12-1.7.6.jar
    
RUN curl -L -o /opt/spark/jars/postgresql-42.7.4.jar \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar


ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

WORKDIR /opt/app
