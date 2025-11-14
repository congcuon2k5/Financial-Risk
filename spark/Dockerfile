FROM apache/spark:3.4.1

USER root

# Cài Python3 + pip
RUN apt-get update && \
    apt-get install -y python3 python3-pip libpq-dev gcc build-essential && \
    apt-get install -y --no-install-recommends
# Cài ML libs
RUN python3 -m pip install --upgrade pip setuptools wheel && \
    python3 -m pip install --no-cache-dir \
      xgboost==1.6.2 \
      scikit-learn==1.2.2 \
      pandas==1.5.3 \
      matplotlib==3.7.1 \
      seaborn==0.12.2 \
      joblib==1.3.2 \
      psycopg2==2.9.9

ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

WORKDIR /opt/app
