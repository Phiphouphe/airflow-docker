FROM apache/airflow:3.1.5

USER airflow

RUN pip install --upgrade pip

RUN pip install --no-cache-dir \
    pandas==2.1.4 \
    scikit-learn==1.4.0 \
    requests \
    psycopg2-binary \
    pyarrow \
    mlflow==2.12.1

RUN pip install --no-cache-dir --no-build-isolation xgboost==1.7.6
