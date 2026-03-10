FROM apache/airflow:3.0.6

USER airflow

COPY requirements.txt .

RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir --no-build-isolation xgboost==1.7.6
