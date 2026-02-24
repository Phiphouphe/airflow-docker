# Base image
FROM apache/airflow:3.1.5

# Passer en root pour installer les packages
USER root

# Installer les packages ML nécessaires
RUN pip install --no-cache-dir \
    pandas==2.1.4 \
    scikit-learn==1.8.0 \
    xgboost==3.2.0 \
    requests \
    psycopg2-binary \
    pyarrow

# Revenir à l'utilisateur airflow
USER airflow
