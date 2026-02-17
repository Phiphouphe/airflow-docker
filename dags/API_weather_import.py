import pandas as pd
import sys
import os
import pendulum

from datetime import timedelta
from airflow import DAG

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import app.helper as helper

from app.tasks.Extract.API_extraction import API_extraction
from app.tasks.Extract.DB_extraction import DB_extraction
from app.tasks.Transform.API_transform_data import API_transform_data
from app.tasks.Load.Load_to_database import Load_to_database
from airflow.models import Variable

# Importation de la clé API depuis les Variables Airflow
API_KEY = Variable.get("OPENWEATHER_API_KEY")

# Définition du DAG
DAG_ID = "API_weather_import"
LIBELLE = "Import des datas météo via API"
DESCRIPTION = "Import des données météo depuis une API externe vers la base de données flight_dw."


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=5),
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    start_date=pendulum.datetime(2025, 1, 1, tz="Europe/Paris"),
    schedule="0 8,14,20 * * *",
    tags=["API", "WEATHER",],
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=15),
    description=DESCRIPTION,
    doc_md="""
            Import des données météo depuis une API externe vers la base de données flight_dw.
        """
) as dag:
    
    pass