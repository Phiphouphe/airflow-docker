import pandas as pd
import sys
import os
import pendulum

from datetime import timedelta
from airflow import DAG

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import app.helper as helper

from app.tasks.Extract.API_extraction import API_extraction
from app.tasks.Transform.API_transform_data import API_transform_data
from app.static.simplify_air_france_flights import AirFranceAPI
from airflow.models import Variable

# Importation de la clé API depuis les Variables Airflow
API_KEY = Variable.get("AIRFRANCE_API_KEY")

# Définition du DAG
DAG_ID = "API_flight_import"
LIBELLE = "Import des datas vol via API"
DESCRIPTION = "Import des données de vol depuis une API externe vers la base de données A2PO."


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
    schedule="0 9,14,20 * * *",
    tags=["API", "FLIGHT",],
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=15),
    description=DESCRIPTION,
    doc_md="""
            Import des données de vol depuis une API externe vers la base de données A2PO.
        """
) as dag:

    task_export_API_flight = API_extraction(
        url_api="https://api.airfranceklm.com/opendata/flightstatus",
        headers={"API-Key": API_KEY},
        api_params={"endRange": "2025-12-03T23:59:59.000Z",
                "startRange": "2025-12-03T01:00:00.000Z",
                "destination": "CDG",
                "carrierCode": "AF",
                "operatingAirlineCode": "AF",
                "movementType": "A",
                "origin": "NCE",
                },
        output_parquet_file="task_export_API_flight",
        task_id="task_export_API_flight",
    )

    task_transform_API_data = API_transform_data(
        input_parquet_file="task_export_API_flight",
        output_parquet_file="task_transform_API_data",
        transform_function=AirFranceAPI.simplify_flights,
        task_id="task_transform_API_data",
    )   