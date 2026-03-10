import pandas as pd
import sys
import os
import pendulum

from datetime import timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import app.helper as helper

from app.tasks.Extract.API_extraction import API_extraction
from app.tasks.Extract.API_extraction2 import API_extraction2
from app.tasks.Transform.API_transform_data import API_transform_data
from app.tasks.Transform.TechnicalInfo import TechnicalInfo
from app.tasks.Load.Parquet_to_snapshot import Parquet_to_snapshot
from app.tasks.Load.Parquet_to_snapshot2 import Parquet_to_snapshot2
from app.static.extract_flights import extract_flights
from app.datasets import raw_flights_marseille_done, raw_scheduled_flights_marseille_done


# Importation de la clé API depuis les Variables Airflow
API_KEY = Variable.get("AIRFRANCE_API_KEY", default_var=None)

# Définition du DAG
DAG_ID = "API_MARSEILLE_flights_raw"
LIBELLE = "Import des datas vol pour les étapes Raw et Staging via API"
DESCRIPTION = "Import des données de vol en partance de MARSEILLE depuis une API externe (Air France) vers la base de données flight_dw."


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    start_date=pendulum.datetime(2025, 1, 1, tz="Europe/Paris"),
    schedule="10 5,7,9,11,13,15,17,19 * * *",
    tags=["API", "FLIGHTS", "IMPORT", "RAW", "MARSEILLE",],
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=15),
    description=DESCRIPTION,
    doc_md="""
            Import des données de vol du jour (scheduled) et de la veille (raw) à l'origine de MARSEILLE depuis une API externe vers la base de données flight_dw.
        """
) as dag:

    with TaskGroup('extraction') as extraction:
        # Extraction des données du vol de la veille (raw)
        task_extract_API_raw_flights = API_extraction2(
            url_api="https://api.airfranceklm.com/opendata/flightstatus",
            headers={"API-Key": API_KEY},
            api_params={
                    "destination": "CDG",
                    "carrierCode": "AF",
                    "operatingAirlineCode": "AF",
                    "movementType": "A",
                    "origin": "MRS",
                    },
            output_parquet_file="task_extract_API_raw_flights",
            transform_function=extract_flights,
            flight_mode="raw",
            api_type="airfrance",
            task_id="task_extract_API_raw_flights",
        )

        # Extraction des données de vol du jour même (scheduled)
        task_extract_API_scheduled_flights = API_extraction2(
            url_api="https://api.airfranceklm.com/opendata/flightstatus",
            headers={"API-Key": API_KEY},
            api_params={
                    "destination": "CDG",
                    "carrierCode": "AF",
                    "operatingAirlineCode": "AF",
                    "movementType": "A",
                    "origin": "MRS",
                    },
            output_parquet_file="task_extract_API_scheduled_flights",
            transform_function=extract_flights,
            flight_mode="scheduled",
            api_type="airfrance",
            task_id="task_extract_API_scheduled_flights",
        )

        task_extract_API_raw_flights >> task_extract_API_scheduled_flights

    with TaskGroup('technical_informations') as technical_informations:
        # Ajout des informations techniques pour le fichier raw
        task_add_technical_info_raw = TechnicalInfo(
            input_file="task_extract_API_raw_flights", 
            output_file="task_add_technical_info_raw",
            task_id="task_add_technical_info_raw", 
        )

        # Ajout des informations techniques pour le fichier scheduled
        task_add_technical_info_scheduled = TechnicalInfo(
            input_file="task_extract_API_scheduled_flights", 
            output_file="task_add_technical_info_scheduled", 
            task_id="task_add_technical_info_scheduled", 
        )

        [task_add_technical_info_raw, task_add_technical_info_scheduled]

    with TaskGroup('loading') as loading:
        # Chargement du fichier raw dans la table raw_flights                                                  
        task_load_raw_to_db = Parquet_to_snapshot2(
            table_name="raw_flights",
            schema="raw",
            mode="raw",
            api_type="airfrance",
            input_parquet_file="task_add_technical_info_raw",
            database_conn_id="flight_dw_postgres",
            outlets=[raw_flights_marseille_done],
            task_id="task_load_raw_to_db",
        )

        # Chargement du fichier scheduled dans la table scheduled_flights
        task_load_scheduled_to_db = Parquet_to_snapshot2(
            table_name="scheduled_flights",
            schema="raw",
            mode="scheduled",
            api_type="airfrance",
            input_parquet_file="task_add_technical_info_scheduled",
            database_conn_id="flight_dw_postgres",
            outlets=[raw_scheduled_flights_marseille_done],
            task_id="task_load_scheduled_to_db",
        )

        [task_load_raw_to_db, task_load_scheduled_to_db]



    # Définition des dépendances entre les tâches
    # Les données de vol "raw" et "scheduled" suivent des chemins parallèles d'extraction, de transformation et de chargement.
    extraction >> technical_informations >> loading