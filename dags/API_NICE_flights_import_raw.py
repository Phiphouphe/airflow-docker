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
from app.tasks.Transform.API_transform_data import API_transform_data
from app.tasks.Transform.Parquet_add_technical_info import Parquet_add_technical_info
from app.tasks.Load.Parquet_to_snapshot import Parquet_to_snapshot
from app.static.simplify_air_france_flights import simplify_flights

# Importation de la clé API depuis les Variables Airflow
API_KEY = Variable.get("AIRFRANCE_API_KEY")

# Définition du DAG
DAG_ID = "API_NICE_flights_import_raw"
LIBELLE = "Import des datas vol pour les étapes Raw et Staging via API"
DESCRIPTION = "Import des données de vol en partance de NICE depuis une API externe (Air France) vers la base de données flight_dw."


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
    schedule="0 8 * * *",
    tags=["API", "FLIGHTS", "IMPORT", "RAW"],
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=15),
    description=DESCRIPTION,
    doc_md="""
            Import des données de vol du jour (scheduled) et de la veille (raw) à l'origine de NICE depuis une API externe vers la base de données flight_dw.
        """
) as dag:

    with TaskGroup('extraction') as extraction:
        # Extraction des données du vol de la veille (raw)
        task_extract_API_raw_flights = API_extraction(
            url_api="https://api.airfranceklm.com/opendata/flightstatus",
            headers={"API-Key": API_KEY},
            api_params={
                    "destination": "CDG",
                    "carrierCode": "AF",
                    "operatingAirlineCode": "AF",
                    "movementType": "A",
                    "origin": "NCE",
                    },
            output_parquet_file="task_extract_API_raw_flights",
            flight_mode="raw",
            task_id="task_extract_API_raw_flights",
        )

        # Extraction des données de vol du jour même (scheduled)
        task_extract_API_scheduled_flights = API_extraction(
            url_api="https://api.airfranceklm.com/opendata/flightstatus",
            headers={"API-Key": API_KEY},
            api_params={
                    "destination": "CDG",
                    "carrierCode": "AF",
                    "operatingAirlineCode": "AF",
                    "movementType": "A",
                    "origin": "NCE",
                    },
            output_parquet_file="task_extract_API_scheduled_flights",
            flight_mode="scheduled",
            task_id="task_extract_API_scheduled_flights",
        )

        task_extract_API_raw_flights >> task_extract_API_scheduled_flights

    with TaskGroup('transformation') as transformation:
        # Transformation des données extraites de l'API pour le fichier raw (vols de la veille)
        task_transform_raw_flights = API_transform_data(
            input_parquet_file="task_extract_API_raw_flight",
            output_parquet_file="task_transform_raw_flights",
            transform_function=simplify_flights,
            task_id="task_transform_raw_flights",
        )

        # Transformation des données extraites de l'API pour le fichier scheduled (vols du jour même)
        task_transform_scheduled_flights = API_transform_data(
            input_parquet_file="task_extract_API_scheduled_flight",
            output_parquet_file="task_transform_scheduled_flights",
            transform_function=simplify_flights,
            task_id="task_transform_scheduled_flights",
        )

        [task_transform_raw_flights, task_transform_scheduled_flights]

    with TaskGroup('technical_informations') as technical_informations:
        # Ajout des informations techniques pour le fichier raw
        task_add_technical_info_raw = Parquet_add_technical_info(
            input_parquet_file="task_transform_raw_flights", 
            output_parquet_file="task_add_technical_info_raw",
            task_id="task_add_technical_info_raw", 
        )

        # Ajout des informations techniques pour le fichier scheduled
        task_add_technical_info_scheduled = Parquet_add_technical_info(
            input_parquet_file="task_transform_scheduled_flights", 
            output_parquet_file="task_add_technical_info_scheduled", 
            task_id="task_add_technical_info_scheduled", 
        )

        [task_add_technical_info_raw, task_add_technical_info_scheduled]

    with TaskGroup('loading') as loading:
        # Chargement du fichier raw dans la table raw_flights                                                  
        task_load_raw_to_db = Parquet_to_snapshot(
            table_name="raw_flights",
            schema="raw",
            input_parquet_file="task_add_technical_info_raw",
            database_conn_id="flight_dw_postgres",
            task_id="task_load_raw_to_db",
        )

        # Chargement du fichier scheduled dans la table raw_scheduled_flights
        task_load_scheduled_to_db = Parquet_to_snapshot(
            table_name="raw_scheduled_flights",
            schema="raw",
            input_parquet_file="task_add_technical_info_scheduled",
            database_conn_id="flight_dw_postgres",
            task_id="task_load_scheduled_to_db",
        )

        [task_load_raw_to_db, task_load_scheduled_to_db]



    # Définition des dépendances entre les tâches
    # Les données de vol "raw" et "scheduled" suivent des chemins parallèles d'extraction, de transformation et de chargement.
    # task_extract_API_raw_flight >> task_extract_API_scheduled_flight

    # task_extract_API_raw_flight >> task_transform_raw_flights >> task_add_technical_info_raw >> task_load_raw_to_db
    # task_extract_API_scheduled_flight >> task_transform_scheduled_flights >> task_add_technical_info_scheduled >> task_load_scheduled_to_db

    extraction >> transformation >> technical_informations >> loading