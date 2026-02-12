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
from app.tasks.Transform.Merge_files import Merge_files
from app.tasks.Transform.Filter_raw_flights import Filter_raw_flights
from app.tasks.Transform.Filter_scheduled_flights import Filter_scheduled_flights
from app.tasks.Transform.Parquet_add_technical_info import Parquet_add_technical_info
from app.tasks.Load.Load_to_database import Load_to_database
from app.tasks.Load.Parquet_to_snapshot import Parquet_to_snapshot
from app.static.simplify_air_france_flights import simplify_flights
from airflow.models import Variable

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
    tags=["API", "FLIGHT", "IMPORT", "RAW"],
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=15),
    description=DESCRIPTION,
    doc_md="""
            Import des données de vol du jour (scheduled) et de la veille (raw) à l'origine de NICE depuis une API externe vers la base de données flight_dw.
        """
) as dag:

    # Extraction des données du vol de la veille (raw)
    task_extract_API_raw_flight = API_extraction(
        url_api="https://api.airfranceklm.com/opendata/flightstatus",
        headers={"API-Key": API_KEY},
        api_params={
                "destination": "CDG",
                "carrierCode": "AF",
                "operatingAirlineCode": "AF",
                "movementType": "A",
                "origin": "NCE",
                },
        output_parquet_file="task_extract_API_raw_flight",
        flight_mode="raw",
        task_id="task_extract_API_raw_flight",
    )

    # Extraction des données de vol du jour même (scheduled)
    task_extract_API_scheduled_flight = API_extraction(
        url_api="https://api.airfranceklm.com/opendata/flightstatus",
        headers={"API-Key": API_KEY},
        api_params={
                "destination": "CDG",
                "carrierCode": "AF",
                "operatingAirlineCode": "AF",
                "movementType": "A",
                "origin": "NCE",
                },
        output_parquet_file="task_extract_API_scheduled_flight",
        flight_mode="scheduled",
        task_id="task_extract_API_scheduled_flight",
    )

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

    # Filtrage des vols du jour même qui sont déjà arrivés pour les charger dans la table de raw
    task_filter_raw_flights = Filter_raw_flights(
        input_parquet_file="task_transform_scheduled_flights",
        output_parquet_file="task_filter_raw_flights",
        task_id="task_filter_raw_flights",
    )

    # Filtrage des vols du jour même qui ne sont pas encore arrivés pour les charger dans la table de staging
    task_filter_scheduled_flights = Filter_scheduled_flights(
        input_parquet_file="task_transform_scheduled_flights",
        output_parquet_file="task_filter_scheduled_flights",
        task_id="task_filter_scheduled_flights",
    )

    # Fusion des vols de la veille (raw) et des vols du jour même déjà arrivés (filtered raw) pour les charger dans la table de raw
    task_merge_raw_files = Merge_files(
        input_parquet_file_1="task_filter_raw_flights",
        input_parquet_file_2="task_transform_raw_flights",
        output_parquet_file="task_merge_raw_files",
        task_id="task_merge_raw_files",
    )

    # Ajout des informations techniques pour le fichier raw
    task_add_technical_info_raw = Parquet_add_technical_info(
        input_parquet_file="task_merge_raw_files", 
        output_parquet_file="task_add_technical_info_raw",
        task_id="task_add_technical_info_raw", 
    )

    # Ajout des informations techniques pour le fichier scheduled
    task_add_technical_info_scheduled = Parquet_add_technical_info(
        input_parquet_file="task_filter_scheduled_flights", 
        output_parquet_file="task_add_technical_info_scheduled", 
        task_id="task_add_technical_info_scheduled", 
    )

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



    # Définition des dépendances entre les tâches
    # Les données de vol "raw" et "scheduled" suivent des chemins parallèles d'extraction, de transformation et de chargement.
    task_extract_API_raw_flight >> task_extract_API_scheduled_flight

    task_extract_API_raw_flight >> task_transform_raw_flights >> task_transform_scheduled_flights >> task_filter_raw_flights >> task_merge_raw_files >> task_add_technical_info_raw >> task_load_raw_to_db
    task_extract_API_scheduled_flight >> task_transform_scheduled_flights >> task_filter_scheduled_flights >> task_add_technical_info_scheduled >> task_load_scheduled_to_db