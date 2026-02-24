import sys
import os
import pendulum

from datetime import timedelta
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.datasets import Dataset

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from app.tasks.Extract.DB_extraction import DB_extraction
from app.tasks.Transform.DateConverter import DateConverter
from app.tasks.Transform.TypeConverter import TypeConverter
from app.tasks.Transform.DuplicateRemover import DuplicateRemover
from app.tasks.Transform.TechnicalInfo import TechnicalInfo
from app.tasks.Load.Parquet_to_snapshot2 import Parquet_to_snapshot2


# Définition du DAG
DAG_ID = "NICE_flights_staging"
LIBELLE = "Nettoyage et transformation technique des données de vol pour les étapes Raw et Staging"
DESCRIPTION = "Nettoyage et transformation technique des données de vol en partance de NICE depuis les tables 'raw' et 'staging' dans la base de données flight_dw."

raw_flights_table = Dataset("postgres://postgres_api/flight_dw/raw/raw_flights")
scheduled_flights_table = Dataset("postgres://postgres_api/flight_dw/raw/scheduled_flights")

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
    schedule=[raw_flights_table, scheduled_flights_table],
    tags=["FLIGHTS", "TRANSFORMATION", "STAGING"],
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=15),
    description=DESCRIPTION,
    doc_md="""
            Nettoyage et transformation technique des données de vol du jour (scheduled) et de la veille (raw) à l'origine de NICE depuis les tables 'raw' et 'staging' dans la base de données flight_dw.
        """
) as dag:
    
    with TaskGroup('extraction_db') as extraction_db:
        # Extraction des données du vol de la veille (raw) depuis la base de données
        task_extract_db_raw_flights = DB_extraction(
            table_name="raw_flights",
            schema_name="raw",
            columns=["flight_id", 
                     "flight_number", 
                     "airline_code",
                     "date",
                     "scheduled_departure", 
                     "actual_departure", 
                     "scheduled_arrival", 
                     "actual_arrival",
                     "origin_airport",
                     "destination_airport",
                     "status",
                     "delay_minutes",
                     "delay_code",
                     "registration",
                     "type_code",
                     "type_name",
                     "owner_airline",
                     "wifi_enabled",
            ],
            database_conn_id="flight_dw_postgres",
            output_parquet_file="task_extract_db_raw_flights",
            task_id="task_extract_db_raw_flights",
        )

        # Extraction des données de vol du jour même (scheduled) depuis la base de données
        task_extract_db_scheduled_flights = DB_extraction(
            table_name="scheduled_flights",
            schema_name="raw",
            columns=["flight_id", 
                     "flight_number", 
                     "airline_code",
                     "date",
                     "scheduled_departure", 
                     "actual_departure", 
                     "scheduled_arrival", 
                     "actual_arrival",
                     "origin_airport",
                     "destination_airport",
                     "status",
                     "delay_minutes",
                     "delay_code",
                     "registration",
                     "type_code",
                     "type_name",
                     "owner_airline",
                     "wifi_enabled",
            ],
            database_conn_id="flight_dw_postgres",
            output_parquet_file="task_extract_db_scheduled_flights",
            task_id="task_extract_db_scheduled_flights",
        )

        [task_extract_db_raw_flights, task_extract_db_scheduled_flights]


    with TaskGroup('convert_date_columns') as convert_date_columns:
        # Conversion des dates pour le fichier raw (vols de la veille)
        task_convert_date_columns_raw_flights = DateConverter(
            input_file="task_extract_db_raw_flights",
            output_file="task_convert_date_columns_raw_flights",
            timestamp_columns=["scheduled_departure",
                                "actual_departure",
                                "scheduled_arrival",
                                "actual_arrival"],
            date_columns=["date"],
            task_id="task_convert_date_columns_raw_flights",
        )

        # Conversion des dates pour le fichier scheduled (vols du jour même)
        task_convert_date_columns_scheduled_flights = DateConverter(
            input_file="task_extract_db_scheduled_flights",
            output_file="task_convert_date_columns_scheduled_flights",
            timestamp_columns=["scheduled_departure",
                                "actual_departure",
                                "scheduled_arrival",
                                "actual_arrival"],
            date_columns=["date"],
            task_id="task_convert_date_columns_scheduled_flights",
        )

        [task_convert_date_columns_raw_flights, task_convert_date_columns_scheduled_flights]


    with TaskGroup('convert_type_columns') as convert_type_columns:
        # Conversion des colonnes pour le fichier raw (vols de la veille)
        task_convert_type_columns_raw_flights = TypeConverter(
            input_file="task_convert_date_columns_raw_flights",
            output_file="task_convert_type_columns_raw_flights",
            text_columns=[
                "flight_id", "flight_number", "airline_code",
                "origin_airport", "destination_airport",
                "status", "delay_code", "registration",
                "type_code", "type_name", "owner_airline"
            ],
            bool_columns={
                "wifi_enabled": {"Y": True, "N": False}
            },
            task_id="task_convert_type_columns_raw_flights",
        )

        # Conversion des colonnes pour le fichier scheduled (vols du jour même)
        task_convert_type_columns_scheduled_flights = TypeConverter(
            input_file="task_convert_date_columns_scheduled_flights",
            output_file="task_convert_type_columns_scheduled_flights",
            text_columns=[
                "flight_id", "flight_number", "airline_code",
                "origin_airport", "destination_airport",
                "status", "delay_code", "registration",
                "type_code", "type_name", "owner_airline"
            ],
            bool_columns={
                "wifi_enabled": {"Y": True, "N": False}
            },
            task_id="task_convert_type_columns_scheduled_flights",
        )

        [task_convert_type_columns_raw_flights, task_convert_type_columns_scheduled_flights]


    with TaskGroup('duplicate_remover') as duplicate_remover:
        # Suppression des doublons pour le fichier raw (vols de la veille)
        task_duplicate_remover_raw_flights = DuplicateRemover(
            input_file="task_convert_type_columns_raw_flights",
            output_file="task_duplicate_remover_raw_flights",
            key_columns=[
                "flight_id",
                "flight_number",
                "date",
                "origin_airport",
                "destination_airport"],
            keep="last",
            null_threshold_percent=20,  
            task_id="task_duplicate_remover_raw_flights",
        ) 

        # Suppression des doublons pour le fichier scheduled (vols du jour même)
        task_duplicate_remover_scheduled_flights = DuplicateRemover(
            input_file="task_convert_type_columns_scheduled_flights",
            output_file="task_duplicate_remover_scheduled_flights",
            key_columns=[
                "flight_id",
                "flight_number",
                "date",
                "origin_airport",
                "destination_airport"],
            keep="last", 
            null_threshold_percent=20,
            task_id="task_duplicate_remover_scheduled_flights",
        ) 

        [task_duplicate_remover_raw_flights, task_duplicate_remover_scheduled_flights]
    

    with TaskGroup('technical_informations') as technical_informations:
        # Extraction des données météo pour le vol de la veille (raw) depuis la base de données
        task_add_technical_info_raw_flights = TechnicalInfo(
            input_file="task_duplicate_remover_raw_flights", 
            output_file="task_add_technical_info_raw",
            task_id="task_add_technical_info_raw_flights", 
        )

        # Extraction des données météo pour le vol du jour même (scheduled) depuis la base de données
        task_add_technical_info_scheduled_flights = TechnicalInfo(
            input_file="task_duplicate_remover_scheduled_flights", 
            output_file="task_add_technical_info_scheduled",
            task_id="task_add_technical_info_scheduled_flights", 
        )

        [task_add_technical_info_raw_flights, task_add_technical_info_scheduled_flights]
        

    with TaskGroup('loading') as loading:
        # Chargement du fichier raw dans la table stg_raw_flights                                                  
        task_load_raw_to_db = Parquet_to_snapshot2(
            table_name="raw_flights",
            schema="staging",
            mode="raw",
            api_type="airfrance",
            input_parquet_file="task_add_technical_info_raw_flights",
            database_conn_id="flight_dw_postgres",
            task_id="task_load_raw_to_db",
        )

        # Chargement du fichier scheduled dans la table stg_scheduled_flights
        task_load_scheduled_to_db = Parquet_to_snapshot2(
            table_name="scheduled_flights",
            schema="staging",
            mode="scheduled",
            api_type="airfrance",
            input_parquet_file="task_add_technical_info_scheduled_flights",
            database_conn_id="flight_dw_postgres",
            task_id="task_load_scheduled_to_db",
        )

        [task_load_raw_to_db, task_load_scheduled_to_db]   
        
        

    # Définition des dépendances entre les tâches
    # Les données de vol "raw" et "scheduled" suivent des chemins parallèles d'extraction, de transformation et de chargement.

    extraction_db >> convert_date_columns >> convert_type_columns >> duplicate_remover >> technical_informations >> loading