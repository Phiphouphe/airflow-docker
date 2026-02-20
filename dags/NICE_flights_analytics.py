import pandas as pd
import sys
import os
import pendulum

from datetime import timedelta
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.datasets import Dataset

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from app.tasks.Extract.DB_extraction import DB_extraction
from app.tasks.Transform.ColumnRemover import ColumnRemover
from app.tasks.Transform.PostgresArrayExtractor import PostgresArrayExtractor
from app.tasks.Transform.ParquetJoin import ParquetJoin
from app.tasks.Transform.FunctionApply import ApplyFunction
from app.tasks.Transform.VersionSelector import VersionSelector


# Définition du DAG
DAG_ID = "NICE_flights_analytics"
LIBELLE = "Transformation et enrichissement métier des données de vol en partance de NICE"
DESCRIPTION = "Transformation et enrichissement métier des données de vol en partance de NICE depuis les tables 'raw' et 'staging' dans la base de données flight_dw."

stg_raw_flights_table = Dataset("postgres://postgres_api/flight_dw/staging/stg_raw_flights")
stg_raw_scheduled_flights_table = Dataset("postgres://postgres_api/flight_dw/staging/stg_raw_scheduled_flights")
stg_raw_weather_table = Dataset("postgres://postgres_api/flight_dw/staging/stg_raw_weather")
stg_raw_scheduled_weather_table = Dataset("postgres://postgres_api/flight_dw/staging/stg_raw_scheduled_weather")
iata_delay_codes_table = Dataset("postgres://postgres_api/flight_dw/ref/iata_delay_codes")

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
    schedule=[stg_raw_flights_table, stg_raw_scheduled_flights_table, stg_raw_weather_table, stg_raw_scheduled_weather_table, iata_delay_codes_table],
    tags=["FLIGHTS", "WEATHER", "IATA","ANALYTICS"],
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=10),
    description=DESCRIPTION,
    doc_md="""
            Transformation et enrichissement métier des données de vol du jour (scheduled) et de la veille (raw) à l'origine de NICE depuis les tables 'raw' et 'staging' dans la base de données flight_dw.
        """
) as dag:
    
    with TaskGroup('extraction_db') as extraction_db:
        # Extraction des données du vol de la veille (raw) depuis la base de données
        task_extract_db_raw_flights = DB_extraction(
            table_name="stg_raw_flights",
            schema_name="staging",
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
            table_name="stg_raw_scheduled_flights",
            schema_name="staging",
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

        # Extraction des données référentielles des codes IATA depuis la base de données
        task_extract_db_iata_codes = DB_extraction(
            table_name="iata_delay_codes",
            schema_name="ref",
            query="""SELECT id, code, title, description 
                    FROM ref.iata_delay_codes""",
            task_id="task_extract_db_iata_codes",
            database_conn_id="flight_dw_postgres",
            output_parquet_file="task_extract_db_iata_codes",
        )

        # Extraction des données météo de la veille depuis la base de données
        task_extract_db_raw_weather = DB_extraction(
            table_name="stg_raw_weather",
            schema_name="staging",
            query="""SELECT date, temp_max, temp_min, temp_mean, precipitation_sum, rain_sum, snowfall_sum, 
                    precipitation_hours, wind_speed_max, wind_gusts_max, 
                    wind_direction, weather_code, airport_iata
                    FROM staging.stg_raw_weather""",
            task_id="task_extract_db_raw_weather",
            database_conn_id="flight_dw_postgres",
            output_parquet_file="task_extract_db_raw_weather",
        )

        # Extraction des données météo du jour courant depuis la base de données
        task_extract_db_scheduled_weather = DB_extraction(
            table_name="stg_raw_scheduled_weather",
            schema_name="staging",
            query="""SELECT date, temp_max, temp_min, temp_mean, precipitation_sum, rain_sum, snowfall_sum, 
                    precipitation_hours, wind_speed_max, wind_gusts_max, 
                    wind_direction, weather_code, airport_iata
                    FROM staging.stg_raw_scheduled_weather""",
            task_id="task_extract_db_scheduled_weather",
            database_conn_id="flight_dw_postgres",
            output_parquet_file="task_extract_db_scheduled_weather",
        )

        [task_extract_db_raw_flights, task_extract_db_scheduled_flights, task_extract_db_iata_codes, task_extract_db_raw_weather, task_extract_db_scheduled_weather]

    with TaskGroup('remove_column') as remove_column:
        # Suppression des colonnes inutiles des données de vol de la veille (raw)
        task_remove_column_raw_flights = ColumnRemover(
            input_file="task_extract_db_raw_flights",
            output_file="task_remove_column_raw_flights",
            columns_to_drop=["delay_minutes"],
            task_id="task_remove_column_raw_flights",
        )

        # Suppression des colonnes inutiles des données de vol du jour même (scheduled)
        task_remove_column_scheduled_flights = ColumnRemover(
            input_file="task_extract_db_scheduled_flights",
            output_file="task_remove_column_scheduled_flights",
            columns_to_drop=["delay_minutes"],
            task_id="task_remove_column_scheduled_flights",
        )

        [task_remove_column_raw_flights, task_remove_column_scheduled_flights]
    
    with TaskGroup('extractor_array') as extractor_array:
        # Conversion des types de colonnes pour le fichier raw (vols de la veille)
        task_extract_array_raw_flights = PostgresArrayExtractor(
            input_file="task_remove_column_raw_flights",
            output_file="task_extract_array_raw_flights",
            columns=["delay_code"],
            target_type="int",
            task_id="task_extract_array_raw_flights",
        )

        # Conversion des types de colonnes pour le fichier scheduled (vols du jour même)
        task_extract_array_scheduled_flights = PostgresArrayExtractor(
            input_file="task_remove_column_scheduled_flights",
            output_file="task_extract_array_scheduled_flights",
            columns=["delay_code"],
            target_type="int",
            task_id="task_extract_array_scheduled_flights",
        )

        [task_extract_array_raw_flights, task_extract_array_scheduled_flights]

    with TaskGroup('join_tables_raw') as join_tables_raw:
        # Jointure des données de vol de la veille (raw) avec les données météo correspondantes
        task_join_raw_flights_weather = ParquetJoin(
            left_file="task_extract_array_raw_flights",
            right_file="task_extract_db_raw_weather",
            on={
                "date": "date",
                "origin_airport": "airport_iata",
            },
            how="left",
            output_file="task_join_raw_flights_weather",
            task_id="task_join_raw_flights_weather"
        )

        # Jointure des données de vol de la veille (raw) avec les données météo correspondantes
        task_join_raw_flights_iata = ParquetJoin(
            left_file="task_join_raw_flights_weather",
            right_file="task_extract_db_iata_codes",
            on={
                "delay_code": "id",
            },
            how="left",
            output_file="task_join_raw_flights_iata",
            task_id="task_join_raw_flights_iata"
        )

        task_join_raw_flights_weather >> task_join_raw_flights_iata

    with TaskGroup('join_tables_scheduled') as join_tables_scheduled:
        # Jointure des données de vol du jour même (scheduled) avec les données météo correspondantes
        task_join_scheduled_flights_weather = ParquetJoin(
            left_file="task_extract_array_scheduled_flights",
            right_file="task_extract_db_scheduled_weather",
            on={
                "date": "date",
                "origin_airport": "airport_iata",
            },
            how="left",
            output_file="task_join_scheduled_flights_weather",
            task_id="task_join_scheduled_flights_weather"
        )

        # Jointure des données de vol du jour même (scheduled) avec les données météo correspondantes
        task_join_scheduled_flights_iata = ParquetJoin(
            left_file="task_join_scheduled_flights_weather",
            right_file="task_extract_db_iata_codes",
            on={
                "delay_code": "id",
            },
            how="left",
            output_file="task_join_scheduled_flights_iata",
            task_id="task_join_scheduled_flights_iata"
        )

        task_join_scheduled_flights_weather >> task_join_scheduled_flights_iata


    with TaskGroup('apply_functions') as apply_functions:

        # Fonction qui calcule le retard des avions en minutes à leur arrivée
        def compute_delay_minutes(row):
            if pd.notna(row["actual_arrival"]) and pd.notna(row["scheduled_arrival"]):
                return (row["actual_arrival"] - row["scheduled_arrival"]).total_seconds() / 60
            return None

        # Chargement des données de vol de la veille (raw)
        task_apply_functions_raw_flights = ApplyFunction(
            input_file="task_remove_column_raw_flights",
            output_file="task_apply_functions_raw_flights",
            columns_functions = {
                "delay_minutes": compute_delay_minutes,
                "is_delayed": lambda row: row["delay_minutes"] > 15,
                "delay_category": lambda row: pd.cut([row["delay_minutes"]], bins=[-1,0,15,float("inf")], labels=["on_time","minor_delay","major_delay"])[0],
                "is_landed": lambda row: pd.notna(row["actual_arrival"]),
                "has_delay_minutes": lambda row: pd.notna(row["delay_minutes"]),
            },
            task_id="task_apply_functions_raw_flights",
        )

        # Chargement des données de vol du jour même (scheduled)
        task_apply_functions_scheduled_flights = ApplyFunction(
            input_file="task_remove_column_scheduled_flights",
            output_file="task_apply_functions_scheduled_flights",
            columns_functions = {
                "delay_minutes": compute_delay_minutes,
                "is_delayed": lambda row: row["delay_minutes"] > 15,
                "delay_category": lambda row: pd.cut([row["delay_minutes"]], bins=[-1,0,15,float("inf")], labels=["on_time","minor_delay","major_delay"])[0],
                "is_landed": lambda row: pd.notna(row["actual_arrival"]),
                "has_delay_minutes": lambda row: pd.notna(row["delay_minutes"]),
            },
            task_id="task_apply_functions_scheduled_flights",
        )

        [task_apply_functions_raw_flights, task_apply_functions_scheduled_flights]   

    with TaskGroup('version_selector') as version_selector:
        # Sélection de la version finale des données de vol de la veille (raw)
        task_select_final_version_raw_flights = VersionSelector(
            input_file="task_apply_functions_raw_flights",
            output_file="task_select_final_version_raw_flights",
            key_columns=["flight_id", 
                         "flight_number",
                         "date",
                         "origin_airport",
                         "destination_airport",
                         ],
            date_column="date_photo",
            task_id="task_select_final_version_raw_flights",
        )

        # Sélection de la version finale des données de vol du jour même (scheduled)
        task_select_final_version_scheduled_flights = VersionSelector(
            input_file="task_apply_functions_scheduled_flights",
            output_file="task_select_final_version_scheduled_flights",
            key_columns=["flight_id", 
                         "flight_number",
                         "date",
                         "origin_airport",
                         "destination_airport",
                         ],
            date_column="date_photo",
            task_id="task_select_final_version_scheduled_flights",
        )

        [task_select_final_version_raw_flights, task_select_final_version_scheduled_flights]
    
    
    # Définition des dépendances entre les tâches
    # Les données de vol "raw" et "scheduled" suivent des chemins parallèles d'extraction, de transformation et de chargement.

    extraction_db >> remove_column >> extractor_array >> [join_tables_raw, join_tables_scheduled] >> apply_functions >> version_selector