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
from app.tasks.Transform.DuplicateRemover import DuplicateRemover
from app.tasks.Load.Parquet_to_snapshot2 import Parquet_to_snapshot2
from app.datasets import stg_weather_table, stg_scheduled_weather_table, raw_weather_table, raw_scheduled_weather_table


DAG_ID = "Cities_weather_staging"
LIBELLE = "Nettoyage et transformation technique des données météo pour les étapes Raw et Staging"
DESCRIPTION = "Nettoyage et transformation technique des données météo en partance des villes depuis les tables 'raw' et 'staging' dans la base de données flight_dw."

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
    schedule=[raw_weather_table, raw_scheduled_weather_table],
    tags=["WEATHER", "TRANSFORMATION", "STAGING"],
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=15),
    description=DESCRIPTION,
    doc_md="""
            Nettoyage et transformation technique des données météo du jour (scheduled) et de la veille (raw) à l'origine des villes depuis les tables 'raw' et 'staging' dans la base de données flight_dw.
        """
) as dag:
    
    TECHNICAL_COLUMNS = [
        "date_photo", "semaine_photo", "annee_photo",
        "execution_date", "instance_id", "dag_id"
    ]

    with TaskGroup('extraction_db') as extraction_db:
        task_extract_db_raw_weather = DB_extraction(
            table_name="raw_weather",
            schema_name="raw",
            columns=[
                "date", "temp_max", "temp_min", "temp_mean",
                "precipitation_sum", "rain_sum", "snowfall_sum",
                "precipitation_hours", "wind_speed_max", "wind_gusts_max",
                "wind_direction", "weather_code", "airport_iata",
            ] + TECHNICAL_COLUMNS,
            airflow_variable_date_photo="date_photo_raw_weather",
            database_conn_id="flight_dw_postgres",
            output_parquet_file="task_extract_db_raw_weather",
            task_id="task_extract_db_raw_weather",
        )

        task_extract_db_scheduled_weather = DB_extraction(
            table_name="scheduled_weather",
            schema_name="raw",
            columns=[
                "date", "temp_max", "temp_min", "temp_mean",
                "precipitation_sum", "rain_sum", "snowfall_sum",
                "precipitation_hours", "wind_speed_max", "wind_gusts_max",
                "wind_direction", "weather_code", "airport_iata",
            ] + TECHNICAL_COLUMNS,
            airflow_variable_date_photo="date_photo_scheduled_weather",
            database_conn_id="flight_dw_postgres",
            output_parquet_file="task_extract_db_scheduled_weather",
            task_id="task_extract_db_scheduled_weather",
            )

        [task_extract_db_raw_weather, task_extract_db_scheduled_weather]


    with TaskGroup('convert_date_columns') as convert_date_columns:
        task_convert_date_columns_raw_weather = DateConverter(
            input_file="task_extract_db_raw_weather",
            output_file="task_convert_date_columns_raw_weather",
            date_columns=["date"],
            task_id="task_convert_date_columns_raw_weather",
        )

        task_convert_date_columns_scheduled_weather = DateConverter(
            input_file="task_extract_db_scheduled_weather",
            output_file="task_convert_date_columns_scheduled_weather",
            date_columns=["date"],
            task_id="task_convert_date_columns_scheduled_weather",
        )

        [task_convert_date_columns_raw_weather, task_convert_date_columns_scheduled_weather]


    with TaskGroup('duplicate_remover') as duplicate_remover:
        task_duplicate_remover_raw_weather = DuplicateRemover(
            input_file="task_convert_date_columns_raw_weather",
            output_file="task_duplicate_remover_raw_weather",
            key_columns=["date", "airport_iata"],
            keep="last",
            null_threshold_percent=20,
            task_id="task_duplicate_remover_raw_weather",
        )

        task_duplicate_remover_scheduled_weather = DuplicateRemover(
            input_file="task_convert_date_columns_scheduled_weather",
            output_file="task_duplicate_remover_scheduled_weather",
            key_columns=["date", "airport_iata"],
            keep="last",
            null_threshold_percent=20,
            task_id="task_duplicate_remover_scheduled_weather",
        )

        [task_duplicate_remover_raw_weather, task_duplicate_remover_scheduled_weather]


    with TaskGroup('loading') as loading:
        task_load_raw_to_db = Parquet_to_snapshot2(
            table_name="raw_weather",
            schema="staging",
            mode="raw",
            api_type="openmeteo",
            input_parquet_file="task_duplicate_remover_raw_weather",
            database_conn_id="flight_dw_postgres",
            outlets=[stg_weather_table],
            task_id="task_load_raw_to_db",
        )

        task_load_scheduled_to_db = Parquet_to_snapshot2(
            table_name="scheduled_weather",
            schema="staging",
            mode="scheduled",
            api_type="openmeteo",
            input_parquet_file="task_duplicate_remover_scheduled_weather",
            database_conn_id="flight_dw_postgres",
            outlets=[stg_scheduled_weather_table],
            task_id="task_load_scheduled_to_db",
        )

        [task_load_raw_to_db, task_load_scheduled_to_db]


    extraction_db >> convert_date_columns >> duplicate_remover >> loading