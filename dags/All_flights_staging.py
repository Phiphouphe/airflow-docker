import sys
import os
import pendulum
import psycopg2

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import ShortCircuitOperator
from airflow.utils.task_group import TaskGroup
from airflow.hooks.base import BaseHook

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from app.tasks.Extract.DB_extraction import DB_extraction
from app.tasks.Transform.DateConverter import DateConverter
from app.tasks.Transform.TypeConverter import TypeConverter
from app.tasks.Transform.DuplicateRemover import DuplicateRemover
from app.tasks.Load.Parquet_to_snapshot2 import Parquet_to_snapshot2
from app.datasets import stg_flights_table, stg_scheduled_flights_table, raw_flights_all_cities_ready, raw_scheduled_flights_all_cities_ready


DAG_ID = "All_flights_staging"
LIBELLE = "Nettoyage et transformation technique des données de vol pour les étapes Raw et Staging"
DESCRIPTION = "Nettoyage et transformation technique des données de vol depuis les tables 'raw' et 'staging' dans la base de données flight_dw."

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=5),
}


def check_staging_ready():

    conn_config = BaseHook.get_connection("flight_dw_postgres")
    conn = psycopg2.connect(
        host=conn_config.host, port=conn_config.port,
        dbname=conn_config.schema, user=conn_config.login,
        password=conn_config.password,
    )

    since = datetime.now() - timedelta(hours=2)
    expected = {"NCE", "LYS", "MRS", "TLS", "BOD"}

    cursor = conn.cursor()
    cursor.execute("""
        SELECT DISTINCT origin_airport
        FROM raw.raw_flights
        WHERE execution_date >= %s
    """, (since,))
    found = {row[0] for row in cursor.fetchall()}
    conn.close()

    missing = expected - found
    if missing:
        print(f"⏳ Villes manquantes dans ce cycle : {missing}")
        return False
    print("✅ Toutes les villes prêtes pour staging")
    return True


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    start_date=pendulum.datetime(2025, 1, 1, tz="Europe/Paris"),
    schedule=[raw_flights_all_cities_ready, raw_scheduled_flights_all_cities_ready],
    tags=["FLIGHTS", "TRANSFORMATION", "STAGING"],
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=15),
    description=DESCRIPTION,
    doc_md="""
            Nettoyage et transformation technique des données de vol du jour (scheduled) et de la veille (raw) depuis les tables 'raw' et 'staging' dans la base de données flight_dw.
        """
) as dag:

    TECHNICAL_COLUMNS = [
        "date_photo", "semaine_photo", "annee_photo",
        "execution_date", "instance_id", "dag_id"
    ]

    FLIGHT_COLUMNS = [
        "flight_id", "flight_number", "airline_code", "date",
        "scheduled_departure", "actual_departure",
        "scheduled_arrival", "actual_arrival",
        "origin_airport", "destination_airport",
        "status", "delay_minutes", "delay_code",
        "registration", "type_code", "type_name",
        "owner_airline", "wifi_enabled",
    ]

    task_check_staging_ready = ShortCircuitOperator(
        task_id="check_staging_ready",
        python_callable=check_staging_ready,
    )

    with TaskGroup('extraction_db') as extraction_db:
        task_extract_db_raw_flights = DB_extraction(
            table_name="raw_flights",
            schema_name="raw",
            columns=FLIGHT_COLUMNS + TECHNICAL_COLUMNS,
            airflow_variable_date_photo="date_photo_raw_flights",
            database_conn_id="flight_dw_postgres",
            output_parquet_file="task_extract_db_raw_flights",
            task_id="task_extract_db_raw_flights",
        )

        task_extract_db_scheduled_flights = DB_extraction(
            table_name="scheduled_flights",
            schema_name="raw",
            columns=FLIGHT_COLUMNS + TECHNICAL_COLUMNS,
            airflow_variable_date_photo="date_photo_scheduled_flights",
            database_conn_id="flight_dw_postgres",
            output_parquet_file="task_extract_db_scheduled_flights",
            task_id="task_extract_db_scheduled_flights",
        )

        [task_extract_db_raw_flights, task_extract_db_scheduled_flights]

    with TaskGroup('convert_date_columns') as convert_date_columns:
        task_convert_date_columns_raw_flights = DateConverter(
            input_file="task_extract_db_raw_flights",
            output_file="task_convert_date_columns_raw_flights",
            timestamp_columns=["scheduled_departure", "actual_departure",
                                "scheduled_arrival", "actual_arrival"],
            date_columns=["date"],
            task_id="task_convert_date_columns_raw_flights",
        )

        task_convert_date_columns_scheduled_flights = DateConverter(
            input_file="task_extract_db_scheduled_flights",
            output_file="task_convert_date_columns_scheduled_flights",
            timestamp_columns=["scheduled_departure", "actual_departure",
                                "scheduled_arrival", "actual_arrival"],
            date_columns=["date"],
            task_id="task_convert_date_columns_scheduled_flights",
        )

        [task_convert_date_columns_raw_flights, task_convert_date_columns_scheduled_flights]

    with TaskGroup('convert_type_columns') as convert_type_columns:
        task_convert_type_columns_raw_flights = TypeConverter(
            input_file="task_convert_date_columns_raw_flights",
            output_file="task_convert_type_columns_raw_flights",
            text_columns=[
                "flight_id", "flight_number", "airline_code",
                "origin_airport", "destination_airport",
                "status", "delay_code", "registration",
                "type_code", "type_name", "owner_airline"
            ],
            bool_columns={"wifi_enabled": {"Y": True, "N": False}},
            task_id="task_convert_type_columns_raw_flights",
        )

        task_convert_type_columns_scheduled_flights = TypeConverter(
            input_file="task_convert_date_columns_scheduled_flights",
            output_file="task_convert_type_columns_scheduled_flights",
            text_columns=[
                "flight_id", "flight_number", "airline_code",
                "origin_airport", "destination_airport",
                "status", "delay_code", "registration",
                "type_code", "type_name", "owner_airline"
            ],
            bool_columns={"wifi_enabled": {"Y": True, "N": False}},
            task_id="task_convert_type_columns_scheduled_flights",
        )

        [task_convert_type_columns_raw_flights, task_convert_type_columns_scheduled_flights]

    with TaskGroup('duplicate_remover') as duplicate_remover:
        task_duplicate_remover_raw_flights = DuplicateRemover(
            input_file="task_convert_type_columns_raw_flights",
            output_file="task_duplicate_remover_raw_flights",
            key_columns=["flight_id", "flight_number", "date",
                         "origin_airport", "destination_airport"],
            keep="last",
            null_threshold_percent=0,
            task_id="task_duplicate_remover_raw_flights",
        )

        task_duplicate_remover_scheduled_flights = DuplicateRemover(
            input_file="task_convert_type_columns_scheduled_flights",
            output_file="task_duplicate_remover_scheduled_flights",
            key_columns=["flight_id", "flight_number", "date",
                         "origin_airport", "destination_airport"],
            keep="last",
            null_threshold_percent=0,
            task_id="task_duplicate_remover_scheduled_flights",
        )

        [task_duplicate_remover_raw_flights, task_duplicate_remover_scheduled_flights]

    with TaskGroup('loading') as loading:
        task_load_raw_to_db = Parquet_to_snapshot2(
            table_name="raw_flights",
            schema="staging",
            mode="raw",
            api_type="airfrance",
            input_parquet_file="task_duplicate_remover_raw_flights",
            database_conn_id="flight_dw_postgres",
            outlets=[stg_flights_table],
            task_id="task_load_raw_to_db",
        )

        task_load_scheduled_to_db = Parquet_to_snapshot2(
            table_name="scheduled_flights",
            schema="staging",
            mode="scheduled",
            api_type="airfrance",
            input_parquet_file="task_duplicate_remover_scheduled_flights",
            database_conn_id="flight_dw_postgres",
            outlets=[stg_scheduled_flights_table],
            task_id="task_load_scheduled_to_db",
        )

        [task_load_raw_to_db, task_load_scheduled_to_db]

    # Définition des dépendances entre les tâches
    task_check_staging_ready >> extraction_db >> convert_date_columns >> convert_type_columns >> duplicate_remover >> loading