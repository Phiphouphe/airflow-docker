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
from app.static.simplify_air_france_flights import simplify_flights
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
    schedule="0 8,14,20 * * *",
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
        transform_function=simplify_flights,
        task_id="task_transform_API_data",
    )

    task_load_API_data_to_db = Load_to_database(
        table_name="flights_db",
        input_parquet_file="task_transform_API_data",
        database_conn_id="postgres_api",
        if_exists="append",
        task_id="task_load_API_data_to_db",
    )

    task_test_database_extraction = DB_extraction(
        table_name="flights_db",
        output_parquet_file="task_test_database_extraction",
        columns = [
            "flight_id",
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
            "created_date",
            "updated_date",
            "week_photo",
            "year_photo",
            ],
        limit_clause=5,
        task_id="task_test_database_extraction",
    )



    # Définition des dépendances entre les tâches
    task_export_API_flight >> task_transform_API_data >> task_load_API_data_to_db >> task_test_database_extraction