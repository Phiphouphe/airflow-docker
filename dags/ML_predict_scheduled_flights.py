import sys
import os
import pendulum

from datetime import datetime, timedelta

from airflow import DAG
from airflow.datasets import Dataset

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from app.tasks.Extract.DB_extraction import DB_extraction
from app.tasks.Transform.TechnicalInfo import TechnicalInfo
from app.tasks.ML.MLPredictTask import MLPredictTask 


# Définition du DAG
DAG_ID = "ML_predict_scheduled_flights"
LIBELLE = "Prédiction de retards de vols programmés"
DESCRIPTION = "Prédiction de retards de vols programmés à partir des données brutes de la table 'scheduled_flights' dans la base de données flight_dw."

scheduled_flights_table = Dataset("postgres://postgres_api/flight_dw/analytics/scheduled_flights")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 2, 24),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    start_date=pendulum.datetime(2025, 1, 1, tz="Europe/Paris"),
    schedule=[scheduled_flights_table,],
    tags=["FLIGHTS","ML", "PREDICTION", "SCHEDULED"],
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=10),
    description=DESCRIPTION,
    doc_md="""
            Prédiction de retards de vols programmés à partir des données brutes de la table 'scheduled_flights' dans la base de données flight_dw.
        """
) as dag:

    task_extract_db = DB_extraction(
            table_name="scheduled_flights",
            schema_name="analytics",
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
                     "delay_code",
                     "registration",
                     "type_code",
                     "type_name",
                     "owner_airline",
                     "wifi_enabled",
                     "temp_max",
                     "temp_min",
                     "temp_mean",
                     "precipitation_sum",
                     "rain_sum",
                     "snowfall_sum",
                     "precipitation_hours",
                     "wind_speed_max",
                     "wind_gusts_max",
                     "wind_direction",
                     "weather_code",
                     "code_iata",
                     "title_iata",
                     "description_iata",
                     "day_of_week",
                     "month",
                     "dep_hour",
                     "arr_hour",
                     "day_name",
                     "month_name",
                     "departure_time_block",
                     "is_cancelled",
            ],
            database_conn_id="flight_dw_postgres",
            output_parquet_file="task_extract_db",
            task_id="task_extract_db",
        )

    task_technical_informations = TechnicalInfo(
        input_file="task_extract_db",
        output_file="task_technical_informations",
        task_id="task_technical_informations",
    )

    task_predict_ml = MLPredictTask(
        input_file="task_technical_informations",
        features=[
            "origin_airport",
            "destination_airport",
            "departure_time_block",
            "day_of_week",
            "month",
            "dep_hour",
            "arr_hour",
            "is_cancelled"
        ],
        task_id="task_predict_ml",   
    )

    # Définition des dépendances
    task_extract_db >> task_technical_informations >> task_predict_ml