import sys
import os
import pendulum

from datetime import timedelta
from airflow import DAG
from airflow.operators.python import ShortCircuitOperator
from airflow.datasets import Dataset

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from app.tasks.Extract.DB_extraction import DB_extraction
from app.tasks.ML.MLPredictTask import MLPredictTask
from app.datasets import ana_scheduled_flights_table


DAG_ID = "ML_predict_scheduled_flights"
LIBELLE = "Prédiction de retards de vols programmés"
DESCRIPTION = "Prédiction de retards de vols programmés à partir des données brutes de la table 'scheduled_flights' dans la base de données flight_dw."

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def check_predict_ready():
    import psycopg2
    from airflow.hooks.base import BaseHook
    from datetime import datetime, timedelta

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
        FROM analytics.scheduled_flights
        WHERE execution_date >= %s
    """, (since,))
    found = {row[0] for row in cursor.fetchall()}
    conn.close()

    missing = expected - found
    if missing:
        print(f"⏳ Villes manquantes dans analytics scheduled : {missing}")
        return False
    print("✅ Analytics scheduled prêt pour prédiction ML")
    return True


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    start_date=pendulum.datetime(2025, 1, 1, tz="Europe/Paris"),
    schedule=[ana_scheduled_flights_table],
    tags=["FLIGHTS", "ML", "PREDICTION", "SCHEDULED"],
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=10),
    description=DESCRIPTION,
    doc_md="""
            Prédiction de retards de vols programmés à partir des données brutes de la table 'scheduled_flights' dans la base de données flight_dw.
        """
) as dag:

    task_check_predict_ready = ShortCircuitOperator(
        task_id="check_predict_ready",
        python_callable=check_predict_ready,
    )

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
                 "weather_description",
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

    task_predict_ml = MLPredictTask(
        input_file="task_extract_db",
        experiment_name="Flight_Delay_Prediction",
        model_registry_name="flight_delay_model",
        features=[
            "origin_airport",
            "destination_airport",
            "departure_time_block",
            "day_of_week",
            "month",
            "dep_hour",
            "arr_hour",
            "is_cancelled",
            "status",
            "precipitation_sum",
            "wind_speed_max",
            "wind_gusts_max",
            "weather_code",
            "temp_min",
        ],
        task_id="task_predict_ml",
    )

    # Définition des dépendances
    task_check_predict_ready >> task_extract_db >> task_predict_ml