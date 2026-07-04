import sys
import os
import pendulum

from datetime import timedelta
from airflow import DAG

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from app.tasks.Extract.DB_extraction import DB_extraction
from app.tasks.ML.MLRegressionTrainTask import MLRegressionTrainTask

DAG_ID = "ML_training_regression"
DESCRIPTION = "Entraînement de modèles de régression pour prédire le nombre de minutes de retard."

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=5),
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    start_date=pendulum.datetime(2026, 3, 1, tz="Europe/Paris"),
    schedule="50 5 * * *",
    tags=["FLIGHTS", "ML", "REGRESSION"],
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30),
    description=DESCRIPTION,
) as dag:

    task_extract_db = DB_extraction(
        schema_name="analytics",
        table_name="raw_flights",
        output_parquet_file="task_extract_db",
        task_id="task_extract_db",
    )

    task_training_regression = MLRegressionTrainTask(
        input_file="task_extract_db",
        experiment_name="Flight_Delay_Regression",
        model_registry_name="flight_delay_regression_model",
        features=[
            "origin_airport",
            "destination_airport",
            "departure_time_block",
            "weather_code",
            "type_code",
            "day_of_week",
            "month",
            "dep_hour",
            "arr_hour",
            "precipitation_sum",
            "wind_speed_max",
            "wind_gusts_max",
            "temp_min",
            "wind_direction",
            "precipitation_hours",
            "temp_max",
        ],
        target="delay_minutes",
        test_size=0.2,
        staging_threshold=25.0,
        task_id="task_training_regression_models",
    )

    task_extract_db >> task_training_regression