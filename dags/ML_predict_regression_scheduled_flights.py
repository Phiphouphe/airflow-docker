import sys
import os
import pendulum

from datetime import timedelta
from airflow import DAG

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from app.tasks.Extract.DB_extraction import DB_extraction
from app.tasks.ML.MLRegressionPredictTask import MLRegressionPredictTask
from app.datasets import ml_predictions_table

DAG_ID = "ML_predict_regression"
DESCRIPTION = "Prédit le nombre de minutes de retard pour les vols prédits en retard."

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    start_date=pendulum.datetime(2026, 3, 1, tz="Europe/Paris"),
    schedule=[ml_predictions_table],
    tags=["FLIGHTS", "ML", "REGRESSION", "PREDICT"],
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=15),
    description=DESCRIPTION,
) as dag:

    task_extract_db = DB_extraction(
        schema_name="analytics",
        table_name="scheduled_flights",
        output_parquet_file="task_extract_db",
        task_id="task_extract_db",
    )

    task_predict_regression = MLRegressionPredictTask(
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
        task_id="task_predict_regression",
    )

    task_extract_db >> task_predict_regression