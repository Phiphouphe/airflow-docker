import pendulum
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from datetime import timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator

from app.datasets import (
    raw_flights_nice_done, raw_flights_lyon_done,
    raw_flights_marseille_done, raw_flights_toulouse_done,
    raw_flights_bordeaux_done,
    raw_scheduled_flights_nice_done, raw_scheduled_flights_lyon_done,
    raw_scheduled_flights_marseille_done, raw_scheduled_flights_toulouse_done,
    raw_scheduled_flights_bordeaux_done,
    raw_flights_all_cities_ready, raw_scheduled_flights_all_cities_ready,
)

with DAG(
    dag_id="ALL_flights_raw_ready",
    start_date=pendulum.datetime(2025, 1, 1, tz="Europe/Paris"),
    schedule=[
        raw_flights_nice_done,
        raw_flights_lyon_done,
        raw_flights_marseille_done,
        raw_flights_toulouse_done,
        raw_flights_bordeaux_done,
        raw_scheduled_flights_nice_done,
        raw_scheduled_flights_lyon_done,
        raw_scheduled_flights_marseille_done,
        raw_scheduled_flights_toulouse_done,
        raw_scheduled_flights_bordeaux_done,
    ],
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=5),
    tags=["FLIGHTS", "RAW", "SENTINEL"],
) as dag:

    task_all_cities_ready = EmptyOperator(
        task_id="all_cities_raw_ready",
        outlets=[
            raw_flights_all_cities_ready,
            raw_scheduled_flights_all_cities_ready,
        ],
    )