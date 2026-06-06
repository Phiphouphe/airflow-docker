import pendulum
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from datetime import timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import ShortCircuitOperator

from app.datasets import (
    raw_flights_nice_done, raw_flights_lyon_done,
    raw_flights_marseille_done, raw_flights_toulouse_done,
    raw_flights_bordeaux_done,
    raw_scheduled_flights_nice_done, raw_scheduled_flights_lyon_done,
    raw_scheduled_flights_marseille_done, raw_scheduled_flights_toulouse_done,
    raw_scheduled_flights_bordeaux_done,
    raw_flights_all_cities_ready, raw_scheduled_flights_all_cities_ready,
)


def check_all_cities_ready():
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
        FROM raw.raw_flights
        WHERE execution_date >= %s
    """, (since,))
    found = {row[0] for row in cursor.fetchall()}
    conn.close()

    missing = expected - found
    if missing:
        print(f"⏳ Villes manquantes dans ce cycle : {missing}")
        return False
    print("✅ Toutes les villes prêtes")
    return True


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

    # Vérifie que les 5 villes ont bien été collectées dans les 2 dernières heures
    task_check_all_cities = ShortCircuitOperator(
        task_id="check_all_cities_ready",
        python_callable=check_all_cities_ready,
    )

    # Si oui, on considère que les datasets globaux sont prêts
    task_all_cities_ready = EmptyOperator(
        task_id="all_cities_raw_ready",
        outlets=[
            raw_flights_all_cities_ready,
            raw_scheduled_flights_all_cities_ready,
        ],
    )

    task_check_all_cities >> task_all_cities_ready