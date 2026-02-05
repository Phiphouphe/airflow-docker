import pandas as pd
import sys
import os
import pendulum

from datetime import timedelta, datetime
from airflow import DAG

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import app.helper as helper

from airflow.utils.task_group import TaskGroup
from app.tasks.Extract.API_extraction import API_extraction
from app.tasks.Extract.DB_extraction import DB_extraction
from app.tasks.Transform.API_transform_data import API_transform_data
from app.tasks.Load.Load_to_database import Load_to_database
from app.static.simplify_air_france_flights import simplify_flights
from airflow.models import Variable

# Importation de la clé API depuis les Variables Airflow
API_KEY = Variable.get("AIRFRANCE_API_KEY")

# Définition du DAG
DAG_ID = "test_boucle_cities_hour"
LIBELLE = "Import des datas vol via API"
DESCRIPTION = "Import des données de vol depuis une API externe vers la base de données flight_dw."


# Créneaux horaires
schedule_hours = [8, 14, 20]
# Villes
origins = ["NCE", "LYS", "MRS", "BOR", "TLS"]

with DAG(
    dag_id="test_boucle_cities_hour",
    start_date=datetime(2025, 12, 3),
    schedule="0 8,14,20 * * *",
    catchup=False,
    max_active_runs=1
) as dag:

    for hour in schedule_hours:
        with TaskGroup(group_id=f"time_slot_{hour}") as tg:
            for origin in origins:
                API_extraction(
                    task_id=f"extract_{origin}_{hour}",
                    url_api="https://api.airfranceklm.com/opendata/flightstatus",
                    headers={"API-Key": API_KEY},
                    api_params={"origin": origin, "schedule_hour": hour, "destination": "CDG",
                                "carrierCode": "AF", "operatingAirlineCode": "AF", "movementType": "A"},
                    output_parquet_file=f"/opt/airflow/data/extract_{origin}_{hour}.parquet",
                    op_kwargs={"execution_date": "{{ execution_date }}"}  # ← passe la date du run
                )
    