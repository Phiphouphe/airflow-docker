import sys
import os
import pendulum

from functools import partial
from datetime import timedelta
from airflow import DAG
from airflow.utils.task_group import TaskGroup

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from app.static.extract_weather import extract_daily_weather
from app.tasks.Extract.API_extraction2 import API_extraction2
from app.tasks.Transform.TechnicalInfo import TechnicalInfo
from app.tasks.Load.Parquet_to_snapshot2 import Parquet_to_snapshot2
from app.datasets import raw_weather_table, raw_scheduled_weather_table


# Définition du DAG
DAG_ID = "API_weather_raw"
LIBELLE = "Import des datas météo via API"
DESCRIPTION = "Import des données météo de la veille et du jour courant depuis une API vers la base de données flight_dw."


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    start_date=pendulum.datetime(2025, 1, 1, tz="Europe/Paris"),
    schedule="0 5 * * *",
    tags=["API", "WEATHER", "OPENMETEO", "IMPORT", "RAW"],
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=15),
    description=DESCRIPTION,
    doc_md="""
            Import des données météo de la veille et du jour courant depuis une API vers la base de données flight_dw.
        """
) as dag:
    
    daily_params = [
    "temperature_2m_max","temperature_2m_min","temperature_2m_mean",
    "precipitation_sum","rain_sum","snowfall_sum","precipitation_hours",
    "wind_speed_10m_max","wind_gusts_10m_max","wind_direction_10m_dominant","weather_code",
    ]

    cities = {
    "MRS": {"lat": 43.3333, "lon": 5.5},
    "LYS": {"lat": 45.7256, "lon": 5.0811},
    "NCE": {"lat": 43.6584, "lon": 7.2159},
    "TLS": {"lat": 43.6293, "lon": 1.3638},
    "BOD": {"lat": 44.8283, "lon": -0.7156},
    }

    # Groupe pour toutes les extractions
    with TaskGroup(group_id="extract_tasks") as tg_extract:

        for iata, coords in cities.items():
            # Extraction des données météo pour le mode raw
            task_extract_weather_raw = API_extraction2(
                url_api="https://api.open-meteo.com/v1/forecast",
                api_params={
                    "latitude": coords["lat"],
                    "longitude": coords["lon"],
                    "daily": daily_params
                },
                output_parquet_file=f"task_extract_weather_raw_{iata}",
                transform_function=partial(extract_daily_weather, airport_iata=iata),
                flight_mode="raw",
                api_type="openmeteo",
                task_id=f"task_extract_weather_raw_{iata}"
            )

            # Extraction des données météo pour le mode scheduled
            task_extract_weather_scheduled = API_extraction2(
                url_api="https://api.open-meteo.com/v1/forecast",
                api_params={
                    "latitude": coords["lat"],
                    "longitude": coords["lon"],
                    "daily": daily_params
                },
                output_parquet_file=f"task_extract_weather_scheduled_{iata}",
                transform_function=partial(extract_daily_weather, airport_iata=iata),
                flight_mode="scheduled",
                api_type="openmeteo",
                task_id=f"task_extract_weather_scheduled_{iata}"
            )

            task_extract_weather_raw >> task_extract_weather_scheduled

    # Groupe pour l'ajout des informations techniques
    with TaskGroup('technical_informations_tasks') as tg_technical_informations:

        for iata, coords in cities.items():
            # Ajout des informations techniques pour le fichier raw
            task_add_technical_info_raw = TechnicalInfo(
                input_file=f"task_extract_weather_raw_{iata}", 
                output_file=f"task_add_technical_info_raw_{iata}",
                task_id=f"task_add_technical_info_raw_{iata}", 
            )

            # Ajout des informations techniques pour le fichier scheduled
            task_add_technical_info_scheduled = TechnicalInfo(
                input_file=f"task_extract_weather_scheduled_{iata}", 
                output_file=f"task_add_technical_info_scheduled_{iata}", 
                task_id=f"task_add_technical_info_scheduled_{iata}", 
            )

            [task_add_technical_info_raw, task_add_technical_info_scheduled]

    # Groupe pour tous les chargements
    with TaskGroup(group_id="load_tasks") as tg_load:

        for iata in cities.keys():
            # Chargement du fichier raw dans la table raw_weather
            task_load_raw_to_db = Parquet_to_snapshot2(
                    table_name="raw_weather",
                    schema="raw",
                    mode="raw",
                    api_type="openmeteo",
                    input_parquet_file=f"task_add_technical_info_raw_{iata}",
                    database_conn_id="flight_dw_postgres",
                    outlets=[raw_weather_table],
                    task_id=f"task_load_raw_{iata}_to_db",
            )

            # Chargement du fichier scheduled dans la table scheduled_weather
            task_load_scheduled_to_db = Parquet_to_snapshot2(
                    table_name="scheduled_weather",
                    schema="raw",
                    mode="scheduled",
                    api_type="openmeteo",
                    input_parquet_file=f"task_add_technical_info_scheduled_{iata}",
                    database_conn_id="flight_dw_postgres",
                    outlets=[raw_scheduled_weather_table],
                    task_id=f"task_load_scheduled_{iata}_to_db",
            )

            [task_load_raw_to_db, task_load_scheduled_to_db]


    # Définir la dépendance globale
    tg_extract >> tg_technical_informations >> tg_load
