import sys
import os
import pendulum

from datetime import timedelta
from airflow import DAG

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from app.tasks.Extract.CSV_reader import CsvReader
from app.tasks.Load.Load_to_database import Load_to_database
from app.datasets import ref_weather_codes_table


# Définition du DAG
DAG_ID = "weather_codes_import"
LIBELLE = "Import des codes météo"
DESCRIPTION = "Import des codes météo vers la base de données flight_dw."


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
    }

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    start_date=pendulum.datetime(2025, 1, 1, tz="Europe/Paris"),
    schedule=None,
    tags=["CODES", "WEATHER",],
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=15),
    description=DESCRIPTION,
    doc_md="""
            Import des données des codes météo vers la base de données flight_dw.
            DAG manuel pour injection unique du référentiel.
        """
) as dag:
    
    # Lecture du CSV météo et conversion en parquet temporaire
    task_read_csv_file = CsvReader(
        csv_file_path="/opt/airflow/data/weather_codes.csv",
        output_file="task_read_csv_file",
        task_id="task_read_csv_file",
    )

    # Chargement dans la table de référentiel
    task_load_data_to_db = Load_to_database(
        table_name="weather_codes",
        schema_name="ref",
        input_parquet_file="task_read_csv_file",
        database_conn_id="flight_dw_postgres",
        if_exists="replace",
        task_id="task_load_data_to_db",
        outlets=[ref_weather_codes_table],
    )

    # Définition des dépendances entre les tâches
    task_read_csv_file >> task_load_data_to_db