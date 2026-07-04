import pandas as pd
import sys
import os
import pendulum
import logging
import calendar
import psycopg2

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import ShortCircuitOperator, PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.datasets import Dataset
from airflow.hooks.base import BaseHook

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from app.tasks.Extract.DB_extraction import DB_extraction
from app.tasks.Transform.ColumnRemover import ColumnRemover
from app.tasks.Transform.PostgresArrayExtractor import PostgresArrayExtractor
from app.tasks.Transform.ParquetJoin import ParquetJoin
from app.tasks.Transform.FunctionApply import ApplyFunction
from app.tasks.Transform.VersionSelector import VersionSelector
from app.tasks.Load.Parquet_to_snapshot2 import Parquet_to_snapshot2
from app.datasets import ana_flights_table, ana_scheduled_flights_table, stg_flights_table, stg_scheduled_flights_table, \
                          stg_weather_table, stg_scheduled_weather_table


DAG_ID = "All_flights_analytics"
LIBELLE = "Transformation et enrichissement métier des données de vol"
DESCRIPTION = "Transformation et enrichissement métier des données de vol depuis les tables 'raw' et 'staging' dans la base de données flight_dw."

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=5),
}


def check_analytics_ready():

    conn_config = BaseHook.get_connection("flight_dw_postgres")
    conn = psycopg2.connect(
        host=conn_config.host, 
        port=conn_config.port,
        dbname=conn_config.schema, 
        user=conn_config.login,
        password=conn_config.password,
    )

    since = datetime.now() - timedelta(hours=2)
    expected = {"NCE", "LYS", "MRS", "TLS", "BOD"}
    cursor = conn.cursor()

    # Vérifier les vols staging
    cursor.execute("""
        SELECT DISTINCT origin_airport
        FROM staging.raw_flights
        WHERE execution_date >= %s
    """, (since,))
    found_flights = {row[0] for row in cursor.fetchall()}

    # Vérifier la météo staging
    cursor.execute("""
        SELECT DISTINCT airport_iata
        FROM staging.raw_weather
        WHERE execution_date >= %s
    """, (since,))
    found_weather = {row[0] for row in cursor.fetchall()}

    conn.close()

    missing_flights = expected - found_flights
    missing_weather = expected - found_weather

    if missing_flights:
        print(f"⏳ Vols manquants dans staging : {missing_flights}")
        return False
    if missing_weather:
        print(f"⏳ Météo manquante dans staging : {missing_weather}")
        return False

    print("✅ Vols et météo prêts pour analytics")
    return True

def compute_day_of_week(row):
            try:
                if pd.isna(row["date"]):
                    return None
                day_num = pd.to_datetime(row["date"]).dayofweek
                return day_num + 1
            except Exception as e:
                logging.warning(f"Erreur compute_day_of_week_one_index pour row {row.name}: {e}")
                return None

def compute_month(row):
            try:
                if pd.isna(row["date"]):
                    return None
                return pd.to_datetime(row["date"]).month
            except Exception as e:
                logging.warning(f"Erreur compute_month pour row {row.name}: {e}")
                return None

def compute_dep_hour(row):
            try:
                if pd.isna(row["scheduled_departure"]):
                    return None
                return pd.to_datetime(row["scheduled_departure"]).hour
            except Exception as e:
                logging.warning(f"Erreur compute_dep_hour pour row {row.name}: {e}")
                return None

def compute_arr_hour(row):
            try:
                if pd.isna(row["scheduled_arrival"]):
                    return None
                return pd.to_datetime(row["scheduled_arrival"]).hour
            except Exception as e:
                logging.warning(f"Erreur compute_arr_hour pour row {row.name}: {e}")
                return None

def compute_day_name(row):
            try:
                if pd.isna(row["date"]):
                    return None
                day_num = pd.to_datetime(row["date"]).dayofweek
                return calendar.day_name[day_num]
            except Exception as e:
                logging.warning(f"Erreur compute_day_name pour row {row.name}: {e}")
                return None

def compute_month_name(row):
            try:
                if pd.isna(row["date"]):
                    return None
                month_num = pd.to_datetime(row["date"]).month
                return calendar.month_name[month_num]
            except Exception as e:
                logging.warning(f"Erreur compute_month_name pour row {row.name}: {e}")
                return None

def compute_delay_minutes(row):
            if (
                pd.notna(row["actual_arrival"]) and
                pd.notna(row["scheduled_arrival"]) and
                row["status"] != "CANCELLED"
            ):
                return (row["actual_arrival"] - row["scheduled_arrival"]).total_seconds() / 60
            return None

def update_predicted_labels():
    """Rapproche les prédictions ML des vols réels pour remplir is_delayed_predicted."""
    conn = None
    cursor = None
    try:
        conn_config = BaseHook.get_connection("flight_dw_postgres")
        conn = psycopg2.connect(
            host=conn_config.host,
            port=conn_config.port,
            dbname=conn_config.schema,
            user=conn_config.login,
            password=conn_config.password,
        )
        cursor = conn.cursor()

        # Garantit que la colonne existe même si la table a été recréée
        cursor.execute("""
            ALTER TABLE analytics.raw_flights 
            ADD COLUMN IF NOT EXISTS is_delayed_predicted BOOLEAN;
        """)

        cursor.execute("""
            UPDATE analytics.raw_flights r
            SET is_delayed_predicted = p.is_delayed
            FROM ml.flight_predictions p
            WHERE r.flight_number = p.flight_number
            AND r.date = p.flight_date
            AND r.origin_airport = p.origin_airport
        """)
        rows_updated = cursor.rowcount
        conn.commit()
        logging.info(f"✅ is_delayed_predicted mis à jour : {rows_updated} lignes modifiées")
    except Exception as e:
        if conn:
            conn.rollback()
        logging.error(f"❌ Erreur lors de la mise à jour de is_delayed_predicted : {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    start_date=pendulum.datetime(2025, 1, 1, tz="Europe/Paris"),
    schedule=[stg_flights_table, stg_scheduled_flights_table,
              stg_weather_table, stg_scheduled_weather_table],
    tags=["FLIGHTS", "WEATHER", "IATA", "ANALYTICS"],
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=10),
    description=DESCRIPTION,
    doc_md="""
            Transformation et enrichissement métier des données de vol du jour (scheduled) et de la veille (raw) depuis les tables 'raw' et 'staging' dans la base de données flight_dw.
        """
) as dag:

    TECHNICAL_COLUMNS = [
        "date_photo", "semaine_photo", "annee_photo",
        "execution_date", "instance_id", "dag_id"
    ]

    task_check_analytics_ready = ShortCircuitOperator(
        task_id="check_analytics_ready",
        python_callable=check_analytics_ready,
    )

    with TaskGroup('extraction_db') as extraction_db:
        task_extract_db_raw_flights = DB_extraction(
            table_name="raw_flights",
            schema_name="staging",
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
                     "delay_minutes",
                     "delay_code",
                     "registration",
                     "type_code",
                     "type_name",
                     "owner_airline",
                     "wifi_enabled",
            ] + TECHNICAL_COLUMNS,
            airflow_variable_date_photo="date_photo_raw_flights",
            database_conn_id="flight_dw_postgres",
            output_parquet_file="task_extract_db_raw_flights",
            task_id="task_extract_db_raw_flights",
        )

        task_extract_db_scheduled_flights = DB_extraction(
            table_name="scheduled_flights",
            schema_name="staging",
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
                     "delay_minutes",
                     "delay_code",
                     "registration",
                     "type_code",
                     "type_name",
                     "owner_airline",
                     "wifi_enabled",
            ] + TECHNICAL_COLUMNS,
            airflow_variable_date_photo="date_photo_scheduled_flights",
            database_conn_id="flight_dw_postgres",
            output_parquet_file="task_extract_db_scheduled_flights",
            task_id="task_extract_db_scheduled_flights",
        )

        task_extract_db_iata_codes = DB_extraction(
            table_name="iata_delay_codes",
            schema_name="ref",
            query="""SELECT id, code, title, description 
                    FROM ref.iata_delay_codes""",
            task_id="task_extract_db_iata_codes",
            database_conn_id="flight_dw_postgres",
            output_parquet_file="task_extract_db_iata_codes",
        )

        task_extract_db_weather_codes = DB_extraction(
            table_name="weather_codes",
            schema_name="ref",
            query="""SELECT code, description 
                    FROM ref.weather_codes""",
            task_id="task_extract_db_weather_codes",
            database_conn_id="flight_dw_postgres",
            output_parquet_file="task_extract_db_weather_codes",
        )

        task_extract_db_raw_weather = DB_extraction(
            table_name="raw_weather",
            schema_name="staging",
            query="""SELECT date, temp_max, temp_min, temp_mean, precipitation_sum, rain_sum, snowfall_sum, 
                    precipitation_hours, wind_speed_max, wind_gusts_max, 
                    wind_direction, weather_code, airport_iata
                    FROM staging.raw_weather""",
            task_id="task_extract_db_raw_weather",
            database_conn_id="flight_dw_postgres",
            output_parquet_file="task_extract_db_raw_weather",
        )

        task_extract_db_scheduled_weather = DB_extraction(
            table_name="scheduled_weather",
            schema_name="staging",
            query="""SELECT date, temp_max, temp_min, temp_mean, precipitation_sum, rain_sum, snowfall_sum, 
                    precipitation_hours, wind_speed_max, wind_gusts_max, 
                    wind_direction, weather_code, airport_iata
                    FROM staging.scheduled_weather""",
            task_id="task_extract_db_scheduled_weather",
            database_conn_id="flight_dw_postgres",
            output_parquet_file="task_extract_db_scheduled_weather",
        )

        [task_extract_db_raw_flights, task_extract_db_scheduled_flights, task_extract_db_iata_codes,
         task_extract_db_raw_weather, task_extract_db_scheduled_weather]

    with TaskGroup('extractor_array') as extractor_array:
        task_extract_array_raw_flights = PostgresArrayExtractor(
            input_file="task_extract_db_raw_flights",
            output_file="task_extract_array_raw_flights",
            columns=["delay_code"],
            target_type="int",
            task_id="task_extract_array_raw_flights",
        )

        task_extract_array_scheduled_flights = PostgresArrayExtractor(
            input_file="task_extract_db_scheduled_flights",
            output_file="task_extract_array_scheduled_flights",
            columns=["delay_code"],
            target_type="int",
            task_id="task_extract_array_scheduled_flights",
        )

        [task_extract_array_raw_flights, task_extract_array_scheduled_flights]

    with TaskGroup('join_tables_raw') as join_tables_raw:
        task_join_raw_flights_weather = ParquetJoin(
            left_file="task_extract_array_raw_flights",
            right_file="task_extract_db_raw_weather",
            on={"date": "date", "origin_airport": "airport_iata"},
            how="left",
            output_file="task_join_raw_flights_weather",
            task_id="task_join_raw_flights_weather"
        )

        task_join_raw_flights_weather_codes = ParquetJoin(
            left_file="task_join_raw_flights_weather",
            right_file="task_extract_db_weather_codes",
            on={"weather_code": "code"},
            how="left",
            rename_right={"description": "weather_description"},
            output_file="task_join_raw_flights_weather_codes",
            task_id="task_join_raw_flights_weather_codes"
        )

        task_join_raw_flights_iata = ParquetJoin(
            left_file="task_join_raw_flights_weather_codes",
            right_file="task_extract_db_iata_codes",
            on={"delay_code": "id"},
            how="left",
            rename_right={"code": "code_iata", "title": "title_iata", "description": "description_iata"},
            output_file="task_join_raw_flights_iata",
            task_id="task_join_raw_flights_iata"
        )

        task_join_raw_flights_weather >> task_join_raw_flights_weather_codes >> task_join_raw_flights_iata

    with TaskGroup('join_tables_scheduled') as join_tables_scheduled:
        task_join_scheduled_flights_weather = ParquetJoin(
            left_file="task_extract_array_scheduled_flights",
            right_file="task_extract_db_scheduled_weather",
            on={"date": "date", "origin_airport": "airport_iata"},
            how="left",
            output_file="task_join_scheduled_flights_weather",
            task_id="task_join_scheduled_flights_weather"
        )

        task_join_scheduled_flights_weather_codes = ParquetJoin(
            left_file="task_join_scheduled_flights_weather",
            right_file="task_extract_db_weather_codes",
            on={"weather_code": "code"},
            how="left",
            rename_right={"description": "weather_description"},
            output_file="task_join_scheduled_flights_weather_codes",
            task_id="task_join_scheduled_flights_weather_codes"
        )

        task_join_scheduled_flights_iata = ParquetJoin(
            left_file="task_join_scheduled_flights_weather_codes",
            right_file="task_extract_db_iata_codes",
            on={"delay_code": "id"},
            how="left",
            rename_right={"code": "code_iata", "title": "title_iata", "description": "description_iata"},
            output_file="task_join_scheduled_flights_iata",
            task_id="task_join_scheduled_flights_iata"
        )

        task_join_scheduled_flights_weather >> task_join_scheduled_flights_weather_codes >> task_join_scheduled_flights_iata

    with TaskGroup('remove_columns') as remove_columns:
        task_remove_columns_raw_flights = ColumnRemover(
            input_file="task_join_raw_flights_iata",
            output_file="task_remove_columns_raw_flights",
            columns_to_drop=["delay_minutes", "id", "airport_iata", "code"],
            task_id="task_remove_columns_raw_flights",
        )

        task_remove_columns_scheduled_flights = ColumnRemover(
            input_file="task_join_scheduled_flights_iata",
            output_file="task_remove_columns_scheduled_flights",
            columns_to_drop=["delay_minutes", "id", "airport_iata", "code"],
            task_id="task_remove_columns_scheduled_flights",
        )

        [task_remove_columns_raw_flights, task_remove_columns_scheduled_flights]

    with TaskGroup('apply_business_functions') as apply_business_functions:

        task_apply_functions_raw_flights = ApplyFunction(
            input_file="task_remove_columns_raw_flights",
            output_file="task_apply_functions_raw_flights",
            columns_functions={
                "day_of_week": compute_day_of_week,
                "month": compute_month,
                "dep_hour": compute_dep_hour,
                "arr_hour": compute_arr_hour,
                "day_name": compute_day_name,
                "month_name": compute_month_name,
                "delay_minutes": compute_delay_minutes,
                "delay_category": lambda row: (
                    None if pd.isna(row["delay_minutes"])
                    else "on_time" if row["delay_minutes"] <= 0
                    else "minor_delay" if row["delay_minutes"] <= 15
                    else "major_delay"
                ),
                "departure_time_block": lambda row: (
                    None if pd.isna(row["scheduled_departure"])
                    else "night" if 0 <= row["scheduled_departure"].hour < 6
                    else "morning" if 6 <= row["scheduled_departure"].hour < 12
                    else "afternoon" if 12 <= row["scheduled_departure"].hour < 18
                    else "evening"
                ),
                "is_delayed": lambda row: (
                    pd.notna(row["delay_minutes"]) and row["delay_minutes"] > 15
                ),
                "is_major_delay": lambda row: (
                    pd.notna(row["delay_minutes"]) and row["delay_minutes"] > 60
                ),
                "is_cancelled": lambda row: row["status"] == "CANCELLED",
                "is_landed": lambda row: row["status"] == "ARRIVED",
            },
            task_id="task_apply_functions_raw_flights",
        )

        task_apply_functions_scheduled_flights = ApplyFunction(
            input_file="task_remove_columns_scheduled_flights",
            output_file="task_apply_functions_scheduled_flights",
            columns_functions={
                "day_of_week": compute_day_of_week,
                "month": compute_month,
                "dep_hour": compute_dep_hour,
                "arr_hour": compute_arr_hour,
                "day_name": compute_day_name,
                "month_name": compute_month_name,
                "departure_time_block": lambda row: (
                    None if pd.isna(row["scheduled_departure"])
                    else "night" if 0 <= row["scheduled_departure"].hour < 6
                    else "morning" if 6 <= row["scheduled_departure"].hour < 12
                    else "afternoon" if 12 <= row["scheduled_departure"].hour < 18
                    else "evening"
                ),
                "is_cancelled": lambda row: row["status"] == "CANCELLED",
            },
            task_id="task_apply_functions_scheduled_flights",
        )

        [task_apply_functions_raw_flights, task_apply_functions_scheduled_flights]

    with TaskGroup('version_selector') as version_selector:
        task_select_final_version_raw_flights = VersionSelector(
            input_file="task_apply_functions_raw_flights",
            output_file="task_select_final_version_raw_flights",
            key_columns=["flight_id", "flight_number", "date", "origin_airport", "destination_airport"],
            date_column="date_photo",
            task_id="task_select_final_version_raw_flights",
        )

        task_select_final_version_scheduled_flights = VersionSelector(
            input_file="task_apply_functions_scheduled_flights",
            output_file="task_select_final_version_scheduled_flights",
            key_columns=["flight_id", "flight_number", "date", "origin_airport", "destination_airport"],
            date_column="date_photo",
            task_id="task_select_final_version_scheduled_flights",
        )

        [task_select_final_version_raw_flights, task_select_final_version_scheduled_flights]

    with TaskGroup('loading') as loading:
        task_load_raw_to_db = Parquet_to_snapshot2(
            table_name="raw_flights",
            schema="analytics",
            mode="raw",
            api_type="airfrance",
            input_parquet_file="task_select_final_version_raw_flights",
            database_conn_id="flight_dw_postgres",
            outlets=[ana_flights_table],
            task_id="task_load_raw_to_db",
        )

        task_load_scheduled_to_db = Parquet_to_snapshot2(
            table_name="scheduled_flights",
            schema="analytics",
            mode="scheduled",
            api_type="airfrance",
            input_parquet_file="task_select_final_version_scheduled_flights",
            database_conn_id="flight_dw_postgres",
            outlets=[ana_scheduled_flights_table],
            task_id="task_load_scheduled_to_db",
        )

        [task_load_raw_to_db, task_load_scheduled_to_db]

    task_update_predicted_labels = PythonOperator(
                task_id="task_update_predicted_labels",
                python_callable=update_predicted_labels,
            )

    # Définition des dépendances entre les tâches
    task_check_analytics_ready >> extraction_db >> extractor_array >> [join_tables_raw, join_tables_scheduled] >> remove_columns >> apply_business_functions >> version_selector >> loading >> task_update_predicted_labels