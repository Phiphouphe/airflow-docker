import sys
import os
import pendulum

from datetime import timedelta
from airflow import DAG
from airflow.datasets import Dataset
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from xgboost import XGBClassifier
from sklearn.pipeline import Pipeline

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from app.tasks.Extract.DB_extraction import DB_extraction
from app.tasks.ML.MLTrainTask import MLTrainTask
from airflow.datasets import Dataset
from app.datasets import ana_flights_table, ml_model_dataset


# Définition du DAG
DAG_ID = "ML_training_raw_flights"
LIBELLE = "Entraînement de modèles ML sur les données brutes de vol"
DESCRIPTION = "Entraînement de modèles ML sur les données brutes de vol depuis la table 'raw_flights' dans la base de données flight_dw."


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=5),
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    start_date=pendulum.datetime(2025, 1, 1, tz="Europe/Paris"),
    schedule="40 5 * * *",
    # schedule=[ana_flights_table],
    tags=["FLIGHTS","ML", "TRAINING", "RAW"],
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=20),
    description=DESCRIPTION,
    doc_md="""
            Entraînement de modèles ML sur les données brutes de vol de la veille (raw) depuis la table 'raw_flights' dans la base de données flight_dw.
        """
) as dag:
    
    # Features 
    features = [
        "origin_airport", "destination_airport", "departure_time_block",
        "day_of_week", "month", "dep_hour", "arr_hour", "is_cancelled",
        "precipitation_sum",
        "wind_speed_max",
        "wind_gusts_max",
        "weather_code",
        "temp_min",
    ]
    categorical_features = ["origin_airport", "destination_airport", "departure_time_block", "weather_code"]
    numeric_features = ["day_of_week", "month", "dep_hour", "arr_hour",
                        "precipitation_sum", "wind_speed_max", "wind_gusts_max", "temp_min"
                    ]

    # Preprocessor pour les modèles ML
    preprocessor = ColumnTransformer([
        ("cat", OneHotEncoder(handle_unknown="ignore"), categorical_features),
        ("num", StandardScaler(), numeric_features)
    ])

    # Dictionnaire de modèles
    models_dict = {
        "RandomForest": Pipeline([
            ("preprocessor", preprocessor),
            ("classifier", RandomForestClassifier(n_estimators=100, random_state=42))
        ]),
        "LogisticRegression": Pipeline([
            ("preprocessor", preprocessor),
            ("classifier", LogisticRegression(max_iter=500))
        ]),
        "XGBoost": Pipeline([
            ("preprocessor", preprocessor),
            ("classifier", XGBClassifier(use_label_encoder=False, eval_metric="logloss"))
        ]),
        "GradientBoosting": Pipeline([
            ("preprocessor", preprocessor),
            ("classifier", GradientBoostingClassifier(n_estimators=100, random_state=42))
        ]),
    }

    task_extract_db = DB_extraction(
            table_name="raw_flights",
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
                     "delay_minutes",
                     "delay_category",
                     "departure_time_block",
                     "is_delayed",
                     "is_major_delay",
                     "is_cancelled",
                     "is_landed",
            ],
            database_conn_id="flight_dw_postgres",
            output_parquet_file="task_extract_db",
            task_id="task_extract_db",
        )


    task_training_models_ml = MLTrainTask(
        experiment_name="Flight_Delay_Prediction",
        model_registry_name="flight_delay_model",
        input_file="task_extract_db",
        features=features,
        target="is_delayed",
        models=models_dict,
        staging_threshold=0.4,
        task_id="task_training_models_ml",
        outlets=[ml_model_dataset],
    )

    # Définition de l'ordre d'exécution
    task_extract_db >> task_training_models_ml